# candb

candb is the **storage and query backend** for the CAN bus waveform recording system. It runs on a Geekom A6 mini PC (or any Linux host) and receives raw analog waveform chunks from one or more [CANlogger](https://github.com/forrest-molg/CANlogger) capture machines, stores them efficiently in TimescaleDB, and serves them back to the [canui](https://github.com/forrest-molg/canui) waveform viewer with on-the-fly CAN frame decoding.

## System Context

```
CANlogger (capture machine)
  └─ POST /ingest ──────────────────────► candb  (this repo)
                                            │
                                            ├─ TimescaleDB (canwaves-db, port 5432)
                                            │  LZ4-compressed int16 chunks
                                            │  24h rolling retention
                                            │
                                            └─ FastAPI (candb-api, port 8000)
                                                 │
                                          GET /query, /decode, ...
                                                 │
                                              canui waveform viewer (port 3000)
```

## What Is Implemented

### Storage

- **TimescaleDB hypertable** `waveform_chunks` — one row per 1 ms waveform chunk, per channel (CAN-H or CAN-L), per bus (1–5).
- Each row stores LZ4-compressed little-endian `int16` ADC counts (`BYTEA`). The 1562 samples in a 1 ms chunk compress to ~400–900 bytes at typical CAN signal duty cycles.
- **Automatic compression** after 2 minutes (TimescaleDB chunk compression, segmented by `bus_id, channel`).
- **24-hour rolling retention** enforced hourly by TimescaleDB background job.
- **Tuned PostgreSQL** parameters for write-heavy time-series workloads: 2 GB `shared_buffers`, `synchronous_commit=off`, optimised WAL settings.

### FastAPI (`api.py`)

All endpoints accept and return JSON. Datetimes are ISO 8601 UTC strings.

| Method | Path | Description |
|---|---|---|
| `POST` | `/ingest` | Receive waveform chunks from CANlogger. Accepts up to 10,000 chunks per request. Each chunk carries base64(lz4(int16)) for CAN-H and CAN-L. |
| `GET` | `/query` | Return waveform samples for a bus/channel/time window. Automatically min-max downsamples to ≤8,000 display points while preserving every edge transition. |
| `GET` | `/export` | Stream full-resolution CSV (no downsampling) for a time window up to 60 s. |
| `GET` | `/decode` | Decode ISO 11898-1 standard CAN frames from the raw waveform. Returns frame list + per-bit annotations for overlay rendering. |
| `GET` | `/find_edge` | Seek to the next or previous CAN SOF (start-of-frame) edge from a reference timestamp. Used by the UI prev/next navigation buttons. |
| `GET` | `/buses` | List bus IDs and channels that have data in the database. |
| `GET` | `/health` | Database reachability check. Returns `{"status":"ok"}` or HTTP 503. |
| `GET` | `/storage` | Disk and database storage statistics (used, free, oldest/newest chunk timestamps). |

### CAN Decoder (`/decode`)

The decoder operates on raw differential voltage (CAN-H minus CAN-L) computed from the stored ADC samples.

- **Baud rate**: 125 kbps (hardcoded — matches the capture sample rate of 1.5625 MS/s = 12.5 samples/bit)
- **Phase accumulator** with fractional SPB (12.5) for sub-sample accurate bit sampling
- **Hard synchronisation** on recessive→dominant edges near bit boundaries (matches CAN spec)
- **Hysteretic thresholds**: dominant > 0.9 V diff, recessive < 0.5 V diff
- **Bit destuffing** (ISO 11898-1 §10.5)
- **15-bit CRC validation** (CAN polynomial 0x4599)
- **Standard (11-bit) frames only** — extended (29-bit) frames are skipped
- **2 ms boundary padding**: the DB query window is expanded ±2 ms before decoding to avoid missing bits at chunk boundaries

Decoded output per frame: timestamp, CAN ID (hex + int), RTR flag, DLC, data bytes (hex + ASCII), CRC OK, EOF OK, stuff error count.

Per-bit output: timestamp, bit value, field label (SOF, ID0–ID10, RTR, IDE, DLC0–DLC3, DATA, CRC, ACK, EOF, etc.) — used by canui to draw the bit-line overlay on the waveform chart.

### Downsampling (min-max)

When a query window contains more than 8,000 samples, the API applies min-max bucket downsampling instead of stride decimation. Each bucket emits both its minimum and maximum sample in chronological order, so every rising and falling edge of the CAN signal is preserved in the rendered waveform regardless of zoom level.

## Data Model

```sql
CREATE TABLE waveform_chunks (
    time         TIMESTAMPTZ NOT NULL,   -- chunk start timestamp (UTC)
    bus_id       SMALLINT    NOT NULL,   -- 1–5
    channel      CHAR(1)     NOT NULL,   -- 'H' (CAN-H) or 'L' (CAN-L)
    sample_rate  INTEGER     NOT NULL,   -- always 1562500 Hz
    n_samples    INTEGER     NOT NULL,   -- samples in this chunk (typically 1563)
    samples      BYTEA       NOT NULL    -- base64(lz4(int16 LE)) ADC counts
);
-- Hypertable: 1-minute chunk intervals
-- Retention: 24 hours (hourly enforcement job)
-- Compression: after 2 minutes (segmented by bus_id, channel)
```

ADC scale: `voltage_V = adc_int16 / 32767 * 5.0`

## Deploy

### Prerequisites

- Docker Engine + Compose plugin
- Python 3.11+ (for `api.py` — runs via systemd, not in Docker)
- Port 5432 available locally (TimescaleDB binds to `127.0.0.1:5432` only)
- Port 8000 available (FastAPI)

### 1. Start the database

```bash
cd /opt/candb
docker compose up -d
# Wait for health check to pass:
docker inspect --format '{{.State.Health.Status}}' canwaves-db
```

The `init.sql` file is automatically applied on first run — it creates the hypertable, compression policy, and retention policy.

### 2. Start the API

```bash
cd /opt/candb
python3 -m venv venv
source venv/bin/activate
pip install fastapi uvicorn asyncpg lz4 numpy pydantic

uvicorn api:app --host 0.0.0.0 --port 8000
```

In production this runs as a systemd service (`candb-api.service`, `User=canops`, `Restart=always`).

### 3. Verify

```bash
curl http://localhost:8000/health
# → {"status":"ok"}

curl http://localhost:8000/storage
# → disk + DB stats
```

## Configuration

The database connection URL is hardcoded in `api.py`:

```python
DB_URL = "postgresql://canops:canopsadmin@127.0.0.1/canwaves"
```

TimescaleDB credentials and port are set in `docker-compose.yml`. The API and DB must be on the same host (DB binds to localhost only).

**To change the retention window:**

```sql
-- Connect to TimescaleDB
docker exec -it canwaves-db psql -U canops -d canwaves

-- View current jobs
SELECT job_id, schedule_interval, config FROM timescaledb_information.jobs
WHERE proc_name = 'policy_retention';

-- Change to 48 hours
SELECT alter_job(<job_id>, schedule_interval => INTERVAL '1 hour',
  config => '{"drop_after":"PT48H","hypertable_id":<id>}'::jsonb);
```

## Wire Format (Ingest)

CANlogger POSTs an array of chunk objects:

```json
[
  {
    "time_utc":      "2026-04-30T19:54:20.123456+00:00",
    "bus_id":        1,
    "n_samples":     1563,
    "samples_h_b64": "<base64(lz4(int16 CAN-H array))>",
    "samples_l_b64": "<base64(lz4(int16 CAN-L array))>"
  }
]
```

The API splits each object into two rows (`channel='H'` and `channel='L'`). `sample_rate` is not in the wire format — both sides hardcode `1562500 Hz`.

## Logs

The API writes rotating logs (10 MB × 3) to:

```
/home/geekoma6database/Documents/logs/CANdatabase
```

Log level is `DEBUG` by default — every request is logged with timing.

## Developer Workflow

```bash
# Start DB only (no systemd required)
cd /opt/candb && docker compose up -d

# Run API with auto-reload
source venv/bin/activate
uvicorn api:app --reload --host 0.0.0.0 --port 8000

# Interactive API docs
open http://localhost:8000/docs
```

## Releasing a New Version

```bash
git checkout develop    # all v2 work happens here
# ... make changes, commit ...

git checkout main
git merge develop
git tag -a v1.1 -m "v1.1 — describe changes"
git push origin main && git push origin v1.1
```
