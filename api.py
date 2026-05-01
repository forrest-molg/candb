"""
CAN Bus Waveform Ingest + Query API
Handles 5 buses x 2 channels (CAN High/Low) at 1.5625 MS/s (1 562 500 Hz).
Samples stored as LZ4-compressed int16 little-endian bytes in TimescaleDB.
"""
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
import logging
import logging.handlers

import asyncpg
import base64
import lz4.frame
import numpy as np
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, field_validator
import time

# ── Logging setup ─────────────────────────────────────────────────────────
LOG_FILE = "/home/geekoma6database/Documents/logs/CANdatabase"

handler = logging.handlers.RotatingFileHandler(
    LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
handler.setFormatter(logging.Formatter(
    "%(asctime)s.%(msecs)03d [%(levelname)-8s] [API] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))

log = logging.getLogger("candb.api")
log.setLevel(logging.DEBUG)
log.addHandler(handler)

# Also push uvicorn + asyncpg logs into the same file
for lib in ("uvicorn", "uvicorn.error", "uvicorn.access", "asyncpg"):
    lib_logger = logging.getLogger(lib)
    lib_logger.setLevel(logging.DEBUG)
    lib_logger.addHandler(handler)
# ──────────────────────────────────────────────────────────────────────────

DB_URL = "postgresql://canops:canopsadmin@127.0.0.1/canwaves"
pool: asyncpg.Pool | None = None


@asynccontextmanager
async def lifespan(application: FastAPI):
    global pool
    log.info("=== candb-api starting up ===")
    try:
        pool = await asyncpg.create_pool(DB_URL, min_size=5, max_size=20)
        log.info("DB connection pool created (min=5, max=20)")
    except Exception as exc:
        log.critical("FAILED to create DB pool: %s", exc, exc_info=True)
        raise
    yield
    log.info("=== candb-api shutting down ===")
    await pool.close()


app = FastAPI(title="CANlogger Waveform API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    t0 = time.perf_counter()
    log.debug("→ %s %s  client=%s", request.method, request.url.path, request.client)
    try:
        response = await call_next(request)
    except Exception as exc:
        log.error("UNHANDLED EXCEPTION on %s %s: %s", request.method, request.url.path, exc, exc_info=True)
        raise
    elapsed = (time.perf_counter() - t0) * 1000
    log.debug("← %s %s  status=%s  %.1fms", request.method, request.url.path, response.status_code, elapsed)
    if response.status_code >= 400:
        log.warning("HTTP %s on %s %s (%.1fms)", response.status_code, request.method, request.url.path, elapsed)
    return response


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

# Fixed sample rate agreed with CANlogger — not sent over the wire.
_WIRE_SAMPLE_RATE = 1_562_500

class BusWindowChunk(BaseModel):
    time_utc:      str
    bus_id:        int
    n_samples:     int
    samples_h_b64: str   # base64(lz4(int16 CAN-H)), empty string if no H channel
    samples_l_b64: str   # base64(lz4(int16 CAN-L)), empty string if no L channel

    @field_validator("bus_id")
    @classmethod
    def validate_bus_id(cls, v: int) -> int:
        if not 1 <= v <= 5:
            raise ValueError("bus_id must be 1–5")
        return v

    @field_validator("n_samples")
    @classmethod
    def validate_n_samples(cls, v: int) -> int:
        if v <= 0 or v > 10_000_000:
            raise ValueError("n_samples out of range")
        return v


@app.post("/ingest", status_code=200)
async def ingest(chunks: list[BusWindowChunk]):
    if not chunks:
        log.warning("Ingest called with empty chunk list")
        raise HTTPException(status_code=400, detail="No chunks provided")
    if len(chunks) > 10_000:
        log.warning("Ingest batch too large: %d chunks", len(chunks))
        raise HTTPException(status_code=400, detail="Batch too large (max 10 000 chunks)")

    rows = []
    for c in chunks:
        try:
            t = datetime.fromisoformat(c.time_utc)
            if t.tzinfo is None:
                t = t.replace(tzinfo=timezone.utc)
        except Exception as exc:
            log.error("Invalid time_utc bus=%s: %s", c.bus_id, exc)
            raise HTTPException(status_code=422, detail=f"Invalid time_utc: {exc}")

        for ch, b64 in (("H", c.samples_h_b64), ("L", c.samples_l_b64)):
            if not b64:
                continue
            try:
                raw = base64.b64decode(b64, validate=True)
            except Exception as exc:
                log.error("Invalid base64 bus=%s ch=%s: %s", c.bus_id, ch, exc)
                raise HTTPException(status_code=422, detail=f"Invalid base64 for channel {ch}: {exc}")
            rows.append((t, c.bus_id, ch, _WIRE_SAMPLE_RATE, c.n_samples, raw))

    if not rows:
        log.warning("Ingest: all chunks had empty H and L — nothing inserted")
        return {"inserted": 0}

    try:
        async with pool.acquire() as conn:
            await conn.executemany(
                """INSERT INTO waveform_chunks
                   (time, bus_id, channel, sample_rate, n_samples, samples)
                   VALUES ($1, $2, $3, $4, $5, $6)""",
                rows,
            )
        log.info("Ingested %d rows from %d chunks", len(rows), len(chunks))
    except Exception as exc:
        log.error("DB insert failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Database insert failed")
    return {"inserted": len(rows)}


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

MAX_DISPLAY_POINTS = 8_000   # hard ceiling sent to browser

@app.get("/query")
async def query(
    bus_id:     int = Query(..., ge=1, le=5),
    channel:    str = Query(..., pattern="^[HL]$"),
    start:      str = Query(...),
    end:        str = Query(...),
    max_points: int = Query(default=MAX_DISPLAY_POINTS, ge=100, le=MAX_DISPLAY_POINTS),
):
    try:
        t_start = datetime.fromisoformat(start)
        t_end   = datetime.fromisoformat(end)
    except ValueError as exc:
        log.warning("Query bad datetime bus=%s ch=%s: %s", bus_id, channel, exc)
        raise HTTPException(status_code=422, detail=f"Invalid datetime: {exc}")

    if t_start >= t_end:
        log.warning("Query bad range: start >= end (bus=%s ch=%s)", bus_id, channel)
        raise HTTPException(status_code=422, detail="start must be before end")

    log.debug("Query bus=%s ch=%s  %s → %s  max_points=%s", bus_id, channel, t_start, t_end, max_points)

    # Expand the DB query window by 2 ms on both sides so we catch chunks that
    # started just before t_start *or* just before t_end but extend into the window.
    # (Each chunk is ~1.0003 ms; without this, boundary chunks are missed.)
    t_fetch_start = t_start - timedelta(milliseconds=2)
    t_fetch_end   = t_end   + timedelta(milliseconds=2)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT time, sample_rate, n_samples, samples
               FROM waveform_chunks
               WHERE bus_id = $1 AND channel = $2
                 AND time >= $3 AND time < $4
               ORDER BY time""",
            bus_id, channel, t_fetch_start, t_fetch_end,
        )

    if not rows:
        log.info("Query returned 0 rows (bus=%s ch=%s)", bus_id, channel)
        return {"times": [], "samples_raw": [], "total_samples": 0, "displayed_samples": 0}

    all_times, all_samples = [], []
    t_start_unix = t_start.timestamp()
    t_end_unix   = t_end.timestamp()
    try:
        for r in rows:
            raw   = lz4.frame.decompress(bytes(r["samples"]))
            s_arr = np.frombuffer(raw, dtype="<i2").astype(np.float32) * (5.0 / 32767)
            t0    = r["time"].timestamp()
            dt    = 1.0 / r["sample_rate"]
            n     = r["n_samples"]
            t_arr = t0 + np.arange(n, dtype=np.float64) * dt
            mask  = (t_arr >= t_start_unix) & (t_arr < t_end_unix)
            all_times.extend(t_arr[mask].tolist())
            all_samples.extend(s_arr[mask].tolist())
    except Exception as exc:
        log.error("Decompression error bus=%s ch=%s: %s", bus_id, channel, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to decompress waveform data")

    total = len(all_samples)

    # Downsample if we have more points than the display limit.
    # Simple stride decimation skips transitions on square-wave CAN signals and
    # produces aliased spikes.  Min-max downsampling emits both the minimum and
    # maximum sample within each bucket so every transition edge is preserved.
    if total > max_points:
        t_arr = np.array(all_times,   dtype=np.float64)
        s_arr = np.array(all_samples, dtype=np.float32)

        # Each bucket contributes 2 output points (min + max in time order),
        # so use max_points // 2 buckets.
        n_buckets  = max_points // 2
        bucket_idx = np.array_split(np.arange(total), n_buckets)

        out_t, out_s = [], []
        for idx in bucket_idx:
            if len(idx) == 0:
                continue
            lo = int(idx[np.argmin(s_arr[idx])])
            hi = int(idx[np.argmax(s_arr[idx])])
            # emit in chronological order
            if lo <= hi:
                out_t.extend([t_arr[lo], t_arr[hi]])
                out_s.extend([s_arr[lo], s_arr[hi]])
            else:
                out_t.extend([t_arr[hi], t_arr[lo]])
                out_s.extend([s_arr[hi], s_arr[lo]])

        all_times   = out_t
        all_samples = [float(v) for v in out_s]
        log.debug("Min-max downsampled %d → %d points (bus=%s ch=%s buckets=%d)",
                  total, len(all_samples), bus_id, channel, n_buckets)
    else:
        log.debug("Query returned %d samples (bus=%s ch=%s)", total, bus_id, channel)

    return {
        "times":             all_times,
        "samples_raw":       all_samples,
        "total_samples":     total,
        "displayed_samples": len(all_samples),
    }


# ---------------------------------------------------------------------------
# Export (full-resolution CSV download, no downsampling)
# ---------------------------------------------------------------------------

@app.get("/export")
async def export_csv(
    bus_id:  int = Query(..., ge=1, le=5),
    channel: str = Query(..., pattern="^[HL]$"),
    start:   str = Query(...),
    end:     str = Query(...),
):
    """Stream all raw samples in [start, end) as a CSV file without downsampling."""
    try:
        t_start = datetime.fromisoformat(start)
        t_end   = datetime.fromisoformat(end)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid datetime: {exc}")
    if t_start >= t_end:
        raise HTTPException(status_code=422, detail="start must be before end")
    span_s = (t_end - t_start).total_seconds()
    if span_s > 60:
        raise HTTPException(status_code=422, detail="Export window too large (max 60 s)")

    log.debug("Export bus=%s ch=%s  %s → %s  (%.1f ms)", bus_id, channel, t_start, t_end, span_s * 1000)

    t_fetch_start = t_start - timedelta(milliseconds=2)
    t_fetch_end   = t_end   + timedelta(milliseconds=2)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT time, sample_rate, n_samples, samples
               FROM waveform_chunks
               WHERE bus_id = $1 AND channel = $2
                 AND time >= $3 AND time < $4
               ORDER BY time""",
            bus_id, channel, t_fetch_start, t_fetch_end,
        )

    t_start_unix = t_start.timestamp()
    t_end_unix   = t_end.timestamp()

    def generate():
        yield "time_unix_s,time_utc,voltage_v\n"
        for r in rows:
            try:
                raw   = lz4.frame.decompress(bytes(r["samples"]))
                s_arr = np.frombuffer(raw, dtype="<i2").astype(np.float64) * (5.0 / 32767)
                t0    = r["time"].timestamp()
                dt    = 1.0 / r["sample_rate"]
                n     = r["n_samples"]
                t_arr = t0 + np.arange(n, dtype=np.float64) * dt
                mask  = (t_arr >= t_start_unix) & (t_arr < t_end_unix)
                for t_val, v_val in zip(t_arr[mask], s_arr[mask]):
                    utc = datetime.utcfromtimestamp(float(t_val)).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
                    yield f"{t_val:.9f},{utc},{v_val:.6f}\n"
            except Exception as exc:
                log.error("Export decomp error bus=%s ch=%s: %s", bus_id, channel, exc)

    fname = f"bus{bus_id}_CAN{channel}_{t_start.strftime('%Y%m%dT%H%M%S')}.csv"
    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={fname}"},
    )


# ---------------------------------------------------------------------------
# Auxiliary endpoints
# ---------------------------------------------------------------------------

@app.get("/buses")
async def list_buses():
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT DISTINCT bus_id, channel
               FROM waveform_chunks
               ORDER BY bus_id, channel"""
        )
    return [{"bus_id": r["bus_id"], "channel": r["channel"]} for r in rows]


@app.get("/health")
async def health():
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        log.debug("Health check: OK")
    except Exception as exc:
        log.error("Health check FAILED: %s", exc, exc_info=True)
        raise HTTPException(status_code=503, detail="Database unreachable")
    return {"status": "ok"}


@app.get("/storage")
async def storage_info():
    """Return DB table size and host disk usage for the data volume."""
    import shutil
    async with pool.acquire() as conn:
        # Total size of the waveform_chunks table (including indexes + TOAST)
        db_bytes = await conn.fetchval(
            "SELECT pg_total_relation_size('waveform_chunks')"
        )
        # Oldest and newest chunk timestamps
        oldest = await conn.fetchval("SELECT MIN(time) FROM waveform_chunks")
        newest = await conn.fetchval("SELECT MAX(time) FROM waveform_chunks")
        # Row count (fast estimate)
        row_est = await conn.fetchval(
            "SELECT reltuples::bigint FROM pg_class WHERE relname='waveform_chunks'"
        )

    # Disk usage for the filesystem where the DB lives (/opt/candb or /var/lib/docker)
    disk = shutil.disk_usage("/opt/candb")

    span_hours = None
    if oldest and newest:
        span_hours = round((newest - oldest).total_seconds() / 3600, 2)

    return {
        "db_table_bytes":   int(db_bytes or 0),
        "db_table_mb":      round((db_bytes or 0) / 1_048_576, 1),
        "disk_total_gb":    round(disk.total / 1_073_741_824, 1),
        "disk_used_gb":     round(disk.used  / 1_073_741_824, 1),
        "disk_free_gb":     round(disk.free  / 1_073_741_824, 1),
        "disk_used_pct":    round(disk.used  / disk.total * 100, 1),
        "chunk_rows_est":   int(row_est or 0),
        "oldest_chunk_utc": oldest.isoformat() if oldest else None,
        "newest_chunk_utc": newest.isoformat() if newest else None,
        "span_hours":       span_hours,
    }



_EDGE_SEARCH_S = 1.0   # look up to 1 s away
_EDGE_THRESH_V = 3.3   # CAN-H dominant threshold (V)
_MIN_REC_SAMP  = 3     # min consecutive samples below threshold before edge counts


@app.get("/find_edge")
async def find_edge(
    bus_id:    int = Query(..., ge=1, le=5),
    ref:       str = Query(...),
    direction: str = Query(..., pattern="^(next|prev)$"),
):
    try:
        t_ref = datetime.fromisoformat(ref)
        if t_ref.tzinfo is None:
            t_ref = t_ref.replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid ref: {exc}")

    if direction == "next":
        t_start = t_ref
        t_end   = t_ref + timedelta(seconds=_EDGE_SEARCH_S)
    else:
        t_start = t_ref - timedelta(seconds=_EDGE_SEARCH_S)
        t_end   = t_ref

    log.debug("find_edge bus=%s dir=%s ref=%s", bus_id, direction, t_ref)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT time, sample_rate, n_samples, samples
               FROM waveform_chunks
               WHERE bus_id = $1 AND channel = 'H'
                 AND time >= $2 AND time < $3
               ORDER BY time""",
            bus_id, t_start, t_end,
        )

    if not rows:
        raise HTTPException(status_code=404, detail="No waveform data in search window")

    all_times, all_samples = [], []
    try:
        for r in rows:
            raw = lz4.frame.decompress(bytes(r["samples"]))
            arr = np.frombuffer(raw, dtype="<i2").astype(np.float32) * (5.0 / 32767)
            t0  = r["time"].timestamp()
            dt  = 1.0 / r["sample_rate"]
            n   = r["n_samples"]
            all_times.extend(t0 + i * dt for i in range(n))
            all_samples.extend(arr.tolist())
    except Exception as exc:
        log.error("find_edge decompression error: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to decompress waveform data")

    t_arr = np.array(all_times,   dtype=np.float64)
    s_arr = np.array(all_samples, dtype=np.float32)
    above = s_arr > _EDGE_THRESH_V

    # Rising edge indices: below-threshold → above-threshold transition
    raw_edges = np.where(~above[:-1] & above[1:])[0] + 1

    # Keep only edges preceded by _MIN_REC_SAMP consecutive recessive samples
    valid_edges = [
        i for i in raw_edges
        if not np.any(above[max(0, i - _MIN_REC_SAMP):i])
    ]

    t_ref_unix = t_ref.timestamp()
    if direction == "next":
        candidates = [i for i in valid_edges if t_arr[i] > t_ref_unix + 1e-6]
        if not candidates:
            raise HTTPException(status_code=404, detail="No rising edge found in next 100 ms")
        edge_idx = candidates[0]
    else:
        candidates = [i for i in valid_edges if t_arr[i] < t_ref_unix - 1e-6]
        if not candidates:
            raise HTTPException(status_code=404, detail="No rising edge found in prev 100 ms")
        edge_idx = candidates[-1]

    edge_unix = float(t_arr[edge_idx])
    edge_utc  = datetime.fromtimestamp(edge_unix, tz=timezone.utc).isoformat()
    log.debug("find_edge result bus=%s dir=%s → %s", bus_id, direction, edge_utc)
    return {"time_unix": edge_unix, "time_utc": edge_utc}


# ---------------------------------------------------------------------------
# CAN Frame Decoder
# ---------------------------------------------------------------------------

_BAUD_RATE = 125_000  # CAN bus baud rate (Hz)
_SPB       = _WIRE_SAMPLE_RATE / _BAUD_RATE  # = 12.5 samples per bit (fractional)
_DOM_HI    = 0.9      # V: diff above this → dominant (bit 0)
_REC_LO    = 0.5      # V: diff below this → recessive (bit 1)
_CRC_POLY  = 0x4599   # CAN 15-bit CRC polynomial


def _crc15(bits: list) -> int:
    crc = 0
    for b in bits:
        inv = b ^ ((crc >> 14) & 1)
        crc = ((crc << 1) & 0x7FFF)
        if inv:
            crc ^= _CRC_POLY
    return crc


def _bits_to_int(bits: list) -> int:
    v = 0
    for b in bits:
        v = (v << 1) | b
    return v


def _destuff(raw: list):
    """Remove bit stuffing. Returns (destuffed_bits, n_stuff_errors)."""
    if not raw:
        return [], 0
    out = [raw[0]]
    run, last, errors = 1, raw[0], 0
    i = 1
    while i < len(raw):
        b = raw[i]
        if b == last:
            run += 1
            if run == 6:
                errors += 1
                run = 1
            out.append(b)
        else:
            if run >= 5:
                # stuff bit — skip it, don't append
                last = b
                run = 1
                i += 1
                continue
            out.append(b)
            run = 1
        last = b
        i += 1
    return out, errors


def _extract_bits(diff_v: np.ndarray, spb: float = _SPB):
    """
    Convert differential voltage array to CAN bits at 125 kbps.

    Uses a phase accumulator for fractional samples-per-bit (12.5 @ 1.5625 MS/s)
    and hard-synchronises (resets accumulator) on every recessive→dominant edge
    within ±0.5 bit of the expected sample point, matching CAN hard-sync behaviour.

    Returns (bits, sample_offsets) where sample_offsets are integer sample indices
    of the mid-point of each bit.
    """
    n = len(diff_v)
    if n < spb * 20:
        return [], []

    # Hysteretic threshold → dominant boolean array
    dom = np.zeros(n, dtype=bool)
    cur = diff_v[0] > _DOM_HI
    for i in range(n):
        v = float(diff_v[i])
        if v > _DOM_HI:
            cur = True
        elif v < _REC_LO:
            cur = False
        dom[i] = cur

    # Find SOF: first dominant edge preceded by ≥ 3 recessive bit-periods
    min_rec = int(3 * spb)
    sof = -1
    for i in range(min_rec, n - int(spb)):
        if dom[i] and not dom[i - 1]:
            if not np.any(dom[max(0, i - min_rec): i]):
                sof = i
                break
    if sof < 0:
        return [], []

    # Phase accumulator: sample midpoint at phase=0.5 (50% into the bit period)
    # Hard-sync resets phase to 0 on recessive→dominant edge near a bit boundary.
    bits, offsets = [], []
    phase = 0.0           # fraction through current bit period [0, 1)
    pos   = float(sof)    # current position in samples (float for accumulation)
    half_window = spb * 0.45  # edges within this many samples of bit start → hard sync

    while int(pos) + int(spb) <= n:
        # Sample at mid-point of this bit
        mid = int(pos + spb * 0.5)
        if mid >= n:
            break
        lo  = max(0, int(pos + spb * 0.25))
        hi  = min(n, int(pos + spb * 0.75))
        is_dom = bool(np.sum(dom[lo:hi]) > (hi - lo) // 2)
        bits.append(0 if is_dom else 1)
        offsets.append(mid)

        next_pos = pos + spb

        # Look for a recessive→dominant edge in the next bit period that indicates
        # a hard-sync opportunity.  Only resync during Bus Idle or SOF detection —
        # within a frame we use resync (adjust by at most 1 tq = spb/8).
        # For simplicity we apply hard-sync only if we're outside a run of consecutive
        # dominant bits (i.e. we could be at a frame boundary / SOF).
        edge_search_start = int(next_pos - half_window)
        edge_search_end   = int(next_pos + half_window)
        edge_search_start = max(int(pos + spb * 0.6), edge_search_start)
        edge_search_end   = min(n - 1, edge_search_end)
        if edge_search_start < edge_search_end:
            seg = dom[edge_search_start:edge_search_end]
            rises = np.where(~seg[:-1] & seg[1:])[0]
            if len(rises) > 0:
                edge_idx = edge_search_start + int(rises[0]) + 1
                # Hard-sync: restart bit clock from this edge
                next_pos = float(edge_idx)

        pos = next_pos

    return bits, offsets


def _parse_can_frames(bits: list, offsets: list, t0: float, sample_rate: float) -> tuple:
    """Walk the bit stream and return (frames, bit_annotations).

    bit_annotations: list of {time_unix, bit, label} for every bit in every frame
    (after destuffing).  Label identifies the field: SOF, ID, RTR, IDE, r0, DLC,
    DATA, CRC, CRCDELIM, ACK, EOF, IFS, STUFF.
    """
    frames = []
    bit_annotations = []
    n = len(bits)
    dt = 1.0 / sample_rate
    i = 0

    while i < n - 44:
        # SOF must be dominant (0); preceding 3+ bits recessive (1)
        if bits[i] != 0:
            i += 1
            continue
        if i >= 3 and any(b == 0 for b in bits[max(0, i - 3): i]):
            i += 1
            continue

        frame_time = t0 + offsets[i] * dt

        # Grab enough raw bits for worst-case stuffed frame (8-byte data ≈ 135 bits stuffed)
        raw = bits[i: i + 160]
        raw_offsets = offsets[i: i + 160]
        destuffed, stuff_errors = _destuff(raw)

        if len(destuffed) < 44:
            i += 1
            continue

        # Map destuffed bit index → original sample offset (skip stuff bits)
        def _destuff_offsets(raw_bits, raw_offs):
            """Return sample offsets for each destuffed bit, mirroring _destuff logic."""
            if not raw_bits:
                return []
            result = [raw_offs[0]]
            run, last = 1, raw_bits[0]
            j = 1
            while j < len(raw_bits):
                b = raw_bits[j]
                if b == last:
                    run += 1
                    if run == 6:
                        run = 1
                    result.append(raw_offs[j])
                else:
                    if run >= 5:
                        last = b
                        run = 1
                        j += 1
                        continue
                    result.append(raw_offs[j])
                    run = 1
                last = b
                j += 1
            return result

        dest_offs = _destuff_offsets(raw, raw_offsets)

        def _annot(bit_val, label, dest_idx):
            if dest_idx < len(dest_offs):
                t_unix = round(t0 + dest_offs[dest_idx] * dt, 9)
                bit_annotations.append({"time_unix": t_unix, "bit": bit_val, "label": label})

        try:
            d = destuffed
            p = 0

            if d[p] != 0:           # SOF must be dominant
                i += 1; continue
            _annot(d[p], "SOF", p); p += 1

            can_id = _bits_to_int(d[p: p + 11])
            for k in range(11):
                _annot(d[p + k], f"ID{10 - k}", p + k)
            p += 11

            rtr = bool(d[p]); _annot(d[p], "RTR", p); p += 1
            ide = d[p];       _annot(d[p], "IDE", p); p += 1

            if ide == 1:            # extended frame — skip
                i += 1; continue

            _annot(d[p], "r0", p); p += 1
            dlc = _bits_to_int(d[p: p + 4])
            for k in range(4):
                _annot(d[p + k], f"DLC{3 - k}", p + k)
            p += 4
            if dlc > 8:
                i += 1; continue

            data_bits = d[p: p + dlc * 8]
            if len(data_bits) < dlc * 8:
                i += 1; continue
            for k in range(dlc * 8):
                byte_n = k // 8
                bit_n  = 7 - (k % 8)
                _annot(d[p + k], f"D{byte_n}.{bit_n}", p + k)
            p += dlc * 8

            if p + 15 > len(d):
                i += 1; continue
            crc_rx   = _bits_to_int(d[p: p + 15])
            crc_calc = _crc15(d[0: p - 15])
            crc_ok   = (crc_calc == crc_rx)
            for k in range(15):
                _annot(d[p + k], f"CRC{14 - k}", p + k)
            p += 15

            # CRC delimiter
            crc_delim_ok = p < len(d) and d[p] == 1
            if not crc_delim_ok:
                crc_ok = False
            if p < len(d):
                _annot(d[p], "CDEL", p)
            p += 1

            # ACK slot + ACK delimiter
            if p < len(d): _annot(d[p], "ACK", p)
            if p + 1 < len(d): _annot(d[p + 1], "ADEL", p + 1)
            p += 2

            eof_ok = (p + 7 <= len(d)) and all(b == 1 for b in d[p: p + 7])
            for k in range(min(7, len(d) - p)):
                _annot(d[p + k], "EOF", p + k)
            p += 7 + 3              # EOF + intermission

            data_bytes = [_bits_to_int(data_bits[j * 8: (j + 1) * 8]) for j in range(dlc)]

            frames.append({
                "time_utc":     datetime.fromtimestamp(frame_time, tz=timezone.utc).isoformat(),
                "time_unix":    round(frame_time, 6),
                "can_id_hex":   f"0x{can_id:03X}",
                "can_id_int":   can_id,
                "rtr":          rtr,
                "dlc":          dlc,
                "data_bytes":   data_bytes,
                "data_hex":     " ".join(f"{b:02X}" for b in data_bytes),
                "crc_ok":       crc_ok,
                "eof_ok":       eof_ok,
                "stuff_errors": stuff_errors,
            })

            # Advance past this frame (raw bit count including stuffing + interframe)
            n_raw_data_bits = 1 + 11 + 1 + 1 + 1 + 4 + dlc * 8 + 15 + 1 + 1 + 1 + 7 + 3
            i += int(n_raw_data_bits * 1.25)

        except Exception:
            i += 1
            continue

    return frames, bit_annotations


@app.get("/decode")
async def decode_can(
    bus_id:     int = Query(..., ge=1, le=5),
    start:      str = Query(...),
    end:        str = Query(...),
    max_frames: int = Query(default=2000, ge=1, le=5000),
):
    try:
        t_start = datetime.fromisoformat(start)
        t_end   = datetime.fromisoformat(end)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"Invalid datetime: {exc}")
    if t_start >= t_end:
        raise HTTPException(status_code=422, detail="start must be before end")

    log.debug("Decode bus=%s  %s → %s", bus_id, t_start, t_end)

    # Fetch 2ms of extra data on each side so frames that straddle chunk
    # boundaries (SOF in the chunk just before t_start) are still decoded.
    _PAD = timedelta(milliseconds=2)
    t_fetch_start = t_start - _PAD
    t_fetch_end   = t_end   + _PAD

    async with pool.acquire() as conn:
        rows_h = await conn.fetch(
            "SELECT time, sample_rate, n_samples, samples FROM waveform_chunks "
            "WHERE bus_id=$1 AND channel='H' AND time >= $2 AND time < $3 ORDER BY time",
            bus_id, t_fetch_start, t_fetch_end,
        )
        rows_l = await conn.fetch(
            "SELECT time, sample_rate, n_samples, samples FROM waveform_chunks "
            "WHERE bus_id=$1 AND channel='L' AND time >= $2 AND time < $3 ORDER BY time",
            bus_id, t_fetch_start, t_fetch_end,
        )

    if not rows_h or not rows_l:
        return {"frames": [], "frame_count": 0, "bit_rate_hz": 0,
                "total_bits": 0, "error": "No data in range"}

    h_map = {r["time"]: r for r in rows_h}
    l_map = {r["time"]: r for r in rows_l}
    common = sorted(set(h_map) & set(l_map))
    if not common:
        return {"frames": [], "frame_count": 0, "bit_rate_hz": 0,
                "total_bits": 0, "error": "H and L chunk timestamps do not match"}

    sample_rate = rows_h[0]["sample_rate"]
    diff_chunks, chunk_t0 = [], []

    for t in common:
        try:
            ah = np.frombuffer(lz4.frame.decompress(bytes(h_map[t]["samples"])), dtype="<i2").astype(np.float32) * (5.0 / 32767)
            al = np.frombuffer(lz4.frame.decompress(bytes(l_map[t]["samples"])), dtype="<i2").astype(np.float32) * (5.0 / 32767)
            nn = min(len(ah), len(al))
            diff_chunks.append(ah[:nn] - al[:nn])
            chunk_t0.append(t.timestamp())
        except Exception as exc:
            log.warning("Decode: decompression failed at %s: %s", t, exc)

    if not diff_chunks:
        raise HTTPException(status_code=500, detail="Failed to decompress any chunks")

    diff_all = np.concatenate(diff_chunks)
    bits, offsets = _extract_bits(diff_all, _SPB)
    bit_rate_hz = round(_BAUD_RATE)

    if not bits:
        return {"frames": [], "frame_count": 0, "bit_rate_hz": bit_rate_hz,
                "total_bits": 0, "error": "No SOF detected — bus may be idle in this window"}

    frames, bit_annotations = _parse_can_frames(bits, offsets, chunk_t0[0], sample_rate)

    # Filter to only frames whose SOF falls within the originally requested window
    t_start_unix = t_start.timestamp()
    t_end_unix   = t_end.timestamp()
    frames         = [f for f in frames if t_start_unix <= f["time_unix"] < t_end_unix]
    bit_annotations = [a for a in bit_annotations if t_start_unix <= a["time_unix"] < t_end_unix]

    frames          = frames[:max_frames]
    bit_annotations = bit_annotations[:max_frames * 160]

    log.info("Decode bus=%s: %d bits → %d frames (%.0f kbps) %d annotations",
             bus_id, len(bits), len(frames), bit_rate_hz / 1000, len(bit_annotations))

    return {
        "frames":          frames,
        "frame_count":     len(frames),
        "total_bits":      len(bits),
        "bit_rate_hz":     bit_rate_hz,
        "samples_per_bit": _SPB,
        "bit_annotations": bit_annotations,
    }
