-- TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Waveform chunk table
-- Each row: one compressed chunk of int16 analog samples from one channel of one CAN bus.
-- samples: LZ4-compressed little-endian int16 array.
CREATE TABLE IF NOT EXISTS waveform_chunks (
    time         TIMESTAMPTZ   NOT NULL,
    bus_id       SMALLINT      NOT NULL,
    channel      CHAR(1)       NOT NULL,
    sample_rate  INTEGER       NOT NULL,
    n_samples    INTEGER       NOT NULL,
    samples      BYTEA         NOT NULL
);

SELECT create_hypertable(
    'waveform_chunks',
    'time',
    chunk_time_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS waveform_chunks_bus_channel_time_idx
    ON waveform_chunks (bus_id, channel, time DESC);

ALTER TABLE waveform_chunks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'bus_id, channel',
    timescaledb.compress_orderby   = 'time DESC'
);

SELECT add_compression_policy('waveform_chunks', INTERVAL '2 minutes');

SELECT add_retention_policy('waveform_chunks', INTERVAL '24 hours');
