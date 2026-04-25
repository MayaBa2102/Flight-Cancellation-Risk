CREATE TABLE IF NOT EXISTS flights_schedule (
    id              SERIAL PRIMARY KEY,
    flight_iata     VARCHAR(20)       NOT NULL,
    airline         VARCHAR(100),
    dep_iata        VARCHAR(10),
    dep_lat         DOUBLE PRECISION,
    dep_lon         DOUBLE PRECISION,
    arr_iata        VARCHAR(10),
    arr_lat         DOUBLE PRECISION,
    arr_lon         DOUBLE PRECISION,
    scheduled_dep   TIMESTAMPTZ,
    scheduled_arr   TIMESTAMPTZ,
    fetched_date    DATE              NOT NULL,
    UNIQUE (flight_iata, scheduled_dep)
);

CREATE TABLE IF NOT EXISTS flight_risk (
    id              SERIAL PRIMARY KEY,
    flight_iata     VARCHAR(20)       NOT NULL,
    airline         VARCHAR(100),
    dep_iata        VARCHAR(10),
    arr_iata        VARCHAR(10),
    scheduled_dep   TIMESTAMPTZ,
    scheduled_arr   TIMESTAMPTZ,
    dep_wind_kmh    DOUBLE PRECISION,
    dep_rain_mm_h   DOUBLE PRECISION,
    arr_wind_kmh    DOUBLE PRECISION,
    arr_rain_mm_h   DOUBLE PRECISION,
    risk_level      VARCHAR(10)       NOT NULL,
    risk_reason     TEXT,
    risk_airport    VARCHAR(10),
    last_updated    TIMESTAMPTZ       DEFAULT NOW(),
    UNIQUE (flight_iata, scheduled_dep)
);

CREATE INDEX IF NOT EXISTS idx_risk_level      ON flight_risk (risk_level);
CREATE INDEX IF NOT EXISTS idx_risk_dep_iata   ON flight_risk (dep_iata);
CREATE INDEX IF NOT EXISTS idx_risk_sched_dep  ON flight_risk (scheduled_dep);
