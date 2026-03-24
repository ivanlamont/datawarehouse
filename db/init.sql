CREATE SCHEMA IF NOT EXISTS market_data;
SET search_path TO market_data;

CREATE TABLE equity_reference (
    symbol             VARCHAR(20) PRIMARY KEY,
    name               VARCHAR(255),
    exchange           VARCHAR(50),
    sector             VARCHAR(100),
    industry           VARCHAR(150),
    currency           VARCHAR(10),
    market_cap         BIGINT,
    shares_outstanding BIGINT,
    updated_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE equity_pricing (
    symbol    VARCHAR(20)      NOT NULL,
    ts        DATE             NOT NULL,
    open      DOUBLE PRECISION,
    high      DOUBLE PRECISION,
    low       DOUBLE PRECISION,
    close     DOUBLE PRECISION,
    volume    DOUBLE PRECISION,
    adj_close DOUBLE PRECISION,
    PRIMARY KEY (symbol, ts)
);
CREATE INDEX ON equity_pricing (symbol, ts DESC);

CREATE TABLE options_reference (
    contract_symbol VARCHAR(30) PRIMARY KEY,
    underlying      VARCHAR(20)      NOT NULL,
    strike          DOUBLE PRECISION NOT NULL,
    expiry          DATE             NOT NULL,
    option_type     CHAR(1)          NOT NULL  -- 'C' or 'P'
);
CREATE INDEX ON options_reference (underlying);

CREATE TABLE options_pricing (
    contract_symbol    VARCHAR(30)      NOT NULL,
    ts                 DATE             NOT NULL,
    underlying         VARCHAR(20)      NOT NULL,
    bid                DOUBLE PRECISION,
    ask                DOUBLE PRECISION,
    last_price         DOUBLE PRECISION,
    volume             DOUBLE PRECISION,
    open_interest      DOUBLE PRECISION,
    implied_volatility DOUBLE PRECISION,
    delta              DOUBLE PRECISION,
    gamma              DOUBLE PRECISION,
    theta              DOUBLE PRECISION,
    vega               DOUBLE PRECISION,
    PRIMARY KEY (contract_symbol, ts)
);
CREATE INDEX ON options_pricing (underlying, ts DESC);

CREATE TABLE fx_rates (
    pair  VARCHAR(10)      NOT NULL,
    ts    TIMESTAMPTZ      NOT NULL,
    open  DOUBLE PRECISION,
    high  DOUBLE PRECISION,
    low   DOUBLE PRECISION,
    close DOUBLE PRECISION,
    PRIMARY KEY (pair, ts)
);
CREATE INDEX ON fx_rates (pair, ts DESC);
