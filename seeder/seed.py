"""
Seeder: fetches S&P 500 equity + options data and writes to PostgreSQL.

Equity data: yfinance (free)
Options data: Polygon flat files via S3/boto3
"""

import csv
import gzip
import io
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta

import requests

import boto3
import pandas as pd
import psycopg2
import psycopg2.extras
import yfinance as yf
from botocore.config import Config
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

DB_HOST = os.environ["POSTGRES_HOST"]
DB_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
DB_NAME = os.environ["POSTGRES_DB"]
DB_USER = os.environ["POSTGRES_USER"]
DB_PASS = os.environ["POSTGRES_PASSWORD"]

POLYGON_ACCESS_KEY_ID = os.environ.get("POLYGON_ACCESS_KEY_ID", "")
POLYGON_SECRET_ACCESS_KEY = os.environ.get("POLYGON_SECRET_ACCESS_KEY", "")

SEED_START_DATE = os.environ.get("SEED_START_DATE", "2023-01-01")
SEED_OPTIONS_DAYS = int(os.environ.get("SEED_OPTIONS_DAYS", 5))
SEED_SYMBOLS_ENV = os.environ.get("SEED_SYMBOLS", "")
# SP500 | RUSSELL2000 | BOTH  (default: SP500)
SEED_UNIVERSE = os.environ.get("SEED_UNIVERSE", "SP500").upper()
# How many days before equity_reference rows are considered stale (default: 1)
EQUITY_REF_TTL_DAYS = int(os.environ.get("EQUITY_REF_TTL_DAYS", 1))


# ── DB connection ─────────────────────────────────────────────────────────────

def wait_for_db() -> psycopg2.extensions.connection:
    """Retry until PostgreSQL accepts connections."""
    for attempt in range(30):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASS,
                sslmode="require",
                options="-c search_path=market_data",
            )
            log.info("Connected to PostgreSQL (schema: market_data).")
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("DB not ready (attempt %d/30): %s", attempt + 1, exc)
            time.sleep(2)
    log.error("Could not connect to PostgreSQL after 30 attempts.")
    sys.exit(1)


# ── Symbol universe ───────────────────────────────────────────────────────────

_RUSSELL2000_RAW_URL = (
    "https://raw.githubusercontent.com/ikoniaris/Russell2000/master/russell_2000_components.csv"
)


def _fetch_sp500() -> list[str]:
    log.info("Fetching S&P 500 list from Wikipedia…")
    resp = requests.get(
        "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        headers={"User-Agent": "Mozilla/5.0 (compatible; ml4t-seeder/1.0)"},
        timeout=30,
    )
    resp.raise_for_status()
    df = pd.read_html(io.StringIO(resp.text))[0]
    symbols = df["Symbol"].str.replace(".", "-", regex=False).tolist()
    log.info("S&P 500: %d symbols.", len(symbols))
    return symbols


def _fetch_russell2000() -> list[str]:
    log.info("Fetching Russell 2000 list from GitHub…")
    resp = requests.get(
        _RUSSELL2000_RAW_URL,
        headers={"User-Agent": "Mozilla/5.0 (compatible; ml4t-seeder/1.0)"},
        timeout=30,
    )
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text))
    # The CSV has a 'Ticker' column; normalise whatever header casing is present
    ticker_col = next(c for c in df.columns if c.strip().lower() == "ticker")
    symbols = df[ticker_col].str.strip().str.upper().tolist()
    log.info("Russell 2000: %d symbols.", len(symbols))
    return symbols


def get_symbols() -> list[str]:
    """Return deduplicated symbol list according to SEED_UNIVERSE / SEED_SYMBOLS."""
    if SEED_SYMBOLS_ENV:
        symbols = [s.strip().upper() for s in SEED_SYMBOLS_ENV.split(",") if s.strip()]
        log.info("Using %d symbols from SEED_SYMBOLS env.", len(symbols))
        return symbols

    if SEED_UNIVERSE == "RUSSELL2000":
        return _fetch_russell2000()
    elif SEED_UNIVERSE == "BOTH":
        sp500 = _fetch_sp500()
        r2000 = _fetch_russell2000()
        combined = list(dict.fromkeys(sp500 + r2000))  # deduplicate, preserve order
        log.info("Combined universe: %d symbols.", len(combined))
        return combined
    else:  # default: SP500
        return _fetch_sp500()


# ── Staleness checks ──────────────────────────────────────────────────────────

def _equity_ref_stale_symbols(conn, symbols: list[str]) -> list[str]:
    """
    Return the subset of symbols that are missing from equity_reference or
    whose updated_at is older than EQUITY_REF_TTL_DAYS.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol
            FROM   market_data.equity_reference
            WHERE  symbol = ANY(%s)
              AND  updated_at >= NOW() - INTERVAL '1 day' * %s
            """,
            (symbols, EQUITY_REF_TTL_DAYS),
        )
        fresh = {row[0] for row in cur.fetchall()}
    stale = [s for s in symbols if s not in fresh]
    return stale


def _options_ref_covered_underlyings(conn, underlyings: set[str]) -> set[str]:
    """
    Return the subset of underlyings that already have at least one
    non-expired contract in options_reference.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT underlying
            FROM   market_data.options_reference
            WHERE  underlying = ANY(%s)
              AND  expiry >= CURRENT_DATE
            """,
            (list(underlyings),),
        )
        return {row[0] for row in cur.fetchall()}


# ── Equity reference ──────────────────────────────────────────────────────────

def seed_equity_reference(conn, symbols: list[str]) -> None:
    stale = _equity_ref_stale_symbols(conn, symbols)
    if not stale:
        log.info("equity_reference: all %d symbols are fresh (TTL %d day(s)); skipping.", len(symbols), EQUITY_REF_TTL_DAYS)
        return
    if len(stale) < len(symbols):
        log.info(
            "equity_reference: %d/%d symbols are stale or missing; fetching those only.",
            len(stale), len(symbols),
        )
    else:
        log.info("equity_reference: seeding all %d symbols…", len(symbols))

    rows = []
    for i, sym in enumerate(stale, 1):
        try:
            info = yf.Ticker(sym).info
            rows.append((
                sym,
                info.get("longName") or info.get("shortName"),
                info.get("exchange"),
                info.get("sector"),
                info.get("industry"),
                info.get("currency"),
                info.get("marketCap"),
                info.get("sharesOutstanding"),
            ))
            if i % 50 == 0:
                log.info("  equity_reference: %d/%d", i, len(stale))
        except Exception as exc:
            log.warning("  skipping %s: %s", sym, exc)

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO equity_reference
                (symbol, name, exchange, sector, industry, currency, market_cap, shares_outstanding)
            VALUES %s
            ON CONFLICT (symbol) DO UPDATE SET
                name               = EXCLUDED.name,
                exchange           = EXCLUDED.exchange,
                sector             = EXCLUDED.sector,
                industry           = EXCLUDED.industry,
                currency           = EXCLUDED.currency,
                market_cap         = EXCLUDED.market_cap,
                shares_outstanding = EXCLUDED.shares_outstanding,
                updated_at         = NOW()
            """,
            rows,
        )
    conn.commit()
    log.info("equity_reference: inserted/updated %d rows.", len(rows))


# ── Equity pricing ────────────────────────────────────────────────────────────

def seed_equity_pricing(conn, symbols: list[str]) -> None:
    log.info("Seeding equity_pricing from %s…", SEED_START_DATE)
    total = 0
    for i, sym in enumerate(symbols, 1):
        try:
            df = yf.download(sym, start=SEED_START_DATE, auto_adjust=True, progress=False)
            if df.empty:
                continue
            df = df.reset_index()
            # yfinance may return MultiIndex columns when auto_adjust=True
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0].lower() for c in df.columns]
            else:
                df.columns = [c.lower() for c in df.columns]
            rows = [
                (
                    sym,
                    row["date"].to_pydatetime() if hasattr(row["date"], "to_pydatetime") else row["date"],
                    _float(row.get("open")),
                    _float(row.get("high")),
                    _float(row.get("low")),
                    _float(row.get("close")),
                    _float(row.get("volume")),
                    _float(row.get("adj close") or row.get("close")),
                )
                for _, row in df.iterrows()
            ]
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO equity_pricing (symbol, ts, open, high, low, close, volume, adj_close)
                    VALUES %s
                    ON CONFLICT (symbol, ts) DO NOTHING
                    """,
                    rows,
                )
            conn.commit()
            total += len(rows)
            if i % 50 == 0:
                log.info("  equity_pricing: %d/%d symbols, %d rows so far", i, len(symbols), total)
        except Exception as exc:
            log.warning("  skipping equity_pricing for %s: %s", sym, exc)
    log.info("equity_pricing: inserted %d rows total.", total)


# ── Options (Polygon flat files + REST API fallback) ──────────────────────────

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "")
POLYGON_API_BASE_URL = os.environ.get("POLYGON_API_BASE_URL", "https://api.massive.com")
POLYGON_S3_ENDPOINT_URL = os.environ.get("POLYGON_S3_ENDPOINT_URL", "https://files.polygon.io")
POLYGON_S3_BUCKET_NAME = os.environ.get("POLYGON_S3_BUCKET_NAME", "flatfiles")
_API_CALL_INTERVAL = 60.0 / 5  # 5 calls per minute → 12 s between calls
_last_api_call: float = 0.0


def _polygon_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=POLYGON_S3_ENDPOINT_URL,
        aws_access_key_id=POLYGON_ACCESS_KEY_ID,
        aws_secret_access_key=POLYGON_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )


def _recent_trading_days(n: int) -> list[date]:
    """Return the last n calendar days that are Mon–Fri (approximate trading days)."""
    days = []
    d = date.today() - timedelta(days=1)
    while len(days) < n:
        if d.weekday() < 5:  # Mon=0 … Fri=4
            days.append(d)
        d -= timedelta(days=1)
    return days


def _parse_occ_symbol(contract: str) -> tuple[str | None, float | None, date | None]:
    """Return (option_type, strike, expiry) parsed from an OCC contract symbol."""
    try:
        body = contract.lstrip("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        option_type = body[6]  # C or P
        expiry_str = body[:6]
        expiry = date(2000 + int(expiry_str[:2]), int(expiry_str[2:4]), int(expiry_str[4:6]))
        strike = int(body[7:]) / 1000.0
        return option_type, strike, expiry
    except Exception:
        return None, None, None


def _parse_flat_file_rows(
    compressed: bytes,
    day: date,
    sp500_set: set[str],
) -> tuple[list[tuple], list[tuple]]:
    """Decompress and parse a Polygon day-aggs CSV.gz into ref + pricing rows."""
    ref_rows: list[tuple] = []
    pricing_rows: list[tuple] = []
    ts = datetime(day.year, day.month, day.day)
    with gzip.open(io.BytesIO(compressed), "rt", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            underlying = row.get("underlying_symbol", "").upper()
            if underlying not in sp500_set:
                continue
            contract = row.get("ticker", "")
            if not contract:
                continue
            option_type, strike, expiry = _parse_occ_symbol(contract)
            ref_rows.append((contract, underlying, strike, expiry, option_type))
            pricing_rows.append((
                contract,
                ts,
                underlying,
                None,  # bid (not in day_aggs)
                None,  # ask
                _float(row.get("close")),
                _float(row.get("volume")),
                _float(row.get("open_interest")),
                None,  # implied_volatility
                None, None, None, None,  # greeks
            ))
    return ref_rows, pricing_rows


def _throttled_api_get(url: str, params: dict | None = None) -> dict:
    """GET a Massive/Polygon REST endpoint, sleeping to honour 5 req/min."""
    global _last_api_call
    elapsed = time.monotonic() - _last_api_call
    if elapsed < _API_CALL_INTERVAL:
        time.sleep(_API_CALL_INTERVAL - elapsed)
    _last_api_call = time.monotonic()
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _iter_options_contracts(underlying: str) -> list[dict]:
    """
    Fetch all pages of /v3/reference/options/contracts for one underlying,
    following the cursor in next_url until exhausted.
    """
    url = f"{POLYGON_API_BASE_URL}/v3/reference/options/contracts"
    params: dict = {
        "underlying_ticker": underlying,
        "order": "asc",
        "limit": 250,
        "sort": "ticker",
        "apiKey": POLYGON_API_KEY,
    }
    results: list[dict] = []
    while url:
        data = _throttled_api_get(url, params)
        results.extend(data.get("results", []))
        # next_url is a fully-qualified URL with cursor already embedded
        next_url = data.get("next_url")
        if next_url:
            # Append the API key; params are not re-sent for cursor pages
            url = next_url + f"&apiKey={POLYGON_API_KEY}"
            params = None
        else:
            url = None
    return results


def _seed_options_via_api(conn, universe: set[str], trade_date: date) -> None:
    """
    Fallback: use the Massive (/v3/reference/options/contracts) REST API,
    throttled to 5 req/min, for the most recent trade date only.

    This endpoint returns contract reference data; pricing fields are NULL.
    Requires POLYGON_API_KEY env var.
    """
    if not POLYGON_API_KEY:
        log.warning("POLYGON_API_KEY not set; cannot use REST API fallback.")
        return

    already_covered = _options_ref_covered_underlyings(conn, universe)
    symbols_to_fetch = universe - already_covered
    if not symbols_to_fetch:
        log.info("options_reference: all %d underlyings already have active contracts; skipping API fetch.", len(universe))
        return
    if already_covered:
        log.info(
            "options_reference: %d/%d underlyings already covered; fetching %d remaining via API…",
            len(already_covered), len(universe), len(symbols_to_fetch),
        )
    else:
        log.info("API fallback: fetching options contracts from %s for %s…", POLYGON_API_BASE_URL, trade_date)

    ref_rows: list[tuple] = []
    pricing_rows: list[tuple] = []
    ts = datetime(trade_date.year, trade_date.month, trade_date.day)

    for i, sym in enumerate(sorted(symbols_to_fetch), 1):
        try:
            contracts = _iter_options_contracts(sym)
            for r in contracts:
                contract = r.get("ticker", "")
                if not contract:
                    continue
                option_type_raw = r.get("contract_type", "")
                option_type = option_type_raw[0].upper() if option_type_raw else None
                expiry_str = r.get("expiration_date", "")
                try:
                    expiry = date.fromisoformat(expiry_str)
                except (ValueError, TypeError):
                    expiry = None
                strike = _float(r.get("strike_price"))

                ref_rows.append((contract, sym, strike, expiry, option_type))
                # Reference endpoint carries no pricing; insert a NULL pricing row
                # so the contract is represented at this trade date.
                pricing_rows.append((
                    contract, ts, sym,
                    None, None, None, None, None, None,
                    None, None, None, None,
                ))
            if i % 25 == 0:
                log.info(
                    "  API fallback: %d/%d symbols, %d contracts so far",
                    i, len(symbols_to_fetch), len(ref_rows),
                )
        except Exception as exc:
            log.warning("  API fallback skipping %s: %s", sym, exc)

    _upsert_options(conn, ref_rows, pricing_rows)


def _upsert_options(conn, ref_rows: list[tuple], pricing_rows: list[tuple]) -> None:
    if ref_rows:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO options_reference (contract_symbol, underlying, strike, expiry, option_type)
                VALUES %s
                ON CONFLICT (contract_symbol) DO NOTHING
                """,
                ref_rows,
            )
        conn.commit()
        log.info("options_reference: inserted %d rows.", len(ref_rows))

    if pricing_rows:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO options_pricing
                    (contract_symbol, ts, underlying, bid, ask, last_price, volume,
                     open_interest, implied_volatility, delta, gamma, theta, vega)
                VALUES %s
                ON CONFLICT (contract_symbol, ts) DO NOTHING
                """,
                pricing_rows,
            )
        conn.commit()
        log.info("options_pricing: inserted %d rows.", len(pricing_rows))


def seed_options(conn, universe: set[str]) -> None:
    if not POLYGON_ACCESS_KEY_ID or not POLYGON_SECRET_ACCESS_KEY:
        log.warning("POLYGON_ACCESS_KEY_ID / POLYGON_SECRET_ACCESS_KEY not set; skipping options seeding.")
        return

    # Skip underlyings that already have active (non-expired) contracts
    already_covered = _options_ref_covered_underlyings(conn, universe)
    underlyings_to_load = universe - already_covered
    if not underlyings_to_load:
        log.info(
            "options_reference: all %d underlyings already have active contracts; skipping flat-file download.",
            len(universe),
        )
        return
    if already_covered:
        log.info(
            "options_reference: %d/%d underlyings already covered; loading %d remaining.",
            len(already_covered), len(universe), len(underlyings_to_load),
        )

    s3 = _polygon_s3_client()
    trading_days = _recent_trading_days(SEED_OPTIONS_DAYS)
    log.info("Seeding options for %d trading days: %s", len(trading_days), [str(d) for d in trading_days])

    flat_file_forbidden = False
    ref_rows: list[tuple] = []
    pricing_rows: list[tuple] = []

    for day in trading_days:
        if flat_file_forbidden:
            break  # will fall back to API below

        key = f"us_options_opra/day_aggs_v1/{day.year}/{day.month:02d}/{day}.csv.gz"
        log.info("  downloading s3://%s/%s", POLYGON_S3_BUCKET_NAME, key)
        try:
            resp = s3.get_object(Bucket=POLYGON_S3_BUCKET_NAME, Key=key)
            compressed = resp["Body"].read()
        except Exception as exc:
            # botocore wraps HTTP errors; check for 403 in the message/code
            exc_str = str(exc)
            if "403" in exc_str or "Forbidden" in exc_str or "AccessDenied" in exc_str:
                log.warning("  403 Forbidden on flat files — switching to REST API fallback.")
                flat_file_forbidden = True
                break
            log.warning("  could not download %s: %s", key, exc)
            continue

        day_ref, day_pricing = _parse_flat_file_rows(compressed, day, underlyings_to_load)
        ref_rows.extend(day_ref)
        pricing_rows.extend(day_pricing)
        log.info("  %s: accumulated %d contracts", day, len(ref_rows))

    if flat_file_forbidden:
        # Fall back to REST API for the single most recent trading day only
        most_recent = trading_days[0]
        _seed_options_via_api(conn, underlyings_to_load, most_recent)
    else:
        _upsert_options(conn, ref_rows, pricing_rows)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


# ── Main ──────────────────────────────────────────────────────────────────────

def _ensure_schema(conn) -> None:
    """Create the market_data schema and tables if they don't already exist."""
    ddl = """
    CREATE SCHEMA IF NOT EXISTS market_data;
    SET search_path TO market_data;

    CREATE TABLE IF NOT EXISTS equity_reference (
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

    CREATE TABLE IF NOT EXISTS equity_pricing (
        symbol    VARCHAR(20)      NOT NULL,
        ts        TIMESTAMPTZ      NOT NULL,
        open      DOUBLE PRECISION,
        high      DOUBLE PRECISION,
        low       DOUBLE PRECISION,
        close     DOUBLE PRECISION,
        volume    DOUBLE PRECISION,
        adj_close DOUBLE PRECISION,
        PRIMARY KEY (symbol, ts)
    );
    CREATE INDEX IF NOT EXISTS equity_pricing_symbol_ts_idx
        ON equity_pricing (symbol, ts DESC);

    CREATE TABLE IF NOT EXISTS options_reference (
        contract_symbol VARCHAR(30) PRIMARY KEY,
        underlying      VARCHAR(20)      NOT NULL,
        strike          DOUBLE PRECISION NOT NULL,
        expiry          DATE             NOT NULL,
        option_type     CHAR(1)          NOT NULL
    );
    CREATE INDEX IF NOT EXISTS options_reference_underlying_idx
        ON options_reference (underlying);

    CREATE TABLE IF NOT EXISTS options_pricing (
        contract_symbol    VARCHAR(30)      NOT NULL,
        ts                 TIMESTAMPTZ      NOT NULL,
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
    CREATE INDEX IF NOT EXISTS options_pricing_underlying_ts_idx
        ON options_pricing (underlying, ts DESC);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    log.info("Schema market_data: ensured schema and tables exist.")


def main() -> None:
    conn = wait_for_db()
    try:
        _ensure_schema(conn)
        symbols = get_symbols()
        symbol_set = set(symbols)

        seed_equity_reference(conn, symbols)
        seed_equity_pricing(conn, symbols)
        seed_options(conn, symbol_set)

        log.info("Seeding complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
