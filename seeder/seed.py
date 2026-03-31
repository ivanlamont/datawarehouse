"""
Seeder: fetches S&P 500 equity + options data and FX rates, writes to PostgreSQL.

Equity data: yfinance (free)
Options data: Polygon flat files via S3/boto3 (reference + pricing),
              yfinance (daily pricing snapshots — bid/ask/last/volume/OI/IV)
FX data: Tiingo REST API
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
DB_SSLMODE = os.environ.get("POSTGRES_SSLMODE", "prefer")

POLYGON_ACCESS_KEY_ID = os.environ.get("POLYGON_ACCESS_KEY_ID", "")
POLYGON_SECRET_ACCESS_KEY = os.environ.get("POLYGON_SECRET_ACCESS_KEY", "")

SEED_START_DATE = os.environ.get("SEED_START_DATE", "2023-01-01")
SEED_OPTIONS_DAYS = int(os.environ.get("SEED_OPTIONS_DAYS", 5))
SEED_SYMBOLS_ENV = os.environ.get("SEED_SYMBOLS", "")
# SP500 | RUSSELL2000 | BOTH  (default: SP500)
SEED_UNIVERSE = os.environ.get("SEED_UNIVERSE", "SP500").upper()
# How many days before equity_reference rows are considered stale (default: 1)
EQUITY_REF_TTL_DAYS = int(os.environ.get("EQUITY_REF_TTL_DAYS", 1))

# Tiingo FX
TIINGO_API_KEY = os.environ.get("TIINGO_API_KEY", "")
# Comma-separated currency pairs, e.g. "eurusd,gbpusd,usdjpy"
SEED_FX_PAIRS = os.environ.get(
    "SEED_FX_PAIRS",
    "eurusd,gbpusd,usdjpy,usdchf,usdcad,audusd,nzdusd",
)


# ── DB connection ─────────────────────────────────────────────────────────────

def wait_for_db() -> psycopg2.extensions.connection:
    """Retry until PostgreSQL accepts connections."""
    for attempt in range(30):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASS,
                sslmode=DB_SSLMODE,
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
        try:
            r2000 = _fetch_russell2000()
        except Exception as exc:
            log.warning("Russell 2000 fetch failed, falling back to S&P 500 only: %s", exc)
            return sp500
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
                    row["date"].date() if hasattr(row["date"], "date") else row["date"],
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
                day,
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
                    contract, trade_date, sym,
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


def _last_trading_day() -> date:
    """Return today if it's a weekday, otherwise the most recent Friday."""
    today = date.today()
    wd = today.weekday()  # Mon=0, Sun=6
    if wd == 5:  # Saturday
        return today - timedelta(days=1)
    elif wd == 6:  # Sunday
        return today - timedelta(days=2)
    return today


def _float(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


# ── Options pricing via yfinance ──────────────────────────────────────────

def _fetch_yf_options(sym: str, today_ts: date) -> tuple[list[tuple], list[tuple]]:
    """Fetch options chain for one symbol, returning (ref_rows, pricing_rows)."""
    ticker = yf.Ticker(sym)
    expirations = ticker.options  # tuple of date strings
    if not expirations:
        return [], []

    ref_rows: list[tuple] = []
    pricing_rows: list[tuple] = []

    for exp_str in expirations:
        try:
            chain = ticker.option_chain(exp_str)
        except Exception:
            continue

        exp_date = date.fromisoformat(exp_str)

        for opt_type, df in [("C", chain.calls), ("P", chain.puts)]:
            if df is None or df.empty:
                continue
            for _, row in df.iterrows():
                contract = row.get("contractSymbol", "")
                if not contract:
                    continue
                ref_rows.append((contract, sym, _float(row.get("strike")), exp_date, opt_type))
                pricing_rows.append((
                    contract, today_ts, sym,
                    _float(row.get("bid")), _float(row.get("ask")),
                    _float(row.get("lastPrice")), _float(row.get("volume")),
                    _float(row.get("openInterest")), _float(row.get("impliedVolatility")),
                    None, None, None, None,
                ))

    return ref_rows, pricing_rows


def _upsert_yf_options(conn, ref_rows: list[tuple], pricing_rows: list[tuple]) -> tuple[int, int]:
    """Upsert ref + pricing rows, returning counts inserted."""
    ref_count = 0
    pricing_count = 0
    if ref_rows:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO options_reference
                    (contract_symbol, underlying, strike, expiry, option_type)
                VALUES %s ON CONFLICT (contract_symbol) DO NOTHING
            """, ref_rows)
        conn.commit()
        ref_count = len(ref_rows)
    if pricing_rows:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO options_pricing
                    (contract_symbol, ts, underlying, bid, ask, last_price,
                     volume, open_interest, implied_volatility,
                     delta, gamma, theta, vega)
                VALUES %s
                ON CONFLICT (contract_symbol, ts) DO UPDATE SET
                    bid = EXCLUDED.bid, ask = EXCLUDED.ask,
                    last_price = EXCLUDED.last_price, volume = EXCLUDED.volume,
                    open_interest = EXCLUDED.open_interest,
                    implied_volatility = EXCLUDED.implied_volatility
            """, pricing_rows)
        conn.commit()
        pricing_count = len(pricing_rows)
    return ref_count, pricing_count


def seed_options_pricing_yfinance(conn, universe: set[str]) -> None:
    """
    Pull current options chains from yfinance for each underlying and upsert
    bid, ask, last_price, volume, open_interest, and implied_volatility into
    options_reference + options_pricing.

    yfinance only provides the *current* chain (no historical), so this
    accumulates daily snapshots over time as the cron job runs each day.
    """
    log.info("Seeding options pricing via yfinance for %d underlyings…", len(universe))
    total_ref = 0
    total_pricing = 0
    today_ts = _last_trading_day()

    for i, sym in enumerate(sorted(universe), 1):
        if i > 1:
            time.sleep(0.5)  # throttle to avoid yfinance rate limits
        try:
            ref_rows, pricing_rows = _fetch_yf_options(sym, today_ts)
            r, p = _upsert_yf_options(conn, ref_rows, pricing_rows)
            total_ref += r
            total_pricing += p

            if i % 25 == 0:
                log.info(
                    "  yfinance options: %d/%d underlyings, %d ref / %d pricing rows",
                    i, len(universe), total_ref, total_pricing,
                )
        except Exception as exc:
            conn.rollback()
            msg = str(exc)
            if "Too Many Requests" in msg or "Rate limited" in msg:
                log.warning("  yfinance rate limited at %s, backing off 30s…", sym)
                time.sleep(30)
                try:
                    ref_rows, pricing_rows = _fetch_yf_options(sym, today_ts)
                    r, p = _upsert_yf_options(conn, ref_rows, pricing_rows)
                    total_ref += r
                    total_pricing += p
                except Exception as retry_exc:
                    log.warning("  yfinance retry failed for %s: %s", sym, retry_exc)
                    conn.rollback()
            else:
                log.warning("  yfinance options skipping %s: %s", sym, exc)

    log.info(
        "yfinance options: upserted %d reference, %d pricing rows total.",
        total_ref, total_pricing,
    )


# ── FX rates (Tiingo) ─────────────────────────────────────────────────────

def seed_fx_rates(conn) -> None:
    """Fetch daily FX rates from the Tiingo API and upsert into fx_rates."""
    if not TIINGO_API_KEY:
        log.warning("TIINGO_API_KEY not set; skipping FX rate seeding.")
        return

    pairs = [p.strip().lower() for p in SEED_FX_PAIRS.split(",") if p.strip()]
    if not pairs:
        log.info("SEED_FX_PAIRS is empty; skipping FX rate seeding.")
        return

    log.info("Seeding FX rates for %d pairs from %s: %s", len(pairs), SEED_START_DATE, pairs)
    total = 0

    for pair in pairs:
        url = f"https://api.tiingo.com/tiingo/fx/{pair}/prices"
        params = {
            "startDate": SEED_START_DATE,
            "resampleFreq": "1day",
            "token": TIINGO_API_KEY,
        }
        try:
            resp = requests.get(url, params=params, timeout=60,
                                headers={"Content-Type": "application/json"})
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            log.warning("  FX skipping %s: %s", pair, exc)
            continue

        if not data:
            log.info("  FX %s: no data returned.", pair)
            continue

        pair_upper = pair.upper()
        rows = []
        for rec in data:
            ts = rec.get("date")
            if not ts:
                continue
            rows.append((
                pair_upper,
                ts,
                _float(rec.get("open")),
                _float(rec.get("high")),
                _float(rec.get("low")),
                _float(rec.get("close")),
            ))

        if rows:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO fx_rates (pair, ts, open, high, low, close)
                    VALUES %s
                    ON CONFLICT (pair, ts) DO UPDATE SET
                        open  = EXCLUDED.open,
                        high  = EXCLUDED.high,
                        low   = EXCLUDED.low,
                        close = EXCLUDED.close
                    """,
                    rows,
                )
            conn.commit()
            total += len(rows)
            log.info("  FX %s: upserted %d rows.", pair_upper, len(rows))

    log.info("fx_rates: upserted %d rows total.", total)


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
        ts        DATE             NOT NULL,
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
    CREATE INDEX IF NOT EXISTS options_pricing_underlying_ts_idx
        ON options_pricing (underlying, ts DESC);

    CREATE TABLE IF NOT EXISTS fx_rates (
        pair  VARCHAR(10)      NOT NULL,
        ts    TIMESTAMPTZ      NOT NULL,
        open  DOUBLE PRECISION,
        high  DOUBLE PRECISION,
        low   DOUBLE PRECISION,
        close DOUBLE PRECISION,
        PRIMARY KEY (pair, ts)
    );
    CREATE INDEX IF NOT EXISTS fx_rates_pair_ts_idx
        ON fx_rates (pair, ts DESC);
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

        steps: list[tuple[str, callable]] = [
            ("equity_reference", lambda: seed_equity_reference(conn, symbols)),
            ("equity_pricing", lambda: seed_equity_pricing(conn, symbols)),
            ("options", lambda: seed_options(conn, symbol_set)),
            ("options_pricing_yfinance", lambda: seed_options_pricing_yfinance(conn, symbol_set)),
            ("fx_rates", lambda: seed_fx_rates(conn)),
        ]
        failures = []
        for name, step in steps:
            try:
                step()
            except Exception:
                log.exception("Seed step '%s' failed; continuing with remaining steps.", name)
                failures.append(name)

        if failures:
            log.error("Seeding finished with failures: %s", ", ".join(failures))
            sys.exit(1)
        else:
            log.info("Seeding complete.")
    finally:
        conn.close()


SEED_LOOKBACK_DAYS = int(os.environ.get("SEED_LOOKBACK_DAYS", 7))


def _needs_seed(conn) -> tuple[bool, bool]:
    """Return (needs_equity, needs_options) flags."""
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH recent_weekdays AS (
                SELECT d::date AS trade_date
                FROM   generate_series(
                           CURRENT_DATE - %s,
                           CURRENT_DATE,
                           '1 day'::interval
                       ) AS d
                WHERE  EXTRACT(DOW FROM d) BETWEEN 1 AND 5  -- Mon=1 … Fri=5
            )
            SELECT rw.trade_date
            FROM   recent_weekdays rw
            LEFT JOIN (
                SELECT DISTINCT ts::date AS pricing_date
                FROM   market_data.equity_pricing
                WHERE  ts >= CURRENT_DATE - %s
            ) ep ON ep.pricing_date = rw.trade_date
            WHERE  ep.pricing_date IS NULL
            ORDER  BY rw.trade_date
            """,
            (SEED_LOOKBACK_DAYS, SEED_LOOKBACK_DAYS),
        )
        missing_equity = [row[0].isoformat() for row in cur.fetchall()]

        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM market_data.options_pricing
                WHERE  ts = %s
                LIMIT  1
            )
            """,
            (_last_trading_day(),),
        )
        has_today_options = cur.fetchone()[0]

    needs_equity = bool(missing_equity)
    needs_options = not has_today_options

    if needs_equity:
        log.info("Missing equity data for %d day(s) in the last %d: %s",
                 len(missing_equity), SEED_LOOKBACK_DAYS, missing_equity)
    if needs_options:
        log.info("No options pricing snapshot for today.")
    if not needs_equity and not needs_options:
        log.info("Equity data and today's options snapshot are up to date; skipping.")

    return needs_equity, needs_options


# ── Scheduling interval (seconds) ────────────────────────────────────────────
SEED_INTERVAL = int(os.environ.get("SEED_INTERVAL_SECONDS", 0))


def run_scheduler() -> None:
    """Run the seeder in a loop, checking every SEED_INTERVAL seconds."""
    log.info("Scheduler mode: checking every %d seconds.", SEED_INTERVAL)
    while True:
        conn = wait_for_db()
        try:
            _ensure_schema(conn)
            needs_equity, needs_options = _needs_seed(conn)
            if needs_equity:
                conn.close()
                main()
            elif needs_options:
                symbols = get_symbols()
                seed_options_pricing_yfinance(conn, set(symbols))
                conn.close()
            else:
                conn.close()
        except Exception:
            log.exception("Scheduler tick failed.")
            try:
                conn.close()
            except Exception:
                pass
        log.info("Next check in %d seconds (wall-clock aware).", SEED_INTERVAL)
        next_run = time.time() + SEED_INTERVAL
        while True:
            remaining = next_run - time.time()
            if remaining <= 0:
                break
            time.sleep(min(60, remaining))


if __name__ == "__main__":
    if SEED_INTERVAL > 0:
        run_scheduler()
    else:
        main()
