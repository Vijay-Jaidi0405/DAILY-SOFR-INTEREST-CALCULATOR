"""
database.py
SQLite database layer for the SOFR Interest Calculator.
Handles schema creation, all CRUD operations, and the
calculation engine for all three methods.
"""

import sqlite3
import math
from datetime import date, timedelta
from contextlib import contextmanager
from pathlib import Path

DB_PATH = Path(__file__).parent / "sofr_calculator.db"

# ---------------------------------------------------------------------------
# Connection helper
# ---------------------------------------------------------------------------

@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS deal_master (
    deal_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    deal_name           TEXT    NOT NULL,
    client_name         TEXT    NOT NULL,
    cusip               TEXT    NOT NULL UNIQUE,
    notional_amount     REAL    NOT NULL,
    spread              REAL    NOT NULL DEFAULT 0,
    accrual_day_basis   TEXT    NOT NULL DEFAULT 'Calendar Days'
                            CHECK(accrual_day_basis IN ('Calendar Days','Observation Period Days')),
    rate_type           TEXT    NOT NULL CHECK(rate_type IN ('SOFR','SOFR Index')),
    payment_frequency   TEXT    NOT NULL CHECK(payment_frequency IN ('Monthly','Quarterly')),
    observation_shift   TEXT    NOT NULL DEFAULT 'N' CHECK(observation_shift IN ('Y','N')),
    shifted_interest    TEXT    NOT NULL DEFAULT 'N' CHECK(shifted_interest IN ('Y','N')),
    payment_delay       TEXT    NOT NULL DEFAULT 'N' CHECK(payment_delay IN ('Y','N')),
    rounding_decimals   INTEGER NOT NULL DEFAULT 4 CHECK(rounding_decimals IN (4,5,6)),
    look_back_days      INTEGER NOT NULL DEFAULT 2,
    calculation_method  TEXT    NOT NULL CHECK(calculation_method IN (
                            'Compounded in Arrears',
                            'Simple Average in Arrears',
                            'SOFR Index')),
    first_payment_date  TEXT    NOT NULL,
    maturity_date       TEXT    NOT NULL,
    payment_delay_days  INTEGER NOT NULL DEFAULT 0,
    status              TEXT    NOT NULL DEFAULT 'Active' CHECK(status IN ('Active','Inactive','Matured')),
    created_at          TEXT    NOT NULL DEFAULT (datetime('now')),
    modified_at         TEXT    NOT NULL DEFAULT (datetime('now')),
    CHECK(NOT (rate_type='SOFR Index' AND calculation_method != 'SOFR Index')),
    CHECK(NOT (rate_type='SOFR'       AND calculation_method  = 'SOFR Index')),
    CHECK(NOT (shifted_interest='Y'   AND observation_shift   = 'N')),
    CHECK(maturity_date > first_payment_date)
);

-- SOFR daily overnight rate (from NY Fed SOFR page)
CREATE TABLE IF NOT EXISTS sofr_rates (
    rate_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    rate_date        TEXT    NOT NULL UNIQUE,
    sofr_rate        REAL    NOT NULL,
    day_count_factor INTEGER NOT NULL DEFAULT 1 CHECK(day_count_factor IN (1,2,3))
);

-- SOFR compounded index (from NY Fed SOFR Averages & Index page)
CREATE TABLE IF NOT EXISTS sofr_index (
    index_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    rate_date   TEXT    NOT NULL UNIQUE,
    sofr_index  REAL    NOT NULL
);

CREATE TABLE IF NOT EXISTS market_holidays (
    holiday_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    holiday_date    TEXT    NOT NULL UNIQUE,
    holiday_name    TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS payment_schedule (
    schedule_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    deal_id                 INTEGER NOT NULL REFERENCES deal_master(deal_id),
    cusip                   TEXT    NOT NULL,
    period_number           INTEGER NOT NULL,
    period_start_date       TEXT    NOT NULL,
    period_end_date         TEXT    NOT NULL,
    eff_period_start_date   TEXT    NOT NULL,
    eff_period_end_date     TEXT    NOT NULL,
    obs_start_date          TEXT    NOT NULL,
    obs_end_date            TEXT    NOT NULL,
    unadj_payment_date      TEXT    NOT NULL,
    adj_payment_date        TEXT    NOT NULL,
    accrual_days            INTEGER NOT NULL,
    notional_amount         REAL    NOT NULL,
    compounded_rate         REAL,
    annualized_rate         REAL,
    rounded_rate            REAL,
    interest_amount         REAL,
    period_status           TEXT    NOT NULL DEFAULT 'Scheduled'
                                CHECK(period_status IN ('Scheduled','Ready to Calculate','Calculated')),
    is_calc_eligible_today  INTEGER NOT NULL DEFAULT 0,
    obs_rates_available     INTEGER NOT NULL DEFAULT 0,
    period_ended_by_today   INTEGER NOT NULL DEFAULT 0,
    missing_rate_dates      TEXT,
    is_next_payment_period  INTEGER NOT NULL DEFAULT 0,
    rate_determination_date TEXT,
    generated_at            TEXT    NOT NULL DEFAULT (datetime('now')),
    status_refreshed_at     TEXT,
    calculated_at           TEXT,
    calculated_by           TEXT,
    UNIQUE(deal_id, period_number)
);

CREATE TABLE IF NOT EXISTS calculation_log (
    log_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    deal_id                 INTEGER NOT NULL REFERENCES deal_master(deal_id),
    cusip                   TEXT    NOT NULL,
    calculation_method      TEXT    NOT NULL,
    period_start_date       TEXT    NOT NULL,
    period_end_date         TEXT    NOT NULL,
    obs_start_date          TEXT,
    obs_end_date            TEXT,
    payment_date            TEXT    NOT NULL,
    adjusted_payment_date   TEXT    NOT NULL,
    payment_delay_days      INTEGER NOT NULL DEFAULT 0,
    accrual_days            INTEGER NOT NULL,
    day_count_basis         INTEGER NOT NULL DEFAULT 360,
    look_back_days          INTEGER NOT NULL,
    notional_amount         REAL    NOT NULL,
    compounded_rate         REAL,
    annualized_rate         REAL,
    rounded_rate            REAL,
    interest_amount         REAL    NOT NULL,
    batch_id                TEXT,
    calculated_at           TEXT    NOT NULL DEFAULT (datetime('now')),
    calculated_by           TEXT    NOT NULL DEFAULT 'system'
);

CREATE INDEX IF NOT EXISTS ix_sofr_rates_date      ON sofr_rates(rate_date);
CREATE INDEX IF NOT EXISTS ix_sofr_index_date      ON sofr_index(rate_date);
CREATE INDEX IF NOT EXISTS ix_schedule_cusip       ON payment_schedule(cusip);
CREATE INDEX IF NOT EXISTS ix_schedule_status      ON payment_schedule(period_status);
CREATE INDEX IF NOT EXISTS ix_schedule_eligible    ON payment_schedule(is_calc_eligible_today);
CREATE INDEX IF NOT EXISTS ix_schedule_next        ON payment_schedule(is_next_payment_period);
CREATE INDEX IF NOT EXISTS ix_schedule_rdd         ON payment_schedule(rate_determination_date);
CREATE INDEX IF NOT EXISTS ix_log_cusip            ON calculation_log(cusip);
CREATE INDEX IF NOT EXISTS ix_log_batch            ON calculation_log(batch_id);
CREATE INDEX IF NOT EXISTS ix_holidays_date        ON market_holidays(holiday_date);
"""

SEED_HOLIDAYS = [
    ("2024-01-01","New Year's Day"),("2024-01-15","MLK Day"),
    ("2024-02-19","Presidents Day"),("2024-05-27","Memorial Day"),
    ("2024-06-19","Juneteenth"),("2024-07-04","Independence Day"),
    ("2024-09-02","Labor Day"),("2024-11-28","Thanksgiving"),
    ("2024-12-25","Christmas"),
    ("2025-01-01","New Year's Day"),("2025-01-20","MLK Day"),
    ("2025-02-17","Presidents Day"),("2025-05-26","Memorial Day"),
    ("2025-06-19","Juneteenth"),("2025-07-04","Independence Day"),
    ("2025-09-01","Labor Day"),("2025-11-27","Thanksgiving"),
    ("2025-12-25","Christmas"),
    ("2026-01-01","New Year's Day"),("2026-01-19","MLK Day"),
    ("2026-02-16","Presidents Day"),("2026-05-25","Memorial Day"),
    ("2026-06-19","Juneteenth"),("2026-07-03","Independence Day Observed"),
    ("2026-09-07","Labor Day"),("2026-11-26","Thanksgiving"),
    ("2026-12-25","Christmas"),
    ("2027-01-01","New Year's Day"),("2027-01-18","MLK Day"),
    ("2027-02-15","Presidents Day"),("2027-05-31","Memorial Day"),
    ("2027-06-18","Juneteenth Observed"),("2027-07-05","Independence Day Observed"),
    ("2027-09-06","Labor Day"),("2027-11-25","Thanksgiving"),
    ("2027-12-24","Christmas Observed"),
    ("2028-01-03","New Year's Day Observed"),("2028-01-17","MLK Day"),
    ("2028-02-21","Presidents Day"),("2028-05-29","Memorial Day"),
    ("2028-06-19","Juneteenth"),("2028-07-04","Independence Day"),
    ("2028-09-04","Labor Day"),("2028-11-23","Thanksgiving"),
    ("2028-12-25","Christmas"),
]


# Pre-computed holiday set for connection-free date rolling
# (used by _nearest_prev_bday and _nearest_next_bday during schedule generation)
_HOLIDAY_SET: set = {
    date.fromisoformat(h[0]) for h in SEED_HOLIDAYS
}


def _update_holiday_set(conn) -> None:
    """Reload _HOLIDAY_SET from the database (call after importing new holidays)."""
    global _HOLIDAY_SET
    rows = conn.execute("SELECT holiday_date FROM market_holidays").fetchall()
    _HOLIDAY_SET = {date.fromisoformat(r["holiday_date"]) for r in rows}


def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        # Migration: add rate_determination_date if upgrading from older DB
        try:
            conn.execute("ALTER TABLE payment_schedule ADD COLUMN rate_determination_date TEXT")
        except Exception:
            pass  # column already exists

        # Migration: rename start_date -> first_payment_date, add payment_delay_days
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(deal_master)").fetchall()]
            if "start_date" in cols and "first_payment_date" not in cols:
                conn.executescript("""
                    ALTER TABLE deal_master ADD COLUMN first_payment_date TEXT;
                    UPDATE deal_master SET first_payment_date = start_date
                        WHERE first_payment_date IS NULL;
                """)
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE deal_master ADD COLUMN payment_delay_days INTEGER DEFAULT 0")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE deal_master ADD COLUMN spread REAL NOT NULL DEFAULT 0")
        except Exception:
            pass
        try:
            conn.execute(
                "ALTER TABLE deal_master ADD COLUMN accrual_day_basis "
                "TEXT NOT NULL DEFAULT 'Calendar Days'"
            )
        except Exception:
            pass

        # Migration: migrate sofr_index from old combined sofr_rates table
        # (if sofr_index column exists in sofr_rates, copy data and drop column)
        try:
            cols = [r[1] for r in conn.execute(
                "PRAGMA table_info(sofr_rates)").fetchall()]
            if "sofr_index" in cols:
                conn.execute("""
                    INSERT OR IGNORE INTO sofr_index (rate_date, sofr_index)
                    SELECT rate_date, sofr_index FROM sofr_rates
                    WHERE sofr_index IS NOT NULL
                """)
                # SQLite can't DROP COLUMN before 3.35; rebuild the table
                conn.executescript("""
                    CREATE TABLE IF NOT EXISTS sofr_rates_new (
                        rate_id          INTEGER PRIMARY KEY AUTOINCREMENT,
                        rate_date        TEXT    NOT NULL UNIQUE,
                        sofr_rate        REAL    NOT NULL,
                        day_count_factor INTEGER NOT NULL DEFAULT 1
                    );
                    INSERT OR IGNORE INTO sofr_rates_new
                        (rate_date, sofr_rate, day_count_factor)
                    SELECT rate_date, sofr_rate, day_count_factor
                    FROM sofr_rates;
                    DROP TABLE sofr_rates;
                    ALTER TABLE sofr_rates_new RENAME TO sofr_rates;
                    CREATE UNIQUE INDEX IF NOT EXISTS ix_sofr_rates_date
                        ON sofr_rates(rate_date);
                """)
        except Exception as e:
            pass  # already migrated or fresh DB
        conn.executemany(
            "INSERT OR IGNORE INTO market_holidays(holiday_date,holiday_name) VALUES(?,?)",
            SEED_HOLIDAYS
        )
        auto_mature_deals(conn)


# ---------------------------------------------------------------------------
# Date / business day helpers
# ---------------------------------------------------------------------------

def _is_business_day(conn, d: date) -> bool:
    dow = d.weekday()   # Mon=0 … Sun=6
    if dow >= 5:
        return False
    ds = d.isoformat()
    row = conn.execute(
        "SELECT 1 FROM market_holidays WHERE holiday_date=?", (ds,)
    ).fetchone()
    return row is None


def _next_business_day(conn, d: date) -> date:
    for _ in range(14):
        if _is_business_day(conn, d):
            return d
        d += timedelta(days=1)
    return d


def _shift_date_back(d: date, n: int) -> date:
    return d - timedelta(days=n)


def _nearest_rate_date(conn, d: date) -> date | None:
    """Nearest SOFR rate date on or before d (from sofr_rates table)."""
    if _is_good_friday(d):
        return _good_friday_lookup_date(conn, d, "sofr_rates")
    ds = d.isoformat()
    row = conn.execute(
        "SELECT rate_date FROM sofr_rates WHERE rate_date<=? ORDER BY rate_date DESC LIMIT 1",
        (ds,)
    ).fetchone()
    return date.fromisoformat(row["rate_date"]) if row else None


def _nearest_index_date(conn, d: date) -> date | None:
    """Nearest SOFR Index date on or before d (from sofr_index table)."""
    if _is_good_friday(d):
        return _good_friday_lookup_date(conn, d, "sofr_index")
    ds = d.isoformat()
    row = conn.execute(
        "SELECT rate_date FROM sofr_index WHERE rate_date<=? ORDER BY rate_date DESC LIMIT 1",
        (ds,)
    ).fetchone()
    return date.fromisoformat(row["rate_date"]) if row else None


def _accrual_days(start: date, end: date) -> int:
    return (end - start).days


def _deal_accrual_days(deal: dict, calendar_start: date, calendar_end: date,
                       obs_start: date, obs_end: date) -> int:
    basis = deal.get("accrual_day_basis") or "Calendar Days"
    if basis == "Observation Period Days":
        return _accrual_days(obs_start, obs_end)
    return _accrual_days(calendar_start, calendar_end)


def _iter_business_days(start: date, end: date):
    """Yield (calendar_date, day_weight) for each business day in [start, end)."""
    d = start
    while d < end:
        dow = d.weekday()
        if dow < 5:
            wt = 3 if dow == 4 else 1   # Friday=3, else 1
            yield d, wt
        d += timedelta(days=1)


def _is_good_friday(d: date) -> bool:
    """
    Returns True if date d is Good Friday (Friday before Easter Sunday).
    Uses the Anonymous Gregorian algorithm to compute Easter.
    """
    y = d.year
    a = y % 19
    b = y // 100
    c = y % 100
    d_ = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d_ - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day   = ((h + l - 7 * m + 114) % 31) + 1
    easter = date(y, month, day)
    good_friday = easter - timedelta(days=2)
    return d == good_friday


def _good_friday_lookup_date(conn, gf_date: date, table: str) -> date | None:
    """
    For Good Friday use Thursday's publication if available, otherwise
    Wednesday's publication. If both are unavailable, fall back to the
    latest available date on or before Wednesday.
    """
    thursday = gf_date - timedelta(days=1)
    wednesday = gf_date - timedelta(days=2)

    for candidate in (thursday, wednesday):
        row = conn.execute(
            f"SELECT rate_date FROM {table} WHERE rate_date=?",
            (candidate.isoformat(),)
        ).fetchone()
        if row:
            return date.fromisoformat(row["rate_date"])

    row = conn.execute(
        f"SELECT rate_date FROM {table} WHERE rate_date<=? ORDER BY rate_date DESC LIMIT 1",
        (wednesday.isoformat(),)
    ).fetchone()
    return date.fromisoformat(row["rate_date"]) if row else None


def _get_rate(conn, d: date):
    """
    Fetch SOFR rate row for observation date d.
    If d is Good Friday, use Thursday's rate if available, otherwise
    Wednesday's rate. Otherwise return nearest available rate on or before d.
    """
    # Check if this exact date already has a stored publication
    existing = conn.execute(
        "SELECT sofr_rate FROM sofr_rates WHERE rate_date=?",
        (d.isoformat(),)
    ).fetchone()

    if existing:
        return existing

    if _is_good_friday(d):
        lookup_date = _good_friday_lookup_date(conn, d, "sofr_rates")
        if lookup_date is not None:
            return conn.execute(
                "SELECT sofr_rate FROM sofr_rates WHERE rate_date=?",
                (lookup_date.isoformat(),)
            ).fetchone()

    # Normal fallback: nearest rate on or before d
    nd = _nearest_rate_date(conn, d)
    if nd is None:
        return None
    row = conn.execute(
        "SELECT sofr_rate FROM sofr_rates WHERE rate_date=?",
        (nd.isoformat(),)
    ).fetchone()
    return row


def _check_obs_rates_available(conn, obs_start: date, obs_end: date,
                               is_index: bool = False) -> bool:
    """Check whether all required rates are loaded for the observation window."""
    if is_index:
        # For SOFR Index deals: check sofr_index table
        max_row = conn.execute("SELECT MAX(rate_date) AS mx FROM sofr_index").fetchone()
        if not max_row or not max_row["mx"]:
            return False
        max_date = date.fromisoformat(max_row["mx"])
        if max_date < obs_end:
            return False
        r1 = _nearest_index_date(conn, obs_start)
        r2 = _nearest_index_date(conn, obs_end)
        return r1 is not None and r2 is not None
    else:
        # For SOFR rate deals: check sofr_rates table
        max_row = conn.execute("SELECT MAX(rate_date) AS mx FROM sofr_rates").fetchone()
        if not max_row or not max_row["mx"]:
            return False
        max_rate = date.fromisoformat(max_row["mx"])
        if max_rate < obs_end:
            return False
        r1 = _nearest_rate_date(conn, obs_start)
        r2 = _nearest_rate_date(conn, obs_end)
        return r1 is not None and r2 is not None


# ---------------------------------------------------------------------------
# Calculation engine
# All three methods return a daily_rows list for the breakdown table:
#   Compounded:    [{date, obs_date, rate, day_weight, is_business_day,
#                   is_good_friday, daily_factor, running_product}]
#   Simple Avg:    [{date, obs_date, rate, day_weight, is_business_day,
#                   is_good_friday, weighted_rate}]
#   Index:         [{label, date, sofr_index}]  (just start/end rows)
# ---------------------------------------------------------------------------

def _calc_compounded(conn, deal: dict, p_start: date, p_end: date,
                     dc: int = 360):
    lb = deal["look_back_days"]
    if deal["shifted_interest"] == "Y":
        eff_start = _shift_date_back(p_start, lb)
        eff_end   = _shift_date_back(p_end,   lb)
    else:
        eff_start, eff_end = p_start, p_end

    obs_start = _shift_date_back(eff_start, lb)
    obs_end   = _shift_date_back(eff_end,   lb)
    accrual   = _deal_accrual_days(deal, eff_start, eff_end, obs_start, obs_end)

    product    = 1.0
    daily_rows = []

    for cal_date, wt in _iter_business_days(eff_start, eff_end):
        obs_date  = _shift_date_back(cal_date, lb)
        is_gf     = _is_good_friday(obs_date)
        row       = _get_rate(conn, obs_date)
        if row:
            sofr_r       = row["sofr_rate"] if hasattr(row, "sofr_rate") else row["sofr_rate"]
            daily_factor = 1.0 + (sofr_r / 100.0) * wt / dc
            product     *= daily_factor
            daily_rows.append({
                "date":           cal_date.isoformat(),
                "obs_date":       obs_date.isoformat(),
                "sofr_rate":      sofr_r,
                "day_weight":     wt,
                "is_good_friday": is_gf,
                "daily_factor":   daily_factor,
                "running_product": product,
            })

    comp_rate = product - 1.0
    ann_rate  = comp_rate * (dc / accrual) if accrual else 0.0
    interest  = deal["notional_amount"] * comp_rate
    return comp_rate, ann_rate, interest, obs_start, obs_end, accrual, daily_rows


def _calc_simple_average(conn, deal: dict, p_start: date, p_end: date,
                         dc: int = 360):
    lb        = deal["look_back_days"]
    obs_start = _shift_date_back(p_start, lb)
    obs_end   = _shift_date_back(p_end,   lb)
    accrual   = _deal_accrual_days(deal, p_start, p_end, obs_start, obs_end)

    sum_w, sum_d = 0.0, 0
    daily_rows   = []

    for cal_date, wt in _iter_business_days(p_start, p_end):
        obs_date = _shift_date_back(cal_date, lb)
        is_gf    = _is_good_friday(obs_date)
        row      = _get_rate(conn, obs_date)
        if row:
            sofr_r    = row["sofr_rate"] if hasattr(row, "sofr_rate") else row["sofr_rate"]
            sum_w    += sofr_r * wt
            sum_d    += wt
            daily_rows.append({
                "date":           cal_date.isoformat(),
                "obs_date":       obs_date.isoformat(),
                "sofr_rate":      sofr_r,
                "day_weight":     wt,
                "is_good_friday": is_gf,
                "weighted_rate":  sofr_r * wt,
                "is_business_day": True,
            })

    avg_rate = (sum_w / sum_d / 100.0) if sum_d else 0.0
    interest = deal["notional_amount"] * avg_rate * accrual / dc
    return avg_rate, avg_rate, interest, obs_start, obs_end, accrual, daily_rows


def _calc_index(conn, deal: dict, p_start: date, p_end: date,
                dc: int = 360):
    """
    SOFR Index calculation per ARRC/ISDA convention.

    Formula:
        Rate = (Index_end / Index_start - 1) x (DC / d)

    Where:
        Index_start = published SOFR Index on the observation start date
                      = nearest index date on or before
                        (p_start shifted back by look_back_days)
        Index_end   = published SOFR Index on the observation end date
                      = nearest index date on or before
                        (p_end shifted back by look_back_days)
        d           = calendar days from p_start to p_end
                      (p_end is the last accrual day; payment date excluded)
        DC          = day count basis (360 or 365)

    Note: For SOFR Index deals the observation shift convention means
    the index is looked up look_back_days BEFORE the period boundary.
    """
    lb = deal["look_back_days"]

    # Observation dates: period boundaries shifted back by lookback
    obs_start_raw = _shift_date_back(p_start, lb)
    obs_end_raw   = _shift_date_back(p_end,   lb)

    # Find nearest published index dates on or before each obs date
    obs_start_d = _nearest_index_date(conn, obs_start_raw)
    obs_end_d   = _nearest_index_date(conn, obs_end_raw)

    if obs_start_d is None or obs_end_d is None:
        raise ValueError(
            "SOFR Index values not available for the observation window. "
            f"Need index on/before {obs_start_raw} and {obs_end_raw}. "
            "Import the SOFR Averages & Index file first."
        )

    if obs_start_d == obs_end_d:
        raise ValueError(
            f"Observation start and end resolve to the same index date "
            f"({obs_start_d}). Period may be too short or index data missing."
        )

    # Fetch index values
    r1_idx = conn.execute(
        "SELECT sofr_index FROM sofr_index WHERE rate_date=?",
        (obs_start_d.isoformat(),)
    ).fetchone()
    r2_idx = conn.execute(
        "SELECT sofr_index FROM sofr_index WHERE rate_date=?",
        (obs_end_d.isoformat(),)
    ).fetchone()

    if not r1_idx or not r2_idx:
        raise ValueError(
            "SOFR Index rows missing for the resolved observation dates "
            f"({obs_start_d}, {obs_end_d})."
        )

    idx_start = r1_idx["sofr_index"]
    idx_end   = r2_idx["sofr_index"]

    # Fetch rates for display (best-effort; not used in calculation)
    def _rate_on(d):
        row = conn.execute(
            "SELECT sofr_rate FROM sofr_rates WHERE rate_date<=? "
            "ORDER BY rate_date DESC LIMIT 1",
            (d.isoformat(),)
        ).fetchone()
        return row["sofr_rate"] if row else None

    # Accrual days may be based on period dates or observation dates per deal setup.
    accrual = _deal_accrual_days(deal, p_start, p_end, obs_start_d, obs_end_d)

    if accrual <= 0:
        raise ValueError(
            f"Accrual days = {accrual}. "
            f"Period start {p_start} must be before period end {p_end}."
        )

    # Core formula: (Index_end / Index_start - 1) x (DC / accrual_days)
    index_return = idx_end / idx_start - 1.0
    ann_rate     = index_return * (dc / accrual)
    # Interest = Notional x (Index_end/Index_start - 1) x (Accrual Days / DC)
    interest     = deal["notional_amount"] * index_return * accrual / dc

    daily_rows = [
        {
            "label":      "Observation Start",
            "date":       obs_start_d.isoformat(),
            "sofr_rate":  _rate_on(obs_start_d),
            "sofr_index": idx_start,
        },
        {
            "label":      "Observation End",
            "date":       obs_end_d.isoformat(),
            "sofr_rate":  _rate_on(obs_end_d),
            "sofr_index": idx_end,
        },
    ]

    return (index_return, ann_rate, interest,
            obs_start_d, obs_end_d, accrual,
            daily_rows,
            idx_start, idx_end)


def _apply_spread(deal: dict, period_rate: float, annual_rate: float,
                  accrual_days: int, dc: int) -> tuple[float, float, float, float, float]:
    """
    Apply spread as an annualized percentage margin.

    Example: spread=1.25 means 1.25%, so 0.0125 is added to the annualized
    benchmark rate and prorated across the accrual period for interest.
    """
    spread_pct = float(deal.get("spread") or 0.0)
    spread_rate = spread_pct / 100.0
    spread_period_rate = spread_rate * (accrual_days / dc) if accrual_days else 0.0
    all_in_period_rate = period_rate + spread_period_rate
    all_in_annual_rate = annual_rate + spread_rate
    interest_amount = deal["notional_amount"] * all_in_period_rate
    return spread_pct, spread_rate, all_in_period_rate, all_in_annual_rate, interest_amount


def _calculate_interest_for_deal(conn, deal: dict, cusip: str,
                                 p_start: date, p_end: date,
                                 payment_date: date, delay_days: int = 0,
                                 dc: int = 360, log: bool = True,
                                 batch_id: str | None = None) -> dict:
    method = deal["calculation_method"]

    index_start = index_end = None

    if method == "Compounded in Arrears":
        cr, ar, ia, obs_s, obs_e, acc, daily_rows =             _calc_compounded(conn, deal, p_start, p_end, dc)
    elif method == "Simple Average in Arrears":
        cr, ar, ia, obs_s, obs_e, acc, daily_rows =             _calc_simple_average(conn, deal, p_start, p_end, dc)
    elif method == "SOFR Index":
        cr, ar, ia, obs_s, obs_e, acc, daily_rows, index_start, index_end =             _calc_index(conn, deal, p_start, p_end, dc)
    else:
        raise ValueError(f"Unknown method: {method}")

    spread_pct, spread_rate, all_in_period_rate, all_in_annual_rate, ia = (
        _apply_spread(deal, cr, ar, acc, dc)
    )

    actual_delay = delay_days if deal["payment_delay"] == "Y" else 0
    raw_pay      = payment_date + timedelta(days=actual_delay)
    adj_pay      = _next_business_day(conn, raw_pay)
    rounded_rate = round(all_in_annual_rate, deal["rounding_decimals"])

    result = {
        "cusip":               cusip,
        "deal_name":           deal["deal_name"],
        "client_name":         deal["client_name"],
        "notional_amount":     deal["notional_amount"],
        "spread":              spread_pct,
        "spread_rate":         spread_rate,
        "accrual_day_basis":   deal.get("accrual_day_basis") or "Calendar Days",
        "rate_type":           deal["rate_type"],
        "payment_frequency":   deal["payment_frequency"],
        "calculation_method":  method,
        "observation_shift":   deal["observation_shift"],
        "shifted_interest":    deal["shifted_interest"],
        "payment_delay_flag":  deal["payment_delay"],
        "look_back_days":      deal["look_back_days"],
        "rounding_decimals":   deal["rounding_decimals"],
        "period_start_date":   p_start,
        "period_end_date":     p_end,
        "obs_start_date":      obs_s,
        "obs_end_date":        obs_e,
        "accrual_days":        acc,
        "day_count_basis":     dc,
        "benchmark_period_rate": cr,
        "benchmark_annualized_rate": ar,
        "all_in_period_rate":  all_in_period_rate,
        "compounded_rate":     cr,
        "annualized_rate":     all_in_annual_rate,
        "rounded_rate":        rounded_rate,
        "interest_amount":     ia,
        "payment_date":        payment_date,
        "adjusted_payment_date": adj_pay,
        "payment_delay_days":  actual_delay,
        "daily_rows":          daily_rows,      # breakdown table
        "index_start":         index_start,     # SOFR Index only
        "index_end":           index_end,       # SOFR Index only
    }

    if log:
        conn.execute("""
            INSERT INTO calculation_log(
                deal_id, cusip, calculation_method,
                period_start_date, period_end_date,
                obs_start_date, obs_end_date,
                payment_date, adjusted_payment_date, payment_delay_days,
                accrual_days, day_count_basis, look_back_days,
                notional_amount, compounded_rate, annualized_rate,
                rounded_rate, interest_amount, batch_id
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            deal["deal_id"], cusip, method,
            p_start.isoformat(), p_end.isoformat(),
            obs_s.isoformat() if obs_s else None,
            obs_e.isoformat() if obs_e else None,
            payment_date.isoformat(), adj_pay.isoformat(), actual_delay,
            acc, dc, deal["look_back_days"],
            deal["notional_amount"], cr, all_in_annual_rate, rounded_rate, ia, batch_id
        ))

    return result


def calculate_interest(conn, cusip: str, p_start: date, p_end: date,
                       payment_date: date, delay_days: int = 0,
                       dc: int = 360, log: bool = True,
                       batch_id: str | None = None) -> dict:
    deal = conn.execute(
        "SELECT * FROM deal_master WHERE cusip=? AND status='Active'", (cusip,)
    ).fetchone()
    if not deal:
        raise ValueError(f"No active deal found for CUSIP {cusip}")

    return _calculate_interest_for_deal(
        conn, dict(deal), cusip, p_start, p_end,
        payment_date, delay_days=delay_days, dc=dc,
        log=log, batch_id=batch_id
    )


# ---------------------------------------------------------------------------
# Payment schedule generation
# ---------------------------------------------------------------------------

def _add_months(d: date, months: int) -> date:
    """
    Add an exact number of months to d, preserving the day-of-month.
    If the resulting month is shorter (e.g. Jan 31 + 1 month),
    clamp to the last day of that month.
    """
    import calendar as _cal
    total_months = d.year * 12 + (d.month - 1) + months
    y = total_months // 12
    m = total_months % 12 + 1
    last_day = _cal.monthrange(y, m)[1]
    day = min(d.day, last_day)
    return date(y, m, day)


def _nearest_prev_bday(d: date) -> date:
    """Roll backward to the nearest business day (Mon-Fri, non-holiday)."""
    while d.weekday() >= 5 or d in _HOLIDAY_SET:
        d -= timedelta(days=1)
    return d


def _nearest_next_bday(d: date) -> date:
    """Roll forward to the nearest business day (Mon-Fri, non-holiday)."""
    while d.weekday() >= 5 or d in _HOLIDAY_SET:
        d += timedelta(days=1)
    return d


def _gen_periods(deal_start: date, maturity: date, freq: str,
                 delay_days: int = 0):
    """
    Generate (period_number, period_start, period_end, payment_date) tuples.

    Convention (ISDA/ARRC):
      payment_date[N] = next_bday( deal_start + N*months + delay_days )
      period_start[N] = payment_date[N-1]   (= deal_start for period 1)
      period_end[N]   = prev_bday( payment_date[N] - 1 day )
                        *** payment_date is EXCLUDED from the accrual period ***

    All payment dates and period dates are business days.
    The last period ends on or before maturity (also a business day).
    """
    months        = 1 if freq == "Monthly" else 3
    maturity_bday = _nearest_next_bday(maturity)

    # Period 1 starts one period BEFORE the first payment date
    # e.g. first_payment_date=09-Apr-2024, Quarterly -> period 1 start = 09-Jan-2024
    raw_p1_start = _add_months(deal_start, -months)
    p1_start     = _nearest_next_bday(raw_p1_start)

    periods  = []
    prev_pay = p1_start   # period 1 starts here
    num      = 1

    while prev_pay < maturity_bday:
        # Payment date N = first_payment_date + (N-1) * months + delay
        raw_pay  = _add_months(deal_start, months * (num - 1)) + timedelta(days=delay_days)
        pay_date = _nearest_next_bday(raw_pay)

        # Cap at maturity
        if pay_date > maturity_bday:
            pay_date = maturity_bday

        # Period start = previous payment date (deal_start for period 1)
        p_start = prev_pay

        # Period end = last business day BEFORE payment date
        p_end = _nearest_prev_bday(pay_date - timedelta(days=1))

        # Guard: end must be strictly after start
        if p_end <= p_start:
            p_end = _nearest_next_bday(p_start + timedelta(days=1))

        periods.append((num, p_start, p_end, pay_date))

        if pay_date >= maturity_bday:
            break

        prev_pay = pay_date
        num += 1

    return periods


def generate_schedule(conn, cusip: str, rebuild: bool = True):
    deal = conn.execute(
        "SELECT * FROM deal_master WHERE cusip=? AND status='Active'", (cusip,)
    ).fetchone()
    if not deal:
        raise ValueError(f"No active deal for CUSIP {cusip}")
    deal = dict(deal)

    if not rebuild:
        ex = conn.execute(
            "SELECT 1 FROM payment_schedule WHERE deal_id=?", (deal["deal_id"],)
        ).fetchone()
        if ex:
            return

    conn.execute("DELETE FROM payment_schedule WHERE deal_id=?", (deal["deal_id"],))

    lb      = deal["look_back_days"]
    delay   = int(deal.get("payment_delay_days") or 0) if deal["payment_delay"] == "Y" else 0
    si      = deal["shifted_interest"] == "Y"
    # first_payment_date is the anchor for all period calculations
    fpd     = date.fromisoformat(
                  deal.get("first_payment_date") or deal.get("start_date")
              )
    d_mat   = date.fromisoformat(deal["maturity_date"])

    # Pass first_payment_date and delay into _gen_periods
    periods  = _gen_periods(fpd, d_mat, deal["payment_frequency"],
                            delay_days=delay)
    is_index = deal["calculation_method"] == "SOFR Index"
    rows = []
    for num, ps, pe, pay_date in periods:
        # ps  = period start (= previous payment date, deal_start for period 1)
        # pe  = period end   (= prev bday before pay_date; payment EXCLUDED)
        # pay_date = adjusted payment date for this period

        # Shifted-interest: effective accrual period shifted back by lookback
        if si:
            eff_ps = _nearest_next_bday(_shift_date_back(ps, lb))
            eff_pe = _nearest_next_bday(_shift_date_back(pe, lb))
        else:
            eff_ps = ps
            eff_pe = pe

        # Observation window = effective dates shifted back by lookback.
        obs_s = _nearest_next_bday(_shift_date_back(eff_ps, lb))
        obs_e = _nearest_next_bday(_shift_date_back(eff_pe, lb))

        acc   = _deal_accrual_days(deal, eff_ps, eff_pe, obs_s, obs_e)
        unadj = pay_date   # payment date already adjusted in _gen_periods
        adj   = pay_date

        # Rate Determination Date:
        #   SOFR Index            -> obs_end_date
        #   Compounded/Simple Avg -> next bday after obs_end_date
        if is_index:
            rdd = _nearest_next_bday(obs_e)
        else:
            rdd = _nearest_next_bday(obs_e + timedelta(days=1))

        rows.append((
            deal["deal_id"], cusip, num,
            ps.isoformat(), pe.isoformat(),
            eff_ps.isoformat(), eff_pe.isoformat(),
            obs_s.isoformat(), obs_e.isoformat(),
            unadj.isoformat(), adj.isoformat(),
            acc, deal["notional_amount"],
            rdd.isoformat()
        ))

    conn.executemany("""
        INSERT INTO payment_schedule(
            deal_id, cusip, period_number,
            period_start_date, period_end_date,
            eff_period_start_date, eff_period_end_date,
            obs_start_date, obs_end_date,
            unadj_payment_date, adj_payment_date,
            accrual_days, notional_amount,
            rate_determination_date
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)

    refresh_schedule_status(conn, cusip)


def refresh_schedule_accruals(conn, cusip: str) -> int:
    deal = conn.execute(
        "SELECT * FROM deal_master WHERE cusip=?", (cusip,)
    ).fetchone()
    if not deal:
        return 0
    deal = dict(deal)

    rows = conn.execute("""
        SELECT schedule_id, period_start_date, period_end_date,
               eff_period_start_date, eff_period_end_date,
               obs_start_date, obs_end_date
        FROM payment_schedule
        WHERE cusip=?
        ORDER BY period_number
    """, (cusip,)).fetchall()

    updated = 0
    for row in rows:
        eff_ps = date.fromisoformat(row["eff_period_start_date"])
        eff_pe = date.fromisoformat(row["eff_period_end_date"])
        obs_s = date.fromisoformat(row["obs_start_date"])
        obs_e = date.fromisoformat(row["obs_end_date"])
        accrual = _deal_accrual_days(deal, eff_ps, eff_pe, obs_s, obs_e)
        conn.execute(
            "UPDATE payment_schedule SET accrual_days=? WHERE schedule_id=?",
            (accrual, row["schedule_id"])
        )
        updated += 1
    return updated


def generate_all_schedules(conn, rebuild: bool = True,
                           progress_cb=None):
    cusips = [r["cusip"] for r in conn.execute(
        "SELECT cusip FROM deal_master WHERE status='Active'"
    ).fetchall()]
    total = len(cusips)
    for i, cusip in enumerate(cusips):
        try:
            generate_schedule(conn, cusip, rebuild)
        except Exception as e:
            print(f"Schedule error {cusip}: {e}")
        if progress_cb:
            progress_cb(i + 1, total)


def refresh_schedule_status(conn, cusip: str | None = None):
    today = date.today()
    td    = today.isoformat()

    where = "AND ps.cusip=?" if cusip else ""
    args  = (cusip,) if cusip else ()

    rows = conn.execute(
        f"""SELECT ps.*, d.calculation_method
            FROM payment_schedule ps
            JOIN deal_master d ON d.deal_id = ps.deal_id
            WHERE ps.period_status != 'Calculated' {where}""",
        args
    ).fetchall()

    for row in rows:
        obs_s    = date.fromisoformat(row["obs_start_date"])
        obs_e    = date.fromisoformat(row["obs_end_date"])
        eff_e    = date.fromisoformat(row["eff_period_end_date"])
        is_index = row["calculation_method"] == "SOFR Index"

        ended    = eff_e <= today
        rates_ok = _check_obs_rates_available(conn, obs_s, obs_e, is_index=is_index)
        eligible = ended and rates_ok

        status = "Ready to Calculate" if eligible else "Scheduled"

        missing = None
        if ended and not rates_ok:
            missing = f"{obs_s} to {obs_e}"

        conn.execute("""
            UPDATE payment_schedule SET
                period_ended_by_today  = ?,
                obs_rates_available    = ?,
                is_calc_eligible_today = ?,
                period_status          = ?,
                missing_rate_dates     = ?,
                status_refreshed_at    = datetime('now')
            WHERE schedule_id = ?
        """, (int(ended), int(rates_ok), int(eligible),
              status, missing, row["schedule_id"]))

    # Reset next-payment flags
    where_bare = "AND cusip=?" if cusip else ""
    conn.execute(
        f"UPDATE payment_schedule SET is_next_payment_period=0 {where_bare}", args
    )

    # Set next-payment flag per deal
    sub = conn.execute(f"""
        SELECT deal_id, MIN(schedule_id) AS min_id
        FROM payment_schedule
        WHERE adj_payment_date >= ? AND period_status != 'Calculated' {where_bare}
        GROUP BY deal_id
    """, (td, *args)).fetchall()

    for r in sub:
        conn.execute(
            "UPDATE payment_schedule SET is_next_payment_period=1 WHERE schedule_id=?",
            (r["min_id"],)
        )


def mark_period_calculated(conn, cusip: str, period_number: int,
                           comp_rate: float, ann_rate: float,
                           rounded_rate: float, interest: float):
    conn.execute("""
        UPDATE payment_schedule SET
            period_status       = 'Calculated',
            is_calc_eligible_today = 0,
            compounded_rate     = ?,
            annualized_rate     = ?,
            rounded_rate        = ?,
            interest_amount     = ?,
            calculated_at       = datetime('now'),
            calculated_by       = 'user'
        WHERE cusip=? AND period_number=?
    """, (comp_rate, ann_rate, rounded_rate, interest, cusip, period_number))
    refresh_schedule_status(conn, cusip)


def recalculate_existing_results(conn, cusip: str) -> dict:
    deal = conn.execute(
        "SELECT * FROM deal_master WHERE cusip=?", (cusip,)
    ).fetchone()
    if not deal:
        raise ValueError(f"No deal found for CUSIP {cusip}")
    deal = dict(deal)

    schedule_rows = conn.execute("""
        SELECT schedule_id, period_start_date, period_end_date, adj_payment_date
        FROM payment_schedule
        WHERE cusip=? AND period_status='Calculated'
        ORDER BY period_number
    """, (cusip,)).fetchall()

    schedule_updated = 0
    for row in schedule_rows:
        res = _calculate_interest_for_deal(
            conn, deal, cusip,
            date.fromisoformat(row["period_start_date"]),
            date.fromisoformat(row["period_end_date"]),
            date.fromisoformat(row["adj_payment_date"]),
            delay_days=0, dc=360, log=False
        )
        conn.execute("""
            UPDATE payment_schedule SET
                compounded_rate = ?,
                annualized_rate = ?,
                rounded_rate    = ?,
                interest_amount = ?
            WHERE schedule_id = ?
        """, (
            res["compounded_rate"], res["annualized_rate"],
            res["rounded_rate"], res["interest_amount"],
            row["schedule_id"]
        ))
        schedule_updated += 1

    log_rows = conn.execute("""
        SELECT log_id, period_start_date, period_end_date,
               payment_date, payment_delay_days, day_count_basis
        FROM calculation_log
        WHERE cusip=?
        ORDER BY log_id
    """, (cusip,)).fetchall()

    log_updated = 0
    for row in log_rows:
        res = _calculate_interest_for_deal(
            conn, deal, cusip,
            date.fromisoformat(row["period_start_date"]),
            date.fromisoformat(row["period_end_date"]),
            date.fromisoformat(row["payment_date"]),
            delay_days=int(row["payment_delay_days"] or 0),
            dc=int(row["day_count_basis"] or 360),
            log=False
        )
        conn.execute("""
            UPDATE calculation_log SET
                compounded_rate = ?,
                annualized_rate = ?,
                rounded_rate    = ?,
                interest_amount = ?
            WHERE log_id = ?
        """, (
            res["compounded_rate"], res["annualized_rate"],
            res["rounded_rate"], res["interest_amount"],
            row["log_id"]
        ))
        log_updated += 1

    if schedule_updated:
        refresh_schedule_status(conn, cusip)

    return {
        "schedule_rows_updated": schedule_updated,
        "log_rows_updated": log_updated,
    }


# ---------------------------------------------------------------------------
# Deal CRUD
# ---------------------------------------------------------------------------

def get_all_deals(conn, status="Active"):
    q = "SELECT * FROM deal_master"
    args = ()
    if status:
        q += " WHERE status=?"
        args = (status,)
    q += " ORDER BY deal_name"
    return [dict(r) for r in conn.execute(q, args).fetchall()]


def get_deal(conn, cusip: str):
    r = conn.execute("SELECT * FROM deal_master WHERE cusip=?", (cusip,)).fetchone()
    return dict(r) if r else None


def insert_deal(conn, d: dict):
    d = enforce_frequency(d)
    # Support both old start_date and new first_payment_date key names
    fpd = d.get("first_payment_date") or d.get("start_date")
    conn.execute("""
        INSERT INTO deal_master(
            deal_name, client_name, cusip, notional_amount, spread, accrual_day_basis,
            rate_type, payment_frequency, observation_shift,
            shifted_interest, payment_delay, payment_delay_days,
            rounding_decimals, look_back_days, calculation_method,
            first_payment_date, maturity_date, status
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        d["deal_name"], d["client_name"], d["cusip"], d["notional_amount"],
        float(d.get("spread") or 0),
        d.get("accrual_day_basis", "Calendar Days"),
        d["rate_type"], d["payment_frequency"], d["observation_shift"],
        d["shifted_interest"], d["payment_delay"],
        int(d.get("payment_delay_days") or 0),
        d["rounding_decimals"], d["look_back_days"], d["calculation_method"],
        fpd, d["maturity_date"], d.get("status", "Active")
    ))


def update_deal(conn, cusip: str, d: dict):
    d = enforce_frequency(d)
    fpd = d.get("first_payment_date") or d.get("start_date")
    existing = conn.execute(
        "SELECT spread, accrual_day_basis FROM deal_master WHERE cusip=?", (cusip,)
    ).fetchone()
    old_spread = float(existing["spread"] or 0) if existing else 0.0
    old_accrual_basis = (
        existing["accrual_day_basis"] if existing and existing["accrual_day_basis"]
        else "Calendar Days"
    )
    conn.execute("""
        UPDATE deal_master SET
            deal_name=?, client_name=?, notional_amount=?, spread=?, accrual_day_basis=?,
            rate_type=?, payment_frequency=?, observation_shift=?,
            shifted_interest=?, payment_delay=?, payment_delay_days=?,
            rounding_decimals=?, look_back_days=?, calculation_method=?,
            first_payment_date=?, maturity_date=?, status=?,
            modified_at=datetime('now')
        WHERE cusip=?
    """, (
        d["deal_name"], d["client_name"], d["notional_amount"],
        float(d.get("spread") or 0),
        d.get("accrual_day_basis", "Calendar Days"),
        d["rate_type"], d["payment_frequency"], d["observation_shift"],
        d["shifted_interest"], d["payment_delay"],
        int(d.get("payment_delay_days") or 0),
        d["rounding_decimals"], d["look_back_days"], d["calculation_method"],
        fpd, d["maturity_date"], d.get("status", "Active"),
        cusip
    ))
    new_spread = float(d.get("spread") or 0)
    new_accrual_basis = d.get("accrual_day_basis", "Calendar Days")
    if (abs(new_spread - old_spread) > 1e-12
            or new_accrual_basis != old_accrual_basis):
        refresh_schedule_accruals(conn, cusip)
        return recalculate_existing_results(conn, cusip)
    return {"schedule_rows_updated": 0, "log_rows_updated": 0}


def delete_deal(conn, cusip: str):
    deal = conn.execute(
        "SELECT deal_id FROM deal_master WHERE cusip=?", (cusip,)
    ).fetchone()
    if deal:
        conn.execute("DELETE FROM payment_schedule WHERE deal_id=?",
                     (deal["deal_id"],))
        conn.execute("DELETE FROM deal_master WHERE cusip=?", (cusip,))


# ---------------------------------------------------------------------------
# Business rules
# ---------------------------------------------------------------------------

# Frequency rules enforced at save time:
#   Simple Average in Arrears -> Monthly
#   Compounded in Arrears     -> Quarterly
#   SOFR Index                -> Quarterly
FREQ_RULES = {
    "Simple Average in Arrears": "Monthly",
    "Compounded in Arrears":     "Quarterly",
    "SOFR Index":                "Quarterly",
}


def enforce_frequency(d: dict) -> dict:
    """Override payment_frequency based on calculation_method rule."""
    method = d.get("calculation_method", "")
    if method in FREQ_RULES:
        d["payment_frequency"] = FREQ_RULES[method]
    return d


def auto_mature_deals(conn) -> int:
    """Mark Active deals past their maturity date as Matured."""
    today = date.today().isoformat()
    cur = conn.execute("""
        UPDATE deal_master
        SET    status = 'Matured',
               modified_at = datetime('now')
        WHERE  status = 'Active'
          AND  maturity_date < ?
    """, (today,))
    return cur.rowcount


# ---------------------------------------------------------------------------
# SOFR rates
# ---------------------------------------------------------------------------

def import_rates_from_excel(conn, path: str) -> tuple[int, list[str]]:
    """
    Import SOFR rates from Excel.

    Supports two layouts automatically:

    Layout A — NY Fed format (newyorkfed.org download):
        Columns: DATE | BENCHMARK NAME | RATE (%) | 1ST PERCENTILE (%) |
                 25TH PERCENTILE (%) | 75TH PERCENTILE (%) |
                 99TH PERCENTILE (%) | VOLUME ($Billions)
        NOTE: No SOFR Index column in this format — the index is computed
        from the daily rates using ACT/360 compounding starting from 1.0
        anchored at the earliest date in the file (or existing DB value).

    Layout B — Full format (with pre-computed SOFR Index):
        Columns: Date | SOFR Rate (%) | SOFR Index | Day Count Factor (optional)
    """
    import pandas as pd

    df = pd.read_excel(path, header=0)

    # Normalise column names for matching
    raw_cols  = list(df.columns)
    norm_cols = [str(c).strip().lower() for c in raw_cols]

    # ── Detect layout ────────────────────────────────────────────────────────
    is_nyfed = any("benchmark" in c for c in norm_cols)

    if is_nyfed:
        # NY Fed layout — find DATE and RATE (%) columns by position/name
        date_col = None
        rate_col = None
        for i, c in enumerate(norm_cols):
            if "date" in c and date_col is None:
                date_col = raw_cols[i]
            if "rate" in c and "%" in c and rate_col is None:
                rate_col = raw_cols[i]
            # Also accept plain "rate (%)" style
            if c in ("rate (%)", "rate(%)", "sofr rate (%)") and rate_col is None:
                rate_col = raw_cols[i]

        if date_col is None or rate_col is None:
            # Fallback: assume first col = date, third col = rate
            date_col = raw_cols[0]
            rate_col = raw_cols[2]

        # Drop rows where date or rate is missing / non-parseable
        df = df[[date_col, rate_col]].copy()
        df.columns = ["_date", "_rate"]
        df = df.dropna(subset=["_date", "_rate"])

        # Filter to SOFR rows only (exclude SOFR30, SOFR90 etc if present)
        # NY Fed files sometimes have a BENCHMARK NAME column — skip non-SOFR rows
        if "benchmark name" in norm_cols or "benchmark_name" in norm_cols:
            bm_col = raw_cols[norm_cols.index("benchmark name")] \
                     if "benchmark name" in norm_cols \
                     else raw_cols[norm_cols.index("benchmark_name")]
            # Re-read with benchmark column included
            df2 = pd.read_excel(path, header=0)
            df2.columns = [str(c).strip() for c in df2.columns]
            df2 = df2.rename(columns={
                date_col: "_date",
                rate_col: "_rate",
                bm_col:   "_bm",
            })
            df2 = df2[["_date", "_rate", "_bm"]].dropna(subset=["_date", "_rate"])
            # Keep only plain SOFR rows (not SOFR30DRATE, SOFR90DRATE etc)
            df2 = df2[df2["_bm"].astype(str).str.strip().str.upper() == "SOFR"]
            df = df2[["_date", "_rate"]].copy()

        # Parse dates and rates
        parsed = []
        errors_parse = []
        for _, row in df.iterrows():
            try:
                d    = pd.to_datetime(row["_date"]).date()
                rate = float(str(row["_rate"]).replace(",", "."))
                parsed.append((d, rate))
            except Exception as e:
                errors_parse.append(str(e))

        if not parsed:
            raise ValueError(
                "No valid SOFR rows found in file. "
                f"Detected NY Fed format. Date col='{date_col}', "
                f"Rate col='{rate_col}'. Sample data: {df.head(3).to_dict()}"
            )

        # Sort ascending so index computation goes forward in time
        parsed.sort(key=lambda x: x[0])

        # ── Compute SOFR Index ────────────────────────────────────────────
        # Index_t = Index_{t-1} × (1 + r_{t-1} × dcf_{t-1} / 360)
        # Anchor: check if DB already has an index value just before our range
        first_date = parsed[0][0]
        anchor_row = conn.execute("""
            SELECT rate_date, sofr_index FROM sofr_index
            WHERE rate_date < ?
            ORDER BY rate_date DESC LIMIT 1
        """, (first_date.isoformat(),)).fetchone()

        if anchor_row:
            running_index = anchor_row["sofr_index"]
        else:
            running_index = 1.0   # Start at 1.0 if no prior data

        inserted, errors = 0, []
        for i, (d, rate) in enumerate(parsed):
            try:
                # Day count factor: Fri=3 (covers Sat+Sun), else 1
                dcf = 3 if d.weekday() == 4 else 1

                # Advance the index using the PREVIOUS day's rate
                # (index on date T reflects compounding through T-1)
                # On first row we use the anchor; subsequent rows use prior rate
                if i > 0:
                    prev_d, prev_rate = parsed[i - 1]
                    prev_dcf = 3 if prev_d.weekday() == 4 else 1
                    running_index = running_index * (
                        1.0 + prev_rate / 100.0 * prev_dcf / 360.0
                    )

                conn.execute("""
                    INSERT OR REPLACE INTO sofr_rates
                        (rate_date, sofr_rate, day_count_factor)
                    VALUES (?, ?, ?)
                """, (d.isoformat(), rate, dcf))
                # Store computed index in sofr_index table
                conn.execute("""
                    INSERT OR REPLACE INTO sofr_index (rate_date, sofr_index)
                    VALUES (?, ?)
                """, (d.isoformat(), round(running_index, 8)))
                inserted += 1

                # Good Friday is handled at calculation time:
                # use Thursday's publication if available, otherwise Wednesday's.

            except Exception as e:
                errors.append(f"{d}: {e}")

        errors = errors_parse + errors
        return inserted, errors

    else:
        # ── Layout B: Full format with pre-computed SOFR Index ────────────
        col_map = {}
        for i, c in enumerate(norm_cols):
            if "date" in c and "date" not in col_map:
                col_map["date"] = raw_cols[i]
            elif "index" in c and "sofr_index" not in col_map:
                col_map["sofr_index"] = raw_cols[i]
            elif "rate" in c and "sofr_rate" not in col_map:
                col_map["sofr_rate"] = raw_cols[i]
            elif "day" in c and "count" in c:
                col_map["day_count_factor"] = raw_cols[i]

        missing = [k for k in ("date", "sofr_rate", "sofr_index")
                   if k not in col_map]
        if missing:
            raise ValueError(
                f"Could not identify columns: {missing}. "
                f"Found columns: {raw_cols}. "
                "Expected either NY Fed format (DATE | BENCHMARK NAME | RATE (%)) "
                "or full format (Date | SOFR Rate (%) | SOFR Index)."
            )

        inserted, errors = 0, []
        for _, row in df.iterrows():
            try:
                d    = pd.to_datetime(row[col_map["date"]]).date()
                rate = float(row[col_map["sofr_rate"]])
                idx  = float(row[col_map["sofr_index"]])
                dcf  = (int(row[col_map["day_count_factor"]])
                        if "day_count_factor" in col_map
                        else (3 if d.weekday() == 4 else 1))
                conn.execute("""
                    INSERT OR REPLACE INTO sofr_rates
                        (rate_date, sofr_rate, day_count_factor)
                    VALUES (?, ?, ?)
                """, (d.isoformat(), rate, dcf))
                conn.execute("""
                    INSERT OR REPLACE INTO sofr_index (rate_date, sofr_index)
                    VALUES (?, ?)
                """, (d.isoformat(), idx))
                inserted += 1
            except Exception as e:
                errors.append(str(e))

        return inserted, errors


def get_rates(conn, start: str | None = None,
              end: str | None = None, limit: int = 500):
    """Return SOFR daily rates from sofr_rates table."""
    q = "SELECT * FROM sofr_rates"
    args = []
    clauses = []
    if start:
        clauses.append("rate_date >= ?"); args.append(start)
    if end:
        clauses.append("rate_date <= ?"); args.append(end)
    if clauses:
        q += " WHERE " + " AND ".join(clauses)
    q += f" ORDER BY rate_date DESC LIMIT {limit}"
    return [dict(r) for r in conn.execute(q, args).fetchall()]


def get_index_rates(conn, start: str | None = None,
                    end: str | None = None, limit: int = 500):
    """Return SOFR compounded index values from sofr_index table."""
    q = "SELECT * FROM sofr_index"
    args = []
    clauses = []
    if start:
        clauses.append("rate_date >= ?"); args.append(start)
    if end:
        clauses.append("rate_date <= ?"); args.append(end)
    if clauses:
        q += " WHERE " + " AND ".join(clauses)
    q += f" ORDER BY rate_date DESC LIMIT {limit}"
    return [dict(r) for r in conn.execute(q, args).fetchall()]


def get_rates_summary(conn) -> dict:
    r = conn.execute("""
        SELECT COUNT(*) AS cnt,
               MIN(rate_date) AS earliest,
               MAX(rate_date) AS latest,
               AVG(sofr_rate) AS avg_rate
        FROM sofr_rates
    """).fetchone()
    ri = conn.execute("""
        SELECT COUNT(*) AS cnt,
               MIN(rate_date) AS earliest,
               MAX(rate_date) AS latest
        FROM sofr_index
    """).fetchone()
    return {
        "rates_cnt":      dict(r)["cnt"] if r else 0,
        "rates_earliest": dict(r)["earliest"] if r else None,
        "rates_latest":   dict(r)["latest"] if r else None,
        "rates_avg":      dict(r)["avg_rate"] if r else None,
        "index_cnt":      dict(ri)["cnt"] if ri else 0,
        "index_earliest": dict(ri)["earliest"] if ri else None,
        "index_latest":   dict(ri)["latest"] if ri else None,
    }


def import_index_from_excel(conn, path: str) -> tuple[int, list[str]]:
    """
    Import SOFR Index values from a standalone NY Fed Index Excel file.
    Expected columns: DATE/Effective Date | BENCHMARK NAME | INDEX/SOFR Index
    """
    import pandas as pd

    df = pd.read_excel(path, header=0)
    raw_cols  = list(df.columns)
    norm_cols = [str(c).strip().lower() for c in raw_cols]

    def _n(col): return col.strip().lower()

    DATE_VARIANTS  = {"date", "effective date", "effectivedate"}
    INDEX_VARIANTS = {"sofr index", "sofrindex", "index"}
    BM_VARIANTS    = {"rate type", "ratetype", "benchmark name", "benchmarkname",
                      "benchmark", "type"}
    SOFRAI_VALUES  = {"sofrai", "sofr index", "sofr compounded index",
                      "sofr averages and index"}

    date_col  = next((raw_cols[i] for i,c in enumerate(norm_cols)
                      if _n(c) in DATE_VARIANTS), None)
    index_col = next((raw_cols[i] for i,c in enumerate(norm_cols)
                      if _n(c) in INDEX_VARIANTS), None)
    bm_col    = next((raw_cols[i] for i,c in enumerate(norm_cols)
                      if _n(c) in BM_VARIANTS), None)

    if not date_col or not index_col:
        raise ValueError(
            f"Cannot find Date or Index column. "
            f"Found: {raw_cols}. "
            "Expected columns: Date, SOFR Index (or similar)."
        )

    df["_date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    df = df.dropna(subset=["_date"])

    if bm_col:
        mask = df[bm_col].astype(str).str.strip().str.lower().isin(
            {v.lower() for v in SOFRAI_VALUES}
        )
        if mask.any():
            df = df[mask]

    inserted, errors = 0, []
    for _, row in df.iterrows():
        try:
            d   = row["_date"]
            idx = float(str(row[index_col]).replace(",", "."))
            conn.execute("""
                INSERT OR REPLACE INTO sofr_index (rate_date, sofr_index)
                VALUES (?, ?)
            """, (d.isoformat(), idx))
            inserted += 1
        except Exception as e:
            errors.append(f"{row.get('_date','?')}: {e}")

    return inserted, errors


# ---------------------------------------------------------------------------
# Dashboard KPIs
# ---------------------------------------------------------------------------

def get_dashboard_kpis(conn) -> dict:
    today = date.today().isoformat()
    deals = conn.execute(
        "SELECT COUNT(*) AS cnt FROM deal_master WHERE status='Active'"
    ).fetchone()["cnt"]

    ready = conn.execute(
        "SELECT COUNT(*) AS cnt FROM payment_schedule WHERE period_status='Ready to Calculate'"
    ).fetchone()["cnt"]

    scheduled = conn.execute(
        "SELECT COUNT(*) AS cnt FROM payment_schedule WHERE period_status='Scheduled'"
    ).fetchone()["cnt"]

    calculated = conn.execute(
        "SELECT COUNT(*) AS cnt FROM payment_schedule WHERE period_status='Calculated'"
    ).fetchone()["cnt"]

    total_interest = conn.execute(
        "SELECT COALESCE(SUM(interest_amount),0) AS tot FROM payment_schedule WHERE period_status='Calculated'"
    ).fetchone()["tot"]

    rates_cnt  = conn.execute("SELECT COUNT(*) AS cnt FROM sofr_rates").fetchone()["cnt"]
    index_cnt  = conn.execute("SELECT COUNT(*) AS cnt FROM sofr_index").fetchone()["cnt"]
    max_rate   = conn.execute("SELECT MAX(rate_date) AS mx FROM sofr_rates").fetchone()["mx"]
    max_index  = conn.execute("SELECT MAX(rate_date) AS mx FROM sofr_index").fetchone()["mx"]

    # Counts by RDD window (distinct deals, not periods)
    today_count = conn.execute("""
        SELECT COUNT(DISTINCT ps.deal_id) AS cnt
        FROM payment_schedule ps
        JOIN deal_master d ON d.deal_id = ps.deal_id
        WHERE ps.rate_determination_date <= ? AND ps.period_status != 'Calculated'
          AND d.status = 'Active'
    """, (today,)).fetchone()["cnt"]

    week_end = (date.today() + timedelta(days=(6 - date.today().weekday()))).isoformat()
    week_count = conn.execute("""
        SELECT COUNT(DISTINCT ps.deal_id) AS cnt
        FROM payment_schedule ps
        JOIN deal_master d ON d.deal_id = ps.deal_id
        WHERE ps.rate_determination_date <= ? AND ps.period_status != 'Calculated'
          AND d.status = 'Active'
    """, (week_end,)).fetchone()["cnt"]

    import calendar
    t = date.today()
    last_day = calendar.monthrange(t.year, t.month)[1]
    month_end = date(t.year, t.month, last_day).isoformat()
    month_count = conn.execute("""
        SELECT COUNT(DISTINCT ps.deal_id) AS cnt
        FROM payment_schedule ps
        JOIN deal_master d ON d.deal_id = ps.deal_id
        WHERE ps.rate_determination_date <= ? AND ps.period_status != 'Calculated'
          AND d.status = 'Active'
    """, (month_end,)).fetchone()["cnt"]

    return {
        "active_deals":       deals,
        "ready_to_calc":      ready,
        "scheduled":          scheduled,
        "calculated_periods": calculated,
        "total_interest":     total_interest,
        "rates_loaded":       rates_cnt,
        "index_loaded":       index_cnt,
        "latest_rate_date":   max_rate,
        "latest_index_date":  max_index,
        "calc_due_today":     today_count,
        "calc_due_week":      week_count,
        "calc_due_month":     month_count,
    }


def get_deals_by_rdd_window(conn, window: str = "today") -> list[dict]:
    """
    Return deals whose rate_determination_date falls within the window
    and have not yet been calculated.

    window: "today"  -> RDD == today
            "week"   -> RDD within this calendar week (Mon-Sun)
            "month"  -> RDD within this calendar month
    """
    import calendar
    today = date.today()

    if window == "today":
        date_from = today.isoformat()
        date_to   = today.isoformat()
    elif window == "week":
        # Monday of this week to Sunday
        mon = today - timedelta(days=today.weekday())
        sun = mon + timedelta(days=6)
        date_from = mon.isoformat()
        date_to   = sun.isoformat()
    else:   # month
        last_day  = calendar.monthrange(today.year, today.month)[1]
        date_from = date(today.year, today.month, 1).isoformat()
        date_to   = date(today.year, today.month, last_day).isoformat()

    rows = conn.execute("""
        SELECT
            ps.cusip,
            ps.period_number,
            d.deal_name,
            d.client_name,
            d.calculation_method,
            d.notional_amount,
            d.payment_frequency,
            d.rate_type,
            ps.period_start_date,
            ps.period_end_date,
            ps.obs_start_date,
            ps.obs_end_date,
            ps.rate_determination_date,
            ps.adj_payment_date,
            ps.accrual_days,
            ps.period_status,
            ps.is_calc_eligible_today,
            ps.obs_rates_available,
            ps.missing_rate_dates,
            CAST(julianday(ps.rate_determination_date) - julianday(:today) AS INTEGER)
                AS days_to_rdd,
            CAST(julianday(ps.adj_payment_date) - julianday(:today) AS INTEGER)
                AS days_to_payment
        FROM payment_schedule ps
        JOIN deal_master d ON d.deal_id = ps.deal_id
        WHERE ps.rate_determination_date BETWEEN :dfrom AND :dto
          AND ps.period_status != 'Calculated'
          AND d.status = 'Active'
        ORDER BY
            ps.rate_determination_date ASC,
            ps.adj_payment_date ASC,
            d.deal_name ASC
    """, {"today": today.isoformat(), "dfrom": date_from, "dto": date_to}).fetchall()
    return [dict(r) for r in rows]


def get_next_payments(conn, limit: int = 50) -> list[dict]:
    """Legacy function — kept for payment schedule page compatibility."""
    today = date.today().isoformat()
    rows = conn.execute(f"""
        SELECT
            ps.cusip, ps.period_number,
            d.deal_name, d.client_name, d.calculation_method,
            d.notional_amount, d.payment_frequency,
            ps.period_start_date, ps.period_end_date,
            ps.obs_start_date, ps.obs_end_date,
            ps.rate_determination_date,
            ps.adj_payment_date, ps.accrual_days,
            ps.period_status, ps.is_calc_eligible_today,
            ps.obs_rates_available, ps.period_ended_by_today,
            ps.missing_rate_dates,
            ps.compounded_rate, ps.annualized_rate,
            ps.rounded_rate, ps.interest_amount,
            CAST(julianday(ps.adj_payment_date) - julianday(?) AS INTEGER) AS days_to_payment
        FROM payment_schedule ps
        JOIN deal_master d ON d.deal_id = ps.deal_id
        WHERE ps.is_next_payment_period = 1
          AND d.status = 'Active'
        ORDER BY ps.rate_determination_date ASC, ps.adj_payment_date ASC
        LIMIT ?
    """, (today, limit)).fetchall()
    return [dict(r) for r in rows]


def get_full_schedule(conn, cusip: str) -> list[dict]:
    rows = conn.execute("""
        SELECT ps.*, d.deal_name, d.client_name, d.calculation_method,
               d.notional_amount, d.payment_frequency
        FROM payment_schedule ps
        JOIN deal_master d ON d.deal_id = ps.deal_id
        WHERE ps.cusip = ?
        ORDER BY ps.period_number
    """, (cusip,)).fetchall()
    return [dict(r) for r in rows]


def get_calc_log(conn, cusip: str | None = None,
                 limit: int = 200) -> list[dict]:
    q = """
        SELECT l.*, d.deal_name, d.client_name
        FROM calculation_log l
        JOIN deal_master d ON d.deal_id = l.deal_id
    """
    args = []
    if cusip:
        q += " WHERE l.cusip=?"
        args.append(cusip)
    q += " ORDER BY l.calculated_at DESC LIMIT ?"
    args.append(limit)
    return [dict(r) for r in conn.execute(q, args).fetchall()]


def get_eligible_deals(conn) -> list[dict]:
    rows = conn.execute("""
        SELECT ps.*, d.deal_name, d.client_name, d.calculation_method,
               d.notional_amount, d.look_back_days, d.rounding_decimals,
               d.payment_frequency, d.payment_delay
        FROM payment_schedule ps
        JOIN deal_master d ON d.deal_id = ps.deal_id
        WHERE ps.is_calc_eligible_today = 1
          AND ps.period_status = 'Ready to Calculate'
          AND d.status = 'Active'
        ORDER BY ps.adj_payment_date ASC
    """).fetchall()
    return [dict(r) for r in rows]
