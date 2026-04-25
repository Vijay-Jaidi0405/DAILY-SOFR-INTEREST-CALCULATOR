"""
database.py
SQLite database layer for the SOFR Interest Calculator.
Handles schema creation, all CRUD operations, and the
calculation engine for all three methods.
"""

import sqlite3
import math
import re
import json
from datetime import date, timedelta
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

DB_PATH = Path(__file__).parent / "sofr_calculator.db"
NY_FED_API_BASE = "https://markets.newyorkfed.org/api/rates/secured"

DEFAULT_HOLIDAY_CALENDAR = "ALL"
DEFAULT_RATE_HOLIDAY_CALENDAR = DEFAULT_HOLIDAY_CALENDAR
DEFAULT_PERIOD_HOLIDAY_CALENDAR = DEFAULT_HOLIDAY_CALENDAR
BUILTIN_HOLIDAY_CALENDAR_OPTIONS = [
    ("ALL", "All Holidays"),
    ("LONDON", "London"),
    ("TOKYO", "Tokyo"),
    ("NYS", "New York Stock Exchange"),
    ("NYF", "New York Fed"),
    ("AUSTRALIA", "Australia"),
    ("CANADA", "Canada"),
    ("SINGAPORE", "Singapore"),
    ("GERMANY", "Germany"),
]
HOLIDAY_CALENDAR_OPTIONS = list(BUILTIN_HOLIDAY_CALENDAR_OPTIONS)
HOLIDAY_CALENDAR_LABELS = {
    code: label for code, label in HOLIDAY_CALENDAR_OPTIONS
}
SUPPORTED_HOLIDAY_YEARS = range(2024, 2151)

_BUILTIN_HOLIDAY_LABELS = {
    code: label for code, label in BUILTIN_HOLIDAY_CALENDAR_OPTIONS
}
_BUILTIN_HOLIDAY_CODES = {
    code for code, _ in BUILTIN_HOLIDAY_CALENDAR_OPTIONS if code != "ALL"
}


def _calendar_sort_key(item: tuple[str, str]) -> tuple[int, int, str]:
    code, label = item
    builtin_order = [opt[0] for opt in BUILTIN_HOLIDAY_CALENDAR_OPTIONS]
    if code in builtin_order:
        return (0, builtin_order.index(code), label)
    return (1, 999, label)


def _refresh_holiday_calendar_metadata(conn=None) -> None:
    global HOLIDAY_CALENDAR_OPTIONS, HOLIDAY_CALENDAR_LABELS, _HOLIDAY_SETS

    options = list(BUILTIN_HOLIDAY_CALENDAR_OPTIONS)
    labels = dict(_BUILTIN_HOLIDAY_LABELS)
    dynamic_codes = []
    if conn is not None:
        try:
            rows = conn.execute("""
                SELECT calendar_code, calendar_label
                FROM holiday_calendars
                WHERE is_active = 1
                ORDER BY sort_order, calendar_label, calendar_code
            """).fetchall()
            for row in rows:
                code = str(row["calendar_code"]).strip().upper()
                label = str(row["calendar_label"]).strip() or code
                if code == "ALL" or code in labels:
                    continue
                options.append((code, label))
                labels[code] = label
                dynamic_codes.append(code)
        except Exception:
            pass

    options = sorted(options, key=_calendar_sort_key)
    HOLIDAY_CALENDAR_OPTIONS = options
    HOLIDAY_CALENDAR_LABELS = labels

    existing_sets = globals().get("_HOLIDAY_SETS", {})
    refreshed_sets: dict[str, set[date]] = {}
    for code, _ in options:
        if code == "ALL":
            continue
        refreshed_sets[code] = set(existing_sets.get(code, set()))
    for code in dynamic_codes:
        refreshed_sets.setdefault(code, set())
    _HOLIDAY_SETS = refreshed_sets


def list_holiday_calendars(conn=None, include_all: bool = True) -> list[tuple[str, str]]:
    if conn is not None:
        _refresh_holiday_calendar_metadata(conn)
    if include_all:
        return list(HOLIDAY_CALENDAR_OPTIONS)
    return [(code, label) for code, label in HOLIDAY_CALENDAR_OPTIONS if code != "ALL"]


def _sanitize_calendar_code(text: str) -> str:
    code = re.sub(r"[^A-Z0-9]+", "_", str(text or "").strip().upper()).strip("_")
    if not code:
        raise ValueError("Holiday calendar name cannot be blank.")
    if code[0].isdigit():
        code = f"CAL_{code}"
    if len(code) > 32:
        code = code[:32].rstrip("_")
    if code == "ALL":
        raise ValueError("'ALL' is reserved for the combined holiday view.")
    return code


def ensure_holiday_calendar(conn, code_or_label: str, label: str | None = None,
                            *, allow_existing_label_match: bool = True) -> tuple[str, str]:
    raw_label = str(label or code_or_label or "").strip()
    raw_code = _sanitize_calendar_code(code_or_label)

    existing = conn.execute("""
        SELECT calendar_code, calendar_label
        FROM holiday_calendars
        WHERE UPPER(calendar_code) = ?
    """, (raw_code,)).fetchone()
    if existing:
        resolved = (existing["calendar_code"], existing["calendar_label"])
        _refresh_holiday_calendar_metadata(conn)
        return resolved

    if allow_existing_label_match and raw_label:
        existing = conn.execute("""
            SELECT calendar_code, calendar_label
            FROM holiday_calendars
            WHERE UPPER(calendar_label) = ?
        """, (raw_label.upper(),)).fetchone()
        if existing:
            resolved = (existing["calendar_code"], existing["calendar_label"])
            _refresh_holiday_calendar_metadata(conn)
            return resolved

    calendar_label = raw_label or raw_code
    next_sort = conn.execute(
        "SELECT COALESCE(MAX(sort_order), 0) + 1 AS next_sort FROM holiday_calendars"
    ).fetchone()["next_sort"]
    conn.execute("""
        INSERT INTO holiday_calendars(calendar_code, calendar_label, is_system, is_active, sort_order)
        VALUES(?,?,?,?,?)
    """, (raw_code, calendar_label, 0, 1, next_sort))
    _refresh_holiday_calendar_metadata(conn)
    return raw_code, calendar_label


def normalize_holiday_calendar(value) -> str:
    if isinstance(value, (list, tuple, set)):
        raw_codes = [str(v).strip() for v in value]
    else:
        text = str(value or DEFAULT_HOLIDAY_CALENDAR).replace(",", "|")
        raw_codes = [part.strip() for part in text.split("|")]

    allowed_by_code = {code.upper(): code for code, _ in HOLIDAY_CALENDAR_OPTIONS}
    allowed_by_label = {
        label.strip().upper(): code for code, label in HOLIDAY_CALENDAR_OPTIONS
    }
    resolved_codes = []
    for raw_code in raw_codes:
        token = raw_code.strip().upper()
        if not token:
            continue
        resolved = allowed_by_code.get(token) or allowed_by_label.get(token)
        if resolved:
            resolved_codes.append(resolved)
    raw_codes = resolved_codes
    # "ALL" means union of every holiday set, no need to store others
    if "ALL" in raw_codes:
        return "ALL"
    codes = [code for code, _ in HOLIDAY_CALENDAR_OPTIONS if code in raw_codes and code != "ALL"]
    if not codes:
        codes = [DEFAULT_HOLIDAY_CALENDAR]
    return "|".join(dict.fromkeys(codes))


def holiday_calendar_codes(value) -> list[str]:
    return normalize_holiday_calendar(value).split("|")


def holiday_calendar_label(value) -> str:
    codes = holiday_calendar_codes(value)
    if "ALL" in codes:
        return HOLIDAY_CALENDAR_LABELS.get("ALL", "All Holidays")
    return " + ".join(
        HOLIDAY_CALENDAR_LABELS.get(code, code)
        for code in codes
    )


def deal_rate_holiday_calendar(deal: dict) -> str:
    return normalize_holiday_calendar(
        deal.get("rate_holiday_calendar")
        or deal.get("holiday_calendar")
        or DEFAULT_RATE_HOLIDAY_CALENDAR
    )


def deal_period_holiday_calendar(deal: dict) -> str:
    return normalize_holiday_calendar(
        deal.get("period_holiday_calendar")
        or deal.get("holiday_calendar")
        or DEFAULT_PERIOD_HOLIDAY_CALENDAR
    )


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    d = date(year, month, 1)
    while d.weekday() != weekday:
        d += timedelta(days=1)
    d += timedelta(days=(occurrence - 1) * 7)
    return d


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        d = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        d = date(year, month + 1, 1) - timedelta(days=1)
    while d.weekday() != weekday:
        d -= timedelta(days=1)
    return d


def _observed_uk_new_year(year: int) -> tuple[date, str]:
    d = date(year, 1, 1)
    if d.weekday() == 5:
        return date(year, 1, 3), "New Year's Day (substitute day)"
    if d.weekday() == 6:
        return date(year, 1, 2), "New Year's Day (substitute day)"
    return d, "New Year's Day"


def _generate_london_holidays(year: int) -> list[tuple[str, str]]:
    easter = _easter_sunday(year)
    new_year_date, new_year_name = _observed_uk_new_year(year)
    holidays = [
        (new_year_date, new_year_name),
        (easter - timedelta(days=2), "Good Friday"),
        (easter + timedelta(days=1), "Easter Monday"),
        (_nth_weekday(year, 5, 0, 1), "Early May Bank Holiday"),
        (_last_weekday(year, 5, 0), "Spring Bank Holiday"),
        (_last_weekday(year, 8, 0), "Summer Bank Holiday"),
    ]

    dec_25 = date(year, 12, 25)
    if dec_25.weekday() == 5:
        holidays.append((date(year, 12, 27), "Christmas Day (substitute day)"))
        holidays.append((date(year, 12, 28), "Boxing Day (substitute day)"))
    elif dec_25.weekday() == 6:
        holidays.append((date(year, 12, 27), "Christmas Day (substitute day)"))
        holidays.append((date(year, 12, 26), "Boxing Day"))
    else:
        holidays.append((dec_25, "Christmas Day"))
        dec_26 = date(year, 12, 26)
        if dec_26.weekday() == 5:
            holidays.append((date(year, 12, 28), "Boxing Day (substitute day)"))
        else:
            holidays.append((dec_26, "Boxing Day"))

    return [(d.isoformat(), name) for d, name in sorted(holidays)]


def _vernal_equinox_day(year: int) -> int:
    return int(20.8431 + 0.242194 * (year - 1980) - (year - 1980) // 4)


def _autumnal_equinox_day(year: int) -> int:
    return int(23.2488 + 0.242194 * (year - 1980) - (year - 1980) // 4)


def _generate_tokyo_holidays(year: int) -> list[tuple[str, str]]:
    holidays: dict[date, str] = {
        date(year, 1, 1): "New Year's Day",
        _nth_weekday(year, 1, 0, 2): "Coming of Age Day",
        date(year, 2, 11): "National Foundation Day",
        date(year, 2, 23): "Emperor's Birthday",
        date(year, 3, _vernal_equinox_day(year)): "Vernal Equinox Day",
        date(year, 4, 29): "Showa Day",
        date(year, 5, 3): "Constitution Memorial Day",
        date(year, 5, 4): "Greenery Day",
        date(year, 5, 5): "Children's Day",
        _nth_weekday(year, 7, 0, 3): "Marine Day",
        date(year, 8, 11): "Mountain Day",
        _nth_weekday(year, 9, 0, 3): "Respect for the Aged Day",
        date(year, 9, _autumnal_equinox_day(year)): "Autumnal Equinox Day",
        _nth_weekday(year, 10, 0, 2): "Sports Day",
        date(year, 11, 3): "Culture Day",
        date(year, 11, 23): "Labor Thanksgiving Day",
    }

    d = date(year, 1, 2)
    end = date(year, 12, 30)
    while d <= end:
        if d not in holidays and (d - timedelta(days=1)) in holidays and (d + timedelta(days=1)) in holidays:
            holidays[d] = "Citizen's Holiday"
        d += timedelta(days=1)

    base_items = sorted(holidays.items())
    for holiday_date, holiday_name in base_items:
        if holiday_date.weekday() == 6:
            sub = holiday_date + timedelta(days=1)
            while sub in holidays:
                sub += timedelta(days=1)
            holidays[sub] = f"Substitute Holiday for {holiday_name}"

    return [(d.isoformat(), name) for d, name in sorted(holidays.items())]


def _observed_fixed_holiday(year: int, month: int, day: int, name: str) -> tuple[date, str]:
    d = date(year, month, day)
    if d.weekday() == 5:
        return d - timedelta(days=1), f"{name} (observed)"
    if d.weekday() == 6:
        return d + timedelta(days=1), f"{name} (observed)"
    return d, name


def _observed_monday_holiday(year: int, month: int, day: int, name: str) -> tuple[date, str]:
    d = date(year, month, day)
    if d.weekday() == 5:
        return d + timedelta(days=2), f"{name} (observed)"
    if d.weekday() == 6:
        return d + timedelta(days=1), f"{name} (observed)"
    return d, name


def _append_boxing_day_observed(holidays: list[tuple[date, str]], year: int) -> None:
    dec_25 = date(year, 12, 25)
    dec_26 = date(year, 12, 26)
    holidays.append((dec_25, "Christmas Day"))
    if dec_26.weekday() == 5:
        holidays.append((date(year, 12, 28), "Boxing Day (observed)"))
    elif dec_26.weekday() == 6:
        holidays.append((date(year, 12, 28), "Boxing Day (observed)"))
    else:
        holidays.append((dec_26, "Boxing Day"))
    if dec_25.weekday() == 5:
        holidays[-2] = (date(year, 12, 27), "Christmas Day (observed)")
    elif dec_25.weekday() == 6:
        holidays[-2] = (date(year, 12, 27), "Christmas Day (observed)")


def _generate_australia_holidays(year: int) -> list[tuple[str, str]]:
    easter = _easter_sunday(year)
    holidays = [
        _observed_monday_holiday(year, 1, 1, "New Year's Day"),
        _observed_monday_holiday(year, 1, 26, "Australia Day"),
        (easter - timedelta(days=2), "Good Friday"),
        (easter + timedelta(days=1), "Easter Monday"),
        (date(year, 4, 25), "ANZAC Day"),
        (_nth_weekday(year, 6, 0, 2), "King's Birthday"),
    ]
    _append_boxing_day_observed(holidays, year)
    return [(d.isoformat(), name) for d, name in sorted(set(holidays))]


def _generate_canada_holidays(year: int) -> list[tuple[str, str]]:
    easter = _easter_sunday(year)
    canada_day = _observed_fixed_holiday(year, 7, 1, "Canada Day")
    truth_day = _observed_fixed_holiday(year, 9, 30, "National Day for Truth and Reconciliation")
    holidays = [
        _observed_monday_holiday(year, 1, 1, "New Year's Day"),
        (easter - timedelta(days=2), "Good Friday"),
        (_last_weekday(year, 5, 0), "Victoria Day"),
        canada_day,
        (_nth_weekday(year, 9, 0, 1), "Labour Day"),
        truth_day,
        (_nth_weekday(year, 10, 0, 2), "Thanksgiving"),
    ]
    _append_boxing_day_observed(holidays, year)
    return [(d.isoformat(), name) for d, name in sorted(set(holidays))]


def _generate_singapore_holidays(year: int) -> list[tuple[str, str]]:
    easter = _easter_sunday(year)
    holidays = [
        _observed_monday_holiday(year, 1, 1, "New Year's Day"),
        (easter - timedelta(days=2), "Good Friday"),
        _observed_monday_holiday(year, 5, 1, "Labour Day"),
        _observed_monday_holiday(year, 8, 9, "National Day"),
        _observed_monday_holiday(year, 12, 25, "Christmas Day"),
    ]
    return [(d.isoformat(), name) for d, name in sorted(set(holidays))]


def _generate_germany_holidays(year: int) -> list[tuple[str, str]]:
    easter = _easter_sunday(year)
    holidays = [
        (date(year, 1, 1), "New Year's Day"),
        (easter - timedelta(days=2), "Good Friday"),
        (easter + timedelta(days=1), "Easter Monday"),
        (date(year, 5, 1), "Labour Day"),
        (easter + timedelta(days=39), "Ascension Day"),
        (easter + timedelta(days=50), "Whit Monday"),
        (date(year, 10, 3), "German Unity Day"),
        (date(year, 12, 25), "Christmas Day"),
        (date(year, 12, 26), "Second Day of Christmas"),
    ]
    return [(d.isoformat(), name) for d, name in sorted(set(holidays))]

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
    daily_floor         REAL,
    accrual_day_basis   TEXT    NOT NULL DEFAULT 'Calendar Days'
                            CHECK(accrual_day_basis IN ('Calendar Days','Observation Period Days')),
    rate_type           TEXT    NOT NULL CHECK(rate_type IN ('SOFR','SOFR Index')),
    payment_frequency   TEXT    NOT NULL CHECK(payment_frequency IN ('Monthly','Quarterly')),
    observation_shift   TEXT    NOT NULL DEFAULT 'N' CHECK(observation_shift IN ('Y','N')),
    shifted_interest    TEXT    NOT NULL DEFAULT 'N' CHECK(shifted_interest IN ('Y','N')),
    payment_delay       TEXT    NOT NULL DEFAULT 'N' CHECK(payment_delay IN ('Y','N')),
    holiday_calendar    TEXT    NOT NULL DEFAULT 'ALL',
    rate_holiday_calendar   TEXT    NOT NULL DEFAULT 'ALL',
    period_holiday_calendar TEXT    NOT NULL DEFAULT 'ALL',
    rounding_decimals   INTEGER NOT NULL DEFAULT 7 CHECK(rounding_decimals >= 0),
    look_back_days      INTEGER NOT NULL DEFAULT 2,
    calculation_method  TEXT    NOT NULL CHECK(calculation_method IN (
                            'Compounded in Arrears',
                            'Simple Average in Arrears',
                            'SOFR Index')),
    issue_date          TEXT    NOT NULL,
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
    holiday_date    TEXT    NOT NULL UNIQUE,  -- Format: YYYY-MM-DD
    holiday_day     TEXT    NOT NULL,         -- Day of week (e.g., Monday)
    holiday_name    TEXT    NOT NULL,         -- Descriptive name
    is_sifma        INTEGER NOT NULL DEFAULT 0, -- Boolean flags for each calendar
    is_us           INTEGER NOT NULL DEFAULT 0,
    is_london       INTEGER NOT NULL DEFAULT 0,
    is_tokyo        INTEGER NOT NULL DEFAULT 0,
    is_nys          INTEGER NOT NULL DEFAULT 0,
    is_nyf          INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS holiday_calendars (
    calendar_code   TEXT    PRIMARY KEY,
    calendar_label  TEXT    NOT NULL,
    is_system       INTEGER NOT NULL DEFAULT 0,
    is_active       INTEGER NOT NULL DEFAULT 1,
    sort_order      INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS market_holiday_calendar_map (
    holiday_id      INTEGER NOT NULL REFERENCES market_holidays(holiday_id) ON DELETE CASCADE,
    calendar_code   TEXT    NOT NULL REFERENCES holiday_calendars(calendar_code) ON DELETE CASCADE,
    PRIMARY KEY (holiday_id, calendar_code)
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
CREATE INDEX IF NOT EXISTS ix_holiday_map_calendar ON market_holiday_calendar_map(calendar_code);
"""

NY_MARKET_SEED_HOLIDAYS = [
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


def _build_seed_holiday_data():
    from datetime import date
    data = {}  # date -> {name, flags}
    
    def add(code, dt_str, name):
        if dt_str not in data:
            data[dt_str] = {"name": name, "flags": set()}
        data[dt_str]["flags"].add(code)

    for d, name in NY_MARKET_SEED_HOLIDAYS:
        for code in ["NYS", "NYF"]:
            add(code, d, name)

    for year in SUPPORTED_HOLIDAY_YEARS:
        for d, name in _generate_london_holidays(year):
            add("LONDON", d, name)
        for d, name in _generate_tokyo_holidays(year):
            add("TOKYO", d, name)
        for d, name in _generate_australia_holidays(year):
            add("AUSTRALIA", d, name)
        for d, name in _generate_canada_holidays(year):
            add("CANADA", d, name)
        for d, name in _generate_singapore_holidays(year):
            add("SINGAPORE", d, name)
        for d, name in _generate_germany_holidays(year):
            add("GERMANY", d, name)
            
    records = []
    memberships = []
    for dt_str, info in data.items():
        d_obj = date.fromisoformat(dt_str)
        f = info["flags"]
        records.append((
            dt_str, d_obj.strftime("%A"), info["name"],
            0, 0,
            1 if "LONDON" in f else 0, 1 if "TOKYO" in f else 0,
            1 if "NYS" in f else 0, 1 if "NYF" in f else 0
        ))
        for code in sorted(f):
            memberships.append((dt_str, code))
    return records, memberships

SEED_HOLIDAY_ROWS, SEED_HOLIDAY_MEMBERSHIPS = _build_seed_holiday_data()


# Pre-computed holiday sets for connection-free date rolling.
_HOLIDAY_SETS: dict[str, set[date]] = {
    code: set() for code, _ in HOLIDAY_CALENDAR_OPTIONS if code != "ALL"
}
# Logic for initial load is handled in init_db re-seeding

def _update_holiday_set(conn) -> None:
    """Reload cached holiday sets from the database."""
    global _HOLIDAY_SETS
    _refresh_holiday_calendar_metadata(conn)
    holiday_sets = {
        code: set() for code, _ in HOLIDAY_CALENDAR_OPTIONS if code != "ALL"
    }

    rows = conn.execute("""
        SELECT mh.holiday_date, hcm.calendar_code
        FROM market_holidays mh
        JOIN market_holiday_calendar_map hcm
          ON hcm.holiday_id = mh.holiday_id
        JOIN holiday_calendars hc
          ON hc.calendar_code = hcm.calendar_code
         AND hc.is_active = 1
    """).fetchall()
    for row in rows:
        dt = date.fromisoformat(row["holiday_date"])
        holiday_sets.setdefault(row["calendar_code"], set()).add(dt)
    _HOLIDAY_SETS = holiday_sets


def _holiday_dates_for_codes(codes) -> set[date]:
    codes_list = holiday_calendar_codes(codes)
    if "ALL" in codes_list:
        # Union of every available set
        out = set()
        for s in _HOLIDAY_SETS.values():
            out.update(s)
        return out
    dates: set[date] = set()
    for code in codes_list:
        dates.update(_HOLIDAY_SETS.get(code, set()))
    return dates


def _is_business_day_in_set(d: date, holiday_dates: set[date]) -> bool:
    # Treat Good Friday as a business day even if present in holiday sets.
    if _is_good_friday(d):
        return d.weekday() < 5
    return d.weekday() < 5 and d not in holiday_dates


def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)
        conn.executemany("""
            INSERT OR IGNORE INTO holiday_calendars(
                calendar_code, calendar_label, is_system, is_active, sort_order
            ) VALUES(?,?,?,?,?)
        """, [
            (code, label, 1, 1, idx)
            for idx, (code, label) in enumerate(BUILTIN_HOLIDAY_CALENDAR_OPTIONS)
            if code != "ALL"
        ])
        conn.executemany("""
            UPDATE holiday_calendars
            SET calendar_label=?, is_system=1, is_active=1, sort_order=?
            WHERE calendar_code=?
        """, [
            (label, idx, code)
            for idx, (code, label) in enumerate(BUILTIN_HOLIDAY_CALENDAR_OPTIONS)
            if code != "ALL"
        ])
        conn.execute("""
            UPDATE holiday_calendars
            SET is_active=0
            WHERE calendar_code IN ('SIFMA', 'US')
        """)
        conn.execute("""
            DELETE FROM market_holiday_calendar_map
            WHERE calendar_code IN ('SIFMA', 'US')
        """)
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
            conn.execute("ALTER TABLE deal_master ADD COLUMN daily_floor REAL")
        except Exception:
            pass
        try:
            conn.execute(
                "ALTER TABLE deal_master ADD COLUMN accrual_day_basis "
                "TEXT NOT NULL DEFAULT 'Calendar Days'"
            )
        except Exception:
            pass
        try:
            conn.execute(
                "ALTER TABLE deal_master ADD COLUMN holiday_calendar "
                "TEXT NOT NULL DEFAULT 'ALL'"
            )
        except Exception:
            pass
        try:
            conn.execute(
                "ALTER TABLE deal_master ADD COLUMN rate_holiday_calendar "
                "TEXT NOT NULL DEFAULT 'ALL'"
            )
            conn.execute("""
                UPDATE deal_master
                SET rate_holiday_calendar = COALESCE(holiday_calendar, 'ALL')
                WHERE rate_holiday_calendar IS NULL OR rate_holiday_calendar = ''
            """)
        except Exception:
            pass
        try:
            conn.execute(
                "ALTER TABLE deal_master ADD COLUMN period_holiday_calendar "
                "TEXT NOT NULL DEFAULT 'ALL'"
            )
            conn.execute("""
                UPDATE deal_master
                SET period_holiday_calendar = COALESCE(holiday_calendar, 'ALL')
                WHERE period_holiday_calendar IS NULL OR period_holiday_calendar = ''
            """)
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE deal_master ADD COLUMN issue_date TEXT")
            conn.execute("""
                UPDATE deal_master
                SET issue_date = COALESCE(issue_date, first_payment_date)
                WHERE issue_date IS NULL OR issue_date = ''
            """)
        except Exception:
            pass
        try:
            row = conn.execute("""
                SELECT sql
                FROM sqlite_master
                WHERE type='table' AND name='deal_master'
            """).fetchone()
            deal_master_sql = (row["sql"] or "") if row else ""
            if "CHECK(rounding_decimals IN (4,5,6))" in deal_master_sql:
                conn.execute("PRAGMA foreign_keys=OFF")
                conn.executescript("""
                    ALTER TABLE deal_master RENAME TO deal_master_old;
                    CREATE TABLE deal_master (
                        deal_id             INTEGER PRIMARY KEY AUTOINCREMENT,
                        deal_name           TEXT    NOT NULL,
                        client_name         TEXT    NOT NULL,
                        cusip               TEXT    NOT NULL UNIQUE,
                        notional_amount     REAL    NOT NULL,
                        spread              REAL    NOT NULL DEFAULT 0,
                        daily_floor         REAL,
                        accrual_day_basis   TEXT    NOT NULL DEFAULT 'Calendar Days'
                                                CHECK(accrual_day_basis IN ('Calendar Days','Observation Period Days')),
                        rate_type           TEXT    NOT NULL CHECK(rate_type IN ('SOFR','SOFR Index')),
                        payment_frequency   TEXT    NOT NULL CHECK(payment_frequency IN ('Monthly','Quarterly')),
                        observation_shift   TEXT    NOT NULL DEFAULT 'N' CHECK(observation_shift IN ('Y','N')),
                        shifted_interest    TEXT    NOT NULL DEFAULT 'N' CHECK(shifted_interest IN ('Y','N')),
                        payment_delay       TEXT    NOT NULL DEFAULT 'N' CHECK(payment_delay IN ('Y','N')),
                        holiday_calendar    TEXT    NOT NULL DEFAULT 'ALL',
                        rate_holiday_calendar   TEXT    NOT NULL DEFAULT 'ALL',
                        period_holiday_calendar TEXT    NOT NULL DEFAULT 'ALL',
                        rounding_decimals   INTEGER NOT NULL DEFAULT 7 CHECK(rounding_decimals >= 0),
                        look_back_days      INTEGER NOT NULL DEFAULT 2,
                        calculation_method  TEXT    NOT NULL CHECK(calculation_method IN (
                                                'Compounded in Arrears',
                                                'Simple Average in Arrears',
                                                'SOFR Index')),
                        issue_date          TEXT    NOT NULL,
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
                    INSERT INTO deal_master (
                        deal_id, deal_name, client_name, cusip, notional_amount,
                        spread, daily_floor, accrual_day_basis, rate_type, payment_frequency,
                        observation_shift, shifted_interest, payment_delay, holiday_calendar,
                        rate_holiday_calendar, period_holiday_calendar,
                        rounding_decimals, look_back_days, calculation_method,
                        issue_date, first_payment_date, maturity_date, payment_delay_days,
                        status, created_at, modified_at
                    )
                    SELECT
                        deal_id, deal_name, client_name, cusip, notional_amount,
                        spread, daily_floor, accrual_day_basis, rate_type, payment_frequency,
                        observation_shift, shifted_interest, payment_delay,
                        COALESCE(holiday_calendar, 'ALL'),
                        COALESCE(holiday_calendar, 'ALL'),
                        COALESCE(holiday_calendar, 'ALL'),
                        rounding_decimals, look_back_days, calculation_method,
                        first_payment_date, first_payment_date, maturity_date, payment_delay_days,
                        status, created_at, modified_at
                    FROM deal_master_old;
                    DROP TABLE deal_master_old;
                """)
                conn.execute("PRAGMA foreign_keys=ON")
        except Exception:
            pass
        try:
            ps_sql_row = conn.execute("""
                SELECT sql
                FROM sqlite_master
                WHERE type='table' AND name='payment_schedule'
            """).fetchone()
            log_sql_row = conn.execute("""
                SELECT sql
                FROM sqlite_master
                WHERE type='table' AND name='calculation_log'
            """).fetchone()
            ps_sql = (ps_sql_row["sql"] or "") if ps_sql_row else ""
            log_sql = (log_sql_row["sql"] or "") if log_sql_row else ""
            if 'REFERENCES "deal_master_old"' in ps_sql or 'REFERENCES "deal_master_old"' in log_sql:
                conn.execute("PRAGMA foreign_keys=OFF")
                conn.executescript("""
                    ALTER TABLE payment_schedule RENAME TO payment_schedule_old;
                    CREATE TABLE payment_schedule (
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
                    INSERT INTO payment_schedule (
                        schedule_id, deal_id, cusip, period_number,
                        period_start_date, period_end_date,
                        eff_period_start_date, eff_period_end_date,
                        obs_start_date, obs_end_date,
                        unadj_payment_date, adj_payment_date,
                        accrual_days, notional_amount,
                        compounded_rate, annualized_rate,
                        rounded_rate, interest_amount,
                        period_status, is_calc_eligible_today,
                        obs_rates_available, period_ended_by_today,
                        missing_rate_dates, is_next_payment_period,
                        rate_determination_date, generated_at,
                        status_refreshed_at, calculated_at, calculated_by
                    )
                    SELECT
                        schedule_id, deal_id, cusip, period_number,
                        period_start_date, period_end_date,
                        eff_period_start_date, eff_period_end_date,
                        obs_start_date, obs_end_date,
                        unadj_payment_date, adj_payment_date,
                        accrual_days, notional_amount,
                        compounded_rate, annualized_rate,
                        rounded_rate, interest_amount,
                        period_status, is_calc_eligible_today,
                        obs_rates_available, period_ended_by_today,
                        missing_rate_dates, is_next_payment_period,
                        rate_determination_date, generated_at,
                        status_refreshed_at, calculated_at, calculated_by
                    FROM payment_schedule_old;
                    DROP TABLE payment_schedule_old;

                    ALTER TABLE calculation_log RENAME TO calculation_log_old;
                    CREATE TABLE calculation_log (
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
                    INSERT INTO calculation_log (
                        log_id, deal_id, cusip, calculation_method,
                        period_start_date, period_end_date,
                        obs_start_date, obs_end_date,
                        payment_date, adjusted_payment_date,
                        payment_delay_days, accrual_days, day_count_basis,
                        look_back_days, notional_amount, compounded_rate,
                        annualized_rate, rounded_rate, interest_amount,
                        batch_id, calculated_at, calculated_by
                    )
                    SELECT
                        log_id, deal_id, cusip, calculation_method,
                        period_start_date, period_end_date,
                        obs_start_date, obs_end_date,
                        payment_date, adjusted_payment_date,
                        payment_delay_days, accrual_days, day_count_basis,
                        look_back_days, notional_amount, compounded_rate,
                        annualized_rate, rounded_rate, interest_amount,
                        batch_id, calculated_at, calculated_by
                    FROM calculation_log_old;
                    DROP TABLE calculation_log_old;

                    CREATE INDEX IF NOT EXISTS ix_schedule_cusip       ON payment_schedule(cusip);
                    CREATE INDEX IF NOT EXISTS ix_schedule_status      ON payment_schedule(period_status);
                    CREATE INDEX IF NOT EXISTS ix_schedule_eligible    ON payment_schedule(is_calc_eligible_today);
                    CREATE INDEX IF NOT EXISTS ix_schedule_next        ON payment_schedule(is_next_payment_period);
                    CREATE INDEX IF NOT EXISTS ix_schedule_rdd         ON payment_schedule(rate_determination_date);
                    CREATE INDEX IF NOT EXISTS ix_log_cusip            ON calculation_log(cusip);
                    CREATE INDEX IF NOT EXISTS ix_log_batch            ON calculation_log(batch_id);
                """)
                conn.execute("PRAGMA foreign_keys=ON")
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
        try:
            holiday_cols = [
                r[1] for r in conn.execute("PRAGMA table_info(market_holidays)").fetchall()
            ]
            # Migration: if we find the old narrow holiday table, rebuild it into the wide base shape.
            if holiday_cols and ("calendar_code" in holiday_cols or "holiday_day" not in holiday_cols):
                conn.execute("PRAGMA foreign_keys=OFF")
                conn.executescript("""
                    DROP TABLE IF EXISTS market_holidays;
                """)
                conn.execute("PRAGMA foreign_keys=ON")
                # Re-run schema to create the new table immediately
                conn.executescript(SCHEMA)
        except Exception:
            pass

        try:
            legacy_rows = conn.execute("""
                SELECT holiday_id, holiday_date, holiday_name,
                       is_sifma, is_us, is_london, is_tokyo, is_nys, is_nyf
                FROM market_holidays
            """).fetchall()
            mapping_count = conn.execute(
                "SELECT COUNT(*) AS cnt FROM market_holiday_calendar_map"
            ).fetchone()["cnt"]
            if mapping_count == 0 and legacy_rows:
                code_to_col = {
                    "LONDON": "is_london",
                    "TOKYO": "is_tokyo",
                    "NYS": "is_nys",
                    "NYF": "is_nyf",
                }
                membership_rows = []
                for row in legacy_rows:
                    for code, col in code_to_col.items():
                        if row[col]:
                            membership_rows.append((row["holiday_id"], code))
                conn.executemany("""
                    INSERT OR IGNORE INTO market_holiday_calendar_map(holiday_id, calendar_code)
                    VALUES(?,?)
                """, membership_rows)
        except Exception:
            pass

        conn.executemany(
            """INSERT OR IGNORE INTO market_holidays(
                holiday_date, holiday_day, holiday_name, 
                is_sifma, is_us, is_london, is_tokyo, is_nys, is_nyf
            ) VALUES(?,?,?,?,?,?,?,?,?)""",
            SEED_HOLIDAY_ROWS
        )
        seeded_memberships = []
        seeded_rows = conn.execute("""
            SELECT holiday_id, holiday_date, is_sifma, is_us, is_london, is_tokyo, is_nys, is_nyf
            FROM market_holidays
        """).fetchall()
        for row in seeded_rows:
            if row["is_london"]:
                seeded_memberships.append((row["holiday_id"], "LONDON"))
            if row["is_tokyo"]:
                seeded_memberships.append((row["holiday_id"], "TOKYO"))
            if row["is_nys"]:
                seeded_memberships.append((row["holiday_id"], "NYS"))
            if row["is_nyf"]:
                seeded_memberships.append((row["holiday_id"], "NYF"))
        for dt_str, code in SEED_HOLIDAY_MEMBERSHIPS:
            holiday = conn.execute(
                "SELECT holiday_id FROM market_holidays WHERE holiday_date=?",
                (dt_str,)
            ).fetchone()
            if holiday:
                seeded_memberships.append((holiday["holiday_id"], code))
        conn.executemany("""
            INSERT OR IGNORE INTO market_holiday_calendar_map(holiday_id, calendar_code)
            VALUES(?,?)
        """, seeded_memberships)
        _update_holiday_set(conn)
        auto_mature_deals(conn)

def get_holidays(conn, calendar_code=None):
    selected_code = str(calendar_code or "ALL").strip().upper()
    rows = conn.execute("""
        SELECT mh.holiday_id, mh.holiday_date, mh.holiday_day, mh.holiday_name,
               hc.calendar_code, hc.calendar_label
        FROM market_holidays mh
        LEFT JOIN market_holiday_calendar_map hcm
          ON hcm.holiday_id = mh.holiday_id
        LEFT JOIN holiday_calendars hc
          ON hc.calendar_code = hcm.calendar_code
         AND hc.is_active = 1
        ORDER BY mh.holiday_date DESC, hc.sort_order, hc.calendar_label, hc.calendar_code
    """).fetchall()

    calendars = list_holiday_calendars(conn, include_all=False)
    calendar_order = [code for code, _ in calendars]
    holiday_map: dict[int, dict] = {}
    for row in rows:
        holiday_id = row["holiday_id"]
        if selected_code != "ALL" and row["calendar_code"] != selected_code:
            continue
        item = holiday_map.setdefault(holiday_id, {
            "holiday_id": holiday_id,
            "holiday_date": row["holiday_date"],
            "holiday_day": row["holiday_day"],
            "holiday_name": row["holiday_name"],
            "calendar_codes": [],
            "calendar_flags": {},
        })
        if row["calendar_code"]:
            code = row["calendar_code"]
            item["calendar_codes"].append(code)
            item["calendar_flags"][code] = 1

    results = []
    for holiday in holiday_map.values():
        for code in calendar_order:
            holiday["calendar_flags"].setdefault(code, 0)
        holiday["calendar_codes"] = [
            code for code in calendar_order if holiday["calendar_flags"].get(code)
        ]
        results.append(holiday)
    return results


def insert_holiday(conn, holiday_date, holiday_name, calendar_codes: list[str] | set[str]):
    from datetime import date
    codes = [str(code).strip().upper() for code in calendar_codes if str(code).strip()]
    if not codes:
        raise ValueError("Please select at least one holiday calendar.")

    resolved_codes = []
    for code in codes:
        resolved_code, _ = ensure_holiday_calendar(conn, code)
        resolved_codes.append(resolved_code)

    day_name = date.fromisoformat(holiday_date).strftime("%A")
    conn.execute("""
        INSERT INTO market_holidays(
            holiday_date, holiday_day, holiday_name
        ) VALUES(?,?,?)
        ON CONFLICT(holiday_date) DO UPDATE SET
            holiday_day=excluded.holiday_day,
            holiday_name=excluded.holiday_name
    """, (holiday_date, day_name, holiday_name))
    holiday_id = conn.execute(
        "SELECT holiday_id FROM market_holidays WHERE holiday_date=?",
        (holiday_date,)
    ).fetchone()["holiday_id"]
    conn.execute(
        "DELETE FROM market_holiday_calendar_map WHERE holiday_id=?",
        (holiday_id,)
    )
    conn.executemany("""
        INSERT OR IGNORE INTO market_holiday_calendar_map(holiday_id, calendar_code)
        VALUES(?,?)
    """, [(holiday_id, code) for code in dict.fromkeys(resolved_codes)])
    _update_holiday_set(conn)


def add_holiday_calendar(conn, name: str) -> tuple[str, str]:
    return ensure_holiday_calendar(conn, name, name, allow_existing_label_match=False)


def import_holidays_from_excel(conn, path: str) -> tuple[int, list[str]]:
    """
    Import market holidays from Excel.
    Supports columns: Date, Name, and one column per holiday calendar.
    Unknown calendar headers are created automatically.
    """
    import pandas as pd
    from datetime import date

    df = pd.read_excel(path, header=0)
    raw_cols = list(df.columns)
    norm_cols = [str(c).strip().lower() for c in raw_cols]

    if not raw_cols:
        raise ValueError("The selected Excel file does not contain any columns.")

    def _pick_col(*names):
        choices = {name.strip().lower() for name in names}
        for i, col in enumerate(norm_cols):
            if col in choices:
                return raw_cols[i]
        return None

    def _parse_yn(val):
        if pd.isna(val) or str(val).strip() == "": return 0
        s = str(val).strip().upper()
        if s in ("Y", "YES", "TRUE", "1", "✓", "X"): return 1
        return 0

    date_col = _pick_col("date", "holiday date")
    name_col = _pick_col("name", "holiday name", "holiday")
    
    known_calendar_headers: dict[str, tuple[str, str]] = {}
    for code, label in list_holiday_calendars(conn, include_all=False):
        aliases = {
            code.strip().upper(),
            label.strip().upper(),
            label.replace(" Holidays", "").strip().upper(),
        }
        if code == "LONDON":
            aliases.add("LON")
        if code == "TOKYO":
            aliases.add("TOK")
        for alias in aliases:
            known_calendar_headers[alias] = (code, label)

    ignored_headers = {
        "", "DATE", "HOLIDAY DATE", "DAY", "HOLIDAY DAY", "NAME", "HOLIDAY NAME", "HOLIDAY"
    }
    calendar_columns: list[tuple[str, str]] = []
    for raw_col in raw_cols:
        header = str(raw_col).strip()
        alias = header.upper()
        if alias in ignored_headers:
            continue
        if alias in known_calendar_headers:
            calendar_columns.append((raw_col, known_calendar_headers[alias][0]))
            continue
        code, _ = ensure_holiday_calendar(conn, header, header)
        calendar_columns.append((raw_col, code))

    inserted, errors = 0, []
    for idx, row in df.iterrows():
        if all(pd.isna(v) for v in row): continue
        try:
            if not date_col: raise ValueError("Date column missing")
            dt_val = row[date_col]
            dt_obj = pd.to_datetime(dt_val, errors="raise").date()
            dt_str = dt_obj.isoformat()
            day_name = dt_obj.strftime("%A")
            
            h_name = str(row.get(name_col, "Uploaded Holiday")).strip() if name_col else "Uploaded Holiday"
            
            codes = [
                calendar_code
                for column_name, calendar_code in calendar_columns
                if _parse_yn(row.get(column_name))
            ]
            if not codes:
                raise ValueError("No holiday calendar was selected for this row")

            conn.execute("""
                INSERT INTO market_holidays(
                    holiday_date, holiday_day, holiday_name
                ) VALUES(?,?,?)
                ON CONFLICT(holiday_date) DO UPDATE SET
                    holiday_day=excluded.holiday_day,
                    holiday_name=excluded.holiday_name
            """, (dt_str, day_name, h_name))
            holiday_id = conn.execute(
                "SELECT holiday_id FROM market_holidays WHERE holiday_date=?",
                (dt_str,)
            ).fetchone()["holiday_id"]
            conn.execute(
                "DELETE FROM market_holiday_calendar_map WHERE holiday_id=?",
                (holiday_id,)
            )
            conn.executemany("""
                INSERT OR IGNORE INTO market_holiday_calendar_map(holiday_id, calendar_code)
                VALUES(?,?)
            """, [(holiday_id, code) for code in dict.fromkeys(codes)])
            inserted += 1
        except Exception as e:
            errors.append(f"Row {idx+2}: {e}")

    _update_holiday_set(conn)
    return inserted, errors

def delete_holiday(conn, holiday_id):
    conn.execute("DELETE FROM market_holiday_calendar_map WHERE holiday_id = ?", (holiday_id,))
    conn.execute("DELETE FROM market_holidays WHERE holiday_id = ?", (holiday_id,))
    _update_holiday_set(conn)

# ---------------------------------------------------------------------------
# Date / business day helpers
# ---------------------------------------------------------------------------

def _is_business_day(conn, d: date) -> bool:
    return _is_business_day_in_set(d, _holiday_dates_for_codes(DEFAULT_HOLIDAY_CALENDAR))


def _next_business_day(conn, d: date, holiday_calendar=DEFAULT_HOLIDAY_CALENDAR) -> date:
    holiday_dates = _holiday_dates_for_codes(holiday_calendar)
    for _ in range(14):
        if _is_business_day_in_set(d, holiday_dates):
            return d
        d += timedelta(days=1)
    return d


def _shift_date_back(d: date, n: int) -> date:
    return d - timedelta(days=n)


def _shift_business_days_back(d: date, n: int,
                              holiday_calendar=DEFAULT_HOLIDAY_CALENDAR) -> date:
    """Shift backward by n business days using weekends and selected holidays."""
    if n <= 0:
        return d

    holiday_dates = _holiday_dates_for_codes(holiday_calendar)
    remaining = n
    cur = d
    while remaining > 0:
        cur -= timedelta(days=1)
        if _is_business_day_in_set(cur, holiday_dates):
            remaining -= 1
    return cur


def _shift_business_days_forward(d: date, n: int,
                                 holiday_calendar=DEFAULT_HOLIDAY_CALENDAR) -> date:
    """Shift forward by n business days using weekends and selected holidays."""
    if n <= 0:
        return d

    holiday_dates = _holiday_dates_for_codes(holiday_calendar)
    remaining = n
    cur = d
    while remaining > 0:
        cur += timedelta(days=1)
        if _is_business_day_in_set(cur, holiday_dates):
            remaining -= 1
    return cur


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


def _interest_period_days(period_start: date, period_end: date) -> int:
    return _accrual_days(period_start, period_end)


def _observation_period_days(obs_start: date, obs_end: date) -> int:
    return _accrual_days(obs_start, obs_end)


def _selected_accrual_days(deal: dict, interest_days: int, observation_days: int) -> int:
    if deal.get("shifted_interest") == "Y":
        return interest_days
    return observation_days


def _iter_business_days(start: date, end: date,
                        holiday_calendar=DEFAULT_HOLIDAY_CALENDAR):
    """Yield (calendar_date, day_weight) for each business day in [start, end)."""
    holiday_dates = _holiday_dates_for_codes(holiday_calendar)
    d = start
    while d < end:
        if _is_business_day_in_set(d, holiday_dates):
            next_d = d + timedelta(days=1)
            while next_d < end and not _is_business_day_in_set(next_d, holiday_dates):
                next_d += timedelta(days=1)
            wt = (next_d if next_d < end else end) - d
            wt = wt.days
            yield d, wt
        d += timedelta(days=1)


def _natural_business_day_weight(d: date,
                                 holiday_calendar=DEFAULT_HOLIDAY_CALENDAR) -> int:
    """Calendar days from business day d up to but excluding the next business day."""
    holiday_dates = _holiday_dates_for_codes(holiday_calendar)
    next_d = d + timedelta(days=1)
    while not _is_business_day_in_set(next_d, holiday_dates):
        next_d += timedelta(days=1)
    return (next_d - d).days


def _aligned_business_days(interest_start: date, interest_end: date,
                           obs_start: date, obs_end: date,
                           interest_holiday_calendar=DEFAULT_PERIOD_HOLIDAY_CALENDAR,
                           observation_holiday_calendar=DEFAULT_RATE_HOLIDAY_CALENDAR,
                           use_observation_shift: bool = False):
    """
    Return aligned tuples of (interest_date, obs_date, day_weight).

    Without observation shift, the day weight comes from the interest period date
    and the observation date is the lookback date for that interest date.

    With observation shift, the business-day ladder and day weights come from the
    observation period, while interest dates remain the dates in the interest period.
    """
    interest_days = list(_iter_business_days(
        interest_start, interest_end, holiday_calendar=interest_holiday_calendar
    ))
    if not use_observation_shift:
        return interest_days

    observation_days = list(_iter_business_days(
        obs_start, obs_end, holiday_calendar=observation_holiday_calendar
    ))
    if len(interest_days) != len(observation_days):
        raise ValueError(
            "Observation-shift schedule mismatch between interest and observation periods."
        )
    return [
        (
            interest_days[i][0],
            observation_days[i][0],
            _natural_business_day_weight(
                observation_days[i][0],
                holiday_calendar=observation_holiday_calendar
            )
        )
        for i in range(len(interest_days))
    ]


def _last_business_day_before(d: date,
                              holiday_calendar=DEFAULT_HOLIDAY_CALENDAR) -> date:
    """Return the last business day strictly before the exclusive end date."""
    return _shift_business_days_back(d, 1, holiday_calendar=holiday_calendar)


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
                               is_index: bool = False,
                               holiday_calendar=DEFAULT_HOLIDAY_CALENDAR) -> bool:
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
        last_required = _last_business_day_before(
            obs_end,
            holiday_calendar=holiday_calendar
        )
        # For SOFR rate deals: check sofr_rates table
        max_row = conn.execute("SELECT MAX(rate_date) AS mx FROM sofr_rates").fetchone()
        if not max_row or not max_row["mx"]:
            return False
        max_rate = date.fromisoformat(max_row["mx"])
        if max_rate < last_required:
            return False
        r1 = _nearest_rate_date(conn, obs_start)
        r2 = _nearest_rate_date(conn, last_required)
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
    rate_holiday_calendar = deal_rate_holiday_calendar(deal)
    period_holiday_calendar = deal_period_holiday_calendar(deal)
    is_payment_delay = deal.get("payment_delay") == "Y"
    use_obs_shift = deal.get("observation_shift") == "Y" and not is_payment_delay
    shifted_int = deal["shifted_interest"] == "Y"
    if shifted_int:
        eff_start = _shift_business_days_back(p_start, lb, holiday_calendar=rate_holiday_calendar)
        eff_end   = _shift_business_days_back(p_end,   lb, holiday_calendar=rate_holiday_calendar)
    else:
        eff_start, eff_end = p_start, p_end

    # Avoid double-shifting when shifted_interest is enabled
    if shifted_int:
        obs_start, obs_end = eff_start, eff_end
    else:
        obs_start = _shift_business_days_back(eff_start, lb, holiday_calendar=rate_holiday_calendar)
        obs_end   = _shift_business_days_back(eff_end,   lb, holiday_calendar=rate_holiday_calendar)
    interest_days = _interest_period_days(p_start, p_end)
    observation_days = _observation_period_days(obs_start, obs_end)
    accrual   = _selected_accrual_days(deal, interest_days, observation_days)

    product    = 1.0
    daily_rows = []

    for row_info in _aligned_business_days(
            eff_start, eff_end, obs_start, obs_end,
            interest_holiday_calendar=period_holiday_calendar,
            observation_holiday_calendar=rate_holiday_calendar,
            use_observation_shift=use_obs_shift):
        if use_obs_shift:
            cal_date, obs_date, wt = row_info
        else:
            cal_date, wt = row_info
            obs_date = _shift_business_days_back(cal_date, lb, holiday_calendar=rate_holiday_calendar)
        is_gf     = _is_good_friday(obs_date)
        row       = _get_rate(conn, obs_date)
        if row:
            raw_sofr_r   = row["sofr_rate"] if hasattr(row, "sofr_rate") else row["sofr_rate"]
            sofr_r, was_floored = _apply_daily_floor_rate(deal, raw_sofr_r)
            daily_factor = round(1.0 + (sofr_r / 100.0) * wt / dc, 15)
            product      = round(product * daily_factor, 15)
            daily_rows.append({
                "date":           cal_date.isoformat(),
                "obs_date":       obs_date.isoformat(),
                "sofr_rate":      sofr_r,
                "raw_sofr_rate":  raw_sofr_r,
                "day_weight":     wt,
                "is_good_friday": is_gf,
                "is_floored":     was_floored,
                "daily_factor":   daily_factor,
                "running_product": product,
            })

    comp_rate = product - 1.0
    ann_rate  = comp_rate * (dc / accrual) if accrual else 0.0
    interest  = deal["notional_amount"] * comp_rate
    return (comp_rate, ann_rate, interest, obs_start, obs_end, accrual,
            daily_rows, interest_days, observation_days)


def _calc_simple_average(conn, deal: dict, p_start: date, p_end: date,
                         dc: int = 360):
    lb        = deal["look_back_days"]
    rate_holiday_calendar = deal_rate_holiday_calendar(deal)
    period_holiday_calendar = deal_period_holiday_calendar(deal)
    is_payment_delay = deal.get("payment_delay") == "Y"
    use_obs_shift = deal.get("observation_shift") == "Y" and not is_payment_delay
    shifted_int = deal.get("shifted_interest") == "Y"
    if shifted_int:
        obs_start, obs_end = p_start, p_end
    else:
        obs_start = _shift_business_days_back(p_start, lb, holiday_calendar=rate_holiday_calendar)
        obs_end   = _shift_business_days_back(p_end,   lb, holiday_calendar=rate_holiday_calendar)
    interest_days = _interest_period_days(p_start, p_end)
    observation_days = _observation_period_days(obs_start, obs_end)
    accrual   = _selected_accrual_days(deal, interest_days, observation_days)

    sum_w, sum_d = 0.0, 0
    daily_rows   = []

    for row_info in _aligned_business_days(
            p_start, p_end, obs_start, obs_end,
            interest_holiday_calendar=period_holiday_calendar,
            observation_holiday_calendar=rate_holiday_calendar,
            use_observation_shift=use_obs_shift):
        if use_obs_shift:
            cal_date, obs_date, wt = row_info
        else:
            cal_date, wt = row_info
            obs_date = _shift_business_days_back(cal_date, lb, holiday_calendar=rate_holiday_calendar)
        is_gf    = _is_good_friday(obs_date)
        row      = _get_rate(conn, obs_date)
        if row:
            raw_sofr_r = row["sofr_rate"] if hasattr(row, "sofr_rate") else row["sofr_rate"]
            sofr_r, was_floored = _apply_daily_floor_rate(deal, raw_sofr_r)
            sum_w    += sofr_r * wt
            sum_d    += wt
            daily_rows.append({
                "date":           cal_date.isoformat(),
                "obs_date":       obs_date.isoformat(),
                "sofr_rate":      sofr_r,
                "raw_sofr_rate":  raw_sofr_r,
                "day_weight":     wt,
                "is_good_friday": is_gf,
                "is_floored":     was_floored,
                "weighted_rate":  sofr_r * wt,
                "is_business_day": True,
            })

    avg_rate = (sum_w / sum_d / 100.0) if sum_d else 0.0
    interest = deal["notional_amount"] * avg_rate * accrual / dc
    return (avg_rate, avg_rate, interest, obs_start, obs_end, accrual,
            daily_rows, interest_days, observation_days)


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
    rate_holiday_calendar = deal_rate_holiday_calendar(deal)

    # Observation dates: period boundaries shifted back by business-day lookback
    obs_start_raw = _shift_business_days_back(p_start, lb, holiday_calendar=rate_holiday_calendar)
    obs_end_raw   = _shift_business_days_back(p_end,   lb, holiday_calendar=rate_holiday_calendar)

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
    interest_days = _interest_period_days(p_start, p_end)
    observation_days = _observation_period_days(obs_start_d, obs_end_d)
    accrual = _selected_accrual_days(deal, interest_days, observation_days)

    if accrual <= 0:
        raise ValueError(
            f"Accrual days = {accrual}. "
            f"Period start {p_start} must be before period end {p_end}."
        )

    # Index return is already the period return for the accrual window.
    index_return = idx_end / idx_start - 1.0
    ann_rate     = index_return * (dc / accrual)
    interest     = deal["notional_amount"] * index_return

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
            idx_start, idx_end,
            interest_days, observation_days)


def _apply_daily_floor_rate(deal: dict, sofr_rate_pct: float) -> tuple[float, bool]:
    floor = deal.get("daily_floor")
    if floor is None:
        return sofr_rate_pct, False
    floor_value = float(floor)
    floored = max(sofr_rate_pct, floor_value)
    return floored, floored != sofr_rate_pct


def _apply_spread(deal: dict, period_rate: float, annual_rate: float,
                  accrual_days: int, dc: int) -> tuple[float, float, float, float, float]:
    """
    Apply spread as an annualized percentage margin.

    All-in annualized rate = benchmark_annualized_rate + spread_rate.
    The same spread is prorated across the accrual period to produce the
    all-in period rate used for interest amount.

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
    interest_period_days = observation_period_days = None

    if method == "Compounded in Arrears":
        cr, ar, ia, obs_s, obs_e, acc, daily_rows, interest_period_days, observation_period_days = _calc_compounded(conn, deal, p_start, p_end, dc)
    elif method == "Simple Average in Arrears":
        cr, ar, ia, obs_s, obs_e, acc, daily_rows, interest_period_days, observation_period_days = _calc_simple_average(conn, deal, p_start, p_end, dc)
    elif method == "SOFR Index":
        cr, ar, ia, obs_s, obs_e, acc, daily_rows, index_start, index_end, interest_period_days, observation_period_days = _calc_index(conn, deal, p_start, p_end, dc)
    else:
        raise ValueError(f"Unknown method: {method}")

    spread_pct, spread_rate, all_in_period_rate, all_in_annual_rate, ia = (
        _apply_spread(deal, cr, ar, acc, dc)
    )

    actual_delay = delay_days if deal["payment_delay"] == "Y" else 0
    raw_pay = _shift_business_days_forward(
        payment_date,
        actual_delay,
        holiday_calendar=deal_period_holiday_calendar(deal)
    )
    adj_pay = _next_business_day(
        conn, raw_pay,
        holiday_calendar=deal_period_holiday_calendar(deal)
    )
    rounded_rate = round(all_in_annual_rate, 7)
    result_rounding_decimals = 7
    period_days_for_interest = interest_period_days if interest_period_days is not None else acc
    ia = (
        deal["notional_amount"] * rounded_rate * (period_days_for_interest / dc)
        if period_days_for_interest else 0.0
    )

    result = {
        "cusip":               cusip,
        "deal_name":           deal["deal_name"],
        "client_name":         deal["client_name"],
        "notional_amount":     deal["notional_amount"],
        "spread":              spread_pct,
        "daily_floor":         deal.get("daily_floor"),
        "spread_rate":         spread_rate,
        "accrual_day_basis":   deal.get("accrual_day_basis") or "Calendar Days",
        "rate_type":           deal["rate_type"],
        "payment_frequency":   deal["payment_frequency"],
        "calculation_method":  method,
        "observation_shift":   deal["observation_shift"],
        "shifted_interest":    deal["shifted_interest"],
        "payment_delay_flag":  deal["payment_delay"],
        "look_back_days":      deal["look_back_days"],
        "rounding_decimals":   result_rounding_decimals,
        "issue_date":          deal.get("issue_date"),
        "first_payment_date":  deal.get("first_payment_date") or deal.get("start_date"),
        "period_start_date":   p_start,
        "period_end_date":     p_end,
        "obs_start_date":      obs_s,
        "obs_end_date":        obs_e,
        "accrual_days":        acc,
        "interest_period_days": interest_period_days,
        "observation_period_days": observation_period_days,
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
        "rate_holiday_calendar": deal_rate_holiday_calendar(deal),
        "period_holiday_calendar": deal_period_holiday_calendar(deal),
        "rate_holiday_calendar_label": holiday_calendar_label(deal_rate_holiday_calendar(deal)),
        "period_holiday_calendar_label": holiday_calendar_label(deal_period_holiday_calendar(deal)),
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


def _nearest_prev_bday(d: date,
                       holiday_calendar=DEFAULT_HOLIDAY_CALENDAR) -> date:
    """Roll backward to the nearest business day (Mon-Fri, non-holiday)."""
    holiday_dates = _holiday_dates_for_codes(holiday_calendar)
    while not _is_business_day_in_set(d, holiday_dates):
        d -= timedelta(days=1)
    return d


def _nearest_next_bday(d: date,
                       holiday_calendar=DEFAULT_HOLIDAY_CALENDAR) -> date:
    """Roll forward to the nearest business day (Mon-Fri, non-holiday)."""
    holiday_dates = _holiday_dates_for_codes(holiday_calendar)
    while not _is_business_day_in_set(d, holiday_dates):
        d += timedelta(days=1)
    return d


def _gen_periods(anchor_date: date, maturity: date, freq: str,
                 delay_days: int = 0,
                 holiday_calendar=DEFAULT_HOLIDAY_CALENDAR,
                 *,
                 anchor_is_period_end: bool = False,
                 initial_period_start: date | None = None):
    """
    Generate (period_number, period_start, period_end, payment_date) tuples.

    Convention (ISDA/ARRC):
      Standard deals:
        payment_date[N] = next_bday(anchor_date + (N-1)*months, then + delay_days business days)
        period_start[1] = next_bday(anchor_date - months)
        period_end[N]   = payment_date[N]

      Payment-delay deals:
        period_end[N]   = next_bday(anchor_date + (N-1)*months)
        payment_date[N] = next_bday(period_end[N], then + delay_days business days)
        period_start[1] = initial_period_start

    All intermediate payment dates and period dates are business days.
    The final period end and final payment date are always the onboarded
    maturity date for every deal.
    """
    months        = 1 if freq == "Monthly" else 3
    final_maturity = maturity

    if anchor_is_period_end:
        if initial_period_start is None:
            raise ValueError("initial_period_start is required for payment-delay schedules")
        p1_start = _nearest_next_bday(initial_period_start, holiday_calendar=holiday_calendar)
    else:
        # Period 1 starts one period BEFORE the first payment date
        # e.g. first_payment_date=09-Apr-2024, Quarterly -> period 1 start = 09-Jan-2024
        raw_p1_start = _add_months(anchor_date, -months)
        p1_start = _nearest_next_bday(raw_p1_start, holiday_calendar=holiday_calendar)

    periods  = []
    prev_end = p1_start   # periods must be contiguous on interest boundaries
    num      = 1

    while prev_end < final_maturity:
        # Contiguous interest periods: next period starts exactly where the last one ended
        p_start = prev_end

        if anchor_is_period_end:
            # Payment-delay deals: boundary is anchor_date + months
            raw_end = _add_months(anchor_date, months * (num - 1))
            p_end = _nearest_next_bday(raw_end, holiday_calendar=holiday_calendar)
            if p_end >= final_maturity:
                p_end = final_maturity
        else:
            # Standard deals: boundary is the un-delayed marker
            boundary_date = _add_months(anchor_date, months * (num - 1))
            delayed_pay = _shift_business_days_forward(
                boundary_date,
                delay_days,
                holiday_calendar=holiday_calendar
            )
            pay_date = _nearest_next_bday(delayed_pay, holiday_calendar=holiday_calendar)
            if pay_date >= final_maturity:
                pay_date = final_maturity
            # Period end is equal to the payment date for standard deals
            p_end = pay_date

        if anchor_is_period_end:
            delayed_pay = _shift_business_days_forward(
                p_end,
                delay_days,
                holiday_calendar=holiday_calendar
            )
            pay_date = _nearest_next_bday(delayed_pay, holiday_calendar=holiday_calendar)

        if p_end >= final_maturity:
            p_end = final_maturity
            pay_date = final_maturity

        # Guard: ensure end is strictly after start
        if p_end <= p_start:
            p_end = _nearest_next_bday(
                p_start + timedelta(days=1),
                holiday_calendar=holiday_calendar
            )
            if anchor_is_period_end:
                delayed_pay = _shift_business_days_forward(
                    p_end,
                    delay_days,
                    holiday_calendar=holiday_calendar
                )
                pay_date = _nearest_next_bday(
                    delayed_pay,
                    holiday_calendar=holiday_calendar
                )
            if p_end >= final_maturity:
                p_end = final_maturity
                pay_date = final_maturity

        periods.append((num, p_start, p_end, pay_date))
        if p_end >= final_maturity:
            break
        prev_end = p_end
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
    rate_holiday_calendar = deal_rate_holiday_calendar(deal)
    period_holiday_calendar = deal_period_holiday_calendar(deal)
    first_boundary = date.fromisoformat(
        deal.get("first_payment_date") or deal.get("start_date")
    )
    issue_date = date.fromisoformat(deal["issue_date"])
    d_mat   = date.fromisoformat(deal["maturity_date"])
    is_payment_delay = deal["payment_delay"] == "Y"

    periods = _gen_periods(
        first_boundary,
        d_mat,
        deal["payment_frequency"],
        delay_days=delay,
        holiday_calendar=period_holiday_calendar,
        anchor_is_period_end=is_payment_delay,
        initial_period_start=issue_date if is_payment_delay else None,
    )
    is_index = deal["calculation_method"] == "SOFR Index"
    rows = []
    for num, ps, pe, pay_date in periods:
        # ps  = period start (= previous payment date, deal_start for period 1)
        # pe  = period end   (= payment date boundary; end is EXCLUSIVE)
        # pay_date = adjusted payment date for this period

        # Shifted-interest: effective accrual period shifted back by lookback
        if si:
            eff_ps = _shift_business_days_back(ps, lb, holiday_calendar=rate_holiday_calendar)
            eff_pe = _shift_business_days_back(pe, lb, holiday_calendar=rate_holiday_calendar)
        else:
            eff_ps = ps
            eff_pe = pe

        # Observation window = effective dates shifted back by lookback.
        obs_s = _shift_business_days_back(eff_ps, lb, holiday_calendar=rate_holiday_calendar)
        obs_e = _shift_business_days_back(eff_pe, lb, holiday_calendar=rate_holiday_calendar)

        acc   = _interest_period_days(ps, pe)
        unadj = pay_date   # payment date already adjusted in _gen_periods
        adj   = pay_date

        # Rate Determination Date:
        #   SOFR Index            -> obs_end_date
        #   Compounded/Simple Avg -> next bday after obs_end_date
        if is_index:
            rdd = _nearest_next_bday(obs_e, holiday_calendar=period_holiday_calendar)
        else:
            rdd = _nearest_next_bday(
                obs_e + timedelta(days=1),
                holiday_calendar=period_holiday_calendar
            )

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
        accrual = _interest_period_days(
            date.fromisoformat(row["period_start_date"]),
            date.fromisoformat(row["period_end_date"])
        )
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
        f"""SELECT ps.*, d.calculation_method, d.rate_holiday_calendar
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
        holiday_calendar = row["rate_holiday_calendar"] or DEFAULT_RATE_HOLIDAY_CALENDAR

        ended    = eff_e <= today
        rates_ok = _check_obs_rates_available(
            conn, obs_s, obs_e,
            is_index=is_index,
            holiday_calendar=holiday_calendar
        )
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
    rate_holiday_calendar = normalize_holiday_calendar(d.get("rate_holiday_calendar"))
    period_holiday_calendar = normalize_holiday_calendar(d.get("period_holiday_calendar"))
    holiday_calendar = period_holiday_calendar
    # Support both old start_date and new first_payment_date key names
    fpd = d.get("first_payment_date") or d.get("start_date")
    conn.execute("""
        INSERT INTO deal_master(
            deal_name, client_name, cusip, notional_amount, spread, daily_floor, accrual_day_basis,
            rate_type, payment_frequency, observation_shift,
            shifted_interest, payment_delay, holiday_calendar, rate_holiday_calendar,
            period_holiday_calendar, payment_delay_days,
            rounding_decimals, look_back_days, calculation_method,
            issue_date, first_payment_date, maturity_date, status
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        d["deal_name"], d["client_name"], d["cusip"], d["notional_amount"],
        float(d.get("spread") or 0),
        (float(d["daily_floor"]) if d.get("daily_floor") is not None else None),
        d.get("accrual_day_basis", "Calendar Days"),
        d["rate_type"], d["payment_frequency"], d["observation_shift"],
        d["shifted_interest"], d["payment_delay"], holiday_calendar,
        rate_holiday_calendar, period_holiday_calendar,
        int(d.get("payment_delay_days") or 0),
        d["rounding_decimals"], d["look_back_days"], d["calculation_method"],
        d["issue_date"], fpd, d["maturity_date"], d.get("status", "Active")
    ))


def update_deal(conn, cusip: str, d: dict):
    d = enforce_frequency(d)
    rate_holiday_calendar = normalize_holiday_calendar(d.get("rate_holiday_calendar"))
    period_holiday_calendar = normalize_holiday_calendar(d.get("period_holiday_calendar"))
    holiday_calendar = period_holiday_calendar
    fpd = d.get("first_payment_date") or d.get("start_date")
    existing = conn.execute(
        "SELECT spread, daily_floor, accrual_day_basis, holiday_calendar, rate_holiday_calendar, period_holiday_calendar FROM deal_master WHERE cusip=?",
        (cusip,)
    ).fetchone()
    old_spread = float(existing["spread"] or 0) if existing else 0.0
    old_daily_floor = existing["daily_floor"] if existing else None
    old_accrual_basis = (
        existing["accrual_day_basis"] if existing and existing["accrual_day_basis"]
        else "Calendar Days"
    )
    old_holiday_calendar = (
        existing["holiday_calendar"] if existing and existing["holiday_calendar"]
        else DEFAULT_HOLIDAY_CALENDAR
    )
    old_rate_holiday_calendar = (
        existing["rate_holiday_calendar"] if existing and existing["rate_holiday_calendar"]
        else old_holiday_calendar
    )
    old_period_holiday_calendar = (
        existing["period_holiday_calendar"] if existing and existing["period_holiday_calendar"]
        else old_holiday_calendar
    )
    conn.execute("""
        UPDATE deal_master SET
            deal_name=?, client_name=?, notional_amount=?, spread=?, daily_floor=?, accrual_day_basis=?,
            rate_type=?, payment_frequency=?, observation_shift=?,
            shifted_interest=?, payment_delay=?, holiday_calendar=?, rate_holiday_calendar=?,
            period_holiday_calendar=?, payment_delay_days=?,
            rounding_decimals=?, look_back_days=?, calculation_method=?,
            issue_date=?, first_payment_date=?, maturity_date=?, status=?,
            modified_at=datetime('now')
        WHERE cusip=?
    """, (
        d["deal_name"], d["client_name"], d["notional_amount"],
        float(d.get("spread") or 0),
        (float(d["daily_floor"]) if d.get("daily_floor") is not None else None),
        d.get("accrual_day_basis", "Calendar Days"),
        d["rate_type"], d["payment_frequency"], d["observation_shift"],
        d["shifted_interest"], d["payment_delay"], holiday_calendar,
        rate_holiday_calendar, period_holiday_calendar,
        int(d.get("payment_delay_days") or 0),
        d["rounding_decimals"], d["look_back_days"], d["calculation_method"],
        d["issue_date"], fpd, d["maturity_date"], d.get("status", "Active"),
        cusip
    ))
    new_spread = float(d.get("spread") or 0)
    new_daily_floor = d.get("daily_floor")
    new_accrual_basis = d.get("accrual_day_basis", "Calendar Days")
    if (
        normalize_holiday_calendar(old_holiday_calendar) != holiday_calendar
        or normalize_holiday_calendar(old_rate_holiday_calendar) != rate_holiday_calendar
        or normalize_holiday_calendar(old_period_holiday_calendar) != period_holiday_calendar
    ):
        generate_schedule(conn, cusip, rebuild=True)
        return recalculate_existing_results(conn, cusip)
    if ((old_daily_floor is None) != (new_daily_floor is None)
            or (old_daily_floor is not None and new_daily_floor is not None
                and abs(float(old_daily_floor) - float(new_daily_floor)) > 1e-12)
            or abs(new_spread - old_spread) > 1e-12
            or new_accrual_basis != old_accrual_basis):
        refresh_schedule_accruals(conn, cusip)
        return recalculate_existing_results(conn, cusip)
    return {"schedule_rows_updated": 0, "log_rows_updated": 0}


def import_deals_from_excel(conn, path: str) -> tuple[int, list[str]]:
    """
    Import deal master rows from Excel.

    Required columns:
    - Deal Name
    - Client Name
    - CUSIP
    - Notional Amount
    - Rate Type
    - Calculation Method
    - Issue Date
    - First Payment Date (or Start Date)
    - Maturity Date

    Optional columns fall back to the same defaults used by the deal dialog.
    """
    import pandas as pd

    df = pd.read_excel(path, header=0)
    raw_cols = list(df.columns)
    norm_cols = [str(c).strip().lower().replace("_", " ") for c in raw_cols]

    if not raw_cols:
        raise ValueError("The selected Excel file does not contain any columns.")

    rate_types = ("SOFR", "SOFR Index")
    frequencies = ("Monthly", "Quarterly")
    methods = (
        "Compounded in Arrears",
        "Simple Average in Arrears",
        "SOFR Index",
    )

    def _norm_text(value) -> str:
        return str(value or "").strip()

    def _pick_col(*names):
        choices = {name.strip().lower().replace("_", " ") for name in names}
        for i, col in enumerate(norm_cols):
            if col in choices:
                return raw_cols[i]
        return None

    def _cell(row, *names, default=None):
        col = _pick_col(*names)
        if col is None:
            return default
        value = row.get(col, default)
        if pd.isna(value):
            return default
        return value

    def _parse_text(row, *names, default="", required=False):
        value = _norm_text(_cell(row, *names, default=default))
        if required and not value:
            raise ValueError(f"{names[0]} is required")
        return value

    def _parse_float(row, *names, default=0.0, required=False):
        value = _cell(row, *names, default=default)
        if value is None or (isinstance(value, str) and not value.strip()):
            value = default
        if pd.isna(value):
            value = default
        if required and value == default and default in ("", None):
            raise ValueError(f"{names[0]} is required")
        try:
            return float(str(value).replace(",", "").replace("$", "").replace("%", "").strip())
        except Exception as e:
            raise ValueError(f"{names[0]} must be numeric") from e

    def _parse_int(row, *names, default=0):
        value = _cell(row, *names, default=default)
        if value is None or (isinstance(value, str) and not value.strip()) or pd.isna(value):
            return int(default)
        try:
            return int(float(str(value).replace(",", "").strip()))
        except Exception as e:
            raise ValueError(f"{names[0]} must be a whole number") from e

    def _parse_date(row, *names, required=False):
        value = _cell(row, *names, default=None)
        if value is None or (isinstance(value, str) and not value.strip()) or pd.isna(value):
            if required:
                raise ValueError(f"{names[0]} is required")
            return None
        try:
            return pd.to_datetime(value, errors="raise").date().isoformat()
        except Exception as e:
            raise ValueError(f"{names[0]} must be a valid date") from e

    def _parse_choice(row, names, allowed, default):
        value = _cell(row, *names, default=default)
        text = _norm_text(value)
        if not text:
            return default
        allowed_map = {option.upper(): option for option in allowed}
        resolved = allowed_map.get(text.upper())
        if not resolved:
            raise ValueError(f"{names[0]} must be one of: {', '.join(allowed)}")
        return resolved

    def _parse_yn(row, *names, default="N"):
        value = _cell(row, *names, default=default)
        text = _norm_text(value).upper()
        if not text:
            return default
        truthy = {"Y", "YES", "TRUE", "1"}
        falsy = {"N", "NO", "FALSE", "0"}
        if text in truthy:
            return "Y"
        if text in falsy:
            return "N"
        raise ValueError(f"{names[0]} must be Y or N")

    inserted, errors = 0, []
    seen_cusips: set[str] = set()

    for idx, row in df.iterrows():
        if all(pd.isna(v) or not str(v).strip() for v in row.tolist()):
            continue
        try:
            cusip = _parse_text(row, "CUSIP", required=True).upper()
            if len(cusip) != 9:
                raise ValueError("CUSIP must be exactly 9 characters")
            if cusip in seen_cusips:
                raise ValueError(f"Duplicate CUSIP {cusip} in upload file")
            seen_cusips.add(cusip)

            rate_type = _parse_choice(
                row, ("Rate Type",), rate_types, "SOFR"
            )
            calculation_method = _parse_choice(
                row, ("Calculation Method", "Method"), methods, "Compounded in Arrears"
            )
            payment_delay = _parse_yn(row, "Payment Delay", default="N")
            issue_date = _parse_date(row, "Issue Date", required=True)
            first_payment_date = _parse_date(
                row,
                "First Payment Date",
                "Period End Date",
                "Start Date",
                required=True,
            )
            maturity_date = _parse_date(row, "Maturity Date", required=True)
            notional_amount = _parse_float(row, "Notional Amount", "Notional", required=True, default="")
            if notional_amount <= 0:
                raise ValueError("Notional Amount must be greater than zero")

            boundary_label = "Period End Date" if payment_delay == "Y" else "First Payment Date"
            if issue_date > first_payment_date:
                raise ValueError(f"Issue Date must be on or before {boundary_label}")
            if first_payment_date >= maturity_date:
                raise ValueError(f"Maturity Date must be after {boundary_label}")

            deal = {
                "deal_name": _parse_text(row, "Deal Name", required=True),
                "client_name": _parse_text(row, "Client Name", required=True),
                "cusip": cusip,
                "notional_amount": notional_amount,
                "spread": _parse_float(row, "Spread", default=0.0),
                "daily_floor": (
                    _parse_float(row, "Daily Floor", default=0.0)
                    if _norm_text(_cell(row, "Daily Floor", default=""))
                    else None
                ),
                "rate_type": rate_type,
                "payment_frequency": _parse_choice(
                    row, ("Payment Frequency", "Frequency"), frequencies, "Quarterly"
                ),
                "calculation_method": calculation_method,
                "observation_shift": _parse_yn(row, "Observation Shift", default="N"),
                "shifted_interest": _parse_yn(row, "Shifted Interest", default="N"),
                "payment_delay": payment_delay,
                "payment_delay_days": (
                    _parse_int(row, "Payment Delay Days", "Delay Days", default=0)
                    if payment_delay == "Y" else 0
                ),
                "look_back_days": _parse_int(row, "Look Back Days", "Lookback", default=2),
                "accrual_day_basis": _parse_choice(
                    row, ("Accrual Day Basis", "Accrual Basis", "Accrual Days Basis"),
                    ("Calendar Days", "Observation Period Days"),
                    "Calendar Days",
                ),
                "holiday_calendar": normalize_holiday_calendar(
                    _cell(row, "Holiday Calendar", default=DEFAULT_HOLIDAY_CALENDAR)
                ),
                "rate_holiday_calendar": normalize_holiday_calendar(
                    _cell(row, "Rate Holiday Calendar", "Rate Holidays", default=_cell(row, "Holiday Calendar", default=DEFAULT_HOLIDAY_CALENDAR))
                ),
                "period_holiday_calendar": normalize_holiday_calendar(
                    _cell(row, "Period Holiday Calendar", "Period Holidays", default=_cell(row, "Holiday Calendar", default=DEFAULT_HOLIDAY_CALENDAR))
                ),
                "rounding_decimals": _parse_int(row, "Rounding Decimals", "Rounding", default=7),
                "issue_date": issue_date,
                "first_payment_date": first_payment_date,
                "maturity_date": maturity_date,
                "status": _parse_choice(
                    row, ("Status",), ("Active", "Inactive", "Matured"), "Active"
                ),
            }
            if deal["shifted_interest"] == "Y" and deal["observation_shift"] == "N":
                raise ValueError("Shifted Interest = Y requires Observation Shift = Y")
            insert_deal(conn, deal)
            inserted += 1
        except Exception as e:
            errors.append(f"Row {idx + 2}: {e}")

    if inserted == 0 and not errors:
        raise ValueError("No deal rows were found in the selected Excel file.")

    return inserted, errors


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

def _ny_fed_get_json(path: str, params: dict | None = None) -> dict:
    clean_params = {k: v for k, v in (params or {}).items() if v is not None}
    query = f"?{urlencode(clean_params)}" if clean_params else ""
    url = f"{NY_FED_API_BASE}{path}{query}"
    req = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "SOFR-Interest-Calculator/1.0",
        },
    )
    with urlopen(req, timeout=20) as resp:
        payload = resp.read().decode("utf-8")
    return json.loads(payload)


def _ny_fed_rate_day_count_factor(rate_date: date) -> int:
    # Keep the app's current convention used by manual and Excel imports.
    return 3 if rate_date.weekday() == 4 else 1


def _fetch_ny_fed_series(conn, ratetype: str, table: str,
                         start_date: date | None = None,
                         end_date: date | None = None) -> int:
    if start_date and end_date and start_date > end_date:
        return 0

    if start_date is None:
        response = _ny_fed_get_json(f"/{ratetype}/last/1.json")
    else:
        response = _ny_fed_get_json(
            f"/{ratetype}/search.json",
            params={
                "startDate": start_date.isoformat(),
                "endDate": (end_date or date.today()).isoformat(),
                "type": "rate" if ratetype == "sofr" else None,
            },
        )

    rows = response.get("refRates", []) or []
    inserted = 0
    for row in rows:
        eff_date = date.fromisoformat(row["effectiveDate"])
        if table == "sofr_rates":
            conn.execute("""
                INSERT OR REPLACE INTO sofr_rates(rate_date, sofr_rate, day_count_factor)
                VALUES (?, ?, ?)
            """, (
                eff_date.isoformat(),
                float(row["percentRate"]),
                _ny_fed_rate_day_count_factor(eff_date),
            ))
        else:
            conn.execute("""
                INSERT OR REPLACE INTO sofr_index(rate_date, sofr_index)
                VALUES (?, ?)
            """, (
                eff_date.isoformat(),
                float(row["index"]),
            ))
        inserted += 1
    return inserted


def fetch_ny_fed_sofr_updates(conn, today: date | None = None) -> dict:
    """
    Fetch incremental SOFR overnight rates and SOFR index values from the
    New York Fed Markets Data API.
    """
    as_of = today or date.today()
    latest_rate = conn.execute(
        "SELECT MAX(rate_date) AS mx FROM sofr_rates"
    ).fetchone()["mx"]
    latest_index = conn.execute(
        "SELECT MAX(rate_date) AS mx FROM sofr_index"
    ).fetchone()["mx"]

    rate_start = (
        date.fromisoformat(latest_rate) + timedelta(days=1)
        if latest_rate else None
    )
    index_start = (
        date.fromisoformat(latest_index) + timedelta(days=1)
        if latest_index else None
    )

    rates_inserted = _fetch_ny_fed_series(
        conn, "sofr", "sofr_rates", start_date=rate_start, end_date=as_of
    )
    index_inserted = _fetch_ny_fed_series(
        conn, "sofrai", "sofr_index", start_date=index_start, end_date=as_of
    )

    latest_rate_after = conn.execute(
        "SELECT MAX(rate_date) AS mx FROM sofr_rates"
    ).fetchone()["mx"]
    latest_index_after = conn.execute(
        "SELECT MAX(rate_date) AS mx FROM sofr_index"
    ).fetchone()["mx"]

    return {
        "rates_inserted": rates_inserted,
        "index_inserted": index_inserted,
        "latest_rate_date": latest_rate_after,
        "latest_index_date": latest_index_after,
    }


def import_rates_from_excel(conn, path: str) -> tuple[int, list[str]]:
    """
    Import SOFR overnight rates from Excel into the sofr_rates table only.

    Accepted layouts:
    1. NY Fed SOFR download with headers such as DATE / BENCHMARK NAME / RATE (%)
    2. Simplified file with two columns such as Date / Rate (%)
    """
    import pandas as pd

    df = pd.read_excel(path, header=0)
    raw_cols = list(df.columns)
    norm_cols = [str(c).strip().lower().replace("_", " ") for c in raw_cols]

    def _pick_col(predicate):
        for i, col in enumerate(norm_cols):
            if predicate(col):
                return raw_cols[i]
        return None

    date_col = _pick_col(
        lambda c: c in {"date", "effective date", "effective date "}
        or c.startswith("effective date")
    )
    rate_col = _pick_col(
        lambda c: c in {"rate (%)", "rate(%)", "sofr rate (%)", "sofr rate"}
        or c.startswith("rate (%)")
    )
    if rate_col is None:
        rate_col = _pick_col(
            lambda c: (
                "rate" in c
                and "type" not in c
                and "target" not in c
                and "intraday" not in c
                and "30-day" not in c
                and "90-day" not in c
                and "180-day" not in c
                and "index" not in c
            )
        )
    benchmark_col = _pick_col(lambda c: c in {"benchmark name", "benchmark", "rate type"})

    if not date_col or not rate_col:
        raise ValueError(
            f"Cannot find Date and Rate columns. Found columns: {raw_cols}. "
            "Expected a file with columns like Date and Rate (%)."
        )

    rename_map = {date_col: "_date", rate_col: "_rate"}
    selected_cols = [date_col, rate_col]
    if benchmark_col:
        selected_cols.append(benchmark_col)
        rename_map[benchmark_col] = "_benchmark"

    work_df = df[selected_cols].copy()
    work_df = work_df.rename(columns=rename_map)
    work_df = work_df.dropna(subset=["_date", "_rate"])

    if "_benchmark" in work_df.columns:
        benchmark_values = work_df["_benchmark"].astype(str).str.strip().str.upper()
        sofr_mask = benchmark_values.isin({"SOFR", "SOFR RATE", "SECURED OVERNIGHT FINANCING RATE"})
        if sofr_mask.any():
            work_df = work_df[sofr_mask]

    inserted, errors = 0, []
    for _, row in work_df.iterrows():
        try:
            d = pd.to_datetime(row["_date"], errors="raise").date()
            rate_text = str(row["_rate"]).replace(",", "").replace("%", "").strip()
            rate = float(rate_text)
            dcf = 3 if d.weekday() == 4 else 1
            conn.execute("""
                INSERT OR REPLACE INTO sofr_rates
                    (rate_date, sofr_rate, day_count_factor)
                VALUES (?, ?, ?)
            """, (d.isoformat(), rate, dcf))
            inserted += 1
        except Exception as e:
            errors.append(f"{row.get('_date', '?')}: {e}")

    if inserted == 0 and not errors:
        raise ValueError("No valid SOFR rate rows were found in the selected Excel file.")

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
    Import SOFR Index values from Excel into the sofr_index table only.

    Accepted layouts:
    1. NY Fed SOFR Averages & Index download with headers such as
       DATE / BENCHMARK NAME / INDEX
    2. Simplified file with two columns such as Date / Index
    """
    import pandas as pd

    df = pd.read_excel(path, header=0)
    raw_cols = list(df.columns)
    norm_cols = [str(c).strip().lower().replace("_", " ") for c in raw_cols]

    def _pick_col(predicate):
        for i, col in enumerate(norm_cols):
            if predicate(col):
                return raw_cols[i]
        return None

    date_col = _pick_col(lambda c: c in {"date", "effective date"} or "date" in c)
    index_col = _pick_col(
        lambda c: c in {"index", "sofr index"} or ("index" in c and "percent" not in c)
    )
    benchmark_col = _pick_col(lambda c: c in {"benchmark name", "benchmark", "rate type", "type"})

    if not date_col or not index_col:
        raise ValueError(
            f"Cannot find Date or Index column. "
            f"Found: {raw_cols}. "
            "Expected columns: Date, SOFR Index (or similar)."
        )

    rename_map = {date_col: "_date", index_col: "_index"}
    selected_cols = [date_col, index_col]
    if benchmark_col:
        selected_cols.append(benchmark_col)
        rename_map[benchmark_col] = "_benchmark"

    work_df = df[selected_cols].copy()
    work_df = work_df.rename(columns=rename_map)
    work_df["_date"] = pd.to_datetime(work_df["_date"], errors="coerce").dt.date
    work_df = work_df.dropna(subset=["_date", "_index"])

    if "_benchmark" in work_df.columns:
        benchmark_values = work_df["_benchmark"].astype(str).str.strip().str.upper()
        index_mask = benchmark_values.isin({
            "SOFRAI",
            "SOFR INDEX",
            "SOFR COMPOUNDED INDEX",
            "SOFR AVERAGES AND INDEX",
        })
        if index_mask.any():
            work_df = work_df[index_mask]

    inserted, errors = 0, []
    for _, row in work_df.iterrows():
        try:
            d = row["_date"]
            idx_text = str(row["_index"]).replace(",", "").strip()
            idx = float(idx_text)
            conn.execute("""
                INSERT OR REPLACE INTO sofr_index (rate_date, sofr_index)
                VALUES (?, ?)
            """, (d.isoformat(), idx))
            inserted += 1
        except Exception as e:
            errors.append(f"{row.get('_date','?')}: {e}")

    if inserted == 0 and not errors:
        raise ValueError("No valid SOFR index rows were found in the selected Excel file.")

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
