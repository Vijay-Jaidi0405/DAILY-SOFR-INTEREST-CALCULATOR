"""
seed_deals.py
-------------
Seeds 300 synthetic deals into the SQLite database covering
all valid field combinations.

Start dates and maturity dates are random business days within
their respective months — never falling on weekends or US SIFMA
holidays.

Usage:
    python seed_deals.py
"""

import sys
import random
import calendar
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from core.database import init_db, get_conn, insert_deal, FREQ_RULES, SEED_HOLIDAYS

# ---------------------------------------------------------------------------
# Reproducible randomness — same seed gives same 300 deals every time
# ---------------------------------------------------------------------------
RNG = random.Random(20240101)

# ---------------------------------------------------------------------------
# Combination matrix — 6 valid flag combos
# ---------------------------------------------------------------------------
COMBOS = [
    # rate_type,     calc_method,                   obs_shift, shifted_int
    ("SOFR",       "Compounded in Arrears",          "N",       "N"),
    ("SOFR",       "Compounded in Arrears",          "Y",       "N"),
    ("SOFR",       "Compounded in Arrears",          "Y",       "Y"),
    ("SOFR",       "Simple Average in Arrears",      "N",       "N"),
    ("SOFR",       "Simple Average in Arrears",      "Y",       "N"),
    ("SOFR Index", "SOFR Index",                     "Y",       "N"),
]

CLIENTS = [
    "BlackRock", "Vanguard", "Fidelity", "JP Morgan", "Goldman Sachs",
    "Morgan Stanley", "Citibank", "Wells Fargo", "PIMCO", "T. Rowe Price",
    "State Street", "Invesco", "AllianceBernstein", "Nuveen", "Federated Hermes",
]
PREFIXES = [
    "Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta",
    "Iota", "Kappa", "Lambda", "Mu", "Nu", "Xi", "Omicron", "Pi",
    "Rho", "Sigma", "Tau", "Upsilon",
]
SUFFIXES = [
    "Growth Fund", "Capital", "Fixed Income", "Yield", "Bond Trust",
    "Credit", "Structured", "Leveraged", "CLO", "ABS", "MBS", "CDO",
    "Credit Link", "Structured Note", "TRS", "Rate Swap",
    "Basis", "Floating", "Variable", "Hybrid",
]
NOTIONALS = [1e6, 5e6, 10e6, 25e6, 50e6, 100e6, 250e6]
LOOKBACKS = [0, 1, 2, 5]
ROUNDINGS = [4, 5, 6]
TENORS    = [2, 3, 4, 5, 7, 10]
CHAR_SET  = "ABCDEFGHJKLMNPQRSTUVWXYZ0123456789"

# ---------------------------------------------------------------------------
# Holiday set — loaded from SEED_HOLIDAYS constant (same as DB seeds)
# ---------------------------------------------------------------------------
HOLIDAYS: set[date] = {date.fromisoformat(h[0]) for h in SEED_HOLIDAYS}


# ---------------------------------------------------------------------------
# Business day helpers
# ---------------------------------------------------------------------------

def _is_business_day(d: date) -> bool:
    """Return True if d is Mon–Fri and not a US SIFMA holiday."""
    return d.weekday() < 5 and d not in HOLIDAYS


def _business_days_in_month(year: int, month: int) -> list[date]:
    """Return all business days in the given month."""
    last = calendar.monthrange(year, month)[1]
    return [
        date(year, month, day)
        for day in range(1, last + 1)
        if _is_business_day(date(year, month, day))
    ]


def _random_business_day_in_month(year: int, month: int) -> date:
    """Pick a random business day within the month."""
    bdays = _business_days_in_month(year, month)
    if not bdays:
        # Fallback: shouldn't happen for any real month
        return date(year, month, 1)
    return RNG.choice(bdays)


def _next_business_day(d: date) -> date:
    """Roll forward to the next business day if d is not one."""
    while not _is_business_day(d):
        d += timedelta(days=1)
    return d


def _prev_business_day(d: date) -> date:
    """Roll backward to the nearest business day if d is not one."""
    while not _is_business_day(d):
        d -= timedelta(days=1)
    return d


# ---------------------------------------------------------------------------
# CUSIP generator
# ---------------------------------------------------------------------------

def _base34(n: int, width: int = 6) -> str:
    result = []
    for _ in range(width):
        result.append(CHAR_SET[n % 34])
        n //= 34
    return "".join(reversed(result))


def _cusip(i: int) -> str:
    prefix = ["SFR", "SFA", "SFB"][(i - 1) // 100]
    return prefix + _base34(i, 6)


# ---------------------------------------------------------------------------
# Deal builder
# ---------------------------------------------------------------------------

def build_deals() -> list[dict]:
    deals = []

    for i in range(1, 301):
        combo = COMBOS[(i - 1) % 6]
        rate_type, calc_method, obs_shift, shifted_int = combo

        client    = CLIENTS[(i - 1) % len(CLIENTS)]
        prefix    = PREFIXES[(i - 1) % len(PREFIXES)]
        suffix    = SUFFIXES[((i - 1) // len(PREFIXES)) % len(SUFFIXES)]
        deal_name = f"{prefix} {suffix} {i:03d}"
        notional  = NOTIONALS[(i - 1) % len(NOTIONALS)]
        freq      = FREQ_RULES.get(calc_method, "Quarterly")
        pay_delay = "Y" if (i % 4) in (1, 2) else "N"
        lookback  = LOOKBACKS[(i - 1) % len(LOOKBACKS)]
        rounding  = ROUNDINGS[(i - 1) % len(ROUNDINGS)]
        tenor     = TENORS[(i - 1) % len(TENORS)]

        # ── Start date: random business day in a spread of months ────────────
        # Spread deals across 2020-2024, cycling through months
        start_year  = 2020 + ((i - 1) % 5)
        start_month = ((i - 1) % 12) + 1
        start_date  = _random_business_day_in_month(start_year, start_month)

        # ── Maturity date: start + tenor years, land on a business day ───────
        mat_year  = start_date.year  + tenor
        mat_month = start_date.month
        # Same month/day as start but tenor years later; adjust to business day
        try:
            raw_mat = date(mat_year, mat_month, start_date.day)
        except ValueError:
            # Feb 29 → Feb 28 in non-leap year
            raw_mat = date(mat_year, mat_month,
                           calendar.monthrange(mat_year, mat_month)[1])
        maturity_date = _next_business_day(raw_mat)

        deals.append({
            "deal_name":          deal_name,
            "client_name":        client,
            "cusip":              _cusip(i),
            "notional_amount":    notional,
            "rate_type":          rate_type,
            "payment_frequency":  freq,
            "observation_shift":  obs_shift,
            "shifted_interest":   shifted_int,
            "payment_delay":      pay_delay,
            "payment_delay_days": 2 if pay_delay == "Y" else 0,
            "rounding_decimals":  rounding,
            "look_back_days":     lookback,
            "calculation_method": calc_method,
            "first_payment_date": first_payment_date.isoformat(),
            "maturity_date":      maturity_date.isoformat(),
            "status":             "Active",
        })

    return deals


# ---------------------------------------------------------------------------
# Verification helpers
# ---------------------------------------------------------------------------

def _verify_deals(deals: list[dict]) -> list[str]:
    """Sanity-check every deal. Return list of error messages."""
    errors = []
    seen_cusips = set()

    for d in deals:
        c = d["cusip"]

        # Duplicate CUSIP
        if c in seen_cusips:
            errors.append(f"{c}: duplicate CUSIP")
        seen_cusips.add(c)

        start = date.fromisoformat(d["start_date"])
        mat   = date.fromisoformat(d["maturity_date"])

        # Maturity must be after start
        if mat <= start:
            errors.append(f"{c}: maturity {mat} not after start {start}")

        # Start must be a business day
        if not _is_business_day(start):
            errors.append(f"{c}: start {start} is not a business day "
                          f"(weekday={start.weekday()})")

        # Maturity must be a business day
        if not _is_business_day(mat):
            errors.append(f"{c}: maturity {mat} is not a business day "
                          f"(weekday={mat.weekday()})")

        # SOFR Index must use SOFR Index method
        if d["rate_type"] == "SOFR Index" and d["calculation_method"] != "SOFR Index":
            errors.append(f"{c}: SOFR Index rate type with wrong method")

        # Frequency rule
        expected_freq = FREQ_RULES.get(d["calculation_method"])
        if expected_freq and d["payment_frequency"] != expected_freq:
            errors.append(f"{c}: {d['calculation_method']} should be "
                          f"{expected_freq}, got {d['payment_frequency']}")

    return errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Initialising database…")
    init_db()

    print("Building deals…")
    deals = build_deals()

    # Verify before inserting
    print("Verifying deals…")
    errs = _verify_deals(deals)
    if errs:
        print(f"  {len(errs)} verification errors:")
        for e in errs[:10]:
            print(f"    {e}")
        ans = input("Continue anyway? (y/N): ").strip().lower()
        if ans != "y":
            print("Aborted.")
            return
    else:
        print(f"  All {len(deals)} deals pass verification.")

    # Show a sample
    print("\nSample start/maturity dates (first 5 deals):")
    for d in deals[:5]:
        start = date.fromisoformat(d["start_date"])
        mat   = date.fromisoformat(d["maturity_date"])
        bday_s = "bday" if _is_business_day(start) else "NOT-bday"
        bday_m = "bday" if _is_business_day(mat)   else "NOT-bday"
        print(f"  {d['cusip']}  start={start} [{bday_s}]  "
              f"mat={mat} [{bday_m}]  {d['calculation_method'][:20]}")

    # ── Insert ───────────────────────────────────────────────────────────────
    print(f"\nReady to insert {len(deals)} deals.")
    with get_conn() as conn:
        ans = input("Clear existing deals before seeding? (y/N): ").strip().lower()
        if ans == "y":
            conn.execute("DELETE FROM calculation_log")
            conn.execute("DELETE FROM payment_schedule")
            conn.execute("DELETE FROM deal_master")
            print("Cleared existing deals.")

        inserted = 0
        skipped  = 0
        insert_errors = []

        for d in deals:
            try:
                insert_deal(conn, d)
                inserted += 1
            except Exception as e:
                if "UNIQUE constraint" in str(e):
                    skipped += 1
                else:
                    insert_errors.append(f"{d['cusip']}: {e}")

    print(f"\nDone.")
    print(f"  Inserted : {inserted}")
    print(f"  Skipped  : {skipped} (already exist)")
    print(f"  Errors   : {len(insert_errors)}")
    if insert_errors:
        for e in insert_errors[:10]:
            print(f"    {e}")

    # ── Auto-mature ──────────────────────────────────────────────────────────
    with get_conn() as conn:
        matured = conn.execute(
            "UPDATE deal_master SET status='Matured', modified_at=datetime('now')"
            " WHERE status='Active' AND maturity_date < date('now')"
        ).rowcount
    if matured:
        print(f"\nMarked {matured} deals as Matured (past maturity date).")

    # ── Verification report ──────────────────────────────────────────────────
    with get_conn() as conn:
        total    = conn.execute("SELECT COUNT(*) FROM deal_master").fetchone()[0]
        by_method = conn.execute("""
            SELECT calculation_method, COUNT(*) AS cnt
            FROM deal_master GROUP BY calculation_method ORDER BY cnt DESC
        """).fetchall()
        weekend_starts = conn.execute("""
            SELECT COUNT(*) FROM deal_master
            WHERE CAST(strftime('%w', start_date) AS INTEGER) IN (0,6)
        """).fetchone()[0]
        weekend_mats = conn.execute("""
            SELECT COUNT(*) FROM deal_master
            WHERE CAST(strftime('%w', maturity_date) AS INTEGER) IN (0,6)
        """).fetchone()[0]

    print(f"\nDatabase now contains {total} deals:")
    for row in by_method:
        print(f"  {row[0]:<35} {row[1]}")
    print(f"\n  Weekend start dates   : {weekend_starts}  (should be 0)")
    print(f"  Weekend maturity dates: {weekend_mats}  (should be 0)")


if __name__ == "__main__":
    main()
