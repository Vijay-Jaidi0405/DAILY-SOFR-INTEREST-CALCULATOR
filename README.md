# SOFR Interest Calculator
## Python + SQLite + PySide6 Desktop Application

---

## Project Structure

```
sofr_app/
├── main.py                      # Entry point
├── requirements.txt             # Dependencies
├── sofr_calculator.db           # SQLite DB (auto-created on first run)
│
├── core/
│   └── database.py              # All DB logic, schema, calculation engine
│
└── ui/
    ├── styles.py                # Global stylesheet and colour constants
    ├── main_window.py           # Main window with side navigation
    ├── widgets/
    │   └── common.py            # Reusable widgets (KpiCard, DataTable, etc.)
    └── pages/
        ├── dashboard.py         # Dashboard — KPIs + readiness table
        ├── deals.py             # Deal Master — CRUD
        ├── rates.py             # SOFR Rates — Excel import + viewer
        ├── schedule.py          # Payment Schedule — generate + view
        ├── calc_single.py       # Single CUSIP interest calculation
        ├── holidays.py          # Holiday management page
        ├── calc_batch.py        # Batch calculation across multiple deals
        └── history.py           # Calculation audit log
```

---

## Prerequisites

- Python 3.11 or 3.12
- pip

---

## Installation

```bash
# 1. Create and activate a virtual environment
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt
```

---

## Running the App

```bash
python main.py
```

The SQLite database (`sofr_calculator.db`) is created automatically in the
project folder on first launch.

---

## Workflow

### Step 1 — Load SOFR Rates
Go to **SOFR Rates** in the left navigation.
Click **Browse** and select your Excel file.
The file must contain these columns (column names are flexible):

| Column          | Description                          |
|-----------------|--------------------------------------|
| Date            | Rate date (any parseable date format)|
| SOFR Rate (%)   | Annualised SOFR rate, e.g. 5.3000    |
| SOFR Index      | Published SOFR compounded index      |
| Day Count Factor| Optional — 1 Mon-Thu, 3 Fri          |

NY Fed SOFR data: https://www.newyorkfed.org/markets/reference-rates/sofr

Click **Import Rates**. The summary panel updates with count and date range.

---

### Step 2 — Load Deal Master
Go to **Deal Master**.

**Option A — Manual entry:**
Click **+ Add Deal** and fill in the form.

**Option B — Seed synthetic data:**
Run the SQL script `05_SyntheticDeals_300.sql` against the `sofr_calculator.db`
SQLite file using any SQLite client (e.g. DB Browser for SQLite):

```bash
# Using sqlite3 CLI
sqlite3 sofr_calculator.db < 05_SyntheticDeals_300_sqlite.sql
```

See `seed_deals.py` for a Python-based seeder.

---

### Step 3 — Generate Payment Schedules
Go to **Payment Schedule**.

- Select a single deal from the dropdown → click **Generate Schedule**
- Or click **Generate All Schedules** to build schedules for every active deal
  (runs in background thread with progress dialog)

Each period row shows: dates, observation window, adjusted payment date,
status (Scheduled / Ready to Calculate / Calculated).

---

### Step 4 — Check the Dashboard
Go to **Dashboard** and click **Refresh Status**.

The dashboard shows:
- KPI cards: active deals, ready to calculate, scheduled, calculated periods, total interest
- Next-payment table: all deals' upcoming period, colour-coded by readiness
  - **Green** = Ready to Calculate (period ended + SOFR rates available)
  - **White** = Scheduled (future period)
  - **Amber** = Period ended but rates not yet loaded

---

### Step 5 — Calculate Interest

**Single CUSIP:**
Go to **Single CUSIP Calc**.
Select a deal, set the interest period dates, payment date, and delay days.
Click **Calculate Interest**.
Results show compounded/avg rate, annualized rate, rounded rate, interest
amount, adjusted payment date, and the full detail table.
Click **Mark Period as Calculated in Schedule** to update the schedule.

**Batch:**
Go to **Batch Calculation**.
Set period dates and filters (method, eligible-only).
Click **Run Batch Calculation**.
Results show per-deal rates and interest with a running total.
All results are logged to the audit trail automatically.

---

### Step 6 — Review History
Go to **History / Audit**.
Filter by CUSIP, Batch ID, or method.
Every calculation is permanently logged with full inputs and outputs.

---

## Calculation Methods

| Method                    | Rate Type   | Formula                                          |
|---------------------------|-------------|--------------------------------------------------|
| Compounded in Arrears     | SOFR        | ∏(1 + r_i × d_i / DC) − 1                      |
| Simple Average in Arrears | SOFR        | Σ(r_i × d_i) / Σ(d_i)                          |
| SOFR Index                | SOFR Index  | (Index_End / Index_Start − 1) × (DC / Accrual)  |

**Date adjustments:**
- Lookback: obs dates shifted back by `look_back_days`
- Shifted Interest (Y): period start/end also shifted back by `look_back_days`
- Payment Delay (Y): payment date + delay days, rolled to next business day
- Following business day convention using US SIFMA holiday calendar

---

## Valid Field Combinations (enforced by DB constraints)

| Rate Type  | Calc Method               | Obs Shift | Shifted Int |
|------------|---------------------------|-----------|-------------|
| SOFR       | Compounded in Arrears     | N         | N           |
| SOFR       | Compounded in Arrears     | Y         | N           |
| SOFR       | Compounded in Arrears     | Y         | Y           |
| SOFR       | Simple Average in Arrears | N         | N           |
| SOFR       | Simple Average in Arrears | Y         | N           |
| SOFR Index | SOFR Index                | Y         | N           |

---

## Dependencies

| Package    | Version  | Purpose                        |
|------------|----------|--------------------------------|
| PySide6    | >=6.6.0  | Qt6 UI framework               |
| openpyxl   | >=3.1.0  | Excel file reading             |
| pandas     | >=2.0.0  | Excel parsing and data handling|

---

## Database Schema

- **deal_master** — 300 active deals with all flag columns
- **sofr_rates** — Daily SOFR rate and index values
- **market_holidays** — US SIFMA holidays 2024–2028 (pre-seeded)
- **payment_schedule** — Generated period schedules with status tracking
- **calculation_log** — Full audit trail of every calculation run
