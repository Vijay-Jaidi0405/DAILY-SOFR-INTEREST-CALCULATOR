"""ui/pages/history.py — Calculation history and audit log."""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QComboBox, QLineEdit, QDateEdit,
    QMessageBox
)
from PySide6.QtCore import Qt, QDate, QTimer
from ui.widgets.common import (
    Panel, PageHeader, DataTable,
    fmt_money, fmt_rate, fmt_date, make_date_item
)
from ui.styles import GREEN, PURPLE, ACCENT


class HistoryPage(QWidget):
    def __init__(self, db_factory, parent=None):
        super().__init__(parent)
        self._db = db_factory
        self._build_ui()

    def _build_ui(self):
        lay = QVBoxLayout(self)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        lay.addWidget(PageHeader("Calculation History",
            "Audit log of all interest calculations"))

        # Filters
        filt_panel = Panel("Filters")
        filt_lay = QHBoxLayout()

        self._cusip_filt = QLineEdit()
        self._cusip_filt.setPlaceholderText("Filter by CUSIP…")
        self._cusip_filt.setMaximumWidth(180)

        self._batch_filt = QLineEdit()
        self._batch_filt.setPlaceholderText("Filter by Batch ID…")
        self._batch_filt.setMaximumWidth(280)

        self._method_filt = QComboBox()
        self._method_filt.addItems([
            "All Methods",
            "Compounded in Arrears",
            "Simple Average in Arrears",
            "SOFR Index",
        ])

        self._limit = QComboBox()
        self._limit.addItems(["100", "200", "500", "All"])

        search_btn = QPushButton("Search")
        search_btn.setObjectName("PrimaryBtn")
        search_btn.clicked.connect(self._load)

        clear_btn = QPushButton("Clear Filters")
        clear_btn.clicked.connect(self._clear)

        filt_lay.addWidget(QLabel("CUSIP:"))
        filt_lay.addWidget(self._cusip_filt)
        filt_lay.addWidget(QLabel("Batch ID:"))
        filt_lay.addWidget(self._batch_filt)
        filt_lay.addWidget(QLabel("Method:"))
        filt_lay.addWidget(self._method_filt)
        filt_lay.addWidget(QLabel("Show:"))
        filt_lay.addWidget(self._limit)
        filt_lay.addWidget(search_btn)
        filt_lay.addWidget(clear_btn)
        filt_lay.addStretch()
        filt_panel.add_layout(filt_lay)
        lay.addWidget(filt_panel)

        # Summary strip
        self._sum_lbl = QLabel("")
        self._sum_lbl.setStyleSheet("color:#6B7280; font-size:12px;")
        lay.addWidget(self._sum_lbl)

        # Table
        cols = [
            "Log ID", "CUSIP", "Deal Name", "Client", "Method",
            "Period Start", "Period End",
            "Obs Start", "Obs End",
            "Accrual Days", "Day Count",
            "Comp Rate", "Ann Rate", "Rounded Rate",
            "Interest Amount", "Pay Date", "Adj Pay Date",
            "Batch ID", "Calculated At"
        ]
        self._tbl = DataTable(cols)
        lay.addWidget(self._tbl, 1)

    def _load(self):
        from core.database import get_calc_log
        cusip  = self._cusip_filt.text().strip() or None
        lim_t  = self._limit.currentText()
        limit  = int(lim_t) if lim_t != "All" else 9999
        mf     = self._method_filt.currentText()
        batch  = self._batch_filt.text().strip() or None

        with self._db() as conn:
            rows = get_calc_log(conn, cusip=cusip, limit=limit)

        # Client-side filters
        if mf != "All Methods":
            rows = [r for r in rows if r["calculation_method"] == mf]
        if batch:
            rows = [r for r in rows if r.get("batch_id", "") and
                    batch.lower() in (r["batch_id"] or "").lower()]

        total_int = sum(r["interest_amount"] or 0 for r in rows)
        self._sum_lbl.setText(
            f"{len(rows)} records  |  "
            f"Total interest: {fmt_money(total_int)}"
        )

        table_rows = []
        for r in rows:
            rnd = r.get("rounding_decimals") or 7
            bid = r.get("batch_id") or ""
            table_rows.append([
                str(r["log_id"]),
                r["cusip"],
                r.get("deal_name", ""),
                r.get("client_name", ""),
                r["calculation_method"],
                make_date_item(r["period_start_date"]),
                make_date_item(r["period_end_date"]),
                make_date_item(r.get("obs_start_date")),
                make_date_item(r.get("obs_end_date")),
                str(r["accrual_days"]),
                str(r["day_count_basis"]),
                fmt_rate(r.get("compounded_rate")),
                fmt_rate(r.get("annualized_rate")),
                fmt_rate(r.get("rounded_rate"), rnd),
                fmt_money(r["interest_amount"]),
                make_date_item(r.get("payment_date")),
                make_date_item(r.get("adjusted_payment_date")),
                bid[:20] + "…" if len(bid) > 20 else bid,
                r["calculated_at"][:19] if r.get("calculated_at") else "—",
            ])
        self._tbl.populate(table_rows)

    def _clear(self):
        self._cusip_filt.clear()
        self._batch_filt.clear()
        self._method_filt.setCurrentIndex(0)
        self._limit.setCurrentIndex(0)
        self._load()

    def showEvent(self, event):
        super().showEvent(event)
        QTimer.singleShot(0, self._load)
