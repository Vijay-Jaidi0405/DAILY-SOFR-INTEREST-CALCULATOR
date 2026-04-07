"""ui/pages/dashboard.py — Dashboard with RDD-windowed deal tables."""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QTabWidget, QScrollArea, QSizePolicy
)
from PySide6.QtCore import Qt, QTimer
from ui.widgets.common import (
    KpiCard, Panel, PageHeader, DataTable,
    fmt_money, fmt_rate, fmt_date, make_date_item
)
from ui.styles import GREEN, AMBER, PURPLE, ACCENT, RED


# Column definitions for the deal windows table
_COLS = [
    "CUSIP", "Deal Name", "Client",
    "Method", "Rate Type", "Frequency", "Notional",
    "Period Start", "Period End",
    "Obs End", "Rate Det. Date", "Payment Date",
    "Days to RDD", "Status"
]

_METH_COLORS = {
    "Compounded in Arrears":     "#E1F5EE",
    "Simple Average in Arrears": "#EDE9FE",
    "SOFR Index":                "#FFF3E0",
}


def _row_color(r: dict) -> str:
    if r.get("is_calc_eligible_today"):
        return "#DCFCE7"   # green tint — eligible now
    base = _METH_COLORS.get(r.get("calculation_method", ""), "#FFFFFF")
    return base


def _rows_from_deals(deals: list[dict]) -> tuple[list, list]:
    rows, colors = [], []
    for r in deals:
        rows.append([
            r.get("cusip", ""),
            r.get("deal_name", ""),
            r.get("client_name", ""),
            r.get("calculation_method", ""),
            r.get("rate_type", ""),
            r.get("payment_frequency", ""),
            fmt_money(r.get("notional_amount")),
            make_date_item(r.get("period_start_date")),
            make_date_item(r.get("period_end_date")),
            make_date_item(r.get("obs_end_date")),
            make_date_item(r.get("rate_determination_date")),
            make_date_item(r.get("adj_payment_date")),
            str(r.get("days_to_rdd", "—")),
            r.get("period_status", ""),
        ])
        colors.append(_row_color(r))
    return rows, colors


class DashboardPage(QWidget):
    def __init__(self, db_factory, parent=None):
        super().__init__(parent)
        self._db = db_factory
        self._build_ui()

    def _build_ui(self):
        outer = QVBoxLayout(self)
        outer.setContentsMargins(20, 16, 20, 16)
        outer.setSpacing(14)

        # ── Header row ───────────────────────────────────────────────────────
        hdr_row = QHBoxLayout()
        hdr_row.setSpacing(10)
        hdr_row.addWidget(PageHeader(
            "Dashboard",
            "Deals due for calculation today, this week, and this month — "
            "based on Rate Determination Date"
        ))
        hdr_row.addStretch()
        self._refresh_btn = QPushButton("Refresh")
        self._refresh_btn.setObjectName("PrimaryBtn")
        self._refresh_btn.clicked.connect(self.refresh)
        hdr_row.addWidget(self._refresh_btn)
        outer.addLayout(hdr_row)

        # ── KPI cards ────────────────────────────────────────────────────────
        kpi_row = QHBoxLayout()
        kpi_row.setSpacing(12)

        self._kpis = {
            "active_deals":       KpiCard("Active Deals",      "—", color=ACCENT),
            "calc_due_today":     KpiCard("Due Today",          "—", color=RED),
            "calc_due_week":      KpiCard("Due This Week",      "—", color=AMBER),
            "calc_due_month":     KpiCard("Due This Month",     "—", color=PURPLE),
            "calculated_periods": KpiCard("Calculated Periods", "—", color=GREEN),
            "total_interest":     KpiCard("Total Interest",     "—", color=GREEN),
            "rates_loaded":       KpiCard("SOFR Rates",         "—", color=ACCENT),
            "index_loaded":       KpiCard("SOFR Index",          "—", color=ACCENT),
        }
        for card in self._kpis.values():
            card.setSizePolicy(__import__("PySide6.QtWidgets",fromlist=["QSizePolicy"]).QSizePolicy.Expanding,
                               __import__("PySide6.QtWidgets",fromlist=["QSizePolicy"]).QSizePolicy.Preferred)
            kpi_row.addWidget(card)
        outer.addLayout(kpi_row)

        # ── Legend ───────────────────────────────────────────────────────────
        legend = QHBoxLayout()
        legend.setSpacing(16)
        for color, label in [
            ("#DCFCE7", "Eligible to calculate now"),
            ("#E1F5EE", "Compounded in Arrears"),
            ("#EDE9FE", "Simple Average in Arrears"),
            ("#FFF3E0", "SOFR Index"),
        ]:
            dot = QLabel("■")
            dot.setStyleSheet(f"color: {color}; font-size:16px;")
            txt = QLabel(label)
            txt.setStyleSheet("color:#6B7280; font-size:11px;")
            legend.addWidget(dot)
            legend.addWidget(txt)
        legend.addStretch()
        self._last_refresh_lbl = QLabel("")
        self._last_refresh_lbl.setStyleSheet("color:#9CA3AF; font-size:10px;")
        legend.addWidget(self._last_refresh_lbl)
        outer.addLayout(legend)

        # ── RDD note ─────────────────────────────────────────────────────────
        rdd_note = QLabel(
            "<b>Rate Determination Date (RDD):</b>  "
            "Compounded &amp; Simple Average deals → next business day after Obs Period End  |  "
            "SOFR Index deals → same day as Obs Period End"
        )
        rdd_note.setStyleSheet(
            "background:#EFF6FF; color:#1E40AF; border-radius:4px;"
            "padding:5px 10px; font-size:11px;"
        )
        rdd_note.setWordWrap(True)
        outer.addWidget(rdd_note)

        # ── Tab widget: Today / This Week / This Month ────────────────────────
        self._tabs = QTabWidget()
        self._tabs.setDocumentMode(True)

        self._tbl_today = self._make_table()
        self._tbl_week  = self._make_table()
        self._tbl_month = self._make_table()

        self._tabs.addTab(self._wrap(self._tbl_today), "Today  (0)")
        self._tabs.addTab(self._wrap(self._tbl_week),  "This Week  (0)")
        self._tabs.addTab(self._wrap(self._tbl_month), "This Month  (0)")

        outer.addWidget(self._tabs, 1)

        self._status_lbl = QLabel("")
        self._status_lbl.setStyleSheet("color:#6B7280; font-size:10px;")
        outer.addWidget(self._status_lbl)

    @staticmethod
    def _make_table() -> DataTable:
        tbl = DataTable(_COLS)
        tbl.setSortingEnabled(True)
        return tbl

    @staticmethod
    def _wrap(widget: QWidget) -> QWidget:
        """Wrap a table in a plain container widget for the tab."""
        w = QWidget()
        lay = QVBoxLayout(w)
        lay.setContentsMargins(0, 4, 0, 0)
        lay.setSpacing(0)
        lay.addWidget(widget)
        return w

    def refresh(self):
        from core.database import (
            get_dashboard_kpis, get_deals_by_rdd_window,
            refresh_schedule_status
        )
        from datetime import datetime

        self._refresh_btn.setEnabled(False)
        self._refresh_btn.setText("Refreshing…")

        with self._db() as conn:
            refresh_schedule_status(conn)
            kpis        = get_dashboard_kpis(conn)
            today_deals = get_deals_by_rdd_window(conn, "today")
            week_deals  = get_deals_by_rdd_window(conn, "week")
            month_deals = get_deals_by_rdd_window(conn, "month")

        # ── Update KPI cards ──────────────────────────────────────────────
        self._kpis["active_deals"].set_value(str(kpis["active_deals"]))
        self._kpis["calc_due_today"].set_value(
            str(kpis["calc_due_today"]),
            RED if kpis["calc_due_today"] > 0 else "#9CA3AF"
        )
        self._kpis["calc_due_week"].set_value(
            str(kpis["calc_due_week"]),
            AMBER if kpis["calc_due_week"] > 0 else "#9CA3AF"
        )
        self._kpis["calc_due_month"].set_value(
            str(kpis["calc_due_month"]),
            PURPLE if kpis["calc_due_month"] > 0 else "#9CA3AF"
        )
        self._kpis["calculated_periods"].set_value(
            str(kpis["calculated_periods"]), GREEN
        )
        self._kpis["total_interest"].set_value(
            fmt_money(kpis["total_interest"]), GREEN
        )
        self._kpis["rates_loaded"].set_value(
            str(kpis["rates_loaded"]),
            GREEN if kpis["rates_loaded"] > 0 else RED
        )
        self._kpis["rates_loaded"].set_sub(
            f"Latest: {fmt_date(kpis.get('latest_rate_date',''))}"
        )
        self._kpis["index_loaded"].set_value(
            str(kpis.get("index_loaded", 0)),
            GREEN if kpis.get("index_loaded", 0) > 0 else RED
        )
        self._kpis["index_loaded"].set_sub(
            f"Latest: {fmt_date(kpis.get('latest_index_date',''))}"
        )

        # ── Populate tables ───────────────────────────────────────────────
        t_rows, t_cols = _rows_from_deals(today_deals)
        w_rows, w_cols = _rows_from_deals(week_deals)
        m_rows, m_cols = _rows_from_deals(month_deals)

        self._tbl_today.populate(t_rows, t_cols)
        self._tbl_week.populate(w_rows, w_cols)
        self._tbl_month.populate(m_rows, m_cols)

        # ── Update tab labels with counts ─────────────────────────────────
        self._tabs.setTabText(0, f"Today  ({len(today_deals)})")
        self._tabs.setTabText(1, f"This Week  ({len(week_deals)})")
        self._tabs.setTabText(2, f"This Month  ({len(month_deals)})")

        # Highlight Today tab if deals are due
        today_style = (
            "color: #065F46; font-weight: bold;"
            if today_deals else ""
        )
        self._tabs.tabBar().setTabTextColor(
            0,
            Qt.darkGreen if today_deals else Qt.black
        )

        ts = datetime.now().strftime("%d-%b-%Y %H:%M:%S")
        self._last_refresh_lbl.setText(f"Last refreshed: {ts}")
        self._status_lbl.setText(
            f"Today: {len(today_deals)} deals  |  "
            f"This week: {len(week_deals)} deals  |  "
            f"This month: {len(month_deals)} deals  |  "
            f"Eligible now: {kpis['ready_to_calc']} periods"
        )

        self._refresh_btn.setEnabled(True)
        self._refresh_btn.setText("Refresh")

    def showEvent(self, event):
        super().showEvent(event)
        QTimer.singleShot(0, self.refresh)

    # Global search hook from MainWindow
    def apply_search(self, term: str):
        t = (term or "").lower().strip()
        for tbl in [self._tbl_today, self._tbl_week, self._tbl_month]:
            self._filter_table(tbl, t)

    @staticmethod
    def _filter_table(tbl, term: str):
        for r in range(tbl.rowCount()):
            show = (not term)
            if not show:
                for c in range(tbl.columnCount()):
                    item = tbl.item(r, c)
                    if item and term in item.text().lower():
                        show = True
                        break
            tbl.setRowHidden(r, not show)
