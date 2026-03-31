"""ui/pages/schedule.py — Payment schedule generation and viewing."""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QComboBox, QMessageBox, QProgressDialog, QSplitter,
    QScrollArea, QGroupBox, QTableWidgetItem
)
from PySide6.QtCore import Qt, QTimer, QThread, Signal
from ui.widgets.common import (
    Panel, PageHeader, DataTable, KpiCard,
    fmt_money, fmt_rate, fmt_date, readiness_color, make_date_item
)
from ui.styles import GREEN, PURPLE, AMBER, ACCENT, RED


class ScheduleGenThread(QThread):
    progress = Signal(int, int)
    done     = Signal()
    error    = Signal(str)

    def __init__(self, db_factory, rebuild=True):
        super().__init__()
        self._db = db_factory
        self._rebuild = rebuild

    def run(self):
        try:
            from core.database import generate_all_schedules
            with self._db() as conn:
                generate_all_schedules(conn, self._rebuild,
                                       progress_cb=lambda i, t: self.progress.emit(i, t))
            self.done.emit()
        except Exception as e:
            self.error.emit(str(e))


class SchedulePage(QWidget):
    def __init__(self, db_factory, parent=None):
        super().__init__(parent)
        self._db = db_factory
        self._build_ui()

    def _build_ui(self):
        lay = QVBoxLayout(self)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        # Header
        hdr_row = QHBoxLayout()
        hdr_row.setSpacing(10)
        hdr_row.addWidget(PageHeader("Payment Schedule",
            "Generate, view and track payment periods for each deal"))
        hdr_row.addStretch()

        refresh_btn = QPushButton("Refresh Status")
        refresh_btn.clicked.connect(self._refresh_status)
        gen_all_btn = QPushButton("Generate All Schedules")
        gen_all_btn.setObjectName("GreenBtn")
        gen_all_btn.clicked.connect(self._gen_all)

        hdr_row.addWidget(refresh_btn)
        hdr_row.addWidget(gen_all_btn)
        lay.addLayout(hdr_row)

        # CUSIP selector + single schedule generator
        sel_row = QHBoxLayout()
        sel_row.addWidget(QLabel("Select Deal:"))
        self._cusip_combo = QComboBox()
        self._cusip_combo.setMinimumWidth(180)
        self._cusip_combo.currentIndexChanged.connect(self._on_cusip_change)
        gen_one_btn = QPushButton("Generate Schedule")
        gen_one_btn.setObjectName("PrimaryBtn")
        gen_one_btn.clicked.connect(self._gen_one)
        sel_row.addWidget(self._cusip_combo)
        sel_row.addWidget(gen_one_btn)
        sel_row.addStretch()
        lay.addLayout(sel_row)

        # KPI row for selected deal
        kpi_row = QHBoxLayout()
        kpi_row.setSpacing(12)
        self._k_total   = KpiCard("Total Periods", "—", color=ACCENT)
        self._k_sched   = KpiCard("Scheduled", "—", color=ACCENT)
        self._k_ready   = KpiCard("Ready to Calc", "—", color=GREEN)
        self._k_calcd   = KpiCard("Calculated", "—", color=PURPLE)
        self._k_total_i = KpiCard("Total Interest", "—", color=GREEN)
        for k in [self._k_total, self._k_sched, self._k_ready,
                  self._k_calcd, self._k_total_i]:
            kpi_row.addWidget(k)
        lay.addLayout(kpi_row)

        # Schedule table
        panel = Panel("Period Schedule")
        cols = [
            "#", "Period Start", "Period End (Excl.)", "Obs Start", "Obs End (Excl.)",
            "Payment Date", "Days", "Status",
            "Comp/Avg Rate", "Annualized Rate", "Interest", "Running Total"
        ]
        self._tbl = DataTable(cols)
        panel.add_widget(self._tbl)
        lay.addWidget(panel, 1)

        self._status_lbl = QLabel("")
        self._status_lbl.setStyleSheet("color:#6B7280; font-size:11px;")
        lay.addWidget(self._status_lbl)

    def _load_cusips(self):
        from core.database import get_all_deals
        with self._db() as conn:
            deals = get_all_deals(conn)
        self._cusip_combo.clear()
        self._cusip_combo.addItem("— Select Deal —", None)
        for d in deals:
            label = f"{d['cusip']}  —  {d['deal_name']}"
            self._cusip_combo.addItem(label, d["cusip"])

    def _on_cusip_change(self):
        cusip = self._cusip_combo.currentData()
        if cusip:
            self._load_schedule(cusip)
        else:
            self._tbl.populate([])

    def _load_schedule(self, cusip: str):
        from core.database import get_full_schedule
        with self._db() as conn:
            rows = get_full_schedule(conn, cusip)

        if not rows:
            self._tbl.populate([])
            self._status_lbl.setText("No schedule generated yet for this deal.")
            for k in [self._k_total, self._k_sched,
                      self._k_ready, self._k_calcd, self._k_total_i]:
                k.set_value("—")
            return

        total_i = sum(r["interest_amount"] or 0 for r in rows
                      if r["period_status"] == "Calculated")
        sched   = sum(1 for r in rows if r["period_status"] == "Scheduled")
        ready   = sum(1 for r in rows if r["period_status"] == "Ready to Calculate")
        calcd   = sum(1 for r in rows if r["period_status"] == "Calculated")

        self._k_total.set_value(str(len(rows)))
        self._k_sched.set_value(str(sched))
        self._k_ready.set_value(str(ready), GREEN if ready else "#9CA3AF")
        self._k_calcd.set_value(str(calcd), PURPLE)
        self._k_total_i.set_value(fmt_money(total_i), GREEN)

        # Running total of calculated interest
        running = 0.0
        table_rows, colors = [], []
        for r in rows:
            if r["period_status"] == "Calculated" and r["interest_amount"]:
                running += r["interest_amount"]
            rnd = r.get("rounding_decimals") or 4
            period_item = QTableWidgetItem()
            period_item.setData(Qt.EditRole, int(r["period_number"]))
            period_item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
            table_rows.append([
                period_item,
                make_date_item(r["period_start_date"]),
                make_date_item(r["period_end_date"]),
                make_date_item(r["obs_start_date"]),
                make_date_item(r["obs_end_date"]),
                make_date_item(r["adj_payment_date"]),
                str(r["accrual_days"]),
                r["period_status"],
                fmt_rate(r.get("compounded_rate"), rnd),
                fmt_rate(r.get("annualized_rate"), rnd),
                fmt_money(r.get("interest_amount")),
                fmt_money(running) if r["period_status"] == "Calculated" else "—",
            ])
            colors.append(
                readiness_color(r["period_status"], r["is_calc_eligible_today"])
            )

        self._tbl.populate(table_rows, colors)
        self._status_lbl.setText(
            f"{len(rows)} periods  |  "
            f"{ready} ready  |  "
            f"{calcd} calculated  |  "
            f"Total interest: {fmt_money(total_i)}"
        )

    def _refresh_status(self):
        from core.database import refresh_schedule_status
        with self._db() as conn:
            refresh_schedule_status(conn)
        cusip = self._cusip_combo.currentData()
        if cusip:
            self._load_schedule(cusip)
        QMessageBox.information(self, "Done", "Schedule status refreshed.")

    def _gen_one(self):
        cusip = self._cusip_combo.currentData()
        if not cusip:
            QMessageBox.warning(self, "Select Deal", "Please select a deal first.")
            return
        try:
            from core.database import generate_schedule
            with self._db() as conn:
                generate_schedule(conn, cusip, rebuild=True)
            self._load_schedule(cusip)
            QMessageBox.information(self, "Done",
                f"Schedule generated for {cusip}.")
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _gen_all(self):
        if QMessageBox.question(
            self, "Generate All",
            "Generate payment schedules for all 300 active deals?\n"
            "This may take a few minutes.",
            QMessageBox.Yes | QMessageBox.No
        ) != QMessageBox.Yes:
            return

        self._progress_dlg = QProgressDialog(
            "Generating schedules…", "Cancel", 0, 100, self
        )
        self._progress_dlg.setWindowModality(Qt.WindowModal)
        self._progress_dlg.show()

        self._gen_thread = ScheduleGenThread(self._db, rebuild=True)
        self._gen_thread.progress.connect(self._on_gen_progress)
        self._gen_thread.done.connect(self._on_gen_done)
        self._gen_thread.error.connect(self._on_gen_error)
        self._gen_thread.start()

    def _on_gen_progress(self, i, total):
        self._progress_dlg.setMaximum(total)
        self._progress_dlg.setValue(i)
        self._progress_dlg.setLabelText(f"Processing deal {i} of {total}…")

    def _on_gen_done(self):
        self._progress_dlg.close()
        QMessageBox.information(self, "Done",
            "All payment schedules generated successfully.")
        cusip = self._cusip_combo.currentData()
        if cusip:
            self._load_schedule(cusip)

    def _on_gen_error(self, err):
        self._progress_dlg.close()
        QMessageBox.critical(self, "Error", err)

    def showEvent(self, event):
        super().showEvent(event)
        QTimer.singleShot(0, self._load_cusips)
