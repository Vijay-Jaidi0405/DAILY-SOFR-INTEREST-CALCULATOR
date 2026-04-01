"""ui/pages/calc_batch.py — Batch interest calculation."""

import uuid
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QComboBox, QSpinBox, QDateEdit,
    QMessageBox, QProgressDialog, QCheckBox
)
from PySide6.QtCore import Qt, QDate, QTimer, QThread, Signal
from ui.widgets.common import (
    Panel, PageHeader, DataTable, KpiCard,
    fmt_money, fmt_rate, fmt_date, readiness_color, make_date_item
)
from ui.styles import GREEN, PURPLE, AMBER, ACCENT, RED


class BatchThread(QThread):
    progress = Signal(int, int, str)
    done     = Signal(list, str)  # results, batch_id
    error    = Signal(str)

    def __init__(self, db_factory, cusips, p_start, p_end,
                 pay, delay, dc, method_filter):
        super().__init__()
        self._db = db_factory
        self._cusips = cusips
        self._p_start = p_start
        self._p_end   = p_end
        self._pay     = pay
        self._delay   = delay
        self._dc      = dc
        self._mf      = method_filter

    def run(self):
        from core.database import calculate_interest
        batch_id = str(uuid.uuid4())
        results  = []
        total    = len(self._cusips)
        for i, cusip in enumerate(self._cusips):
            self.progress.emit(i + 1, total, cusip)
            try:
                with self._db() as conn:
                    res = calculate_interest(
                        conn, cusip,
                        self._p_start, self._p_end, self._pay,
                        delay_days=self._delay,
                        dc=self._dc, log=True,
                        batch_id=batch_id
                    )
                results.append({**res, "status": "OK"})
            except Exception as e:
                results.append({
                    "cusip": cusip, "status": "Error",
                    "error": str(e), "interest_amount": 0,
                    "annualized_rate": None, "rounded_rate": None,
                    "deal_name": "—", "client_name": "—",
                    "calculation_method": "—", "notional_amount": 0,
                    "period_start_date": self._p_start,
                    "period_end_date": self._p_end,
                    "adjusted_payment_date": self._pay,
                    "accrual_days": 0,
                })
        self.done.emit(results, batch_id)


class CalcBatchPage(QWidget):
    def __init__(self, db_factory, parent=None):
        super().__init__(parent)
        self._db = db_factory
        self._cusips_all = []
        self._last_results = []
        self._build_ui()

    def _build_ui(self):
        lay = QVBoxLayout(self)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        lay.addWidget(PageHeader("Batch Calculation",
            "Calculate SOFR interest for multiple deals in one run"))

        # Parameters panel
        params = Panel("Batch Parameters")
        param_lay = QHBoxLayout()
        param_lay.setSpacing(14)

        left_form = QVBoxLayout()
        left_form.setSpacing(8)

        def row(label, widget):
            r = QHBoxLayout()
            lbl = QLabel(label)
            lbl.setMinimumWidth(120)
            lbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
            r.addWidget(lbl)
            r.addWidget(widget)
            r.addStretch()
            return r

        self._start = QDateEdit(); self._start.setCalendarPopup(True)
        self._start.setDate(QDate(2024, 1, 1))
        self._end   = QDateEdit(); self._end.setCalendarPopup(True)
        self._end.setDate(QDate(2024, 3, 31))
        self._pay   = QDateEdit(); self._pay.setCalendarPopup(True)
        self._pay.setDate(QDate(2024, 4, 2))
        self._delay = QSpinBox(); self._delay.setRange(0, 30); self._delay.setValue(2)
        self._dc    = QComboBox(); self._dc.addItems(["360", "365"])
        self._mf    = QComboBox()
        self._mf.addItems([
            "All Methods",
            "Compounded in Arrears",
            "Simple Average in Arrears",
            "SOFR Index",
        ])
        self._eligible_only = QCheckBox("Only Ready to Calculate")
        self._eligible_only.setChecked(True)

        left_form.addLayout(row("Period Start:", self._start))
        left_form.addLayout(row("Period End:", self._end))
        left_form.addLayout(row("Payment Date:", self._pay))
        left_form.addLayout(row("Delay Days:", self._delay))
        left_form.addLayout(row("Day Count:", self._dc))
        left_form.addLayout(row("Method Filter:", self._mf))
        left_form.addLayout(row("", self._eligible_only))
        param_lay.addLayout(left_form)

        # CUSIP selection
        right_form = QVBoxLayout()
        right_form.setSpacing(6)
        right_form.addWidget(QLabel("Select CUSIPs:"))

        cusip_row = QHBoxLayout()
        self._cusip_filter = QComboBox()
        self._cusip_filter.addItems(["All Active Deals"])
        cusip_row.addWidget(self._cusip_filter)
        right_form.addLayout(cusip_row)

        self._count_lbl = QLabel("0 deals selected")
        self._count_lbl.setStyleSheet("color:#6B7280; font-size:11px;")
        right_form.addWidget(self._count_lbl)
        right_form.addStretch()

        run_btn = QPushButton("Run Batch Calculation")
        run_btn.setObjectName("GreenBtn")
        run_btn.setMinimumHeight(42)
        run_btn.clicked.connect(self._run_batch)
        right_form.addWidget(run_btn)

        param_lay.addLayout(right_form, 1)
        params.add_layout(param_lay)
        lay.addWidget(params)

        # KPI summary row
        kpi_row = QHBoxLayout(); kpi_row.setSpacing(12)
        self._k_total  = KpiCard("Deals Run", "—", color=ACCENT)
        self._k_ok     = KpiCard("Successful", "—", color=GREEN)
        self._k_errors = KpiCard("Errors", "—", color=RED)
        self._k_int    = KpiCard("Total Interest", "—", color=GREEN)
        self._k_batch  = KpiCard("Batch ID", "—", color=PURPLE)
        for k in [self._k_total, self._k_ok, self._k_errors,
                  self._k_int, self._k_batch]:
            kpi_row.addWidget(k)
        lay.addLayout(kpi_row)

        # Results table
        res_panel = Panel("Batch Results")
        cols = [
            "CUSIP", "Deal Name", "Client", "Method",
            "Accrual Days", "Comp/Avg Rate", "Ann Rate",
            "Interest Amount", "Payment Date", "Status"
        ]
        self._tbl = DataTable(cols)
        res_panel.add_widget(self._tbl)
        lay.addWidget(res_panel, 1)

        self._pdf_btn = QPushButton("Export Batch PDF Report")
        self._pdf_btn.setEnabled(False)
        self._pdf_btn.clicked.connect(self._export_pdf)
        lay.addWidget(self._pdf_btn)

    def _load_cusips(self):
        from core.database import get_all_deals, get_eligible_deals
        with self._db() as conn:
            deals = get_all_deals(conn)
        self._cusips_all = [d["cusip"] for d in deals]
        self._count_lbl.setText(f"{len(self._cusips_all)} deals available")

    def _get_target_cusips(self):
        if self._eligible_only.isChecked():
            from core.database import get_eligible_deals
            with self._db() as conn:
                eligible = get_eligible_deals(conn)
            mf = self._mf.currentText()
            if mf != "All Methods":
                eligible = [e for e in eligible if e["calculation_method"] == mf]
            return [e["cusip"] for e in eligible]
        else:
            mf = self._mf.currentText()
            if mf == "All Methods":
                return self._cusips_all
            from core.database import get_all_deals
            with self._db() as conn:
                deals = [d for d in get_all_deals(conn)
                         if d["calculation_method"] == mf]
            return [d["cusip"] for d in deals]

    def _run_batch(self):
        cusips = self._get_target_cusips()
        if not cusips:
            QMessageBox.information(self, "No Deals",
                "No deals match the selected filters.")
            return

        p_start = self._start.date().toPython()
        p_end   = self._end.date().toPython()
        pay     = self._pay.date().toPython()

        if p_end <= p_start:
            QMessageBox.warning(self, "Dates", "Period End must be after Start.")
            return

        if QMessageBox.question(
            self, "Confirm Batch",
            f"Run calculation for {len(cusips)} deals?\n"
            f"Period: {p_start} to {p_end}",
            QMessageBox.Yes | QMessageBox.No
        ) != QMessageBox.Yes:
            return

        self._prog = QProgressDialog("Running batch…", "Cancel", 0, len(cusips), self)
        self._prog.setWindowModality(Qt.WindowModal)
        self._prog.show()

        self._thread = BatchThread(
            self._db, cusips, p_start, p_end, pay,
            self._delay.value(), int(self._dc.currentText()),
            self._mf.currentText()
        )
        self._thread.progress.connect(self._on_progress)
        self._thread.done.connect(self._on_done)
        self._thread.error.connect(self._on_error)
        self._thread.start()

    def _on_progress(self, i, total, cusip):
        self._prog.setValue(i)
        self._prog.setLabelText(f"Processing {cusip} ({i}/{total})…")

    def _on_done(self, results, batch_id):
        self._prog.close()
        self._last_results = results
        ok  = [r for r in results if r.get("status") == "OK"]
        err = [r for r in results if r.get("status") == "Error"]
        total_int = sum(r.get("interest_amount", 0) or 0 for r in ok)

        self._k_total.set_value(str(len(results)))
        self._k_ok.set_value(str(len(ok)), GREEN)
        self._k_errors.set_value(str(len(err)), RED if err else "#9CA3AF")
        self._k_int.set_value(fmt_money(total_int), GREEN)
        self._k_batch.set_value(batch_id[:8] + "…", PURPLE)

        rows, colors = [], []
        for r in results:
            rnd = r.get("rounding_decimals") or 7
            rows.append([
                r["cusip"],
                r.get("deal_name", "—"),
                r.get("client_name", "—"),
                r.get("calculation_method", "—"),
                str(r.get("accrual_days", "—")),
                fmt_rate(r.get("compounded_rate")),
                fmt_rate(r.get("annualized_rate")),
                fmt_money(r.get("interest_amount")),
                make_date_item(r.get("adjusted_payment_date")),
                r.get("status", "—"),
            ])
            colors.append("#FEF2F2" if r.get("status") == "Error" else None)
        self._tbl.populate(rows, [c or "#FFFFFF" for c in colors])
        self._pdf_btn.setEnabled(True)
        # Auto-generate batch PDF
        self._export_pdf(silent=True)

        if err:
            errs = "\n".join(
                f"{r['cusip']}: {r.get('error','')}" for r in err[:5]
            )
            QMessageBox.warning(self, "Batch Errors",
                f"{len(err)} deal(s) failed:\n{errs}")

    def _on_error(self, err):
        self._prog.close()
        QMessageBox.critical(self, "Batch Error", err)

    def _export_pdf(self, silent: bool = False):
        if not self._last_results:
            return
        try:
            from core.pdf_report import generate_batch_pdf
            p_start = self._start.date().toPython()
            p_end   = self._end.date().toPython()
            import uuid
            bid = str(uuid.uuid4())
            # Try to get batch_id from first result log
            path = generate_batch_pdf(
                self._last_results, bid, p_start, p_end
            )
            if not silent:
                QMessageBox.information(
                    self, "PDF Saved",
                    f"Batch report saved successfully.\n\n"
                    f"File:\n{path}\n\n"
                    f"Folder:\n{path.parent}"
                )
            else:
                self.window().status(f"Batch PDF saved: {path.name}")
        except ImportError:
            if not silent:
                QMessageBox.warning(
                    self, "reportlab not installed",
                    "Install reportlab to enable PDF reports:\n"
                    "    pip install reportlab"
                )
        except Exception as e:
            if not silent:
                QMessageBox.critical(self, "PDF Error", str(e))

    def showEvent(self, event):
        super().showEvent(event)
        QTimer.singleShot(0, self._load_cusips)
