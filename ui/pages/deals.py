"""ui/pages/deals.py — Deal Master management page."""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QLineEdit, QComboBox, QDialog, QFormLayout, QDialogButtonBox,
    QMessageBox, QDoubleSpinBox, QSpinBox, QDateEdit, QScrollArea,
    QSizePolicy, QFrame, QGroupBox
)
from PySide6.QtCore import Qt, QDate, QTimer
from PySide6.QtGui import QFont
from ui.widgets.common import (
    Panel, PageHeader, DataTable, fmt_money, fmt_date, make_date_item
)
from ui.styles import GREEN, RED, ACCENT, BORDER

METHODS    = ["Compounded in Arrears", "Simple Average in Arrears", "SOFR Index"]
RATE_TYPES = ["SOFR", "SOFR Index"]
FREQS      = ["Monthly", "Quarterly"]
YN         = ["N", "Y"]
ROUNDINGS  = [4, 5, 6]
LOOKBACKS  = [2, 3, 5]
ACCRUAL_BASES = ["Calendar Days", "Observation Period Days"]


def _divider():
    f = QFrame(); f.setFrameShape(QFrame.HLine)
    f.setStyleSheet(f"background:{BORDER}; max-height:1px; border:none;")
    return f


def _sec(text):
    l = QLabel(text.upper())
    l.setStyleSheet("color:#6B7280;font-size:10px;font-weight:700;"
                    "letter-spacing:1px;padding:8px 0 4px 0;")
    return l


# ---------------------------------------------------------------------------
# Deal Add / Edit dialog
# ---------------------------------------------------------------------------

class DealDialog(QDialog):
    """
    Collects deal information including First Payment Date.
    Period start/end and observation dates are derived automatically
    and shown in a live preview panel inside the dialog.
    """

    def __init__(self, parent=None, deal: dict | None = None):
        super().__init__(parent)
        self.setWindowTitle("Add Deal" if deal is None else "Edit Deal")
        self.setMinimumWidth(680)
        self.setMinimumHeight(700)
        self._deal = deal
        self._build()
        if deal:
            self._populate(deal)
        else:
            self._refresh_preview()

    # ── build ─────────────────────────────────────────────────────────────────

    def _build(self):
        root = QVBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # Scrollable body
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.NoFrame)
        body = QWidget()
        body_lay = QVBoxLayout(body)
        body_lay.setContentsMargins(24, 20, 24, 16)
        body_lay.setSpacing(0)

        # ── Section 1: Deal identity ────────────────────────────────────────
        body_lay.addWidget(_sec("Deal Information"))
        form1 = QFormLayout()
        form1.setSpacing(10)
        form1.setLabelAlignment(Qt.AlignRight)
        form1.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)

        self.f_deal_name = QLineEdit()
        self.f_client    = QLineEdit()
        self.f_cusip     = QLineEdit(); self.f_cusip.setMaxLength(9)
        self.f_cusip.setPlaceholderText("9 characters")
        self.f_notional  = QDoubleSpinBox()
        self.f_notional.setRange(1, 1_000_000_000)
        self.f_notional.setDecimals(2)
        self.f_notional.setValue(10_000_000)
        self.f_notional.setSingleStep(1_000_000)
        self.f_spread = QDoubleSpinBox()
        self.f_spread.setRange(-100.0, 100.0)
        self.f_spread.setDecimals(4)
        self.f_spread.setSingleStep(0.01)
        self.f_spread.setValue(0.0)

        form1.addRow("Deal Name *",       self.f_deal_name)
        form1.addRow("Client Name *",     self.f_client)
        form1.addRow("CUSIP (9 chars) *", self.f_cusip)
        form1.addRow("Notional Amount *", self.f_notional)
        form1.addRow("Spread",            self.f_spread)
        body_lay.addLayout(form1)
        body_lay.addSpacing(16)
        body_lay.addWidget(_divider())

        # ── Section 2: Rate / method ────────────────────────────────────────
        body_lay.addWidget(_sec("Rate & Calculation"))
        form2 = QFormLayout()
        form2.setSpacing(10)
        form2.setLabelAlignment(Qt.AlignRight)
        form2.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)

        def combo(opts):
            c = QComboBox()
            c.addItems([str(o) for o in opts])
            return c

        self.f_rate_type   = combo(RATE_TYPES)
        self.f_method      = combo(METHODS)
        self.f_frequency   = combo(FREQS)
        self.f_rounding    = combo(ROUNDINGS)

        form2.addRow("Rate Type *",          self.f_rate_type)
        form2.addRow("Calculation Method *", self.f_method)
        form2.addRow("Payment Frequency *",  self.f_frequency)
        form2.addRow("Rounding Decimals",    self.f_rounding)
        body_lay.addLayout(form2)
        body_lay.addSpacing(16)
        body_lay.addWidget(_divider())

        # ── Section 3: Observation & delay parameters ───────────────────────
        body_lay.addWidget(_sec("Observation & Payment Parameters"))
        form3 = QFormLayout()
        form3.setSpacing(10)
        form3.setLabelAlignment(Qt.AlignRight)
        form3.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)

        self.f_obs_shift   = combo(YN)
        self.f_shifted_int = combo(YN)
        self.f_pay_delay   = combo(YN)
        self.f_lookback    = combo(LOOKBACKS)
        self.f_accrual_basis = combo(ACCRUAL_BASES)

        # Payment delay days — only active when Pay Delay = Y
        self.f_delay_days = QSpinBox()
        self.f_delay_days.setRange(0, 30)
        self.f_delay_days.setValue(2)
        self.f_delay_days.setEnabled(False)
        self.f_delay_days.setToolTip(
            "Calendar days added to period end to compute payment date"
        )

        form3.addRow("Observation Shift",  self.f_obs_shift)
        form3.addRow("Shifted Interest",   self.f_shifted_int)
        form3.addRow("Payment Delay",      self.f_pay_delay)
        form3.addRow("Delay Days",         self.f_delay_days)
        form3.addRow("Look Back Days",     self.f_lookback)
        form3.addRow("Accrual Days Basis", self.f_accrual_basis)
        body_lay.addLayout(form3)
        body_lay.addSpacing(16)
        body_lay.addWidget(_divider())

        # ── Section 4: Key dates ────────────────────────────────────────────
        body_lay.addWidget(_sec("Key Dates"))

        # Two date pickers side by side: First Payment Date | Maturity Date
        dates_row = QHBoxLayout()
        dates_row.setSpacing(20)

        fpd_col = QVBoxLayout(); fpd_col.setSpacing(6)
        fpd_lbl = QLabel("First Payment Date *")
        fpd_lbl.setStyleSheet("font-size:12px;font-weight:600;color:#4B5563;")
        self.f_first_payment = QDateEdit()
        self.f_first_payment.setCalendarPopup(True)
        self.f_first_payment.setDate(QDate(2024, 4, 9))
        fpd_col.addWidget(fpd_lbl)
        fpd_col.addWidget(self.f_first_payment)

        mat_col = QVBoxLayout(); mat_col.setSpacing(6)
        mat_lbl = QLabel("Maturity Date *")
        mat_lbl.setStyleSheet("font-size:12px;font-weight:600;color:#4B5563;")
        self.f_maturity = QDateEdit()
        self.f_maturity.setCalendarPopup(True)
        self.f_maturity.setDate(QDate(2029, 4, 9))
        mat_col.addWidget(mat_lbl)
        mat_col.addWidget(self.f_maturity)

        dates_row.addLayout(fpd_col)
        dates_row.addLayout(mat_col)
        body_lay.addLayout(dates_row)
        body_lay.addSpacing(16)
        body_lay.addWidget(_divider())

        # ── Section 5: Live date preview ────────────────────────────────────
        body_lay.addWidget(_sec("Period 1 Date Preview  (auto-calculated)"))
        body_lay.addSpacing(4)

        preview_card = QWidget()
        preview_card.setObjectName("Panel")
        preview_lay = QVBoxLayout(preview_card)
        preview_lay.setContentsMargins(16, 12, 16, 14)
        preview_lay.setSpacing(8)

        def _prow(label: str) -> tuple[QHBoxLayout, QLabel]:
            row = QHBoxLayout(); row.setSpacing(12)
            lbl = QLabel(label)
            lbl.setStyleSheet(
                "color:#6B7280;font-size:12px;font-weight:600;min-width:200px;"
            )
            lbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
            val = QLabel("—")
            val.setStyleSheet(
                "color:#1E2533;font-size:13px;font-weight:700;"
                "font-family:monospace;"
            )
            row.addWidget(lbl)
            row.addWidget(val, 1)
            return row, val

        r1, self._prev_period_start = _prow("Interest Period Start:")
        r2, self._prev_period_end   = _prow("Interest Period End:")
        r3, self._prev_obs_start    = _prow("Observation Period Start:")
        r4, self._prev_obs_end      = _prow("Observation Period End:")
        r5, self._prev_pay_date     = _prow("Adjusted Payment Date:")
        r6, self._prev_accrual      = _prow("Accrual Days:")

        for r in (r1, r2, r3, r4, r5, r6):
            preview_lay.addLayout(r)

        self._prev_note = QLabel("")
        self._prev_note.setStyleSheet(
            "color:#92400E;font-size:11px;padding:4px 0 0 0;"
        )
        self._prev_note.setWordWrap(True)
        preview_lay.addWidget(self._prev_note)

        body_lay.addWidget(preview_card)
        body_lay.addStretch()

        scroll.setWidget(body)
        root.addWidget(scroll, 1)

        # Footer hint
        hint = QLabel(
            "<i>Interest period includes the start date and excludes the end date.  "
            "Observation dates are derived by shifting the period boundary back by "
            "lookback days, then rolling forward to the next business day.</i>"
        )
        hint.setStyleSheet(
            "color:#6B7280;font-size:11px;padding:8px 24px;"
            f"border-top:1px solid {BORDER};"
        )
        hint.setWordWrap(True)
        root.addWidget(hint)

        # Dialog buttons
        btns = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        btns.setContentsMargins(24, 8, 24, 16)
        btns.accepted.connect(self._validate_and_accept)
        btns.rejected.connect(self.reject)
        root.addWidget(btns)

        if self._deal:
            self.f_cusip.setEnabled(False)

        # Wire signals for live preview + business rules
        for w in [self.f_first_payment, self.f_maturity,
                  self.f_obs_shift, self.f_shifted_int,
                  self.f_lookback, self.f_pay_delay, self.f_delay_days]:
            if hasattr(w, "dateChanged"):
                w.dateChanged.connect(self._refresh_preview)
            elif hasattr(w, "currentTextChanged"):
                w.currentTextChanged.connect(self._refresh_preview)
            elif hasattr(w, "valueChanged"):
                w.valueChanged.connect(self._refresh_preview)

        self.f_rate_type.currentTextChanged.connect(self._on_rate_type)
        self.f_obs_shift.currentTextChanged.connect(self._on_obs_shift)
        self.f_method.currentTextChanged.connect(self._on_method_change)
        self.f_pay_delay.currentTextChanged.connect(self._on_pay_delay)
        self._on_rate_type(self.f_rate_type.currentText())

    # ── signal handlers ───────────────────────────────────────────────────────

    def _on_rate_type(self, rt):
        if rt == "SOFR Index":
            self.f_method.setCurrentText("SOFR Index")
            self.f_method.setEnabled(False)
        else:
            self.f_method.setEnabled(True)
            if self.f_method.currentText() == "SOFR Index":
                self.f_method.setCurrentIndex(0)
        self._on_method_change(self.f_method.currentText())

    def _on_method_change(self, method: str):
        from core.database import FREQ_RULES
        if method in FREQ_RULES:
            forced = FREQ_RULES[method]
            self.f_frequency.setCurrentText(forced)
            self.f_frequency.setEnabled(False)
            self.f_frequency.setToolTip(f"{method} requires {forced} (auto-set)")
        else:
            self.f_frequency.setEnabled(True)
            self.f_frequency.setToolTip("")
        self._refresh_preview()

    def _on_obs_shift(self, v):
        if v == "N":
            self.f_shifted_int.setCurrentText("N")
            self.f_shifted_int.setEnabled(False)
        else:
            self.f_shifted_int.setEnabled(True)
        self._refresh_preview()

    def _on_pay_delay(self, v):
        self.f_delay_days.setEnabled(v == "Y")
        self._refresh_preview()

    # ── live preview ──────────────────────────────────────────────────────────

    def _refresh_preview(self, *_):
        """Compute Period 1 dates from current form values and display them."""
        try:
            from core.database import (
                _nearest_next_bday, _nearest_prev_bday,
                _add_months, _shift_date_back, _HOLIDAY_SET
            )
            from datetime import date, timedelta

            fpd      = self.f_first_payment.date().toPython()
            lb       = int(self.f_lookback.currentText())
            obs_sh   = self.f_obs_shift.currentText() == "Y"
            sh_int   = self.f_shifted_int.currentText() == "Y"
            pay_del  = self.f_pay_delay.currentText() == "Y"
            delay_d  = self.f_delay_days.value() if pay_del else 0
            freq     = self.f_frequency.currentText()
            months   = 1 if freq == "Monthly" else 3

            # Period 1:
            #   payment date  = first_payment_date (already given by user)
            #   period end    = prev bday before payment date
            #   period start  = first_payment_date - 1 period (rolled to next bday)
            pay_date = _nearest_next_bday(fpd)
            p_end    = _nearest_prev_bday(pay_date - timedelta(days=1))
            raw_start = _add_months(fpd, -months)   # go back one period
            p_start   = _nearest_next_bday(raw_start)

            # Shifted interest: effective period shifted back by lookback
            if sh_int:
                eff_ps = _nearest_next_bday(_shift_date_back(p_start, lb))
                eff_pe = _nearest_next_bday(_shift_date_back(p_end, lb))
            else:
                eff_ps, eff_pe = p_start, p_end

            # Observation dates = effective dates shifted back by lookback
            obs_s = _nearest_next_bday(_shift_date_back(eff_ps, lb))
            obs_e = _nearest_next_bday(_shift_date_back(eff_pe, lb))

            accrual = (p_end - p_start).days

            self._prev_period_start.setText(fmt_date(p_start))
            self._prev_period_end.setText(fmt_date(p_end))
            self._prev_obs_start.setText(fmt_date(obs_s))
            self._prev_obs_end.setText(fmt_date(obs_e))
            self._prev_pay_date.setText(fmt_date(pay_date))
            self._prev_accrual.setText(f"{accrual} calendar days")
            self._prev_note.setText(
                f"Period includes {fmt_date(p_start)} to {fmt_date(p_end)} "
                f"(excludes {fmt_date(pay_date)})"
                + (f"  ·  Obs window shifted {lb}d back" if obs_sh else "")
                + (f"  ·  Eff period shifted {lb}d back" if sh_int else "")
            )

        except Exception as e:
            self._prev_note.setText(f"Preview unavailable: {e}")

    # ── populate (edit mode) ──────────────────────────────────────────────────

    def _populate(self, d):
        self.f_deal_name.setText(d["deal_name"])
        self.f_client.setText(d["client_name"])
        self.f_cusip.setText(d["cusip"])
        self.f_notional.setValue(d["notional_amount"])
        self.f_spread.setValue(float(d.get("spread") or 0.0))
        self.f_rate_type.setCurrentText(d["rate_type"])
        self.f_frequency.setCurrentText(d["payment_frequency"])
        self.f_method.setCurrentText(d["calculation_method"])
        self.f_obs_shift.setCurrentText(d["observation_shift"])
        self.f_shifted_int.setCurrentText(d["shifted_interest"])
        self.f_pay_delay.setCurrentText(d["payment_delay"])
        self.f_lookback.setCurrentText(str(d["look_back_days"]))
        self.f_accrual_basis.setCurrentText(
            d.get("accrual_day_basis") or "Calendar Days"
        )
        self.f_rounding.setCurrentText(str(d["rounding_decimals"]))
        self.f_delay_days.setValue(int(d.get("payment_delay_days") or 0))
        self.f_delay_days.setEnabled(d["payment_delay"] == "Y")

        fpd = d.get("first_payment_date") or d.get("start_date")
        if fpd:
            self.f_first_payment.setDate(
                QDate.fromString(fpd[:10], "yyyy-MM-dd")
            )
        if d.get("maturity_date"):
            self.f_maturity.setDate(
                QDate.fromString(d["maturity_date"][:10], "yyyy-MM-dd")
            )
        self._on_method_change(self.f_method.currentText())
        self._refresh_preview()

    # ── validation ────────────────────────────────────────────────────────────

    def _validate_and_accept(self):
        errs = []
        if not self.f_deal_name.text().strip():
            errs.append("Deal Name is required")
        if not self.f_client.text().strip():
            errs.append("Client Name is required")
        cusip = self.f_cusip.text().strip().upper()
        if len(cusip) != 9:
            errs.append("CUSIP must be exactly 9 characters")
        fpd = self.f_first_payment.date()
        mat = self.f_maturity.date()
        if fpd >= mat:
            errs.append("Maturity Date must be after First Payment Date")
        rt = self.f_rate_type.currentText()
        m  = self.f_method.currentText()
        if rt == "SOFR Index" and m != "SOFR Index":
            errs.append("SOFR Index rate type requires SOFR Index method")
        if rt == "SOFR" and m == "SOFR Index":
            errs.append("SOFR rate type cannot use SOFR Index method")
        if (self.f_shifted_int.currentText() == "Y"
                and self.f_obs_shift.currentText() == "N"):
            errs.append("Shifted Interest = Y requires Observation Shift = Y")
        if errs:
            QMessageBox.warning(self, "Validation Errors",
                "\n".join(f"• {e}" for e in errs))
            return
        self.accept()

    def get_data(self) -> dict:
        return {
            "deal_name":          self.f_deal_name.text().strip(),
            "client_name":        self.f_client.text().strip(),
            "cusip":              self.f_cusip.text().strip().upper(),
            "notional_amount":    self.f_notional.value(),
            "spread":             self.f_spread.value(),
            "rate_type":          self.f_rate_type.currentText(),
            "payment_frequency":  self.f_frequency.currentText(),
            "calculation_method": self.f_method.currentText(),
            "observation_shift":  self.f_obs_shift.currentText(),
            "shifted_interest":   self.f_shifted_int.currentText(),
            "payment_delay":      self.f_pay_delay.currentText(),
            "payment_delay_days": (self.f_delay_days.value()
                                   if self.f_pay_delay.currentText() == "Y"
                                   else 0),
            "look_back_days":     int(self.f_lookback.currentText()),
            "accrual_day_basis":  self.f_accrual_basis.currentText(),
            "rounding_decimals":  int(self.f_rounding.currentText()),
            "first_payment_date": self.f_first_payment.date().toString("yyyy-MM-dd"),
            "maturity_date":      self.f_maturity.date().toString("yyyy-MM-dd"),
        }


# ---------------------------------------------------------------------------
# Deals list page
# ---------------------------------------------------------------------------

class DealsPage(QWidget):
    def __init__(self, db_factory, parent=None):
        super().__init__(parent)
        self._db = db_factory
        self._all_rows = []
        self._build_ui()

    def _build_ui(self):
        lay = QVBoxLayout(self)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        # Header + action buttons
        hdr = QHBoxLayout()
        hdr.setSpacing(10)
        hdr.addWidget(PageHeader("Deal Master",
            "Manage all active deals — dates are derived from First Payment Date"))
        hdr.addStretch()
        for label, obj, slot in [
            ("+ Add Deal",   "PrimaryBtn", self._add),
            ("Edit",         "",           self._edit),
            ("Delete",       "DangerBtn",  self._delete),
            ("Gen Schedule", "GreenBtn",   self._gen_schedule),
        ]:
            btn = QPushButton(label)
            if obj:
                btn.setObjectName(obj)
            btn.clicked.connect(slot)
            hdr.addWidget(btn)
        lay.addLayout(hdr)

        # Filters
        filt = QHBoxLayout()
        filt.setSpacing(10)
        self._search = QLineEdit()
        self._search.setPlaceholderText("Search CUSIP, deal name or client…")
        self._search.setObjectName("SearchBox")
        self._search.textChanged.connect(self._filter)
        self._method_filter = QComboBox()
        self._method_filter.addItems(["All Methods"] + METHODS)
        self._method_filter.currentIndexChanged.connect(self._filter)
        self._rt_filter = QComboBox()
        self._rt_filter.addItems(["All Rate Types"] + RATE_TYPES)
        self._rt_filter.currentIndexChanged.connect(self._filter)
        self._count_lbl = QLabel("")
        self._count_lbl.setStyleSheet("color:#6B7280;font-size:11px;")
        filt.addWidget(self._search, 2)
        filt.addWidget(self._method_filter)
        filt.addWidget(self._rt_filter)
        filt.addStretch()
        filt.addWidget(self._count_lbl)
        lay.addLayout(filt)

        # Table — note First Payment Date replaces Start Date
        cols = [
            "CUSIP", "Deal Name", "Client", "Notional", "Spread",
            "Rate Type", "Frequency", "Method",
            "Obs Shift", "SI", "Pay Delay", "Delay Days",
            "Accrual Basis",
            "Lookback", "Rounding",
            "First Payment Date", "Maturity Date", "Status"
        ]
        self._tbl = DataTable(cols)
        lay.addWidget(self._tbl, 1)

    def _load(self):
        from core.database import get_all_deals
        with self._db() as conn:
            deals = get_all_deals(conn, status=None)
        self._all_rows = deals
        self._render(deals)

    def _render(self, deals):
        rows = []
        for d in deals:
            rows.append([
                d["cusip"],
                d["deal_name"],
                d["client_name"],
                fmt_money(d["notional_amount"]),
                f"{float(d.get('spread') or 0):.4f}",
                d["rate_type"],
                d["payment_frequency"],
                d["calculation_method"],
                d["observation_shift"],
                d["shifted_interest"],
                d["payment_delay"],
                str(d.get("payment_delay_days") or 0),
                d.get("accrual_day_basis") or "Calendar Days",
                str(d["look_back_days"]),
                str(d["rounding_decimals"]),
                make_date_item(d.get("first_payment_date") or d.get("start_date")),
                make_date_item(d["maturity_date"]),
                d["status"],
            ])
        self._tbl.populate(rows)
        self._count_lbl.setText(f"{len(deals)} deals")

    def _filter(self):
        q   = self._search.text().lower()
        mf  = self._method_filter.currentText()
        rtf = self._rt_filter.currentText()
        filtered = [
            d for d in self._all_rows
            if (not q
                or q in d["cusip"].lower()
                or q in d["deal_name"].lower()
                or q in d["client_name"].lower())
            and (mf  == "All Methods"   or d["calculation_method"] == mf)
            and (rtf == "All Rate Types" or d["rate_type"] == rtf)
        ]
        self._render(filtered)

    def _selected_cusip(self):
        data = self._tbl.selected_row_data()
        return data[0] if data else None

    def _selected_deal(self):
        cusip = self._selected_cusip()
        if not cusip:
            return None
        return next((d for d in self._all_rows if d["cusip"] == cusip), None)

    def _add(self):
        dlg = DealDialog(self)
        if dlg.exec() != QDialog.Accepted:
            return
        data = dlg.get_data()
        try:
            from core.database import insert_deal
            with self._db() as conn:
                insert_deal(conn, data)
            QMessageBox.information(self, "Success",
                f"Deal {data['cusip']} added successfully.")
            self._load()
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _edit(self):
        deal = self._selected_deal()
        if not deal:
            QMessageBox.warning(self, "Select Deal",
                "Please select a deal to edit.")
            return
        dlg = DealDialog(self, deal)
        if dlg.exec() != QDialog.Accepted:
            return
        data = dlg.get_data()
        try:
            from core.database import update_deal
            with self._db() as conn:
                recalc = update_deal(conn, deal["cusip"], data)
            msg = "Deal updated."
            if recalc["schedule_rows_updated"] or recalc["log_rows_updated"]:
                msg += (
                    f"\n\nRecalculated {recalc['schedule_rows_updated']} calculated "
                    f"schedule row(s) and {recalc['log_rows_updated']} "
                    f"history row(s) for the updated spread."
                )
            QMessageBox.information(self, "Success", msg)
            self._load()
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _delete(self):
        cusip = self._selected_cusip()
        if not cusip:
            QMessageBox.warning(self, "Select Deal",
                "Please select a deal to delete.")
            return
        if QMessageBox.question(
            self, "Confirm Delete",
            f"Delete deal {cusip} and its entire payment schedule?",
            QMessageBox.Yes | QMessageBox.No
        ) != QMessageBox.Yes:
            return
        try:
            from core.database import delete_deal
            with self._db() as conn:
                delete_deal(conn, cusip)
            self._load()
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _gen_schedule(self):
        cusip = self._selected_cusip()
        if not cusip:
            QMessageBox.warning(self, "Select Deal",
                "Please select a deal to generate its schedule.")
            return
        try:
            from core.database import generate_schedule
            with self._db() as conn:
                generate_schedule(conn, cusip, rebuild=True)
            QMessageBox.information(self, "Done",
                f"Payment schedule generated for {cusip}.")
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def showEvent(self, event):
        super().showEvent(event)
        QTimer.singleShot(0, self._load)
