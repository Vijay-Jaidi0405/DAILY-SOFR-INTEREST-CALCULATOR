"""ui/pages/deals.py — Deal Master management page."""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QLineEdit, QComboBox, QDialog, QFormLayout, QDialogButtonBox,
    QMessageBox, QDoubleSpinBox, QSpinBox, QDateEdit, QScrollArea,
    QSizePolicy, QFrame, QGroupBox, QCheckBox, QFileDialog, QProgressBar
)
from PySide6.QtCore import Qt, QDate, QTimer, QThread, Signal
from PySide6.QtGui import QFont, QIntValidator, QGuiApplication
from ui.widgets.common import (
    Panel, PageHeader, DataTable, fmt_money, fmt_date, make_date_item,
    CheckableComboBox
)
from core.database import (
    DEFAULT_HOLIDAY_CALENDAR,
    deal_period_holiday_calendar,
    deal_rate_holiday_calendar,
    list_holiday_calendars,
)
from ui.styles import GREEN, RED, ACCENT, BORDER

METHODS    = ["Compounded in Arrears", "Simple Average in Arrears", "SOFR Index"]
RATE_TYPES = ["SOFR", "SOFR Index"]
FREQS      = ["Monthly", "Quarterly"]
YN         = ["N", "Y"]
LOOKBACKS  = [0, 1, 2, 5]
ACCRUAL_BASES = ["Calendar Days", "Observation Period Days"]
class _DealImportThread(QThread):
    done = Signal(int, list)
    error = Signal(str)

    def __init__(self, db_factory, path):
        super().__init__()
        self._db = db_factory
        self._path = path

    def run(self):
        try:
            from core.database import import_deals_from_excel
            with self._db() as conn:
                n, errs = import_deals_from_excel(conn, self._path)
            self.done.emit(n, errs)
        except Exception as e:
            self.error.emit(str(e))


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
    Collects deal information including the first schedule boundary date.
    Period start/end and observation dates are derived automatically
    and shown in a live preview panel inside the dialog.
    """

    def __init__(self, parent=None, deal: dict | None = None):
        super().__init__(parent)
        self.setWindowTitle("Add Deal" if deal is None else "Edit Deal")
        self.setMinimumSize(620, 560)
        self._deal = deal
        self._db = getattr(parent, "_db", None)
        self._build()
        self._apply_dialog_geometry()
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
        self.f_use_floor = QCheckBox("Enable Daily Floor")
        self.f_daily_floor = QDoubleSpinBox()
        self.f_daily_floor.setRange(-100.0, 100.0)
        self.f_daily_floor.setDecimals(4)
        self.f_daily_floor.setSingleStep(0.01)
        self.f_daily_floor.setValue(0.0)
        self.f_daily_floor.setEnabled(False)

        form1.addRow("Deal Name *",       self.f_deal_name)
        form1.addRow("Client Name *",     self.f_client)
        form1.addRow("CUSIP (9 chars) *", self.f_cusip)
        form1.addRow("Notional Amount *", self.f_notional)
        form1.addRow("Spread",            self.f_spread)
        form1.addRow(self.f_use_floor,    self.f_daily_floor)
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
        self.f_rounding    = QSpinBox()
        self.f_rounding.setRange(0, 20)
        self.f_rounding.setValue(7)
        self.f_rounding.setToolTip("Number of decimal places for rounding the final rate.")

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
        self.f_lookback    = QSpinBox()
        self.f_lookback.setRange(0, 999)
        self.f_lookback.setValue(2)
        self.f_accrual_basis = combo(ACCRUAL_BASES)
        self.f_rate_holiday_sets = CheckableComboBox()
        self.f_rate_holiday_sets.setToolTip(
            "Holiday calendar used to shift observation dates and rate lookups."
        )
        self.f_period_holiday_sets = CheckableComboBox()
        self.f_period_holiday_sets.setToolTip(
            "Holiday calendar used for period dates, payment dates, and schedule rolling."
        )
        self._reload_holiday_calendar_pickers()
        self._manage_holiday_btn = QPushButton("Manage Holiday Calendars")
        self._manage_holiday_btn.clicked.connect(self._open_holiday_calendar_manager)
        self._manage_holiday_btn.setToolTip(
            "Open Holiday Management to add or update market calendars."
        )

        # Payment delay days — only active when Pay Delay = Y
        self.f_delay_days = QSpinBox()
        self.f_delay_days.setRange(0, 30)
        self.f_delay_days.setValue(2)
        self.f_delay_days.setEnabled(False)
        self.f_delay_days.setToolTip(
            "Business days added to period end to compute payment date"
        )

        form3.addRow("Observation Shift",  self.f_obs_shift)
        form3.addRow("Shifted Interest",   self.f_shifted_int)
        form3.addRow("Payment Delay",      self.f_pay_delay)
        form3.addRow("Delay Days",         self.f_delay_days)
        form3.addRow("Look Back Days",     self.f_lookback)
        form3.addRow("Accrual Days Basis",  self.f_accrual_basis)
        form3.addRow("Rate Holidays",       self.f_rate_holiday_sets)
        form3.addRow("Period Holidays",     self.f_period_holiday_sets)
        form3.addRow("",                    self._manage_holiday_btn)
        body_lay.addLayout(form3)
        body_lay.addSpacing(16)
        body_lay.addWidget(_divider())

        # ── Section 4: Key dates ────────────────────────────────────────────
        body_lay.addWidget(_sec("Key Dates"))

        # Three date pickers side by side: Issue Date | First Boundary Date | Maturity Date
        dates_row = QHBoxLayout()
        dates_row.setSpacing(20)

        issue_col = QVBoxLayout(); issue_col.setSpacing(6)
        issue_lbl = QLabel("Issue Date *")
        issue_lbl.setStyleSheet("font-size:12px;font-weight:600;color:#4B5563;")
        self.f_issue_date = QDateEdit()
        self.f_issue_date.setCalendarPopup(True)
        self.f_issue_date.setDate(QDate(2024, 1, 9))
        issue_col.addWidget(issue_lbl)
        issue_col.addWidget(self.f_issue_date)

        fpd_col = QVBoxLayout(); fpd_col.setSpacing(6)
        self.f_boundary_lbl = QLabel("First Payment Date *")
        self.f_boundary_lbl.setStyleSheet("font-size:12px;font-weight:600;color:#4B5563;")
        self.f_first_payment = QDateEdit()
        self.f_first_payment.setCalendarPopup(True)
        self.f_first_payment.setDate(QDate(2024, 4, 9))
        fpd_col.addWidget(self.f_boundary_lbl)
        fpd_col.addWidget(self.f_first_payment)

        mat_col = QVBoxLayout(); mat_col.setSpacing(6)
        mat_lbl = QLabel("Maturity Date *")
        mat_lbl.setStyleSheet("font-size:12px;font-weight:600;color:#4B5563;")
        self.f_maturity = QDateEdit()
        self.f_maturity.setCalendarPopup(True)
        self.f_maturity.setDate(QDate(2029, 4, 9))
        mat_col.addWidget(mat_lbl)
        mat_col.addWidget(self.f_maturity)

        dates_row.addLayout(issue_col)
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

        for r in (r1, r2, r3, r4, r5):
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
            "lookback business days, then rolling forward to the next business day.</i>"
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
        for w in [self.f_issue_date, self.f_first_payment, self.f_maturity,
                  self.f_obs_shift, self.f_shifted_int,
                  self.f_lookback, self.f_pay_delay, self.f_delay_days]:
            if hasattr(w, "dateChanged"):
                w.dateChanged.connect(self._refresh_preview)
            elif hasattr(w, "currentTextChanged"):
                w.currentTextChanged.connect(self._refresh_preview)
            elif hasattr(w, "valueChanged"):
                w.valueChanged.connect(self._refresh_preview)
        self.f_rate_holiday_sets.selection_changed.connect(self._refresh_preview)
        self.f_period_holiday_sets.selection_changed.connect(self._refresh_preview)

        self.f_rate_type.currentTextChanged.connect(self._on_rate_type)
        self.f_obs_shift.currentTextChanged.connect(self._on_obs_shift)
        self.f_method.currentTextChanged.connect(self._on_method_change)
        self.f_pay_delay.currentTextChanged.connect(self._on_pay_delay)
        self.f_use_floor.toggled.connect(self._on_use_floor)
        self._on_rate_type(self.f_rate_type.currentText())

    def _apply_dialog_geometry(self):
        screen = self.screen() or QGuiApplication.primaryScreen()
        if not screen:
            self.resize(760, 680)
            return
        available = screen.availableGeometry()
        width = min(820, max(620, int(available.width() * 0.72)))
        height = min(760, max(560, int(available.height() * 0.82)))
        self.resize(width, height)

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
        allow_floor = method != "SOFR Index"
        self.f_use_floor.setEnabled(allow_floor)
        if not allow_floor:
            self.f_use_floor.setChecked(False)
        self._on_use_floor(self.f_use_floor.isChecked())
        self._refresh_preview()

    def _on_obs_shift(self, v):
        if v == "N":
            self.f_shifted_int.setCurrentText("N")
            self.f_shifted_int.setEnabled(False)
        else:
            self.f_shifted_int.setEnabled(True)
        self._refresh_preview()

    def _on_pay_delay(self, v):
        is_payment_delay = v == "Y"
        self.f_delay_days.setEnabled(is_payment_delay)
        if is_payment_delay:
            self.f_obs_shift.setCurrentText("N")
            self.f_obs_shift.setEnabled(False)
            self.f_shifted_int.setCurrentText("N")
            self.f_shifted_int.setEnabled(False)
        else:
            self.f_obs_shift.setEnabled(True)
            self.f_shifted_int.setEnabled(self.f_obs_shift.currentText() == "Y")
        self.f_boundary_lbl.setText("Period End Date *" if v == "Y" else "First Payment Date *")
        self._refresh_preview()

    def _on_use_floor(self, enabled):
        self.f_daily_floor.setEnabled(enabled and self.f_use_floor.isEnabled())
        self._refresh_preview()

    # ── live preview ──────────────────────────────────────────────────────────

    def _refresh_preview(self, *_):
        """Compute Period 1 dates from current form values and display them."""
        try:
            from core.database import (
                _nearest_next_bday, _nearest_prev_bday,
                _add_months, _shift_business_days_back, holiday_calendar_label
            )
            from datetime import date, timedelta

            boundary = self.f_first_payment.date().toPython()
            issue    = self.f_issue_date.date().toPython()
            lb       = self.f_lookback.value()
            pay_del  = self.f_pay_delay.currentText() == "Y"
            obs_sh   = self.f_obs_shift.currentText() == "Y" and not pay_del
            sh_int   = self.f_shifted_int.currentText() == "Y"
            delay_d  = self.f_delay_days.value() if pay_del else 0
            freq     = self.f_frequency.currentText()
            months   = 1 if freq == "Monthly" else 3
            rate_holiday_calendar = self._selected_rate_holiday_calendar()
            period_holiday_calendar = self._selected_period_holiday_calendar()
            floor_note = (
                f"  ·  Daily floor {self.f_daily_floor.value():.4f}%"
                if self.f_use_floor.isChecked() and self.f_use_floor.isEnabled()
                else ""
            )

            if pay_del:
                # For payment-delay deals, onboarding captures the first period-end boundary.
                p_start = _nearest_next_bday(issue, holiday_calendar=period_holiday_calendar)
                p_end = _nearest_next_bday(boundary, holiday_calendar=period_holiday_calendar)
                pay_date = _nearest_next_bday(
                    p_end + timedelta(days=delay_d),
                    holiday_calendar=period_holiday_calendar
                )
            else:
                # Standard deals use first payment date as the first period boundary.
                pay_date = _nearest_next_bday(boundary, holiday_calendar=period_holiday_calendar)
                p_end = _nearest_prev_bday(
                    pay_date - timedelta(days=1),
                    holiday_calendar=period_holiday_calendar
                )
                raw_start = _add_months(boundary, -months)
                p_start = _nearest_next_bday(raw_start, holiday_calendar=period_holiday_calendar)

            # Shifted interest: effective period shifted back by lookback
            if sh_int:
                eff_ps = _nearest_next_bday(
                    _shift_business_days_back(p_start, lb, holiday_calendar=rate_holiday_calendar),
                    holiday_calendar=period_holiday_calendar
                )
                eff_pe = _nearest_next_bday(
                    _shift_business_days_back(p_end, lb, holiday_calendar=rate_holiday_calendar),
                    holiday_calendar=period_holiday_calendar
                )
            else:
                eff_ps, eff_pe = p_start, p_end

            # Observation dates = effective dates shifted back by lookback
            obs_s = _nearest_next_bday(
                _shift_business_days_back(eff_ps, lb, holiday_calendar=rate_holiday_calendar),
                holiday_calendar=rate_holiday_calendar
            )
            obs_e = _nearest_next_bday(
                _shift_business_days_back(eff_pe, lb, holiday_calendar=rate_holiday_calendar),
                holiday_calendar=rate_holiday_calendar
            )

            self._prev_period_start.setText(fmt_date(p_start))
            self._prev_period_end.setText(fmt_date(p_end))
            self._prev_obs_start.setText(fmt_date(obs_s))
            self._prev_obs_end.setText(fmt_date(obs_e))
            self._prev_pay_date.setText(fmt_date(pay_date))
            period_note = (
                f"Period includes {fmt_date(p_start)} to {fmt_date(p_end)} "
                f"(excludes {fmt_date(p_end)})"
                if pay_del else
                f"Period includes {fmt_date(p_start)} to {fmt_date(p_end)} "
                f"(excludes {fmt_date(pay_date)})"
            )
            self._prev_note.setText(
                period_note
                + (f"  ·  Obs window shifted {lb}d back" if obs_sh else "")
                + (f"  ·  Eff period shifted {lb}d back" if sh_int else "")
                + f"  ·  Rate holidays: {holiday_calendar_label(rate_holiday_calendar)}"
                + f"  ·  Period holidays: {holiday_calendar_label(period_holiday_calendar)}"
                + floor_note
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
        daily_floor = d.get("daily_floor")
        self.f_use_floor.setChecked(daily_floor is not None)
        self.f_daily_floor.setValue(float(daily_floor or 0.0))
        self.f_rate_type.setCurrentText(d["rate_type"])
        self.f_frequency.setCurrentText(d["payment_frequency"])
        self.f_method.setCurrentText(d["calculation_method"])
        self.f_obs_shift.setCurrentText(d["observation_shift"])
        self.f_shifted_int.setCurrentText(d["shifted_interest"])
        self.f_pay_delay.setCurrentText(d["payment_delay"])
        self.f_obs_shift.setEnabled(d["payment_delay"] != "Y")
        self.f_shifted_int.setEnabled(
            d["payment_delay"] != "Y" and d["observation_shift"] == "Y"
        )
        self.f_lookback.setValue(int(d["look_back_days"]))
        self.f_accrual_basis.setCurrentText(
            d.get("accrual_day_basis") or "Calendar Days"
        )
        self.f_rate_holiday_sets.set_checked_values(
            str(deal_rate_holiday_calendar(d) or DEFAULT_HOLIDAY_CALENDAR).split("|")
        )
        self.f_period_holiday_sets.set_checked_values(
            str(deal_period_holiday_calendar(d) or DEFAULT_HOLIDAY_CALENDAR).split("|")
        )
        self.f_rounding.setValue(int(d["rounding_decimals"]))
        self.f_delay_days.setValue(int(d.get("payment_delay_days") or 0))
        self.f_delay_days.setEnabled(d["payment_delay"] == "Y")

        if d.get("issue_date"):
            self.f_issue_date.setDate(
                QDate.fromString(d["issue_date"][:10], "yyyy-MM-dd")
            )
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
        issue = self.f_issue_date.date()
        fpd = self.f_first_payment.date()
        mat = self.f_maturity.date()
        boundary_label = "Period End Date" if self.f_pay_delay.currentText() == "Y" else "First Payment Date"
        if issue > fpd:
            errs.append(f"Issue Date must be on or before {boundary_label}")
        if fpd >= mat:
            errs.append(f"Maturity Date must be after {boundary_label}")
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
            "daily_floor":        (self.f_daily_floor.value()
                                   if self.f_use_floor.isChecked()
                                   and self.f_use_floor.isEnabled()
                                   else None),
            "rate_type":          self.f_rate_type.currentText(),
            "payment_frequency":  self.f_frequency.currentText(),
            "calculation_method": self.f_method.currentText(),
            "observation_shift":  ("N" if self.f_pay_delay.currentText() == "Y"
                                   else self.f_obs_shift.currentText()),
            "shifted_interest":   ("N" if self.f_pay_delay.currentText() == "Y"
                                   else self.f_shifted_int.currentText()),
            "payment_delay":      self.f_pay_delay.currentText(),
            "payment_delay_days": (self.f_delay_days.value()
                                   if self.f_pay_delay.currentText() == "Y"
                                   else 0),
            "look_back_days":     self.f_lookback.value(),
            "accrual_day_basis":  self.f_accrual_basis.currentText(),
            "holiday_calendar":   self._selected_period_holiday_calendar(),
            "rate_holiday_calendar": self._selected_rate_holiday_calendar(),
            "period_holiday_calendar": self._selected_period_holiday_calendar(),
            "rounding_decimals":  self.f_rounding.value(),
            "issue_date":         self.f_issue_date.date().toString("yyyy-MM-dd"),
            "first_payment_date": self.f_first_payment.date().toString("yyyy-MM-dd"),
            "maturity_date":      self.f_maturity.date().toString("yyyy-MM-dd"),
        }

    def _selected_calendar_values(self, combo: CheckableComboBox) -> str:
        values = combo.checked_values()
        if not values:
            values = [DEFAULT_HOLIDAY_CALENDAR]
        if "ALL" in values:
            return "ALL"
        order = [value for value, _ in self._holiday_calendar_options(include_all=False)]
        selected = [value for value in order if value in values]
        return "|".join(selected)

    def _holiday_calendar_options(self, include_all: bool = True):
        if self._db:
            with self._db() as conn:
                return list_holiday_calendars(conn, include_all=include_all)
        return list_holiday_calendars(None, include_all=include_all)

    def _reload_holiday_calendar_pickers(self):
        options = self._holiday_calendar_options(include_all=True)
        for combo in (self.f_rate_holiday_sets, self.f_period_holiday_sets):
            combo.model().clear()
            combo.set_required_values(set())
            for value, label in options:
                combo.add_check_item(label, value, checked=(value == "ALL"))

    def _open_holiday_calendar_manager(self):
        main_window = self.window()
        self.reject()

        def _switch():
            try:
                from ui.pages.holidays import HolidaysPage
                for idx, page in enumerate(getattr(main_window, "_pages", [])):
                    if isinstance(page, HolidaysPage):
                        main_window._switch_page(idx)
                        if hasattr(page, "_refresh_calendar_controls"):
                            page._refresh_calendar_controls()
                        if hasattr(page, "_load_holidays"):
                            page._load_holidays()
                        break
            except Exception:
                pass

        QTimer.singleShot(0, _switch)

    def _selected_rate_holiday_calendar(self) -> str:
        return self._selected_calendar_values(self.f_rate_holiday_sets)

    def _selected_period_holiday_calendar(self) -> str:
        return self._selected_calendar_values(self.f_period_holiday_sets)


# ---------------------------------------------------------------------------
# Deals list page
# ---------------------------------------------------------------------------

class DealsPage(QWidget):
    def __init__(self, db_factory, parent=None):
        super().__init__(parent)
        self._db = db_factory
        self._all_rows = []
        self._visible_rows = []
        self._build_ui()

    def _build_ui(self):
        lay = QVBoxLayout(self)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        # Header + action buttons
        hdr = QHBoxLayout()
        hdr.setSpacing(10)
        hdr.addWidget(PageHeader("Deal Master",
            "Manage all active deals — use First Payment Date normally, or Period End Date for payment-delay deals"))
        hdr.addStretch()
        for label, obj, slot in [
            ("+ Add Deal",   "PrimaryBtn", self._add),
            ("Bulk Upload",  "",           self._bulk_upload),
            ("Download",     "",           self._export_deals),
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

        import_row = QHBoxLayout()
        import_row.setSpacing(10)
        self._bulk_file_lbl = QLabel("No Excel file selected")
        self._bulk_file_lbl.setStyleSheet("color:#6B7280;font-size:11px;")
        self._bulk_browse_btn = QPushButton("Browse Excel…")
        self._bulk_import_btn = QPushButton("Import Deals")
        self._bulk_import_btn.setObjectName("PrimaryBtn")
        self._bulk_prog = QProgressBar()
        self._bulk_prog.setVisible(False)
        self._bulk_prog.setRange(0, 0)
        self._bulk_path = None

        self._bulk_browse_btn.clicked.connect(self._browse_bulk_file)
        self._bulk_import_btn.clicked.connect(self._import_bulk_file)

        import_row.addWidget(self._bulk_file_lbl, 1)
        import_row.addWidget(self._bulk_browse_btn)
        import_row.addWidget(self._bulk_import_btn)
        lay.addLayout(import_row)
        lay.addWidget(self._bulk_prog)

        bulk_hint = QLabel(
            "Bulk upload deals from Excel. Required columns: "
            "<b>Deal Name</b>, <b>Client Name</b>, <b>CUSIP</b>, "
            "<b>Notional Amount</b>, <b>Rate Type</b>, "
            "<b>Calculation Method</b>, <b>Issue Date</b>, "
            "<b>First Payment Date</b> (or <b>Period End Date</b> for payment-delay deals), "
            "<b>Maturity Date</b>."
        )
        bulk_hint.setWordWrap(True)
        bulk_hint.setStyleSheet("color:#6B7280;font-size:10px;")
        lay.addWidget(bulk_hint)

        # Table — note First Payment Date replaces Start Date
        cols = [
            "CUSIP", "Deal Name", "Client", "Issue Date", "Notional", "Spread", "Daily Floor",
            "Rate Type", "Frequency", "Method",
            "Obs Shift", "SI", "Pay Delay", "Delay Days",
            "Accrual Basis", "Rate Holidays", "Period Holidays",
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
        from core.database import holiday_calendar_label
        self._visible_rows = list(deals)
        rows = []
        for d in deals:
            rows.append([
                d["cusip"],
                d["deal_name"],
                d["client_name"],
                make_date_item(d.get("issue_date")),
                fmt_money(d["notional_amount"]),
                f"{float(d.get('spread') or 0):.4f}",
                (f"{float(d['daily_floor']):.4f}" if d.get("daily_floor") is not None else "—"),
                d["rate_type"],
                d["payment_frequency"],
                d["calculation_method"],
                d["observation_shift"],
                d["shifted_interest"],
                d["payment_delay"],
                str(d.get("payment_delay_days") or 0),
                d.get("accrual_day_basis") or "Calendar Days",
                holiday_calendar_label(d.get("rate_holiday_calendar") or d.get("holiday_calendar")),
                holiday_calendar_label(d.get("period_holiday_calendar") or d.get("holiday_calendar")),
                str(d["look_back_days"]),
                str(d["rounding_decimals"]),
                make_date_item(d.get("first_payment_date") or d.get("start_date")),
                make_date_item(d["maturity_date"]),
                d["status"],
            ])
        self._tbl.populate(rows)
        self._count_lbl.setText(f"{len(deals)} deals")

    def _export_deals(self):
        if not self._visible_rows:
            QMessageBox.information(self, "No Data",
                "There are no deal rows to download.")
            return
        path, selected_filter = QFileDialog.getSaveFileName(
            self,
            "Download Deal Master Table",
            "deal_master_export.xlsx",
            "Excel Files (*.xlsx);;CSV Files (*.csv)"
        )
        if not path:
            return
        try:
            import pandas as pd
            from core.database import holiday_calendar_label

            export_rows = []
            for d in self._visible_rows:
                export_rows.append({
                    "CUSIP": d["cusip"],
                    "Deal Name": d["deal_name"],
                    "Client Name": d["client_name"],
                    "Issue Date": d.get("issue_date"),
                    "Notional Amount": d["notional_amount"],
                    "Spread": float(d.get("spread") or 0.0),
                    "Daily Floor": d.get("daily_floor"),
                    "Rate Type": d["rate_type"],
                    "Payment Frequency": d["payment_frequency"],
                    "Calculation Method": d["calculation_method"],
                    "Observation Shift": d["observation_shift"],
                    "Shifted Interest": d["shifted_interest"],
                    "Payment Delay": d["payment_delay"],
                    "Payment Delay Days": int(d.get("payment_delay_days") or 0),
                    "Accrual Day Basis": d.get("accrual_day_basis") or "Calendar Days",
                    "Look Back Days": int(d.get("look_back_days") or 0),
                    "Rounding Decimals": int(d.get("rounding_decimals") or 7),
                    "Rate Holiday Calendar": d.get("rate_holiday_calendar") or d.get("holiday_calendar"),
                    "Rate Holiday Label": holiday_calendar_label(
                        d.get("rate_holiday_calendar") or d.get("holiday_calendar")
                    ),
                    "Period Holiday Calendar": d.get("period_holiday_calendar") or d.get("holiday_calendar"),
                    "Period Holiday Label": holiday_calendar_label(
                        d.get("period_holiday_calendar") or d.get("holiday_calendar")
                    ),
                    "First Payment Date": d.get("first_payment_date") or d.get("start_date"),
                    "Maturity Date": d.get("maturity_date"),
                    "Status": d["status"],
                })

            df = pd.DataFrame(export_rows)
            lower_path = path.lower()
            if selected_filter.startswith("CSV") or lower_path.endswith(".csv"):
                if not lower_path.endswith(".csv"):
                    path += ".csv"
                df.to_csv(path, index=False)
            else:
                if not lower_path.endswith(".xlsx"):
                    path += ".xlsx"
                df.to_excel(path, index=False)
            QMessageBox.information(
                self, "Download Complete",
                f"Exported {len(export_rows)} deal row(s) to:\n{path}"
            )
        except Exception as e:
            QMessageBox.critical(self, "Download Error", str(e))

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

    def _bulk_upload(self):
        self._browse_bulk_file()

    def _browse_bulk_file(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Select Deal Excel File", "", "Excel Files (*.xlsx *.xls)"
        )
        if not path:
            return
        self._bulk_path = path
        self._bulk_file_lbl.setText(path.split("/")[-1])

    def _import_bulk_file(self):
        if not self._bulk_path:
            QMessageBox.warning(self, "No File",
                "Please browse and select an Excel file first.")
            return
        self._bulk_prog.setVisible(True)
        self._bulk_browse_btn.setEnabled(False)
        self._bulk_import_btn.setEnabled(False)

        thread = _DealImportThread(self._db, self._bulk_path)
        thread.done.connect(self._on_bulk_import_done)
        thread.error.connect(self._on_bulk_import_error)
        self._bulk_thread = thread
        thread.start()

    def _on_bulk_import_done(self, inserted, errs):
        self._bulk_prog.setVisible(False)
        self._bulk_browse_btn.setEnabled(True)
        self._bulk_import_btn.setEnabled(True)
        msg = f"{inserted} deal row(s) imported into Deal Master."
        if errs:
            msg += f"\n{len(errs)} row(s) failed:\n" + "\n".join(errs[:8])
            if len(errs) > 8:
                msg += "\n..."
        QMessageBox.information(self, "Import Complete", msg)
        self._load()

    def _on_bulk_import_error(self, err):
        self._bulk_prog.setVisible(False)
        self._bulk_browse_btn.setEnabled(True)
        self._bulk_import_btn.setEnabled(True)
        QMessageBox.critical(self, "Import Error", err)

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

    # Global search hook from MainWindow
    def apply_search(self, term: str):
        self._search.setText(term)

    def showEvent(self, event):
        super().showEvent(event)
        QTimer.singleShot(0, self._load)
