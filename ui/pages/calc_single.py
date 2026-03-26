"""ui/pages/calc_single.py — Single CUSIP interest calculation page."""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QComboBox, QSpinBox, QDateEdit,
    QMessageBox, QFormLayout, QFrame, QSizePolicy,
    QScrollArea, QGroupBox, QSpacerItem
)
from PySide6.QtCore import Qt, QDate, QTimer, QSize
from PySide6.QtGui import QFont
from ui.widgets.common import (
    Panel, PageHeader, KpiCard,
    fmt_money, fmt_rate, fmt_date
)
from ui.styles import GREEN, PURPLE, AMBER, ACCENT, RED, BORDER, BG_LIGHT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _section_label(text: str) -> QLabel:
    """Small uppercase section divider label."""
    lbl = QLabel(text.upper())
    lbl.setStyleSheet(
        "color:#6B7280; font-size:10px; font-weight:700; "
        "letter-spacing:1.2px; padding:4px 0 2px 0;"
    )
    return lbl


def _divider() -> QFrame:
    f = QFrame()
    f.setFrameShape(QFrame.HLine)
    f.setStyleSheet(f"background:{BORDER}; max-height:1px; border:none;")
    return f


def _info_row(label: str, widget: QLabel) -> QHBoxLayout:
    """A label:value row used in the deal info block."""
    row = QHBoxLayout()
    row.setSpacing(8)
    lbl = QLabel(label)
    lbl.setStyleSheet("color:#6B7280; font-size:12px; min-width:110px;")
    lbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
    widget.setStyleSheet(
        "color:#1E2533; font-size:13px; font-weight:600;"
    )
    row.addWidget(lbl)
    row.addWidget(widget, 1)
    return row


class CalcSinglePage(QWidget):
    def __init__(self, db_factory, parent=None):
        super().__init__(parent)
        self._db      = db_factory
        self._deals   = {}
        self._periods = []
        self._last_result = None
        self._build_ui()

    # =========================================================================
    # UI construction
    # =========================================================================

    def _build_ui(self):
        """
        Layout:
          ┌─────────────────────────────────────────────────────────────┐
          │  [Scrollable left sidebar]   │   [Right results panel]      │
          │  - Page header               │   - KPI cards (2 rows)       │
          │  - Deal selection            │   - Calculation detail scroll │
          │  - Upcoming period           │   - Action buttons            │
          │  - Calc parameters           │                               │
          │  - Calculate button          │                               │
          └─────────────────────────────────────────────────────────────┘
        """
        root = QHBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # Left scrollable sidebar
        root.addWidget(self._build_left_scroll(), 38)

        # Thin vertical separator
        sep = QFrame()
        sep.setFrameShape(QFrame.VLine)
        sep.setStyleSheet(f"background:{BORDER}; max-width:1px; border:none;")
        root.addWidget(sep)

        # Right results area
        root.addLayout(self._build_right(), 62)

    # ── Left scrollable sidebar ───────────────────────────────────────────────

    def _build_left_scroll(self) -> QScrollArea:
        """Wrap the entire left column in a scroll area."""
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        scroll.setFrameShape(QScrollArea.NoFrame)
        scroll.setStyleSheet(f"background:{BG_LIGHT};")

        container = QWidget()
        container.setStyleSheet(f"background:{BG_LIGHT};")
        lay = QVBoxLayout(container)
        lay.setContentsMargins(20, 20, 16, 20)
        lay.setSpacing(0)

        # ── Page header ───────────────────────────────────────────────────────
        lay.addWidget(PageHeader(
            "Interest Calculator",
            "Select a deal — dates auto-populate from the payment schedule"
        ))
        lay.addSpacing(16)

        # ── 1. Deal selection ─────────────────────────────────────────────────
        lay.addWidget(_section_label("1  ·  Deal Selection"))
        lay.addSpacing(6)
        lay.addWidget(self._build_deal_card())
        lay.addSpacing(20)

        # ── 2. Upcoming period ────────────────────────────────────────────────
        lay.addWidget(_section_label("2  ·  Upcoming Payment Period"))
        lay.addSpacing(6)
        lay.addWidget(self._build_period_card())
        lay.addSpacing(20)

        # ── 3. Calculation parameters ─────────────────────────────────────────
        lay.addWidget(_section_label("3  ·  Calculation Parameters"))
        lay.addSpacing(6)
        lay.addWidget(self._build_params_card())
        lay.addSpacing(20)

        # ── Calculate button ──────────────────────────────────────────────────
        calc_btn = QPushButton("  Calculate Interest")
        calc_btn.setObjectName("PrimaryBtn")
        calc_btn.setMinimumHeight(48)
        calc_btn.setFont(QFont("Segoe UI", 13, QFont.Bold))
        calc_btn.setStyleSheet("""
            QPushButton {
                background: #0F2147;
                color: #FFFFFF;
                border: none;
                border-radius: 8px;
                font-size: 14px;
                font-weight: 700;
                letter-spacing: 0.5px;
            }
            QPushButton:hover  { background: #162E5F; }
            QPushButton:pressed { background: #0A1830; }
        """)
        calc_btn.clicked.connect(self._calculate)
        lay.addWidget(calc_btn)

        lay.addStretch()
        scroll.setWidget(container)
        return scroll

    def _build_deal_card(self) -> QWidget:
        card = QWidget()
        card.setObjectName("Panel")
        lay  = QVBoxLayout(card)
        lay.setContentsMargins(18, 16, 18, 18)
        lay.setSpacing(14)

        # CUSIP combo — full width
        self._cusip_combo = QComboBox()
        self._cusip_combo.setPlaceholderText("Select deal…")
        self._cusip_combo.setSizePolicy(
            QSizePolicy.Expanding, QSizePolicy.Fixed
        )
        self._cusip_combo.currentIndexChanged.connect(self._on_deal_select)
        lay.addWidget(QLabel("Deal / CUSIP"))
        lay.addWidget(self._cusip_combo)

        lay.addWidget(_divider())

        # Deal info grid
        self._lbl_method   = QLabel("—")
        self._lbl_rt       = QLabel("—")
        self._lbl_notional = QLabel("—")
        self._lbl_obs      = QLabel("—")
        self._lbl_lb       = QLabel("—")

        for label, widget in [
            ("Method:",        self._lbl_method),
            ("Rate Type:",     self._lbl_rt),
            ("Notional:",      self._lbl_notional),
            ("Obs Shift / SI:",self._lbl_obs),
            ("Lookback:",      self._lbl_lb),
        ]:
            lay.addLayout(_info_row(label, widget))

        return card

    def _build_period_card(self) -> QWidget:
        card = QWidget()
        card.setObjectName("Panel")
        lay  = QVBoxLayout(card)
        lay.setContentsMargins(18, 16, 18, 18)
        lay.setSpacing(12)

        # Status banner
        self._next_banner = QLabel("Select a deal to see upcoming periods.")
        self._next_banner.setWordWrap(True)
        self._next_banner.setMinimumHeight(38)
        self._next_banner.setAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        self._next_banner.setStyleSheet(
            "background:#EFF6FF; color:#1E40AF; border-radius:8px;"
            "padding:10px 14px; font-size:12px; font-weight:600;"
        )
        lay.addWidget(self._next_banner)

        # Period selector
        lay.addWidget(QLabel("Select Period:"))
        self._period_combo = QComboBox()
        self._period_combo.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self._period_combo.currentIndexChanged.connect(self._on_period_select)
        lay.addWidget(self._period_combo)

        lay.addWidget(_divider())

        # Obs dates — read-only display
        obs_grid = QHBoxLayout()
        obs_grid.setSpacing(16)

        obs_start_col = QVBoxLayout()
        obs_start_col.setSpacing(4)
        obs_start_col.addWidget(QLabel("Obs Period Start"))
        self._lbl_obs_start = self._ro_label()
        obs_start_col.addWidget(self._lbl_obs_start)

        obs_end_col = QVBoxLayout()
        obs_end_col.setSpacing(4)
        obs_end_col.addWidget(QLabel("Obs Period End"))
        self._lbl_obs_end = self._ro_label()
        obs_end_col.addWidget(self._lbl_obs_end)

        obs_grid.addLayout(obs_start_col)
        obs_grid.addLayout(obs_end_col)
        lay.addLayout(obs_grid)

        return card

    def _build_params_card(self) -> QWidget:
        card = QWidget()
        card.setObjectName("Panel")
        lay  = QVBoxLayout(card)
        lay.setContentsMargins(18, 16, 18, 18)
        lay.setSpacing(14)

        # Each date field: label above, field below (clear and spacious)
        def date_col(label_text: str, widget: QDateEdit) -> QVBoxLayout:
            col = QVBoxLayout()
            col.setSpacing(5)
            lbl = QLabel(label_text)
            lbl.setStyleSheet("font-size:12px; color:#4B5563; font-weight:600;")
            col.addWidget(lbl)
            col.addWidget(widget)
            return col

        # Row 1: Period Start / Period End
        row1 = QHBoxLayout()
        row1.setSpacing(14)
        self._start = QDateEdit(); self._start.setCalendarPopup(True)
        self._end   = QDateEdit(); self._end.setCalendarPopup(True)
        row1.addLayout(date_col("Period Start", self._start))
        row1.addLayout(date_col("Period End",   self._end))
        lay.addLayout(row1)

        # Row 2: Payment Date
        row2 = QHBoxLayout()
        row2.setSpacing(14)
        self._pay = QDateEdit(); self._pay.setCalendarPopup(True)
        row2.addLayout(date_col("Payment Date", self._pay))
        row2.addStretch()
        lay.addLayout(row2)

        lay.addWidget(_divider())

        # Row 3: Delay Days / Day Count Basis
        row3 = QHBoxLayout()
        row3.setSpacing(14)

        delay_col = QVBoxLayout()
        delay_col.setSpacing(5)
        delay_lbl = QLabel("Delay Days")
        delay_lbl.setStyleSheet("font-size:12px; color:#4B5563; font-weight:600;")
        self._delay = QSpinBox()
        self._delay.setRange(0, 30)
        self._delay.setValue(2)
        self._delay.setMinimumHeight(38)
        delay_col.addWidget(delay_lbl)
        delay_col.addWidget(self._delay)

        dc_col = QVBoxLayout()
        dc_col.setSpacing(5)
        dc_lbl = QLabel("Day Count Basis")
        dc_lbl.setStyleSheet("font-size:12px; color:#4B5563; font-weight:600;")
        self._dc = QComboBox()
        self._dc.addItems(["360", "365"])
        self._dc.setMinimumHeight(38)
        dc_col.addWidget(dc_lbl)
        dc_col.addWidget(self._dc)

        row3.addLayout(delay_col)
        row3.addLayout(dc_col)
        lay.addLayout(row3)

        return card

    # ── Right results panel ───────────────────────────────────────────────────

    def _build_right(self) -> QVBoxLayout:
        right = QVBoxLayout()
        right.setContentsMargins(20, 20, 20, 20)
        right.setSpacing(16)

        right.addWidget(PageHeader("Results", "Calculation output appears here after running"))

        # KPI row 1
        kpi1 = QHBoxLayout(); kpi1.setSpacing(12)
        self._k_comp  = KpiCard("Comp / Avg Rate", "—", color=GREEN)
        self._k_ann   = KpiCard("Annualized Rate",  "—", color=ACCENT)
        self._k_round = KpiCard("Rounded Rate",     "—", color=PURPLE)
        kpi1.addWidget(self._k_comp)
        kpi1.addWidget(self._k_ann)
        kpi1.addWidget(self._k_round)
        right.addLayout(kpi1)

        # KPI row 2
        kpi2 = QHBoxLayout(); kpi2.setSpacing(12)
        self._k_int   = KpiCard("Interest Amount",    "—", color=GREEN)
        self._k_days  = KpiCard("Accrual Days",       "—")
        self._k_pay   = KpiCard("Adj Payment Date",   "—", color=AMBER)
        kpi2.addWidget(self._k_int)
        kpi2.addWidget(self._k_days)
        kpi2.addWidget(self._k_pay)
        right.addLayout(kpi2)

        # Detail panel with scroll
        detail_panel = QWidget()
        detail_panel.setObjectName("Panel")
        dp_lay = QVBoxLayout(detail_panel)
        dp_lay.setContentsMargins(0, 0, 0, 0)
        dp_lay.setSpacing(0)

        dp_hdr = QLabel("CALCULATION DETAIL")
        dp_hdr.setObjectName("PanelTitle")
        dp_lay.addWidget(dp_hdr)

        self._detail_lbl = QLabel(
            "<span style='color:#9CA3AF; font-size:13px;'>"
            "Select a deal and press <b>Calculate Interest</b> to see results."
            "</span>"
        )
        self._detail_lbl.setWordWrap(True)
        self._detail_lbl.setAlignment(Qt.AlignTop)
        self._detail_lbl.setStyleSheet("padding:16px; font-size:13px;")

        scroll = QScrollArea()
        scroll.setWidget(self._detail_lbl)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.NoFrame)
        scroll.setStyleSheet("background:white;")
        dp_lay.addWidget(scroll)
        right.addWidget(detail_panel, 1)

        # Action buttons
        btn_row = QHBoxLayout()
        btn_row.setSpacing(10)
        self._mark_btn = QPushButton("Mark as Calculated")
        self._mark_btn.setObjectName("GreenBtn")
        self._mark_btn.setVisible(False)
        self._mark_btn.clicked.connect(self._mark_calculated)

        self._pdf_btn = QPushButton("Export PDF")
        self._pdf_btn.setVisible(False)
        self._pdf_btn.clicked.connect(self._export_pdf)

        btn_row.addStretch()
        btn_row.addWidget(self._pdf_btn)
        btn_row.addWidget(self._mark_btn)
        right.addLayout(btn_row)

        return right

    # =========================================================================
    # Helpers
    # =========================================================================

    @staticmethod
    def _ro_label() -> QLabel:
        lbl = QLabel("—")
        lbl.setStyleSheet(
            "background:#EFF6FF; color:#1E40AF; border-radius:6px;"
            "padding:8px 12px; font-size:13px; font-weight:700;"
            "font-family:monospace;"
        )
        lbl.setAlignment(Qt.AlignCenter)
        lbl.setMinimumHeight(38)
        return lbl

    @staticmethod
    def _to_qdate(v) -> QDate:
        from datetime import date as dt
        if isinstance(v, dt):
            return QDate(v.year, v.month, v.day)
        if v and str(v) != "None":
            try:
                d = dt.fromisoformat(str(v)[:10])
                return QDate(d.year, d.month, d.day)
            except Exception:
                pass
        return QDate.currentDate()

    # =========================================================================
    # Data loading
    # =========================================================================

    def _load_deals(self):
        from core.database import get_all_deals
        with self._db() as conn:
            deals = get_all_deals(conn)

        prev = self._cusip_combo.currentData()
        self._cusip_combo.blockSignals(True)
        self._cusip_combo.clear()
        self._cusip_combo.addItem("— Select a deal —", None)
        self._deals = {d["cusip"]: d for d in deals}
        for d in deals:
            self._cusip_combo.addItem(
                f"{d['cusip']}  ·  {d['deal_name']}", d["cusip"]
            )
        if prev:
            idx = self._cusip_combo.findData(prev)
            if idx >= 0:
                self._cusip_combo.setCurrentIndex(idx)
        self._cusip_combo.blockSignals(False)
        if self._cusip_combo.currentData():
            self._on_deal_select()

    def _on_deal_select(self):
        cusip = self._cusip_combo.currentData()

        # Reset period UI
        self._period_combo.blockSignals(True)
        self._period_combo.clear()
        self._period_combo.blockSignals(False)
        self._next_banner.setText("Select a deal to see upcoming periods.")
        self._next_banner.setStyleSheet(
            "background:#F3F4F6; color:#6B7280; border-radius:8px;"
            "padding:10px 14px; font-size:12px;"
        )
        self._lbl_obs_start.setText("—")
        self._lbl_obs_end.setText("—")
        self._periods = []

        if not cusip or cusip not in self._deals:
            for lbl in [self._lbl_method, self._lbl_rt,
                        self._lbl_notional, self._lbl_obs, self._lbl_lb]:
                lbl.setText("—")
            return

        d   = self._deals[cusip]
        rnd = d["rounding_decimals"]

        self._lbl_method.setText(d["calculation_method"])
        self._lbl_rt.setText(d["rate_type"])
        self._lbl_notional.setText(fmt_money(d["notional_amount"]))
        self._lbl_obs.setText(
            f"Shift={d['observation_shift']}  SI={d['shifted_interest']}  "
            f"PayDelay={d['payment_delay']}"
        )
        self._lbl_lb.setText(f"{d['look_back_days']} days  ·  Round {rnd} dp")
        self._delay.setEnabled(d["payment_delay"] == "Y")

        self._load_periods(cusip)

    def _load_periods(self, cusip: str):
        from core.database import get_full_schedule
        from datetime import date

        with self._db() as conn:
            all_periods = get_full_schedule(conn, cusip)

        today = date.today()
        upcoming = [p for p in all_periods if p["period_status"] != "Calculated"]

        if not upcoming:
            calculated = [p for p in all_periods if p["period_status"] == "Calculated"]
            if calculated:
                self._next_banner.setText("All periods calculated. Showing last.")
                self._next_banner.setStyleSheet(
                    "background:#EDE9FE; color:#5B21B6; border-radius:8px;"
                    "padding:10px 14px; font-size:12px; font-weight:600;"
                )
                upcoming = [calculated[-1]]
            else:
                self._next_banner.setText(
                    "No schedule found. Go to Payment Schedule to generate it."
                )
                self._next_banner.setStyleSheet(
                    "background:#FEF3C7; color:#92400E; border-radius:8px;"
                    "padding:10px 14px; font-size:12px;"
                )
                return

        self._periods = upcoming
        self._period_combo.blockSignals(True)
        self._period_combo.clear()
        for p in upcoming:
            tag = "  ✓ Ready" if p["period_status"] == "Ready to Calculate" else "  · Scheduled"
            self._period_combo.addItem(
                f"Period {p['period_number']}  "
                f"{fmt_date(p['period_start_date'])} → {fmt_date(p['period_end_date'])}"
                f"{tag}",
                p["period_number"]
            )
        self._period_combo.blockSignals(False)
        self._period_combo.setCurrentIndex(0)
        self._populate_dates_from_period(upcoming[0])

        next_p   = upcoming[0]
        days_left = (
            date.fromisoformat(next_p["adj_payment_date"]) - today
        ).days if next_p.get("adj_payment_date") else "?"

        if next_p["period_status"] == "Ready to Calculate":
            style = ("background:#DCFCE7; color:#065F46; border-radius:8px;"
                     "padding:10px 14px; font-size:12px; font-weight:600;")
            icon = "✓ Ready to Calculate"
        else:
            style = ("background:#EFF6FF; color:#1E40AF; border-radius:8px;"
                     "padding:10px 14px; font-size:12px; font-weight:600;")
            icon = "⏱ Scheduled"

        self._next_banner.setStyleSheet(style)
        self._next_banner.setText(
            f"{icon}  —  Next Payment: {fmt_date(next_p['adj_payment_date'])}"
            f"  ({days_left} days away)  ·  "
            f"Period {next_p['period_number']} of {len(all_periods)}"
        )

    def _on_period_select(self, index: int):
        if 0 <= index < len(self._periods):
            self._populate_dates_from_period(self._periods[index])

    def _populate_dates_from_period(self, period: dict):
        self._start.setDate(self._to_qdate(period["period_start_date"]))
        self._end.setDate(self._to_qdate(period["period_end_date"]))
        pay = period.get("adj_payment_date") or period.get("unadj_payment_date")
        self._pay.setDate(self._to_qdate(pay))
        self._lbl_obs_start.setText(fmt_date(period["obs_start_date"]))
        self._lbl_obs_end.setText(fmt_date(period["obs_end_date"]))

        cusip = self._cusip_combo.currentData()
        if cusip and cusip in self._deals:
            d = self._deals[cusip]
            if d["payment_delay"] == "Y":
                self._delay.setValue(int(period.get("payment_delay_days") or 2))

    # =========================================================================
    # Calculation
    # =========================================================================

    def _calculate(self):
        cusip = self._cusip_combo.currentData()
        if not cusip:
            QMessageBox.warning(self, "No Deal", "Please select a deal first.")
            return

        p_start = self._start.date().toPython()
        p_end   = self._end.date().toPython()
        pay     = self._pay.date().toPython()

        if p_end <= p_start:
            QMessageBox.warning(self, "Date Error",
                "Period End must be after Period Start.")
            return

        try:
            from core.database import calculate_interest
            with self._db() as conn:
                res = calculate_interest(
                    conn, cusip, p_start, p_end, pay,
                    delay_days=self._delay.value(),
                    dc=int(self._dc.currentText()),
                    log=True
                )
            self._last_result = res
            self._show_result(res)
            self._export_pdf(silent=True)
        except Exception as e:
            QMessageBox.critical(self, "Calculation Error", str(e))

    # =========================================================================
    # Result display
    # =========================================================================

    def _show_result(self, res: dict):
        rnd = res["rounding_decimals"]
        self._k_comp.set_value(fmt_rate(res["compounded_rate"], rnd),  GREEN)
        self._k_ann.set_value(fmt_rate(res["annualized_rate"],  rnd),  ACCENT)
        self._k_round.set_value(fmt_rate(res["rounded_rate"],   rnd),  PURPLE)
        self._k_int.set_value(fmt_money(res["interest_amount"]),       GREEN)
        self._k_days.set_value(str(res["accrual_days"]))
        self._k_pay.set_value(fmt_date(res["adjusted_payment_date"]),  AMBER)

        delay_note = (
            f" (+{res['payment_delay_days']}d delay)"
            if res["payment_delay_days"] else ""
        )
        formula = {
            "Compounded in Arrears":     "PRODUCT(1 + r_i × d_i / DC) − 1",
            "Simple Average in Arrears": "SUM(r_i × d_i) / SUM(d_i)",
            "SOFR Index":               "(Index_End / Index_Start − 1) × (DC / Accrual)",
        }.get(res["calculation_method"], "")
        if res.get("spread"):
            formula += f"  +  Spread ({res['spread']:.4f}%)"

        html = f"""
<div style='font-family:Segoe UI,Arial,sans-serif; font-size:13px; padding:16px;'>

<table width='100%' cellspacing='0' cellpadding='0'
       style='border-collapse:collapse; margin-bottom:16px;
              background:#F8FAFC; border-radius:8px;
              border:1px solid #E2E8F0;'>
  <tr style='background:#1B3A6B;'>
    <td colspan='4' style='padding:10px 14px; color:white; font-weight:700;
        font-size:12px; letter-spacing:0.5px; border-radius:8px 8px 0 0;'>
      DEAL INFORMATION
    </td>
  </tr>
  <tr>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px; width:18%;'>Deal</td>
    <td style='padding:10px 14px; font-weight:600; width:32%;'>{res['deal_name']}</td>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px; width:18%;'>Client</td>
    <td style='padding:10px 14px; font-weight:600; width:32%;'>{res['client_name']}</td>
  </tr>
  <tr style='background:#F1F5F9;'>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px;'>CUSIP</td>
    <td style='padding:10px 14px; font-family:monospace; font-weight:600;'>{res['cusip']}</td>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px;'>Method</td>
    <td style='padding:10px 14px; font-weight:600;'>{res['calculation_method']}</td>
  </tr>
  <tr>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px;'>Spread</td>
    <td style='padding:10px 14px; font-weight:600;'>{res.get('spread', 0):.4f}%</td>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px;'>All-in Annualized</td>
    <td style='padding:10px 14px; font-weight:700; color:#1E40AF;'>{fmt_rate(res['annualized_rate'], rnd)}</td>
  </tr>
</table>

<table width='100%' cellspacing='0' cellpadding='0'
       style='border-collapse:collapse; margin-bottom:16px;
              border:1px solid #E2E8F0; border-radius:8px;'>
  <tr style='background:#0D5C63;'>
    <td colspan='4' style='padding:10px 14px; color:white; font-weight:700;
        font-size:12px; letter-spacing:0.5px; border-radius:8px 8px 0 0;'>
      INTEREST PERIOD &amp; OBSERVATION DATES
    </td>
  </tr>
  <tr>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px; width:22%;'>Period Start</td>
    <td style='padding:10px 14px; font-weight:600; width:28%;'>{fmt_date(res['period_start_date'])}</td>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px; width:22%;'>Period End</td>
    <td style='padding:10px 14px; font-weight:600; width:28%;'>{fmt_date(res['period_end_date'])}</td>
  </tr>
  <tr style='background:#F1F5F9;'>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px;'>Obs Start</td>
    <td style='padding:10px 14px; color:#1E40AF; font-weight:700;'>{fmt_date(res['obs_start_date'])}</td>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px;'>Obs End</td>
    <td style='padding:10px 14px; color:#1E40AF; font-weight:700;'>{fmt_date(res['obs_end_date'])}</td>
  </tr>
  <tr>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px;'>Payment Date</td>
    <td style='padding:10px 14px; font-weight:600;'>{fmt_date(res['payment_date'])}</td>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px;'>Adj Payment Date</td>
    <td style='padding:10px 14px; font-weight:700; color:#065F46;'>{fmt_date(res['adjusted_payment_date'])}{delay_note}</td>
  </tr>
  <tr style='background:#F1F5F9;'>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px;'>Lookback</td>
    <td style='padding:10px 14px; font-weight:600;'>{res['look_back_days']}d</td>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px;'>Day Count</td>
    <td style='padding:10px 14px; font-weight:600;'>ACT / {res['day_count_basis']}</td>
  </tr>
  <tr>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px;'>Accrual Basis</td>
    <td style='padding:10px 14px; font-weight:600;'>{res.get('accrual_day_basis', 'Calendar Days')}</td>
    <td style='padding:10px 14px; color:#6B7280; font-size:12px;'>Accrual Days</td>
    <td style='padding:10px 14px; font-weight:600;'>{res['accrual_days']}</td>
  </tr>
</table>

<div style='background:#ECFDF5; border:1px solid #A7F3D0; border-radius:8px;
     padding:12px 16px; margin-bottom:16px;'>
  <span style='color:#6B7280; font-size:11px; font-weight:700; letter-spacing:0.5px;'>
    FORMULA
  </span><br/>
  <span style='font-family:monospace; color:#065F46; font-size:12px;'>{formula}</span>
</div>
"""
        html += self._build_daily_table(res)
        html += "</div>"

        self._detail_lbl.setText(html)
        self._mark_btn.setVisible(True)
        self._pdf_btn.setVisible(True)

    @staticmethod
    def _build_daily_table(res: dict) -> str:
        method     = res.get("calculation_method", "")
        daily_rows = res.get("daily_rows") or []
        if not daily_rows:
            return ""

        if method == "SOFR Index":
            r = daily_rows
            html = (
                "<table width='100%' cellspacing='0' cellpadding='0' "
                "style='border-collapse:collapse; border:1px solid #E2E8F0; border-radius:8px;'>"
                "<tr style='background:#1B3A6B;'>"
                "<td style='padding:10px 12px; color:white; font-weight:700; "
                "font-size:11px; letter-spacing:0.5px;' colspan='4'>"
                "SOFR INDEX VALUES USED</td></tr>"
                "<tr style='background:#F8FAFC;'>"
                "<td style='padding:8px 12px; font-weight:700; font-size:11px; "
                "color:#6B7280;'>LABEL</td>"
                "<td style='padding:8px 12px; font-weight:700; font-size:11px; "
                "color:#6B7280;'>DATE</td>"
                "<td style='padding:8px 12px; font-weight:700; font-size:11px; "
                "color:#6B7280; text-align:right;'>SOFR RATE (%)</td>"
                "<td style='padding:8px 12px; font-weight:700; font-size:11px; "
                "color:#6B7280; text-align:right;'>SOFR INDEX</td></tr>"
            )
            colors = ["#FFFFFF", "#EFF6FF"]
            for i, row in enumerate(r[:2]):
                bg = colors[i % 2]
                html += (
                    f"<tr style='background:{bg};'>"
                    f"<td style='padding:10px 12px; font-weight:600;'>{row['label']}</td>"
                    f"<td style='padding:10px 12px; font-family:monospace;'>"
                    f"{fmt_date(row['date'])}</td>"
                    f"<td style='padding:10px 12px; text-align:right; color:#065F46;'>"
                    f"{row['sofr_rate']:.4f}%</td>"
                    f"<td style='padding:10px 12px; text-align:right; font-family:monospace; "
                    f"font-weight:700; color:#1E40AF;'>{row['sofr_index']:.8f}</td></tr>"
                )
            if len(r) == 2:
                ratio = r[1]["sofr_index"] / r[0]["sofr_index"]
                html += (
                    "<tr style='background:#ECFDF5;'>"
                    "<td colspan='3' style='padding:10px 12px; color:#6B7280; font-size:11px;'>"
                    f"Index Ratio = {r[1]['sofr_index']:.8f} ÷ {r[0]['sofr_index']:.8f}"
                    f" = {ratio:.8f}</td>"
                    f"<td style='padding:10px 12px; text-align:right; color:#065F46; "
                    f"font-weight:700;'>= {(ratio-1)*100:.6f}%</td></tr>"
                )
            html += "</table>"
            return html

        n = len(daily_rows)
        if method == "Compounded in Arrears":
            hdr_color = "#0D5C63"
            cols      = ["Period Date", "Obs Date", "SOFR Rate", "Day Wt",
                         "Daily Factor", "Running Product"]
        else:
            hdr_color = "#4C3D9E"
            cols      = ["Period Date", "Obs Date", "SOFR Rate", "Day Wt",
                         "Business Day", "Weighted Rate"]

        title = ("DAILY COMPOUNDING DETAIL"
                 if method == "Compounded in Arrears"
                 else "DAILY RATE DETAIL")

        html = (
            f"<table width='100%' cellspacing='0' cellpadding='0' "
            f"style='border-collapse:collapse; border:1px solid #E2E8F0; border-radius:8px;'>"
            f"<tr style='background:{hdr_color};'>"
            f"<td colspan='6' style='padding:10px 12px; color:white; font-weight:700; "
            f"font-size:11px; letter-spacing:0.5px; border-radius:8px 8px 0 0;'>"
            f"{title} &nbsp;<span style='font-weight:400; font-size:10px;'>"
            f"({n} business days)</span></td></tr>"
            f"<tr style='background:#F8FAFC;'>"
        )
        for col in cols:
            html += (f"<td style='padding:8px 10px; font-weight:700; font-size:11px; "
                     f"color:#6B7280; border-bottom:1px solid #E2E8F0;'>{col}</td>")
        html += "</tr>"

        for i, row in enumerate(daily_rows):
            gf = row.get("is_good_friday", False)
            bg = "#FFFDE7" if gf else ("#FFFFFF" if i % 2 == 0 else "#F8FAFC")
            gf_tag = (" <span style='color:#92400E; font-size:9px; "
                      "font-weight:700;'>GF</span>" if gf else "")

            if method == "Compounded in Arrears":
                c4 = str(row["day_weight"])
                c5 = f"{row['daily_factor']:.8f}"
                c6 = (f"<span style='color:#4C3D9E; font-weight:700;'>"
                      f"{row['running_product']:.8f}</span>")
            else:
                c4 = str(row["day_weight"])
                c5 = "Yes"
                c6 = f"{row['weighted_rate']:.4f}"

            html += (
                f"<tr style='background:{bg};'>"
                f"<td style='padding:7px 10px; font-family:monospace; font-size:12px;'>"
                f"{fmt_date(row['date'])}</td>"
                f"<td style='padding:7px 10px; font-family:monospace; font-size:12px; "
                f"color:#1E40AF;'>{fmt_date(row['obs_date'])}{gf_tag}</td>"
                f"<td style='padding:7px 10px; text-align:right; color:#065F46; "
                f"font-weight:600;'>{row['sofr_rate']:.4f}%</td>"
                f"<td style='padding:7px 10px; text-align:center;'>{c4}</td>"
                f"<td style='padding:7px 10px; text-align:right; font-family:monospace;'>{c5}</td>"
                f"<td style='padding:7px 10px; text-align:right; font-family:monospace;'>{c6}</td>"
                f"</tr>"
            )

        html += (
            "</table>"
            "<p style='font-size:11px; color:#9CA3AF; margin:8px 0 0 0;'>"
            "GF = Good Friday uses Thursday's rate if available, otherwise Wednesday's"
            "</p>"
        )
        return html

    # =========================================================================
    # Actions
    # =========================================================================

    def _mark_calculated(self):
        if not self._last_result:
            return
        res   = self._last_result
        cusip = res["cusip"]
        ps    = str(res["period_start_date"])
        pe    = str(res["period_end_date"])

        from core.database import get_full_schedule
        with self._db() as conn:
            periods = get_full_schedule(conn, cusip)

        matched = next(
            (p for p in periods
             if p["period_start_date"] == ps and p["period_end_date"] == pe),
            None
        )
        if not matched:
            QMessageBox.warning(self, "Not Found",
                "No matching period found. Generate the schedule first.")
            return

        try:
            from core.database import mark_period_calculated
            with self._db() as conn:
                mark_period_calculated(
                    conn, cusip, matched["period_number"],
                    res["compounded_rate"], res["annualized_rate"],
                    res["rounded_rate"],    res["interest_amount"]
                )
            QMessageBox.information(self, "Marked",
                f"Period {matched['period_number']} marked as Calculated.")
            self._load_periods(cusip)
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _export_pdf(self, silent: bool = False):
        if not self._last_result:
            return
        try:
            from core.pdf_report import generate_calculation_pdf
            path = generate_calculation_pdf(self._last_result)
            if not silent:
                QMessageBox.information(self, "PDF Saved",
                    f"Report saved to:\n{path}")
            else:
                try:
                    self.window().status(f"PDF saved: {path.name}")
                except Exception:
                    pass
        except ImportError:
            if not silent:
                QMessageBox.warning(self, "reportlab not installed",
                    "pip install reportlab")
        except Exception as e:
            if not silent:
                QMessageBox.critical(self, "PDF Error", str(e))

    def showEvent(self, event):
        super().showEvent(event)
        QTimer.singleShot(0, self._load_deals)
