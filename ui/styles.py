"""ui/styles.py — Application-wide stylesheet and colour constants."""

NAV_WIDTH = 184
ACCENT    = "#1B3A6B"
ACCENT2   = "#0D5C63"
GREEN     = "#0F6E56"
AMBER     = "#92400E"
PURPLE    = "#4C3D9E"
RED       = "#991B1B"
BG_LIGHT  = "#F0F2F5"
BG_WHITE  = "#FFFFFF"
BORDER    = "#C8D0DC"
TEXT_DARK = "#1E2533"
TEXT_MID  = "#4B5563"
TEXT_SOFT = "#9CA3AF"

APP_STYLE = f"""
/* ═══════════════════════════════════════════════════════
   GLOBAL
═══════════════════════════════════════════════════════ */
* {{
    font-family: "Segoe UI", "Inter", "Arial", sans-serif;
    font-size: 13px;
    color: {TEXT_DARK};
    box-sizing: border-box;
}}
QMainWindow, QDialog, QWidget {{
    background: {BG_LIGHT};
}}

/* ═══════════════════════════════════════════════════════
   NAV PANEL
═══════════════════════════════════════════════════════ */
#NavPanel {{
    background: {ACCENT};
    border-right: 2px solid #122a52;
    min-width: {NAV_WIDTH}px;
    max-width: {NAV_WIDTH}px;
}}
#AppTitle {{
    color: #FFFFFF;
    font-size: 15px;
    font-weight: 700;
    padding: 18px 14px 2px 14px;
    letter-spacing: 0.5px;
}}
#AppSubtitle {{
    color: #93B8DC;
    font-size: 10px;
    padding: 0 14px 14px 14px;
    letter-spacing: 0.3px;
}}
#NavDivider {{
    background: rgba(255,255,255,0.12);
    max-height: 1px;
    margin: 0 12px;
}}
#NavBtn {{
    background: transparent;
    color: #A8C4E0;
    border: none;
    border-left: 3px solid transparent;
    text-align: left;
    padding: 10px 14px 10px 11px;
    font-size: 12px;
    border-radius: 0;
    min-height: 40px;
}}
#NavBtn:hover {{
    background: rgba(255,255,255,0.09);
    color: #FFFFFF;
    border-left: 3px solid rgba(255,255,255,0.3);
}}
#NavBtn[active="true"] {{
    background: rgba(255,255,255,0.16);
    color: #FFFFFF;
    border-left: 3px solid #5BC8D8;
    font-weight: 600;
}}
#NavSep {{
    color: #4E6D96;
    font-size: 9px;
    font-weight: 700;
    padding: 14px 14px 4px 14px;
    letter-spacing: 1.5px;
    text-transform: uppercase;
}}
#NavVersion {{
    color: #3D5A82;
    font-size: 10px;
    padding: 10px 14px;
}}

/* ═══════════════════════════════════════════════════════
   PAGE HEADERS
═══════════════════════════════════════════════════════ */
#PageTitle {{
    font-size: 20px;
    font-weight: 700;
    color: {ACCENT};
    letter-spacing: -0.3px;
}}
#PageSubtitle {{
    font-size: 12px;
    color: {TEXT_MID};
    margin-top: 2px;
}}

/* ═══════════════════════════════════════════════════════
   PANELS / CARDS
═══════════════════════════════════════════════════════ */
#Panel {{
    background: {BG_WHITE};
    border: 1px solid {BORDER};
    border-radius: 12px;
}}
#PanelTitle {{
    font-size: 11px;
    font-weight: 700;
    color: {TEXT_MID};
    letter-spacing: 0.8px;
    text-transform: uppercase;
    padding: 14px 18px 10px 18px;
    border-bottom: 1px solid {BORDER};
    background: #F8FAFC;
    border-radius: 12px 12px 0 0;
}}

/* ═══════════════════════════════════════════════════════
   KPI CARDS
═══════════════════════════════════════════════════════ */
#KpiCard {{
    background: {BG_WHITE};
    border: 1px solid {BORDER};
    border-radius: 12px;
    padding: 18px 20px 16px 20px;
    min-width: 130px;
}}
#KpiLabel {{
    font-size: 10px;
    font-weight: 700;
    color: {TEXT_SOFT};
    letter-spacing: 1px;
    text-transform: uppercase;
}}
#KpiValue {{
    font-size: 26px;
    font-weight: 700;
    color: {ACCENT};
    letter-spacing: -0.5px;
    margin-top: 4px;
}}
#KpiSub {{
    font-size: 11px;
    color: {TEXT_SOFT};
    margin-top: 2px;
}}

/* ═══════════════════════════════════════════════════════
   BUTTONS
═══════════════════════════════════════════════════════ */
QPushButton {{
    background: {BG_WHITE};
    border: 1.5px solid {BORDER};
    border-radius: 8px;
    padding: 3px;
    color: {TEXT_MID};
    font-size: 13px;
    font-weight: 500;
    min-height: 0px;
}}
QPushButton:hover  {{
    background: #F1F5F9;
    border-color: #9BADC4;
    color: {TEXT_DARK};
}}
QPushButton:pressed {{ background: #E2E8F0; }}
QPushButton:disabled {{
    background: #F8FAFC;
    color: {TEXT_SOFT};
    border-color: #E2E8F0;
}}

#PrimaryBtn {{
    background: {ACCENT};
    color: #FFFFFF;
    border: none;
    border-radius: 8px;
    padding: 3px;
    font-weight: 600;
    min-height: 0px;
}}
#PrimaryBtn:hover  {{ background: #22497E; }}
#PrimaryBtn:pressed {{ background: #162F5C; }}
#PrimaryBtn:disabled {{ background: #8FAAC8; }}

#GreenBtn {{
    background: {GREEN};
    color: #FFFFFF;
    border: none;
    border-radius: 8px;
    padding: 3px;
    font-weight: 600;
    min-height: 0px;
}}
#GreenBtn:hover  {{ background: #0C5C47; }}
#GreenBtn:pressed {{ background: #094A38; }}

#DangerBtn {{
    background: {RED};
    color: #FFFFFF;
    border: none;
    border-radius: 8px;
    padding: 3px;
    font-weight: 600;
    min-height: 0px;
}}
#DangerBtn:hover {{ background: #7F1D1D; }}

/* ═══════════════════════════════════════════════════════
   FORM INPUTS  (extra height and clear font for dates)
═══════════════════════════════════════════════════════ */
QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox, QTextEdit {{
    background: {BG_WHITE};
    border: 1.5px solid {BORDER};
    border-radius: 8px;
    padding: 8px 12px;
    color: {TEXT_DARK};
    font-size: 13px;
    min-height: 36px;
    selection-background-color: #DBEAFE;
}}
QLineEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus,
QComboBox:focus, QTextEdit:focus {{
    border: 1.5px solid {ACCENT};
    background: #FAFCFF;
}}
QLineEdit:hover, QSpinBox:hover, QDoubleSpinBox:hover,
QComboBox:hover {{
    border-color: #9BADC4;
}}

/* ── Date pickers — bigger, clearer ── */
QDateEdit {{
    background: {BG_WHITE};
    border: 1.5px solid {BORDER};
    border-radius: 8px;
    padding: 8px 12px;
    color: {TEXT_DARK};
    font-size: 14px;
    font-weight: 600;
    min-height: 38px;
    min-width: 130px;
    selection-background-color: #DBEAFE;
}}
QDateEdit:focus {{
    border: 1.5px solid {ACCENT};
    background: #FAFCFF;
}}
QDateEdit:hover {{
    border-color: #9BADC4;
}}
QDateEdit::drop-down {{
    subcontrol-origin: padding;
    subcontrol-position: center right;
    width: 32px;
    border-left: 1px solid {BORDER};
    border-radius: 0 8px 8px 0;
    background: #F1F5F9;
}}
QDateEdit::drop-down:hover {{
    background: #E2E8F0;
}}
QDateEdit::up-button, QDateEdit::down-button {{
    width: 0; height: 0;
}}

/* Calendar popup */
QCalendarWidget {{
    background: {BG_WHITE};
    border: 1px solid {BORDER};
    border-radius: 8px;
    font-size: 13px;
    min-width: 320px;
    min-height: 260px;
}}
QCalendarWidget QWidget {{
    background: {BG_WHITE};
    color: {TEXT_DARK};
}}
QCalendarWidget QToolButton {{
    background: {ACCENT};
    color: white;
    border-radius: 4px;
    padding: 2px 6px;
    font-weight: 600;
    font-size: 12px;
    min-height: 24px;
    border: none;
}}
QCalendarWidget QToolButton:hover {{ background: #22497E; }}
QCalendarWidget QToolButton#qt_calendar_prevmonth,
QCalendarWidget QToolButton#qt_calendar_nextmonth {{
    min-width: 24px;
    max-width: 24px;
    padding: 0;
}}
QCalendarWidget QToolButton#qt_calendar_monthbutton {{
    min-width: 96px;
}}
QCalendarWidget QToolButton#qt_calendar_yearbutton {{
    min-width: 56px;
}}
QCalendarWidget QMenu {{
    background: {BG_WHITE};
    border: 1px solid {BORDER};
}}
QCalendarWidget QSpinBox {{
    background: {BG_WHITE};
    border: 1px solid {BORDER};
    border-radius: 4px;
    color: {TEXT_DARK};
    font-size: 12px;
    padding: 0 4px;
    min-height: 24px;
    min-width: 64px;
}}
QCalendarWidget QWidget#qt_calendar_navigationbar {{
    background: {ACCENT};
    border-radius: 8px 8px 0 0;
    padding: 4px;
}}
QCalendarWidget QTableView {{
    background: {BG_WHITE};
    alternate-background-color: #F8FAFC;
    color: {TEXT_DARK};
    selection-background-color: {ACCENT};
    selection-color: #FFFFFF;
    gridline-color: #E5E7EB;
    outline: 0;
}}
QCalendarWidget QTableView:enabled {{
    border: none;
}}
QCalendarWidget QTableView::item {{
    background: {BG_WHITE};
    color: {TEXT_DARK};
    padding: 2px;
}}
QCalendarWidget QTableView::item:selected {{
    background: {ACCENT};
    color: #FFFFFF;
}}
QCalendarWidget QTableView::item:disabled {{
    color: {TEXT_SOFT};
}}
QCalendarWidget QAbstractItemView {{
    background: {BG_WHITE};
    selection-background-color: {ACCENT};
    selection-color: white;
    alternate-background-color: #F8FAFC;
    font-size: 13px;
    gridline-color: #F0F2F5;
}}
QCalendarWidget QAbstractItemView:enabled {{
    color: {TEXT_DARK};
}}
QCalendarWidget QAbstractItemView:disabled {{
    color: {TEXT_SOFT};
}}

/* Spin-box arrows */
QSpinBox::up-button, QDoubleSpinBox::up-button {{
    width: 18px; border-left: 1px solid {BORDER};
    border-radius: 0 8px 0 0;
}}
QSpinBox::down-button, QDoubleSpinBox::down-button {{
    width: 18px; border-left: 1px solid {BORDER};
    border-radius: 0 0 8px 0;
}}

/* ComboBox arrow */
QComboBox::drop-down {{
    width: 28px;
    border-left: 1px solid {BORDER};
    border-radius: 0 8px 8px 0;
    background: #F1F5F9;
}}
QComboBox::drop-down:hover {{ background: #E2E8F0; }}
QComboBox QAbstractItemView {{
    background: {BG_WHITE};
    border: 1px solid {BORDER};
    selection-background-color: #DBEAFE;
    selection-color: {TEXT_DARK};
    padding: 4px;
}}

/* ═══════════════════════════════════════════════════════
   TABLES
═══════════════════════════════════════════════════════ */
QTableWidget, QTableView {{
    background: {BG_WHITE};
    gridline-color: #EEF0F4;
    border: 1px solid {BORDER};
    border-radius: 8px;
    font-size: 13px;
    selection-background-color: #DBEAFE;
    selection-color: {TEXT_DARK};
    alternate-background-color: #F8FAFC;
    show-decoration-selected: 1;
}}
QTableWidget::item, QTableView::item {{
    padding: 8px 12px;
    border-bottom: 1px solid #EEF0F4;
}}
QTableWidget::item:selected, QTableView::item:selected {{
    background: #DBEAFE;
    color: {TEXT_DARK};
}}
QHeaderView::section {{
    background: #F1F5F9;
    border: none;
    border-bottom: 2px solid {BORDER};
    border-right: 1px solid #E2E8F0;
    padding: 10px 12px;
    font-weight: 700;
    font-size: 11px;
    color: {TEXT_MID};
    letter-spacing: 0.5px;
    text-transform: uppercase;
}}
QHeaderView::section:first {{ border-radius: 8px 0 0 0; }}
QHeaderView::section:last  {{ border-right: none; border-radius: 0 8px 0 0; }}
QHeaderView::section:hover {{ background: #E2E8F0; }}

/* ═══════════════════════════════════════════════════════
   SEARCH BOX
═══════════════════════════════════════════════════════ */
#SearchBox {{
    border: 1.5px solid {BORDER};
    border-radius: 20px;
    padding: 8px 16px;
    background: {BG_WHITE};
    font-size: 13px;
    min-height: 36px;
}}
#SearchBox:focus {{ border-color: {ACCENT}; }}

/* ═══════════════════════════════════════════════════════
   TABS
═══════════════════════════════════════════════════════ */
QTabWidget::pane {{
    border: 1px solid {BORDER};
    border-radius: 0 8px 8px 8px;
    background: {BG_WHITE};
    top: -1px;
}}
QTabBar::tab {{
    background: #E8EDF4;
    border: 1px solid {BORDER};
    border-bottom: none;
    padding: 10px 22px;
    border-radius: 8px 8px 0 0;
    margin-right: 3px;
    font-size: 13px;
    font-weight: 500;
    color: {TEXT_MID};
    min-width: 100px;
}}
QTabBar::tab:selected {{
    background: {BG_WHITE};
    color: {ACCENT};
    font-weight: 700;
    border-color: {BORDER};
}}
QTabBar::tab:hover:!selected {{
    background: #D6E0EE;
    color: {TEXT_DARK};
}}

/* ═══════════════════════════════════════════════════════
   SCROLLBARS
═══════════════════════════════════════════════════════ */
QScrollBar:vertical {{
    background: transparent;
    width: 10px;
    margin: 0;
}}
QScrollBar::handle:vertical {{
    background: #C4CEDB;
    border-radius: 5px;
    min-height: 40px;
    margin: 2px;
}}
QScrollBar::handle:vertical:hover {{ background: #9BADC4; }}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{ height: 0; }}
QScrollBar:horizontal {{
    background: transparent;
    height: 10px;
}}
QScrollBar::handle:horizontal {{
    background: #C4CEDB;
    border-radius: 5px;
    min-width: 40px;
    margin: 2px;
}}
QScrollBar::handle:horizontal:hover {{ background: #9BADC4; }}
QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal {{ width: 0; }}

/* ═══════════════════════════════════════════════════════
   PROGRESS BAR
═══════════════════════════════════════════════════════ */
QProgressBar {{
    border: 1px solid {BORDER};
    border-radius: 8px;
    background: #EEF0F4;
    text-align: center;
    min-height: 16px;
    font-size: 11px;
    color: {TEXT_MID};
}}
QProgressBar::chunk {{
    background: {ACCENT};
    border-radius: 7px;
}}

/* ═══════════════════════════════════════════════════════
   STATUS BAR
═══════════════════════════════════════════════════════ */
QStatusBar {{
    background: #EEF0F4;
    border-top: 1px solid {BORDER};
    font-size: 11px;
    color: {TEXT_MID};
    padding: 2px 8px;
}}

/* ═══════════════════════════════════════════════════════
   DIALOGS & FORMS
═══════════════════════════════════════════════════════ */
QDialog {{
    background: {BG_WHITE};
    border-radius: 12px;
}}
QFormLayout QLabel {{
    color: {TEXT_MID};
    font-size: 12px;
    font-weight: 600;
    padding-right: 8px;
}}
QDialogButtonBox QPushButton {{
    min-width: 90px;
    padding: 3px;
}}
QMessageBox {{
    background: {BG_WHITE};
}}
QMessageBox QLabel {{
    font-size: 13px;
    color: {TEXT_DARK};
}}

/* ═══════════════════════════════════════════════════════
   MISC
═══════════════════════════════════════════════════════ */
QSplitter::handle {{
    background: {BORDER};
    width: 2px;
    height: 2px;
}}
QFrame[frameShape="4"],   /* HLine */
QFrame[frameShape="5"] {{ /* VLine */
    color: {BORDER};
    background: {BORDER};
    max-height: 1px;
    border: none;
}}
QToolTip {{
    background: {TEXT_DARK};
    color: #FFFFFF;
    border: none;
    border-radius: 6px;
    padding: 6px 10px;
    font-size: 12px;
}}
"""
