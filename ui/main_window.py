"""ui/main_window.py — Main application window with side nav."""

from PySide6.QtWidgets import (
    QMainWindow, QWidget, QHBoxLayout, QVBoxLayout,
    QLabel, QPushButton, QStackedWidget, QStatusBar,
    QSizePolicy, QLineEdit, QScrollArea, QFrame
)
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QFont, QIcon
from PySide6.QtGui import QGuiApplication

from ui.styles import APP_STYLE, NAV_WIDTH
from ui.pages.dashboard   import DashboardPage
from ui.pages.deals       import DealsPage
from ui.pages.rates       import RatesPage
from ui.pages.schedule    import SchedulePage
from ui.pages.calc_single import CalcSinglePage
from ui.pages.calc_batch  import CalcBatchPage
from ui.pages.history     import HistoryPage
from ui.pages.holidays    import HolidaysPage


NAV_ITEMS = [
    # (icon_text, label, page_class, section_before)
    ("▣",  "Dashboard",          DashboardPage,   ""),
    ("≡",  "Deal Master",        DealsPage,       "DEALS"),
    ("↑",  "SOFR Rates",         RatesPage,       "RATES & CALC"),
    ("📅", "Market Holidays",    HolidaysPage,    ""),
    ("⊡",  "Payment Schedule",   SchedulePage,    ""),
    ("◎",  "Single CUSIP Calc",  CalcSinglePage,  ""),
    ("⊕",  "Batch Calculation",  CalcBatchPage,   ""),
    ("☰",  "History / Audit",    HistoryPage,     "REPORTS"),
]


class NavButton(QPushButton):
    def __init__(self, icon_txt: str, label: str, parent=None):
        super().__init__(f"  {icon_txt}  {label}", parent)
        self.setObjectName("NavBtn")
        self.setCheckable(False)
        self.setCursor(Qt.PointingHandCursor)
        self.setMinimumHeight(42)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

    def set_active(self, active: bool):
        self.setProperty("active", "true" if active else "false")
        self.style().unpolish(self)
        self.style().polish(self)


class MainWindow(QMainWindow):
    def __init__(self, db_factory):
        super().__init__()
        self._db = db_factory
        self._nav_btns = []
        self._pages = []
        self.setWindowTitle("SOFR Interest Calculator")
        self.setMinimumSize(820, 560)
        self.setStyleSheet(APP_STYLE)
        self._build_ui()
        self._apply_initial_window_geometry()
        self._switch_page(0)

    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        root_lay = QHBoxLayout(root)
        root_lay.setContentsMargins(0, 0, 0, 0)
        root_lay.setSpacing(0)

        # ── Nav panel ─────────────────────────────────────────
        nav = QWidget()
        nav.setObjectName("NavPanel")
        nav.setFixedWidth(NAV_WIDTH)
        nav_lay = QVBoxLayout(nav)
        nav_lay.setContentsMargins(0, 0, 0, 0)
        nav_lay.setSpacing(0)

        # App title
        title = QLabel("SOFR")
        title.setObjectName("AppTitle")
        sub   = QLabel("Interest Calculator")
        sub.setObjectName("AppSubtitle")
        nav_lay.addWidget(title)
        nav_lay.addWidget(sub)

        sep_line = QWidget(); sep_line.setFixedHeight(1)
        sep_line.setStyleSheet("background: rgba(255,255,255,0.1);")
        nav_lay.addWidget(sep_line)
        nav_lay.addSpacing(10)

        # Stack widget for pages
        self._stack = QStackedWidget()
        self._stack.setObjectName("ContentStack")

        last_section = ""
        for i, (icon, label, PageClass, section) in enumerate(NAV_ITEMS):
            if section and section != last_section:
                sec_lbl = QLabel(section)
                sec_lbl.setObjectName("NavSep")
                nav_lay.addWidget(sec_lbl)
                last_section = section

            btn = NavButton(icon, label)
            btn.clicked.connect(lambda _, idx=i: self._switch_page(idx))
            nav_lay.addWidget(btn)
            self._nav_btns.append(btn)

            page = PageClass(self._db)
            self._stack.addWidget(page)
            self._pages.append(page)

        nav_lay.addStretch()

        version = QLabel("v1.0  ·  SQLite + PySide6")
        version.setObjectName("NavVersion")
        nav_lay.addWidget(version)

        root_lay.addWidget(nav)

        # Right side: search bar + scrollable content
        right = QWidget()
        right_lay = QVBoxLayout(right)
        right_lay.setContentsMargins(0, 0, 0, 0)
        right_lay.setSpacing(4)

        top_bar = QHBoxLayout()
        top_bar.setContentsMargins(10, 6, 10, 0)
        top_bar.setSpacing(8)
        self._search = QLineEdit()
        self._search.setPlaceholderText("Search CUSIP / Deal / Client across pages…")
        self._search.setClearButtonEnabled(True)
        self._search.textChanged.connect(self._apply_global_search)
        top_bar.addWidget(self._search)
        right_lay.addLayout(top_bar)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        scroll.setWidget(self._stack)
        right_lay.addWidget(scroll, 1)

        root_lay.addWidget(right, 1)

        # Status bar
        bar = QStatusBar()
        self.setStatusBar(bar)
        self._status = QLabel("Ready")
        bar.addWidget(self._status)
        self._db_lbl = QLabel("")
        bar.addPermanentWidget(self._db_lbl)

        from core.database import DB_PATH
        self._db_lbl.setText(f"DB: {DB_PATH}")

    def _apply_initial_window_geometry(self):
        screen = self.screen() or QGuiApplication.primaryScreen()
        if not screen:
            self.resize(1280, 800)
            return
        available = screen.availableGeometry()
        target_width = min(available.width() - 40, max(820, int(available.width() * 0.92)))
        target_height = min(available.height() - 30, max(560, int(available.height() * 0.9)))
        self.resize(target_width, target_height)
        self.move(
            available.x() + max(0, (available.width() - target_width) // 2),
            available.y() + max(0, (available.height() - target_height) // 2),
        )

    def _switch_page(self, idx: int):
        for i, btn in enumerate(self._nav_btns):
            btn.set_active(i == idx)
        self._stack.setCurrentIndex(idx)
        _, label, _, _ = NAV_ITEMS[idx]
        self._status.setText(f"{label}")
        # Re-apply current search to the new page
        self._apply_global_search(self._search.text())

    def status(self, msg: str):
        self._status.setText(msg)

    def _apply_global_search(self, text: str):
        page = self._stack.currentWidget()
        if hasattr(page, "apply_search"):
            page.apply_search(text or "")
