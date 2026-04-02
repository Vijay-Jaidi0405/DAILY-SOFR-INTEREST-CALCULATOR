"""ui/widgets/common.py — Reusable widget building blocks."""

from PySide6.QtWidgets import (
    QWidget, QLabel, QVBoxLayout, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QHeaderView,
    QFrame, QPushButton, QSizePolicy, QComboBox,
    QListView, QLineEdit
)
from PySide6.QtCore import Qt, Signal
from PySide6.QtGui import QColor, QBrush, QFont, QStandardItemModel, QStandardItem
from ui.styles import GREEN, AMBER, PURPLE, RED, ACCENT


# ---------------------------------------------------------------------------
# KPI Card
# ---------------------------------------------------------------------------

class KpiCard(QWidget):
    def __init__(self, label: str, value: str = "—",
                 sub: str = "", color: str = ACCENT, parent=None):
        super().__init__(parent)
        self.setObjectName("KpiCard")
        lay = QVBoxLayout(self)
        lay.setSpacing(4)
        lay.setContentsMargins(0, 0, 0, 0)

        self._lbl = QLabel(label.upper())
        self._lbl.setObjectName("KpiLabel")

        self._val = QLabel(value)
        self._val.setObjectName("KpiValue")
        self._val.setStyleSheet(f"color: {color};")

        self._sub = QLabel(sub)
        self._sub.setObjectName("KpiSub")

        lay.addWidget(self._lbl)
        lay.addWidget(self._val)
        if sub:
            lay.addWidget(self._sub)

    def set_value(self, v: str, color: str | None = None):
        self._val.setText(v)
        if color:
            self._val.setStyleSheet(f"color: {color};")

    def set_sub(self, s: str):
        self._sub.setText(s)
        self._sub.show()


# ---------------------------------------------------------------------------
# Section panel
# ---------------------------------------------------------------------------

class Panel(QWidget):
    def __init__(self, title: str = "", parent=None):
        super().__init__(parent)
        self.setObjectName("Panel")
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(0, 0, 0, 0)
        self._layout.setSpacing(0)
        if title:
            lbl = QLabel(title.upper())
            lbl.setObjectName("PanelTitle")
            self._layout.addWidget(lbl)
        self._body = QWidget()
        self._body_lay = QVBoxLayout(self._body)
        self._body_lay.setContentsMargins(16, 12, 16, 16)
        self._layout.addWidget(self._body)

    def body_layout(self):
        return self._body_lay

    def add_widget(self, w):
        self._body_lay.addWidget(w)

    def add_layout(self, lay):
        self._body_lay.addLayout(lay)


# ---------------------------------------------------------------------------
# Page header
# ---------------------------------------------------------------------------

class PageHeader(QWidget):
    def __init__(self, title: str, subtitle: str = "", parent=None):
        super().__init__(parent)
        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 12)
        lay.setSpacing(4)
        t = QLabel(title)
        t.setObjectName("PageTitle")
        lay.addWidget(t)
        if subtitle:
            s = QLabel(subtitle)
            s.setObjectName("PageSubtitle")
            lay.addWidget(s)


# ---------------------------------------------------------------------------
# Status badge
# ---------------------------------------------------------------------------

STATUS_COLORS = {
    "Ready to Calculate": ("#D1FAE5", "#065F46"),
    "Scheduled":          ("#EFF6FF", "#1E40AF"),
    "Calculated":         ("#EDE9FE", "#5B21B6"),
    "Active":             ("#D1FAE5", "#065F46"),
    "Inactive":           ("#F3F4F6", "#6B7280"),
    "Matured":            ("#FEF3C7", "#92400E"),
}

def make_badge(text: str) -> QLabel:
    lbl = QLabel(text)
    lbl.setAlignment(Qt.AlignCenter)
    bg, fg = STATUS_COLORS.get(text, ("#F3F4F6", "#374151"))
    lbl.setStyleSheet(
        f"background:{bg}; color:{fg}; border-radius:8px;"
        f"padding:2px 10px; font-size:11px; font-weight:bold;"
    )
    return lbl


# ---------------------------------------------------------------------------
# Date-aware table item
# Displays DD-Mon-YYYY but sorts by YYYY-MM-DD stored in UserRole.
# ---------------------------------------------------------------------------

class DateItem(QTableWidgetItem):
    """
    Table item that displays a human-readable date (DD-Mon-YYYY) but
    uses the ISO-format string (YYYY-MM-DD) as the sort key so that
    clicking the column header sorts chronologically, not alphabetically.
    """
    def __init__(self, display: str, iso: str):
        super().__init__(display)
        # Store ISO date string as user data for comparison
        self.setData(Qt.UserRole, iso)
        self.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)

    def __lt__(self, other: QTableWidgetItem) -> bool:
        # Compare by ISO string (YYYY-MM-DD) — lexicographic = chronological
        my_iso    = self.data(Qt.UserRole) or ""
        other_iso = other.data(Qt.UserRole) or ""
        return my_iso < other_iso


# ---------------------------------------------------------------------------
# Generic sortable table
# ---------------------------------------------------------------------------

class DataTable(QTableWidget):
    row_selected = Signal(int)

    def __init__(self, headers: list[str], parent=None):
        super().__init__(0, len(headers), parent)
        self.setHorizontalHeaderLabels(headers)
        self.setAlternatingRowColors(True)
        self.setSelectionBehavior(QTableWidget.SelectRows)
        self.setSelectionMode(QTableWidget.SingleSelection)
        self.setEditTriggers(QTableWidget.NoEditTriggers)
        self.verticalHeader().setVisible(False)
        self.verticalHeader().setDefaultSectionSize(40)
        self.horizontalHeader().setStretchLastSection(True)
        self.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.horizontalHeader().setMinimumSectionSize(60)
        self.setSortingEnabled(True)
        self.setShowGrid(True)
        self.clicked.connect(lambda idx: self.row_selected.emit(idx.row()))

    def populate(self, rows: list[list], colors: list[str] | None = None):
        """
        Populate the table.  Each cell value may be:
          - A plain string / number  → standard QTableWidgetItem
          - A DateItem instance      → already constructed, used as-is
            (pages should call make_date_item() for date columns)
        """
        self.setSortingEnabled(False)
        self.setRowCount(0)
        for i, row in enumerate(rows):
            self.insertRow(i)
            bg = colors[i] if colors else None
            for j, val in enumerate(row):
                if isinstance(val, QTableWidgetItem):
                    item = val
                else:
                    item = QTableWidgetItem(str(val) if val is not None else "")
                    item.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)
                if bg:
                    item.setBackground(QBrush(QColor(bg)))
                self.setItem(i, j, item)
        self.setSortingEnabled(True)

    def selected_row_data(self) -> list[str]:
        row = self.currentRow()
        if row < 0:
            return []
        return [
            self.item(row, c).text() if self.item(row, c) else ""
            for c in range(self.columnCount())
        ]


# ---------------------------------------------------------------------------
# Separator line
# ---------------------------------------------------------------------------

def h_separator():
    line = QFrame()
    line.setFrameShape(QFrame.HLine)
    line.setFrameShadow(QFrame.Sunken)
    line.setStyleSheet("color: #E5E7EB;")
    return line


# ---------------------------------------------------------------------------
# Format helpers
# ---------------------------------------------------------------------------

def fmt_money(v) -> str:
    if v is None:
        return "—"
    try:
        return f"${float(v):,.2f}"
    except Exception:
        return str(v)


def fmt_rate(v, decimals=7) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v)*100:.{decimals}f}%"
    except Exception:
        return str(v)


def fmt_date(v) -> str:
    """Return human-readable DD-Mon-YYYY string (for labels / HTML)."""
    if not v:
        return "—"
    s = str(v)
    try:
        from datetime import date, datetime
        if isinstance(v, date):
            return v.strftime("%d-%b-%Y")
        dt = datetime.fromisoformat(s[:10])
        return dt.strftime("%d-%b-%Y")
    except Exception:
        return s


def _to_iso(v) -> str:
    """Convert any date-like value to YYYY-MM-DD string for sorting."""
    if not v:
        return ""
    try:
        from datetime import date, datetime
        if isinstance(v, date):
            return v.isoformat()
        s = str(v).strip()
        if len(s) >= 10:
            # Already ISO or ISO-prefix
            datetime.fromisoformat(s[:10])
            return s[:10]
    except Exception:
        pass
    return ""


def make_date_item(v) -> "DateItem":
    """
    Create a DateItem that displays DD-Mon-YYYY and sorts by YYYY-MM-DD.
    Pass any date, datetime, or ISO string.
    Returns a plain QTableWidgetItem showing "—" if value is empty.
    """
    iso     = _to_iso(v)
    display = fmt_date(v) if iso else "—"
    if not iso:
        item = QTableWidgetItem("—")
        item.setTextAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        return item
    return DateItem(display, iso)


def readiness_color(status: str, eligible: int) -> str:
    if status == "Calculated":
        return "#F5F3FF"
    if eligible:
        return "#ECFDF5"
    if status == "Scheduled":
        return "#FFFFFF"
    return "#FFFBEB"


# ---------------------------------------------------------------------------
# Checkable multi-select combo
# ---------------------------------------------------------------------------

class CheckableComboBox(QComboBox):
    selection_changed = Signal(list)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setModel(QStandardItemModel(self))
        self.setView(QListView(self))
        self.setEditable(True)
        line_edit = QLineEdit(self)
        line_edit.setReadOnly(True)
        line_edit.setPlaceholderText("Select…")
        self.setLineEdit(line_edit)
        self.lineEdit().selectionChanged.connect(self.lineEdit().deselect)
        self.view().pressed.connect(self._toggle_item)
        self._required_values: set[str] = set()

    def add_check_item(self, label: str, value: str, checked: bool = False):
        item = QStandardItem(label)
        item.setFlags(Qt.ItemIsEnabled | Qt.ItemIsUserCheckable)
        item.setData(value, Qt.UserRole)
        item.setData(Qt.Checked if checked else Qt.Unchecked, Qt.CheckStateRole)
        self.model().appendRow(item)
        self._refresh_text()

    def set_required_values(self, values: list[str] | set[str]):
        self._required_values = set(values)
        for i in range(self.model().rowCount()):
            item = self.model().item(i)
            if item.data(Qt.UserRole) in self._required_values:
                item.setData(Qt.Checked, Qt.CheckStateRole)
        self._refresh_text()

    def checked_values(self) -> list[str]:
        vals = []
        for i in range(self.model().rowCount()):
            item = self.model().item(i)
            if item.data(Qt.CheckStateRole) == Qt.Checked:
                vals.append(item.data(Qt.UserRole))
        return vals

    def set_checked_values(self, values: list[str] | set[str]):
        values = set(values)
        values.update(self._required_values)
        for i in range(self.model().rowCount()):
            item = self.model().item(i)
            state = Qt.Checked if item.data(Qt.UserRole) in values else Qt.Unchecked
            item.setData(state, Qt.CheckStateRole)
        self._refresh_text()
        self.selection_changed.emit(self.checked_values())

    def _toggle_item(self, index):
        item = self.model().itemFromIndex(index)
        if not item:
            return
        value = item.data(Qt.UserRole)
        if value in self._required_values:
            item.setData(Qt.Checked, Qt.CheckStateRole)
            return
        next_state = Qt.Unchecked if item.checkState() == Qt.Checked else Qt.Checked
        item.setData(next_state, Qt.CheckStateRole)
        self._refresh_text()
        self.selection_changed.emit(self.checked_values())

    def hidePopup(self):
        self._refresh_text()
        super().hidePopup()

    def _refresh_text(self):
        labels = []
        for i in range(self.model().rowCount()):
            item = self.model().item(i)
            if item.data(Qt.CheckStateRole) == Qt.Checked:
                labels.append(item.text())
        self.lineEdit().setText(", ".join(labels))
