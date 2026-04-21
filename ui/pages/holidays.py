"""ui/pages/holidays.py — Holiday management page."""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QLineEdit, QComboBox, QDateEdit, QMessageBox, QCheckBox, QTableWidgetItem,
    QFileDialog, QProgressBar
)
from PySide6.QtCore import Qt, QDate, QTimer, QThread, Signal
from ui.widgets.common import Panel, PageHeader, DataTable, make_date_item
from ui.styles import RED, GREEN, ACCENT

class _HolidayImportThread(QThread):
    done = Signal(int, list)
    error = Signal(str)

    def __init__(self, db_factory, path):
        super().__init__()
        self._db = db_factory
        self._path = path

    def run(self):
        try:
            from core.database import import_holidays_from_excel
            with self._db() as conn:
                n, errs = import_holidays_from_excel(conn, self._path)
            self.done.emit(n, errs)
        except Exception as e:
            self.error.emit(str(e))

class HolidaysPage(QWidget):
    def __init__(self, db_factory, parent=None):
        super().__init__(parent)
        self._db = db_factory
        self._bulk_path = None
        self._build_ui()

    def _build_ui(self):
        lay = QVBoxLayout(self)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        hdr = QHBoxLayout()
        hdr.setSpacing(10)
        hdr.addWidget(PageHeader("Holiday Management", 
            "Add or remove market holidays (supported up to year 2150)"))
        hdr.addStretch()
        
        for label, obj, slot in [
            ("Bulk Upload",  "",           self._bulk_upload),
            ("Download",     "",           self._export_holidays),
        ]:
            btn = QPushButton(label)
            if obj: btn.setObjectName(obj)
            btn.clicked.connect(slot)
            hdr.addWidget(btn)
        lay.addLayout(hdr)

        # Add Holiday Panel
        add_panel = Panel("Add New Holiday")
        add_lay = QVBoxLayout()
        
        top_row = QHBoxLayout()
        top_row.setSpacing(10)

        self._date_edit = QDateEdit()
        self._date_edit.setCalendarPopup(True)
        self._date_edit.setDate(QDate.currentDate())
        self._date_edit.setMaximumDate(QDate(2150, 12, 31))

        self._name_edit = QLineEdit()
        self._name_edit.setPlaceholderText("Holiday Name (e.g. Independence Day)")
        
        top_row.addWidget(QLabel("Date:"))
        top_row.addWidget(self._date_edit)
        top_row.addWidget(QLabel("Name:"))
        top_row.addWidget(self._name_edit, 1)
        add_lay.addLayout(top_row)

        # Calendar Flags
        flag_lay = QHBoxLayout()
        flag_lay.addWidget(QLabel("Applicable To:"))
        from core.database import HOLIDAY_CALENDAR_OPTIONS
        self._flags = {}
        for code, label in HOLIDAY_CALENDAR_OPTIONS:
            if code == "ALL": continue
            cb = QCheckBox(code)
            cb.setToolTip(label)
            self._flags[code] = cb
            flag_lay.addWidget(cb)
        
        add_btn = QPushButton("Save Holiday")
        add_btn.setObjectName("PrimaryBtn")
        add_btn.clicked.connect(self._add_holiday)
        flag_lay.addStretch()
        flag_lay.addWidget(add_btn)
        add_lay.addLayout(flag_lay)

        add_panel.add_layout(add_lay)
        lay.addWidget(add_panel)

        # Bulk Import Panel
        import_panel = Panel("Bulk Upload")
        import_lay = QHBoxLayout()
        import_lay.setSpacing(10)
        self._bulk_file_lbl = QLabel("No Excel file selected")
        self._bulk_file_lbl.setStyleSheet("color:#6B7280;font-size:11px;")
        self._bulk_browse_btn = QPushButton("Browse Excel…")
        self._bulk_import_btn = QPushButton("Import Holidays")
        self._bulk_import_btn.setObjectName("PrimaryBtn")
        self._bulk_prog = QProgressBar()
        self._bulk_prog.setVisible(False)
        self._bulk_prog.setRange(0, 0)

        self._bulk_browse_btn.clicked.connect(self._browse_bulk_file)
        self._bulk_import_btn.clicked.connect(self._import_bulk_file)

        import_lay.addWidget(self._bulk_file_lbl, 1)
        import_lay.addWidget(self._bulk_browse_btn)
        import_lay.addWidget(self._bulk_import_btn)
        
        import_panel.add_layout(import_lay)
        import_panel.add_widget(self._bulk_prog)
        lay.addWidget(import_panel)

        # List / Filter
        filt_row = QHBoxLayout()
        filt_row.addWidget(QLabel("Filter by Calendar:"))
        self._filt_combo = QComboBox()
        self._filt_combo.addItem("All Calendars", "ALL")
        from core.database import HOLIDAY_CALENDAR_OPTIONS
        for code, label in HOLIDAY_CALENDAR_OPTIONS:
            if code != "ALL":
                self._filt_combo.addItem(f"{code} - {label}", code)
        self._filt_combo.currentIndexChanged.connect(self._load_holidays)
        
        del_btn = QPushButton("Delete Selected")
        del_btn.setObjectName("DangerBtn")
        del_btn.clicked.connect(self._delete_holiday)

        filt_row.addWidget(self._filt_combo)
        filt_row.addStretch()
        filt_row.addWidget(del_btn)
        lay.addLayout(filt_row)

        # Table
        self._tbl = DataTable(["Date", "Day", "Name", "SIFMA", "US", "LON", "TOK", "NYS", "NYF"])
        # Align flag headers to center to match the checkmark cells
        for col in range(3, 9):
            h_item = self._tbl.horizontalHeaderItem(col)
            if h_item:
                h_item.setTextAlignment(Qt.AlignCenter)
        lay.addWidget(self._tbl, 1)

    def _load_holidays(self):
        from core.database import get_holidays
        code = self._filt_combo.currentData()
        with self._db() as conn:
            rows = get_holidays(conn, calendar_code=code)
        
        table_rows = []
        for r in rows:
            def check(val):
                item = QTableWidgetItem("✓" if val else "")
                item.setTextAlignment(Qt.AlignCenter)
                return item
            
            # Store hidden ID in the date item for deletion logic
            date_item = make_date_item(r["holiday_date"])
            date_item.setData(Qt.UserRole + 1, r["holiday_id"])
            
            table_rows.append([
                date_item,
                r["holiday_day"],
                r["holiday_name"],
                check(r["is_sifma"]),
                check(r["is_us"]),
                check(r["is_london"]),
                check(r["is_tokyo"]),
                check(r["is_nys"]),
                check(r["is_nyf"]),
            ])
        self._tbl.populate(table_rows)

    def _add_holiday(self):
        dt = self._date_edit.date().toString("yyyy-MM-dd")
        name = self._name_edit.text().strip()
        flags = {code: (1 if cb.isChecked() else 0) for code, cb in self._flags.items()}
        
        if not name:
            QMessageBox.warning(self, "Input Error", "Please enter a holiday name.")
            return
        if not any(flags.values()):
            QMessageBox.warning(self, "Input Error", "Please select at least one calendar.")
            return
            
        with self._db() as conn:
            from core.database import insert_holiday
            insert_holiday(conn, dt, name, flags)
            
        self._name_edit.clear()
        for cb in self._flags.values(): cb.setChecked(False)
        self._load_holidays()

    def _bulk_upload(self):
        self._browse_bulk_file()

    def _browse_bulk_file(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Select Holiday Excel File", "", "Excel Files (*.xlsx *.xls)"
        )
        if not path:
            return
        self._bulk_path = path
        self._bulk_file_lbl.setText(path.split("/")[-1])

    def _import_bulk_file(self):
        if not self._bulk_path:
            QMessageBox.warning(self, "No File", "Please browse and select an Excel file first.")
            return
        self._bulk_prog.setVisible(True)
        self._bulk_browse_btn.setEnabled(False)
        self._bulk_import_btn.setEnabled(False)

        thread = _HolidayImportThread(self._db, self._bulk_path)
        thread.done.connect(self._on_bulk_import_done)
        thread.error.connect(self._on_bulk_import_error)
        self._bulk_thread = thread
        thread.start()

    def _on_bulk_import_done(self, inserted, errs):
        self._bulk_prog.setVisible(False)
        self._bulk_browse_btn.setEnabled(True)
        self._bulk_import_btn.setEnabled(True)
        msg = f"{inserted} holiday(s) imported."
        if errs:
            msg += f"\n{len(errs)} row(s) failed:\n" + "\n".join(errs[:5])
        QMessageBox.information(self, "Import Complete", msg)
        self._load_holidays()

    def _on_bulk_import_error(self, err):
        self._bulk_prog.setVisible(False)
        self._bulk_browse_btn.setEnabled(True)
        self._bulk_import_btn.setEnabled(True)
        QMessageBox.critical(self, "Import Error", err)

    def _export_holidays(self):
        from core.database import get_holidays
        with self._db() as conn:
            rows = get_holidays(conn, calendar_code="ALL")
        
        if not rows:
            QMessageBox.warning(self, "No Data", "No holidays found to export.")
            return

        path, _ = QFileDialog.getSaveFileName(
            self, "Export Holidays", "market_holidays_export.xlsx", "Excel Files (*.xlsx)"
        )
        if not path: return

        try:
            import pandas as pd
            export_data = []
            for r in rows:
                export_data.append({
                    "Date": r["holiday_date"],
                    "Day": r["holiday_day"],
                    "Name": r["holiday_name"],
                    "SIFMA": "✓" if r["is_sifma"] else "",
                    "US": "✓" if r["is_us"] else "",
                    "LON": "✓" if r["is_london"] else "",
                    "TOK": "✓" if r["is_tokyo"] else "",
                    "NYS": "✓" if r["is_nys"] else "",
                    "NYF": "✓" if r["is_nyf"] else "",
                })
            
            df = pd.DataFrame(export_data)
            df.to_excel(path, index=False)
            QMessageBox.information(self, "Export Complete", f"Holidays exported to:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Export Error", str(e))

    def _delete_holiday(self):
        row = self._tbl.currentRow()
        if row < 0: return
        
        # ID is hidden in the first column's item data
        item = self._tbl.item(row, 0)
        if not item: return
        holiday_id = item.data(Qt.UserRole + 1)
        
        with self._db() as conn:
            from core.database import delete_holiday
            delete_holiday(conn, holiday_id)
        self._load_holidays()

    def showEvent(self, event):
        super().showEvent(event)
        QTimer.singleShot(0, self._load_holidays)