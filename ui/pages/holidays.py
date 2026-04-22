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
        self._calendar_options = []
        self._calendar_checkboxes = {}
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

        calendar_row = QHBoxLayout()
        calendar_row.setSpacing(10)
        self._new_calendar_edit = QLineEdit()
        self._new_calendar_edit.setPlaceholderText(
            "New holiday calendar name (e.g. Mumbai)"
        )
        add_calendar_btn = QPushButton("Add Calendar")
        add_calendar_btn.clicked.connect(self._add_calendar)
        calendar_row.addWidget(QLabel("Holiday Calendar:"))
        calendar_row.addWidget(self._new_calendar_edit, 1)
        calendar_row.addWidget(add_calendar_btn)
        add_lay.addLayout(calendar_row)
        
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
        self._flag_lay = flag_lay
        
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
        self._filt_combo.currentIndexChanged.connect(self._load_holidays)
        
        del_btn = QPushButton("Delete Selected")
        del_btn.setObjectName("DangerBtn")
        del_btn.clicked.connect(self._delete_holiday)

        filt_row.addWidget(self._filt_combo)
        filt_row.addStretch()
        filt_row.addWidget(del_btn)
        lay.addLayout(filt_row)

        # Table
        self._tbl = DataTable(["Date", "Day", "Name"])
        lay.addWidget(self._tbl, 1)
        self._refresh_calendar_controls()

    def _fetch_calendar_options(self):
        from core.database import list_holiday_calendars
        with self._db() as conn:
            return list_holiday_calendars(conn, include_all=True)

    def _refresh_calendar_controls(self):
        self._calendar_options = self._fetch_calendar_options()
        calendar_only = [(code, label) for code, label in self._calendar_options if code != "ALL"]

        current_filter = self._filt_combo.currentData() or "ALL"
        self._filt_combo.blockSignals(True)
        self._filt_combo.clear()
        self._filt_combo.addItem("All Calendars", "ALL")
        for code, label in calendar_only:
            self._filt_combo.addItem(f"{code} - {label}", code)
        idx = self._filt_combo.findData(current_filter)
        self._filt_combo.setCurrentIndex(idx if idx >= 0 else 0)
        self._filt_combo.blockSignals(False)

        while self._flag_lay.count() > 2:
            item = self._flag_lay.takeAt(1)
            widget = item.widget()
            if widget:
                widget.deleteLater()
        self._calendar_checkboxes = {}
        insert_at = max(1, self._flag_lay.count() - 2)
        for offset, (code, label) in enumerate(calendar_only):
            cb = QCheckBox(code)
            cb.setToolTip(label)
            self._calendar_checkboxes[code] = cb
            self._flag_lay.insertWidget(insert_at + offset, cb)

        headers = ["Date", "Day", "Name"] + [code for code, _ in calendar_only]
        self._tbl.setColumnCount(len(headers))
        self._tbl.setHorizontalHeaderLabels(headers)
        for col in range(3, len(headers)):
            h_item = self._tbl.horizontalHeaderItem(col)
            if h_item:
                h_item.setTextAlignment(Qt.AlignCenter)

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
            ] + [
                check(r["calendar_flags"].get(code, 0))
                for code, _ in self._calendar_options if code != "ALL"
            ])
        self._tbl.populate(table_rows)

    def _add_holiday(self):
        dt = self._date_edit.date().toString("yyyy-MM-dd")
        name = self._name_edit.text().strip()
        selected_codes = [
            code for code, cb in self._calendar_checkboxes.items() if cb.isChecked()
        ]
        
        if not name:
            QMessageBox.warning(self, "Input Error", "Please enter a holiday name.")
            return
        if not selected_codes:
            QMessageBox.warning(self, "Input Error", "Please select at least one calendar.")
            return
            
        with self._db() as conn:
            from core.database import insert_holiday
            insert_holiday(conn, dt, name, selected_codes)
            
        self._name_edit.clear()
        for cb in self._calendar_checkboxes.values():
            cb.setChecked(False)
        self._load_holidays()

    def _add_calendar(self):
        name = self._new_calendar_edit.text().strip()
        if not name:
            QMessageBox.warning(self, "Input Error", "Please enter a holiday calendar name.")
            return
        try:
            with self._db() as conn:
                from core.database import add_holiday_calendar
                code, label = add_holiday_calendar(conn, name)
            self._new_calendar_edit.clear()
            self._refresh_calendar_controls()
            QMessageBox.information(
                self,
                "Calendar Added",
                f"Holiday calendar {code} - {label} is ready to use."
            )
        except Exception as e:
            QMessageBox.critical(self, "Calendar Error", str(e))

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
        self._refresh_calendar_controls()
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
                record = {
                    "Date": r["holiday_date"],
                    "Day": r["holiday_day"],
                    "Name": r["holiday_name"],
                }
                for code, _ in self._calendar_options:
                    if code == "ALL":
                        continue
                    record[code] = "✓" if r["calendar_flags"].get(code) else ""
                export_data.append(record)
            
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
        QTimer.singleShot(0, self._refresh_calendar_controls)
        QTimer.singleShot(0, self._load_holidays)
