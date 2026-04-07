"""ui/pages/rates.py — SOFR Rate and SOFR Index management with manual entry."""

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QFileDialog, QMessageBox,
    QDateEdit, QProgressBar, QTabWidget,
    QDoubleSpinBox, QDialog, QFormLayout,
    QDialogButtonBox, QFrame
)
from PySide6.QtCore import Qt, QDate, QTimer, QThread, Signal
from ui.widgets.common import Panel, PageHeader, DataTable, fmt_date, make_date_item
from ui.styles import GREEN, RED, ACCENT


# ---------------------------------------------------------------------------
# Background import thread
# ---------------------------------------------------------------------------

class _ImportThread(QThread):
    done  = Signal(int, list)
    error = Signal(str)

    def __init__(self, db_factory, path, mode):
        super().__init__()
        self._db, self._path, self._mode = db_factory, path, mode

    def run(self):
        try:
            if self._mode == "rate":
                from core.database import import_rates_from_excel
                with self._db() as conn:
                    n, errs = import_rates_from_excel(conn, self._path)
            else:
                from core.database import import_index_from_excel
                with self._db() as conn:
                    n, errs = import_index_from_excel(conn, self._path)
            self.done.emit(n, errs)
        except Exception as e:
            self.error.emit(str(e))


# ---------------------------------------------------------------------------
# Manual entry dialog — shared for both rate and index
# ---------------------------------------------------------------------------

class _ManualEntryDialog(QDialog):
    """Single-row entry dialog for SOFR rate or index value."""

    def __init__(self, mode: str, existing: dict | None = None, parent=None):
        super().__init__(parent)
        self._mode = mode   # "rate" or "index"
        title = "SOFR Overnight Rate" if mode == "rate" else "SOFR Compounded Index"
        verb  = "Edit" if existing else "Add"
        self.setWindowTitle(f"{verb} — {title}")
        self.setMinimumWidth(340)
        self._build(existing)

    def _build(self, existing: dict | None):
        lay = QVBoxLayout(self)
        lay.setSpacing(12)

        form = QFormLayout()
        form.setSpacing(12)
        form.setLabelAlignment(Qt.AlignRight)

        # Date
        self._date = QDateEdit()
        self._date.setCalendarPopup(True)
        if existing:
            self._date.setDate(
                QDate.fromString(existing["rate_date"][:10], "yyyy-MM-dd")
            )
            self._date.setEnabled(False)   # date is the PK — can't change
        else:
            self._date.setDate(QDate.currentDate())
        form.addRow("Date:", self._date)

        if self._mode == "rate":
            # SOFR Rate
            self._rate = QDoubleSpinBox()
            self._rate.setRange(0.0, 20.0)
            self._rate.setDecimals(4)
            self._rate.setSingleStep(0.0001)
            self._rate.setSuffix(" %")
            if existing:
                self._rate.setValue(existing.get("sofr_rate", 0.0))
            form.addRow("SOFR Rate (%):", self._rate)

            # Day count factor
            from PySide6.QtWidgets import QComboBox
            self._dcf = QComboBox()
            self._dcf.addItems(["1  (Mon–Thu)", "3  (Friday)"])
            if existing:
                dcf = existing.get("day_count_factor", 1)
                self._dcf.setCurrentIndex(0 if dcf == 1 else 1)
            else:
                # Auto-set based on selected date
                self._date.dateChanged.connect(self._auto_dcf)
                self._auto_dcf(self._date.date())
            form.addRow("Day Count Factor:", self._dcf)

        else:
            # SOFR Index
            self._index = QDoubleSpinBox()
            self._index.setRange(0.0, 999999.0)
            self._index.setDecimals(8)
            self._index.setSingleStep(0.00001)
            if existing:
                self._index.setValue(existing.get("sofr_index", 1.0))
            form.addRow("SOFR Index Value:", self._index)

        lay.addLayout(form)

        note = QLabel(
            "Saving will <b>overwrite</b> any existing entry for this date."
            if not existing else
            "Editing the value for this date."
        )
        note.setStyleSheet("color:#6B7280; font-size:11px;")
        note.setWordWrap(True)
        lay.addWidget(note)

        btns = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel)
        btns.accepted.connect(self._validate)
        btns.rejected.connect(self.reject)
        lay.addWidget(btns)

    def _auto_dcf(self, qdate: QDate):
        """Auto-set day count factor: Friday = 3, else 1."""
        dow = qdate.dayOfWeek()   # 1=Mon … 7=Sun
        self._dcf.setCurrentIndex(1 if dow == 5 else 0)  # Friday = index 1

    def _validate(self):
        if self._mode == "rate" and self._rate.value() == 0.0:
            QMessageBox.warning(self, "Invalid", "SOFR Rate cannot be zero.")
            return
        if self._mode == "index" and self._index.value() == 0.0:
            QMessageBox.warning(self, "Invalid", "SOFR Index cannot be zero.")
            return
        self.accept()

    def get_data(self) -> dict:
        d = {"rate_date": self._date.date().toString("yyyy-MM-dd")}
        if self._mode == "rate":
            dcf_text = self._dcf.currentText()
            d["sofr_rate"]        = self._rate.value()
            d["day_count_factor"] = 3 if "Friday" in dcf_text else 1
        else:
            d["sofr_index"] = self._index.value()
        return d


# ---------------------------------------------------------------------------
# Main page
# ---------------------------------------------------------------------------

class RatesPage(QWidget):
    def __init__(self, db_factory, parent=None):
        super().__init__(parent)
        self._db = db_factory
        self._build_ui()

    def _build_ui(self):
        lay = QVBoxLayout(self)
        lay.setContentsMargins(20, 16, 20, 16)
        lay.setSpacing(14)

        lay.addWidget(PageHeader(
            "SOFR Rate Tables",
            "Manage SOFR overnight rates and SOFR compounded index in separate tables — "
            "upload NY Fed Excel files directly or enter / edit individual values"
        ))

        # ── Two panels side by side: import + manual entry ────────────────────
        panels_row = QHBoxLayout()
        panels_row.setSpacing(10)
        panels_row.addWidget(self._build_rate_panel(),  1)
        panels_row.addWidget(self._build_index_panel(), 1)
        lay.addLayout(panels_row)

        # ── Summary ───────────────────────────────────────────────────────────
        summary = Panel("Database Summary")
        self._sum_lbl = QLabel("Loading…")
        self._sum_lbl.setWordWrap(True)
        summary.add_widget(self._sum_lbl)
        lay.addWidget(summary)

        # ── Date filter ───────────────────────────────────────────────────────
        filt = QHBoxLayout()
        filt.addWidget(QLabel("View from:"))
        self._from_dt = QDateEdit()
        self._from_dt.setCalendarPopup(True)
        self._from_dt.setDate(QDate.currentDate().addDays(-30))
        self._to_dt = QDateEdit()
        self._to_dt.setCalendarPopup(True)
        self._to_dt.setDate(QDate.currentDate())
        filt.addWidget(self._from_dt)
        filt.addWidget(QLabel("to:"))
        filt.addWidget(self._to_dt)
        go_btn = QPushButton("Apply")
        go_btn.clicked.connect(self._load_tables)
        filt.addWidget(go_btn)
        filt.addStretch()
        lay.addLayout(filt)

        # ── Tab tables ────────────────────────────────────────────────────────
        tabs = QTabWidget()
        tabs.setDocumentMode(True)

        # SOFR Rates tab
        rate_tab = QWidget()
        rate_lay = QVBoxLayout(rate_tab)
        rate_lay.setContentsMargins(0, 4, 0, 0)
        rate_lay.setSpacing(4)

        rate_btn_row = QHBoxLayout()
        edit_rate_btn   = QPushButton("Edit Selected")
        delete_rate_btn = QPushButton("Delete Selected")
        delete_rate_btn.setObjectName("DangerBtn")
        edit_rate_btn.clicked.connect(lambda: self._edit_row("rate"))
        delete_rate_btn.clicked.connect(lambda: self._delete_row("rate"))
        rate_btn_row.addStretch()
        rate_btn_row.addWidget(edit_rate_btn)
        rate_btn_row.addWidget(delete_rate_btn)
        rate_lay.addLayout(rate_btn_row)

        self._rate_tbl = DataTable([
            "Effective Date", "SOFR Rate (%)", "Day Count Factor", "Daily Accrual Factor"
        ])
        rate_lay.addWidget(self._rate_tbl)
        tabs.addTab(rate_tab, "SOFR Overnight Rates")

        # SOFR Index tab
        idx_tab = QWidget()
        idx_lay = QVBoxLayout(idx_tab)
        idx_lay.setContentsMargins(0, 4, 0, 0)
        idx_lay.setSpacing(4)

        idx_btn_row = QHBoxLayout()
        edit_idx_btn   = QPushButton("Edit Selected")
        delete_idx_btn = QPushButton("Delete Selected")
        delete_idx_btn.setObjectName("DangerBtn")
        edit_idx_btn.clicked.connect(lambda: self._edit_row("index"))
        delete_idx_btn.clicked.connect(lambda: self._delete_row("index"))
        idx_btn_row.addStretch()
        idx_btn_row.addWidget(edit_idx_btn)
        idx_btn_row.addWidget(delete_idx_btn)
        idx_lay.addLayout(idx_btn_row)

        self._idx_tbl = DataTable([
            "Date", "SOFR Index", "Daily Change", "Change (%)"
        ])
        idx_lay.addWidget(self._idx_tbl)
        tabs.addTab(idx_tab, "SOFR Compounded Index")

        lay.addWidget(tabs, 1)

        # Keep raw data for edit/delete
        self._rate_data  = []
        self._index_data = []

    # ── Panel builders ────────────────────────────────────────────────────────

    def _build_rate_panel(self) -> QWidget:
        panel = Panel("SOFR Overnight Rate")

        # ── Manual entry row ────────────────────
        entry_row = QHBoxLayout()
        self._rate_date = QDateEdit()
        self._rate_date.setCalendarPopup(True)
        self._rate_date.setDate(QDate.currentDate())
        self._rate_date.dateChanged.connect(self._auto_rate_dcf)

        self._rate_val = QDoubleSpinBox()
        self._rate_val.setRange(0.0, 20.0)
        self._rate_val.setDecimals(4)
        self._rate_val.setSingleStep(0.0001)
        self._rate_val.setSuffix(" %")

        from PySide6.QtWidgets import QComboBox
        self._rate_dcf = QComboBox()
        self._rate_dcf.addItems(["1  (Mon–Thu)", "3  (Friday)"])
        self._auto_rate_dcf(self._rate_date.date())

        save_rate_btn = QPushButton("Save Rate")
        save_rate_btn.setObjectName("GreenBtn")
        save_rate_btn.clicked.connect(self._save_rate)

        entry_row.addWidget(QLabel("Date:"))
        entry_row.addWidget(self._rate_date)
        entry_row.addWidget(QLabel("Rate:"))
        entry_row.addWidget(self._rate_val)
        entry_row.addWidget(QLabel("DCF:"))
        entry_row.addWidget(self._rate_dcf)
        entry_row.addWidget(save_rate_btn)
        panel.add_layout(entry_row)

        # ── Divider ─────────────────────────────
        line = QFrame(); line.setFrameShape(QFrame.HLine)
        line.setStyleSheet("color:#E5E7EB; margin:4px 0;")
        panel.add_widget(line)

        # ── Excel import row ─────────────────────
        imp_row = QHBoxLayout()
        self._rate_file_lbl = QLabel("No file selected")
        self._rate_file_lbl.setStyleSheet("color:#6B7280; font-size:11px;")
        browse_btn = QPushButton("Browse Excel…")
        self._rate_import_btn = QPushButton("Import")
        self._rate_import_btn.setObjectName("PrimaryBtn")
        self._rate_prog = QProgressBar()
        self._rate_prog.setVisible(False)
        self._rate_prog.setRange(0, 0)
        self._rate_path = None

        browse_btn.clicked.connect(lambda: self._browse("rate"))
        self._rate_import_btn.clicked.connect(lambda: self._import("rate"))

        imp_row.addWidget(self._rate_file_lbl, 1)
        imp_row.addWidget(browse_btn)
        imp_row.addWidget(self._rate_import_btn)
        panel.add_layout(imp_row)
        panel.add_widget(self._rate_prog)

        hint = QLabel(
            "Upload NY Fed SOFR Rate Excel directly — expected columns: "
            "<b>DATE</b>, <b>RATE (%)</b>"
        )
        hint.setStyleSheet("color:#6B7280; font-size:10px;")
        panel.add_widget(hint)

        return panel

    def _build_index_panel(self) -> QWidget:
        panel = Panel("SOFR Compounded Index")

        # ── Manual entry row ────────────────────
        entry_row = QHBoxLayout()
        self._idx_date = QDateEdit()
        self._idx_date.setCalendarPopup(True)
        self._idx_date.setDate(QDate.currentDate())

        self._idx_val = QDoubleSpinBox()
        self._idx_val.setRange(0.0, 999999.0)
        self._idx_val.setDecimals(8)
        self._idx_val.setSingleStep(0.00001)

        save_idx_btn = QPushButton("Save Index")
        save_idx_btn.setObjectName("GreenBtn")
        save_idx_btn.clicked.connect(self._save_index)

        entry_row.addWidget(QLabel("Date:"))
        entry_row.addWidget(self._idx_date)
        entry_row.addWidget(QLabel("Index:"))
        entry_row.addWidget(self._idx_val)
        entry_row.addWidget(save_idx_btn)
        panel.add_layout(entry_row)

        # ── Divider ─────────────────────────────
        line = QFrame(); line.setFrameShape(QFrame.HLine)
        line.setStyleSheet("color:#E5E7EB; margin:4px 0;")
        panel.add_widget(line)

        # ── Excel import row ─────────────────────
        imp_row = QHBoxLayout()
        self._idx_file_lbl = QLabel("No file selected")
        self._idx_file_lbl.setStyleSheet("color:#6B7280; font-size:11px;")
        browse_btn = QPushButton("Browse Excel…")
        self._idx_import_btn = QPushButton("Import")
        self._idx_import_btn.setObjectName("PrimaryBtn")
        self._idx_prog = QProgressBar()
        self._idx_prog.setVisible(False)
        self._idx_prog.setRange(0, 0)
        self._idx_path = None

        browse_btn.clicked.connect(lambda: self._browse("index"))
        self._idx_import_btn.clicked.connect(lambda: self._import("index"))

        imp_row.addWidget(self._idx_file_lbl, 1)
        imp_row.addWidget(browse_btn)
        imp_row.addWidget(self._idx_import_btn)
        panel.add_layout(imp_row)
        panel.add_widget(self._idx_prog)

        hint = QLabel(
            "Upload NY Fed SOFR Averages &amp; Index Excel directly — expected columns: "
            "<b>DATE</b>, <b>INDEX</b>"
        )
        hint.setStyleSheet("color:#6B7280; font-size:10px;")
        panel.add_widget(hint)

        return panel

    # ── Auto day-count factor ─────────────────────────────────────────────────

    def _auto_rate_dcf(self, qdate: QDate):
        dow = qdate.dayOfWeek()   # 1=Mon … 7=Sun
        self._rate_dcf.setCurrentIndex(1 if dow == 5 else 0)

    # ── Manual save ───────────────────────────────────────────────────────────

    def _save_rate(self):
        d   = self._rate_date.date().toString("yyyy-MM-dd")
        val = self._rate_val.value()
        if val == 0.0:
            QMessageBox.warning(self, "Invalid", "SOFR Rate cannot be zero.")
            return
        dcf_text = self._rate_dcf.currentText()
        dcf = 3 if "Friday" in dcf_text else 1
        try:
            with self._db() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO sofr_rates
                        (rate_date, sofr_rate, day_count_factor)
                    VALUES (?, ?, ?)
                """, (d, val, dcf))
            self._load_summary()
            self._load_tables()
            # Advance date by 1 for next entry
            self._rate_date.setDate(self._rate_date.date().addDays(1))
            QMessageBox.information(
                self, "Saved",
                f"SOFR Rate {val:.4f}% saved for {fmt_date(d)}."
            )
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _save_index(self):
        d   = self._idx_date.date().toString("yyyy-MM-dd")
        val = self._idx_val.value()
        if val == 0.0:
            QMessageBox.warning(self, "Invalid", "SOFR Index cannot be zero.")
            return
        try:
            with self._db() as conn:
                conn.execute("""
                    INSERT OR REPLACE INTO sofr_index (rate_date, sofr_index)
                    VALUES (?, ?)
                """, (d, val))
            self._load_summary()
            self._load_tables()
            self._idx_date.setDate(self._idx_date.date().addDays(1))
            QMessageBox.information(
                self, "Saved",
                f"SOFR Index {val:.8f} saved for {fmt_date(d)}."
            )
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    # ── Edit / delete selected row ────────────────────────────────────────────

    def _edit_row(self, mode: str):
        tbl  = self._rate_tbl if mode == "rate" else self._idx_tbl
        data = self._rate_data if mode == "rate" else self._index_data
        row  = tbl.currentRow()
        if row < 0 or row >= len(data):
            QMessageBox.warning(self, "Select Row",
                "Please select a row to edit.")
            return
        record = data[row]
        dlg = _ManualEntryDialog(mode, existing=record, parent=self)
        if dlg.exec() != QDialog.Accepted:
            return
        upd = dlg.get_data()
        try:
            with self._db() as conn:
                if mode == "rate":
                    conn.execute("""
                        UPDATE sofr_rates SET sofr_rate=?, day_count_factor=?
                        WHERE rate_date=?
                    """, (upd["sofr_rate"], upd["day_count_factor"], upd["rate_date"]))
                else:
                    conn.execute("""
                        UPDATE sofr_index SET sofr_index=?
                        WHERE rate_date=?
                    """, (upd["sofr_index"], upd["rate_date"]))
            self._load_summary()
            self._load_tables()
            QMessageBox.information(self, "Updated",
                f"Entry for {fmt_date(upd['rate_date'])} updated.")
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def _delete_row(self, mode: str):
        tbl  = self._rate_tbl if mode == "rate" else self._idx_tbl
        data = self._rate_data if mode == "rate" else self._index_data
        row  = tbl.currentRow()
        if row < 0 or row >= len(data):
            QMessageBox.warning(self, "Select Row",
                "Please select a row to delete.")
            return
        record  = data[row]
        rate_dt = record["rate_date"]
        table   = "sofr_rates" if mode == "rate" else "sofr_index"
        label   = "SOFR Rate" if mode == "rate" else "SOFR Index"

        if QMessageBox.question(
            self, "Confirm Delete",
            f"Delete {label} entry for {fmt_date(rate_dt)}?",
            QMessageBox.Yes | QMessageBox.No
        ) != QMessageBox.Yes:
            return
        try:
            with self._db() as conn:
                conn.execute(
                    f"DELETE FROM {table} WHERE rate_date=?", (rate_dt,)
                )
            self._load_summary()
            self._load_tables()
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    # ── Excel file import ─────────────────────────────────────────────────────

    def _browse(self, mode: str):
        p, _ = QFileDialog.getOpenFileName(
            self, "Select Excel File", "", "Excel Files (*.xlsx *.xls)"
        )
        if not p:
            return
        if mode == "rate":
            self._rate_path = p
            self._rate_file_lbl.setText(p.split("/")[-1])
        else:
            self._idx_path = p
            self._idx_file_lbl.setText(p.split("/")[-1])

    def _import(self, mode: str):
        path = self._rate_path if mode == "rate" else self._idx_path
        if not path:
            QMessageBox.warning(self, "No File",
                "Please browse and select an Excel file first.")
            return
        prog = self._rate_prog if mode == "rate" else self._idx_prog
        ibtn = self._rate_import_btn if mode == "rate" else self._idx_import_btn
        prog.setVisible(True)
        ibtn.setEnabled(False)

        t = _ImportThread(self._db, path, mode)
        t.done.connect(lambda n, e: self._on_import_done(mode, n, e, prog, ibtn))
        t.error.connect(lambda err: self._on_import_error(mode, err, prog, ibtn))
        setattr(self, f"_thread_{mode}", t)   # keep alive
        t.start()

    def _on_import_done(self, mode, n, errs, prog, ibtn):
        prog.setVisible(False)
        ibtn.setEnabled(True)
        label = "SOFR Rate" if mode == "rate" else "SOFR Index"
        msg = f"{n} {label} rows imported into the {label} table."
        if errs:
            msg += f"\n{len(errs)} errors:\n" + "\n".join(errs[:5])
        QMessageBox.information(self, "Import Complete", msg)
        self._load_summary()
        self._load_tables()

    def _on_import_error(self, mode, err, prog, ibtn):
        prog.setVisible(False)
        ibtn.setEnabled(True)
        QMessageBox.critical(self, "Import Error", err)

    # ── Summary & table loading ───────────────────────────────────────────────

    def _load_summary(self):
        from core.database import get_rates_summary
        with self._db() as conn:
            s = get_rates_summary(conn)

        def _fmt(cnt, earliest, latest):
            if not cnt:
                return "<span style='color:#9CA3AF;'>No data</span>"
            return (f"<b>{cnt}</b> records &nbsp;|&nbsp; "
                    f"{fmt_date(earliest)} → {fmt_date(latest)}")

        self._sum_lbl.setText(
            f"<b>SOFR Overnight Rates:</b> &nbsp;"
            f"{_fmt(s.get('rates_cnt',0), s.get('rates_earliest'), s.get('rates_latest'))}"
            f"&nbsp;&nbsp;|&nbsp;&nbsp;"
            f"<b>SOFR Compounded Index:</b> &nbsp;"
            f"{_fmt(s.get('index_cnt',0), s.get('index_earliest'), s.get('index_latest'))}"
        )

    def _load_tables(self):
        s = self._from_dt.date().toString("yyyy-MM-dd")
        e = self._to_dt.date().toString("yyyy-MM-dd")

        from core.database import get_rates, get_index_rates
        with self._db() as conn:
            rates   = get_rates(conn, start=s, end=e, limit=500)
            indexes = get_index_rates(conn, start=s, end=e, limit=500)

        # Keep raw data for edit/delete
        self._rate_data  = rates
        self._index_data = indexes

        # SOFR Rates table (newest first)
        rate_rows = []
        for r in rates:
            accrual = r["sofr_rate"] / 100.0 * r["day_count_factor"] / 360.0
            rate_rows.append([
                make_date_item(r["rate_date"]),
                f"{r['sofr_rate']:.4f}%",
                str(r["day_count_factor"]),
                f"{accrual * 100:.6f}%",
            ])
        self._rate_tbl.populate(rate_rows)

        # SOFR Index table — compute daily change (work ascending, display descending)
        idx_rows = []
        prev_idx = None
        for r in reversed(indexes):
            cur = r["sofr_index"]
            if prev_idx is not None and prev_idx != 0:
                chg     = cur - prev_idx
                chg_pct = (chg / prev_idx) * 100.0
                chg_str = f"{chg:+.8f}"
                pct_str = f"{chg_pct:+.6f}%"
            else:
                chg_str = "—"
                pct_str = "—"
            idx_rows.append([
                make_date_item(r["rate_date"]),
                f"{cur:.8f}",
                chg_str,
                pct_str,
            ])
            prev_idx = cur
        self._idx_tbl.populate(list(reversed(idx_rows)))

    def showEvent(self, event):
        super().showEvent(event)
        QTimer.singleShot(0, self._load_summary)
        QTimer.singleShot(0, self._load_tables)

    # Global search hook from MainWindow (no filtering needed here)
    def apply_search(self, term: str):
        pass
