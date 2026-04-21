"""main.py — SOFR Interest Calculator entry point."""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from PySide6.QtWidgets import QApplication

from core.database import init_db, get_conn
from ui.main_window import MainWindow


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("SOFR Interest Calculator")
    app.setOrganizationName("FinTech")

    # Initialise database and auto-mature past-maturity deals
    init_db()
    from core.database import auto_mature_deals
    with get_conn() as conn:
        n = auto_mature_deals(conn)
        if n:
            print(f"Auto-matured {n} deals past their maturity date.")

    window = MainWindow(get_conn)
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
