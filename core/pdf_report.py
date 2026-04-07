"""
core/pdf_report.py
------------------
Generates a professional PDF report for each SOFR interest calculation.
Uses reportlab (pure-Python, no external binaries required).

Install:  pip install reportlab
"""

from pathlib import Path
from datetime import datetime, date
from typing import Optional
import re

# ---------------------------------------------------------------------------
# Colour palette (matches app UI)
# ---------------------------------------------------------------------------
NAVY   = (0.106, 0.227, 0.420)   # #1B3A6B
TEAL   = (0.051, 0.361, 0.388)   # #0D5C63
GREEN  = (0.059, 0.431, 0.337)   # #0F6E56
PURPLE = (0.325, 0.290, 0.718)   # #534AB7
AMBER  = (0.522, 0.310, 0.043)   # #854F0B
LIGHT  = (0.961, 0.969, 0.980)   # #F5F7FA
WHITE  = (1.0,   1.0,   1.0)
DARK   = (0.102, 0.102, 0.118)   # #1A1A1E
GREY   = (0.420, 0.443, 0.502)   # #6B7280
LGREY  = (0.953, 0.953, 0.969)   # #F3F4F6

PDF_DIR = Path("/home/vijay/Desktop/SOFR Reports")


def _safe_path_part(value, fallback: str) -> str:
    text = str(value or "").strip()
    if not text:
        text = fallback
    text = re.sub(r"[<>:\"/\\\\|?*]+", "_", text)
    text = re.sub(r"\s+", " ", text).strip().replace(" ", "_")
    text = text.strip("._")
    return text or fallback


def _report_folder_name(result: dict, created_at: datetime) -> str:
    deal_name = _safe_path_part(result.get("deal_name"), "Unknown_Deal")
    cusip = _safe_path_part(result.get("cusip"), "UNKNOWN")
    created = created_at.strftime("%Y%m%d")
    return f"{deal_name}_{cusip}_{created}"

def _fmt_money(v) -> str:
    if v is None: return "—"
    try:    return f"${float(v):,.2f}"
    except: return str(v)

def _fmt_rate(v, dec=6) -> str:
    if v is None: return "—"
    try:    return f"{float(v)*100:.{dec}f}%"
    except: return str(v)

def _fmt_date(v) -> str:
    if not v: return "—"
    try:
        if isinstance(v, (date, datetime)):
            return v.strftime("%d-%b-%Y")
        return datetime.fromisoformat(str(v)[:10]).strftime("%d-%b-%Y")
    except: return str(v)


def _fmt_pct_value(v, dec=4) -> str:
    if v is None:
        return "—"
    try:
        return f"{float(v):.{dec}f}%"
    except:
        return str(v)


def _metric_font_size(value) -> int:
    text = str(value or "")
    if len(text) >= 14:
        return 12
    if len(text) >= 11:
        return 13
    if len(text) >= 9:
        return 14
    return 16


def generate_calculation_pdf(result: dict,
                              output_dir: Optional[Path] = None) -> Path:
    """
    Generate a PDF report for a single interest calculation result.

    Parameters
    ----------
    result : dict
        The dict returned by core.database.calculate_interest()
    output_dir : Path, optional
        Where to save the PDF. Defaults to /home/vijay/Desktop/SOFR Reports/

    Returns
    -------
    Path to the generated PDF file.
    """
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.units import cm, mm
        from reportlab.lib.colors import HexColor, Color
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer, Table,
            TableStyle, HRFlowable, KeepTogether
        )
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    except ImportError:
        raise ImportError(
            "reportlab is required for PDF generation.\n"
            "Install it with:  pip install reportlab"
        )

    # ── Output path ──────────────────────────────────────────────────────────
    created_at = datetime.now()
    base_dir = output_dir or PDF_DIR
    report_dir = base_dir / _report_folder_name(result, created_at)
    report_dir.mkdir(parents=True, exist_ok=True)

    ts     = created_at.strftime("%Y%m%d_%H%M%S")
    cusip  = result.get("cusip", "UNKNOWN")
    fname  = f"SOFR_Calc_{cusip}_{ts}.pdf"
    fpath  = report_dir / fname

    # ── Document setup ───────────────────────────────────────────────────────
    doc = SimpleDocTemplate(
        str(fpath),
        pagesize=A4,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm, bottomMargin=2*cm,
        title=f"SOFR Interest Calculation — {cusip}",
        author="SOFR Interest Calculator",
    )

    W = A4[0] - 4*cm   # usable width

    # ── Colour objects ───────────────────────────────────────────────────────
    c_navy   = Color(*NAVY)
    c_teal   = Color(*TEAL)
    c_green  = Color(*GREEN)
    c_purple = Color(*PURPLE)
    c_amber  = Color(*AMBER)
    c_light  = Color(*LIGHT)
    c_lgrey  = Color(*LGREY)
    c_grey   = Color(*GREY)
    c_dark   = Color(*DARK)
    c_white  = Color(*WHITE)

    # ── Styles ───────────────────────────────────────────────────────────────
    base = getSampleStyleSheet()

    def S(name, **kw):
        return ParagraphStyle(name, parent=base["Normal"], **kw)

    s_title  = S("title",  fontSize=20, textColor=c_white,
                 fontName="Helvetica-Bold", alignment=TA_LEFT,
                 spaceAfter=2)
    s_sub    = S("sub",    fontSize=10, textColor=HexColor("#B5D4F4"),
                 fontName="Helvetica", alignment=TA_LEFT)
    s_sect   = S("sect",   fontSize=9,  textColor=c_white,
                 fontName="Helvetica-Bold", alignment=TA_LEFT,
                 spaceAfter=4)
    s_label  = S("label",  fontSize=8,  textColor=c_grey,
                 fontName="Helvetica", spaceAfter=1)
    s_value  = S("value",  fontSize=11, textColor=c_dark,
                 fontName="Helvetica-Bold", spaceAfter=6)
    s_normal = S("normal", fontSize=9,  textColor=c_dark,
                 fontName="Helvetica",  leading=14)
    s_mono   = S("mono",   fontSize=8,  textColor=c_dark,
                 fontName="Courier",    leading=12)
    s_footer = S("footer", fontSize=7,  textColor=c_grey,
                 fontName="Helvetica", alignment=TA_CENTER)
    s_tbl_hdr = S("tblhdr", fontSize=8, textColor=c_white,
                  fontName="Helvetica-Bold", alignment=TA_LEFT)
    s_tbl_val = S("tblval", fontSize=8, textColor=c_dark,
                  fontName="Helvetica", alignment=TA_LEFT)

    # ── Common table style factories ─────────────────────────────────────────
    def detail_tbl_style():
        return TableStyle([
            ("BACKGROUND",  (0,0), (-1,0),  c_navy),
            ("FONTNAME",    (0,0), (-1,0),  "Helvetica-Bold"),
            ("FONTSIZE",    (0,0), (-1,-1), 8),
            ("TEXTCOLOR",   (0,0), (-1,0),  c_white),
            ("ROWBACKGROUNDS", (0,1), (-1,-1), [c_white, c_lgrey]),
            ("GRID",        (0,0), (-1,-1), 0.3, c_lgrey),
            ("LEFTPADDING",  (0,0), (-1,-1), 8),
            ("RIGHTPADDING", (0,0), (-1,-1), 8),
            ("TOPPADDING",   (0,0), (-1,-1), 5),
            ("BOTTOMPADDING",(0,0), (-1,-1), 5),
            ("VALIGN",       (0,0), (-1,-1), "MIDDLE"),
        ])

    # ── Story ────────────────────────────────────────────────────────────────
    story = []
    rnd   = result.get("rounding_decimals", 7)
    calc_ts = created_at.strftime("%d-%b-%Y %H:%M:%S")

    method_color_map = {
        "Compounded in Arrears":     c_green,
        "Simple Average in Arrears": c_purple,
        "SOFR Index":                c_amber,
    }
    method = result.get("calculation_method", "—")
    m_color = method_color_map.get(method, c_teal)

    # ── Header banner ────────────────────────────────────────────────────────
    hdr_data = [[
        Paragraph("SOFR Interest Report", s_title),
        Paragraph(f"Generated: {calc_ts}", s_sub),
    ]]
    hdr_tbl = Table(hdr_data, colWidths=[W*0.65, W*0.35])
    hdr_tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,-1), c_navy),
        ("VALIGN",       (0,0), (-1,-1), "MIDDLE"),
        ("LEFTPADDING",  (0,0), (-1,-1), 14),
        ("RIGHTPADDING", (0,0), (-1,-1), 14),
        ("TOPPADDING",   (0,0), (-1,-1), 14),
        ("BOTTOMPADDING",(0,0), (-1,-1), 14),
        ("ALIGN",        (1,0), (1,0),   "RIGHT"),
    ]))
    story.append(hdr_tbl)
    story.append(Spacer(1, 8))

    # ── Deal information ─────────────────────────────────────────────────────
    def sect_header(label: str, color=c_teal) -> Table:
        t = Table([[Paragraph(label.upper(), s_sect)]],
                  colWidths=[W])
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,-1), color),
            ("LEFTPADDING",   (0,0), (-1,-1), 10),
            ("TOPPADDING",    (0,0), (-1,-1), 6),
            ("BOTTOMPADDING", (0,0), (-1,-1), 6),
        ]))
        return t

    story.append(sect_header("Deal Information", c_navy))
    story.append(Spacer(1, 4))

    deal_rows = [
        ["CUSIP",        result.get("cusip","—"),
         "Deal Name",    result.get("deal_name","—")],
        ["Client",       result.get("client_name","—"),
         "Rate Type",    result.get("rate_type","—")],
        ["Notional",     _fmt_money(result.get("notional_amount")),
         "Frequency",    result.get("payment_frequency","—")],
        ["Spread",       f"{float(result.get('spread', 0) or 0):.4f}%",
         "All-in Ann.",  _fmt_rate(result.get("annualized_rate"), 7)],
        ["Calc Method",  method,
         "Obs Shift",    result.get("observation_shift","—")],
        ["Shifted Int",  result.get("shifted_interest","—"),
         "Look Back",    f"{result.get('look_back_days','—')}d"],
        ["Stored Basis", result.get("accrual_day_basis","Calendar Days"),
         "Accrual Days Used",  str(result.get("accrual_days","—"))],
        ["Pay Delay",    result.get("payment_delay_flag", "—"),
         "Rounding",     f"{rnd} decimals"],
    ]

    cw = [W*0.18, W*0.32, W*0.18, W*0.32]
    deal_tbl = Table(deal_rows, colWidths=cw)
    tst = detail_tbl_style()
    # Shade label columns
    for row_idx in range(len(deal_rows)):
        tst.add("TEXTCOLOR",   (0, row_idx), (0, row_idx), c_grey)
        tst.add("TEXTCOLOR",   (2, row_idx), (2, row_idx), c_grey)
        tst.add("FONTNAME",    (0, row_idx), (0, row_idx), "Helvetica")
        tst.add("FONTNAME",    (2, row_idx), (2, row_idx), "Helvetica")
    deal_tbl.setStyle(tst)
    story.append(deal_tbl)
    story.append(Spacer(1, 10))

    # ── Period and observation dates ─────────────────────────────────────────
    story.append(sect_header("Interest Period & Observation Dates", c_teal))
    story.append(Spacer(1, 4))

    period_rows = [
        ["Period Start",      _fmt_date(result.get("period_start_date")),
         "Period End",        _fmt_date(result.get("period_end_date"))],
        ["Obs Start (shifted)",_fmt_date(result.get("obs_start_date")),
         "Obs End (shifted)", _fmt_date(result.get("obs_end_date"))],
        ["Interest Period Days", str(result.get("interest_period_days", result.get("accrual_days","—"))),
         "Observation Period Days", str(result.get("observation_period_days","—"))],
        ["Accrual Days Used", str(result.get("accrual_days","—")),
         "Day Count",         f"ACT/{result.get('day_count_basis', 360)}"],
        ["Payment Date",      _fmt_date(result.get("payment_date")),
         "Adj Payment Date",  _fmt_date(result.get("adjusted_payment_date"))],
        ["Payment Delay Days",str(result.get("payment_delay_days",0)),
         "Calc Timestamp",    calc_ts],
    ]

    per_tbl = Table(period_rows, colWidths=cw)
    pts = detail_tbl_style()
    for row_idx in range(len(period_rows)):
        pts.add("TEXTCOLOR", (0, row_idx), (0, row_idx), c_grey)
        pts.add("TEXTCOLOR", (2, row_idx), (2, row_idx), c_grey)
        pts.add("FONTNAME",  (0, row_idx), (0, row_idx), "Helvetica")
        pts.add("FONTNAME",  (2, row_idx), (2, row_idx), "Helvetica")
    per_tbl.setStyle(pts)
    story.append(per_tbl)
    story.append(Spacer(1, 10))

    # ── Calculation results ───────────────────────────────────────────────────
    story.append(sect_header("Calculation Results", m_color))
    story.append(Spacer(1, 4))

    # Large result metrics in a 4-column grid
    metrics = [
        ("Compounded / Avg Rate", _fmt_rate(result.get("compounded_rate"), 7), c_green),
        ("Annualized Rate",       _fmt_rate(result.get("annualized_rate"), 7), c_navy),
        ("Rounded Rate",          _fmt_rate(result.get("rounded_rate"),    rnd), c_purple),
        ("Interest Amount",       _fmt_money(result.get("interest_amount")),     c_green),
    ]

    metric_rows = []
    for label, value, color in metrics:
        value_font_size = _metric_font_size(value)
        metric_rows.append(
            Table(
                [[Paragraph(label.upper(), s_label)],
                 [Paragraph(value, ParagraphStyle(
                     "mv", parent=base["Normal"],
                     fontSize=value_font_size, leading=value_font_size + 2,
                     fontName="Helvetica-Bold",
                     textColor=color))]],
                colWidths=[W/4 - 3*mm],
            )
        )

    for t in [m for m in metric_rows]:
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,-1), c_lgrey),
            ("LEFTPADDING",   (0,0), (-1,-1), 10),
            ("TOPPADDING",    (0,0), (-1,-1), 8),
            ("BOTTOMPADDING", (0,0), (-1,-1), 10),
            ("ROUNDEDCORNERS",(0,0), (-1,-1), [4,4,4,4]),
        ]))

    metric_tbl = Table(
        [[metric_rows[0], metric_rows[1], metric_rows[2], metric_rows[3]]],
        colWidths=[W/4]*4,
        spaceBefore=0,
    )
    metric_tbl.setStyle(TableStyle([
        ("VALIGN",      (0,0), (-1,-1), "TOP"),
        ("LEFTPADDING", (0,0), (-1,-1), 3),
        ("RIGHTPADDING",(0,0), (-1,-1), 3),
    ]))
    story.append(metric_tbl)
    story.append(Spacer(1, 10))

    # ── Formula note ─────────────────────────────────────────────────────────
    formula_map = {
        "Compounded in Arrears":
            "Formula: PRODUCT(1 + r_i x d_i / DC) - 1  |  "
            "Daily rates looked up at observation dates "
            f"(period date - {result.get('look_back_days','?')} lookback days)",
        "Simple Average in Arrears":
            "Formula: SUM(r_i x d_i) / SUM(d_i)  |  "
            "Weighted daily average of SOFR over observation period",
        "SOFR Index":
            "Formula: (Index_End / Index_Start - 1) x (DC / Accrual Days)  |  "
            "SOFR Index ratio at shifted observation dates",
    }
    formula = formula_map.get(method, "")
    if result.get("spread"):
        formula += f"  |  Spread added to annualized rate: {float(result.get('spread') or 0):.4f}%"
    if formula:
        formula_tbl = Table(
            [[Paragraph(formula, s_mono)]],
            colWidths=[W],
        )
        formula_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0,0), (-1,-1), HexColor("#FFFDE7")),
            ("LEFTPADDING",   (0,0), (-1,-1), 10),
            ("TOPPADDING",    (0,0), (-1,-1), 8),
            ("BOTTOMPADDING", (0,0), (-1,-1), 8),
            ("BOX",           (0,0), (-1,-1), 0.5, c_amber),
        ]))
        story.append(formula_tbl)
        story.append(Spacer(1, 10))

    # ── Detailed supporting table ────────────────────────────────────────────
    daily_rows = result.get("daily_rows") or []
    if method == "SOFR Index":
        story.append(sect_header("Index Values Used", c_amber))
        story.append(Spacer(1, 4))
        idx_hdr = ["", "Date", "SOFR Rate (%)", "SOFR Index Value"]
        idx_rows = [
            idx_hdr,
            ["Observation Start",
             _fmt_date(result.get("obs_start_date")),
             _fmt_pct_value(daily_rows[0].get("sofr_rate")) if daily_rows else "—",
             f"{result.get('index_start', '—')}"],
            ["Observation End",
             _fmt_date(result.get("obs_end_date")),
             _fmt_pct_value(daily_rows[1].get("sofr_rate")) if len(daily_rows) > 1 else "—",
             f"{result.get('index_end', '—')}"],
        ]
        idx_tbl = Table(idx_rows, colWidths=[W*0.24, W*0.26, W*0.20, W*0.30], repeatRows=1)
        idx_tbl.setStyle(detail_tbl_style())
        story.append(idx_tbl)
        story.append(Spacer(1, 10))
    elif daily_rows:
        if method == "Compounded in Arrears":
            story.append(sect_header("Daily Compounding Details", c_green))
            story.append(Spacer(1, 4))
            hdr = ["Period Date", "Obs Date", "SOFR Rate", "Day Wt",
                   "Daily Factor", "Running Product"]
            rows = [
                hdr,
                *[
                    [
                        _fmt_date(row.get("date")),
                        _fmt_date(row.get("obs_date")),
                        _fmt_pct_value(row.get("sofr_rate")),
                        str(row.get("day_weight", "—")),
                        f"{float(row.get('daily_factor', 0)):.12f}",
                        f"{float(row.get('running_product', 0)):.12f}",
                    ]
                    for row in daily_rows
                ]
            ]
            tbl = Table(
                rows,
                colWidths=[W*0.16, W*0.16, W*0.14, W*0.10, W*0.20, W*0.24],
                repeatRows=1
            )
        else:
            story.append(sect_header("Daily Rate Details", c_purple))
            story.append(Spacer(1, 4))
            hdr = ["Period Date", "Obs Date", "SOFR Rate", "Day Wt",
                   "Business Day", "Weighted Rate"]
            rows = [
                hdr,
                *[
                    [
                        _fmt_date(row.get("date")),
                        _fmt_date(row.get("obs_date")),
                        _fmt_pct_value(row.get("sofr_rate")),
                        str(row.get("day_weight", "—")),
                        "Yes" if row.get("is_business_day", True) else "No",
                        _fmt_pct_value(row.get("weighted_rate")),
                    ]
                    for row in daily_rows
                ]
            ]
            tbl = Table(
                rows,
                colWidths=[W*0.18, W*0.18, W*0.16, W*0.10, W*0.14, W*0.24],
                repeatRows=1
            )

        tbl.setStyle(detail_tbl_style())
        story.append(tbl)
        story.append(Spacer(1, 6))
        story.append(Paragraph(
            "GF = Good Friday uses Thursday's rate if available, otherwise Wednesday's.",
            s_footer
        ))
        story.append(Spacer(1, 10))

    # ── Disclaimer / footer strip ────────────────────────────────────────────
    story.append(HRFlowable(width=W, thickness=0.5,
                            color=c_lgrey, spaceAfter=6))
    footer_text = (
        "This report is generated by the SOFR Interest Calculator application for "
        "informational purposes only. All calculations use daily SOFR rates sourced "
        "from the NY Federal Reserve Bank. ACT/360 day count convention applied unless "
        "otherwise specified. Results should be independently verified before use in "
        "any financial transaction."
    )
    story.append(Paragraph(footer_text, s_footer))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        f"SOFR Interest Calculator  |  Report ID: {cusip}_{ts}  |  {calc_ts}",
        s_footer
    ))

    # ── Build ────────────────────────────────────────────────────────────────
    doc.build(story)
    return fpath


def generate_batch_pdf(results: list[dict],
                       batch_id: str,
                       period_start,
                       period_end,
                       output_dir: Optional[Path] = None) -> Path:
    """
    Generate a summary PDF for a batch calculation run.
    """
    try:
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib.units import cm, mm
        from reportlab.lib.colors import Color, HexColor
        from reportlab.platypus import (
            SimpleDocTemplate, Paragraph, Spacer, Table,
            TableStyle, HRFlowable
        )
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    except ImportError:
        raise ImportError("pip install reportlab")

    out_dir = output_dir or PDF_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    ts    = datetime.now().strftime("%Y%m%d_%H%M%S")
    fpath = out_dir / f"SOFR_Batch_{batch_id[:8]}_{ts}.pdf"

    doc = SimpleDocTemplate(
        str(fpath), pagesize=landscape(A4),
        leftMargin=1.5*cm, rightMargin=1.5*cm,
        topMargin=1.5*cm, bottomMargin=1.5*cm,
        title=f"SOFR Batch Calculation — {batch_id[:8]}",
    )

    W = landscape(A4)[0] - 3*cm
    base = getSampleStyleSheet()
    c_navy  = Color(*NAVY)
    c_teal  = Color(*TEAL)
    c_green = Color(*GREEN)
    c_lgrey = Color(*LGREY)
    c_grey  = Color(*GREY)
    c_white = Color(*WHITE)
    c_dark  = Color(*DARK)

    def S(n, **kw): return ParagraphStyle(n, parent=base["Normal"], **kw)

    s_title = S("t", fontSize=18, textColor=c_white,
                fontName="Helvetica-Bold")
    s_sub   = S("s", fontSize=9,  textColor=HexColor("#B5D4F4"),
                fontName="Helvetica")
    s_foot  = S("f", fontSize=7,  textColor=c_grey,
                fontName="Helvetica", alignment=TA_CENTER)
    s_cell  = S("c", fontSize=7.5, textColor=c_dark, fontName="Helvetica")
    s_chdr  = S("ch", fontSize=7.5, textColor=c_white,
                fontName="Helvetica-Bold")

    calc_ts = datetime.now().strftime("%d-%b-%Y %H:%M:%S")
    ok_res  = [r for r in results if r.get("status") == "OK"]
    err_res = [r for r in results if r.get("status") != "OK"]
    total_i = sum(r.get("interest_amount", 0) or 0 for r in ok_res)

    story = []

    # Header
    hdr = Table([[
        Paragraph("SOFR Batch Calculation Report", s_title),
        Table([
            [Paragraph(f"Period: {_fmt_date(period_start)} to {_fmt_date(period_end)}", s_sub)],
            [Paragraph(f"Batch ID: {batch_id[:20]}  |  Generated: {calc_ts}", s_sub)],
        ], colWidths=[W*0.45])
    ]], colWidths=[W*0.45, W*0.55])
    hdr.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,-1), c_navy),
        ("LEFTPADDING",   (0,0), (-1,-1), 12),
        ("TOPPADDING",    (0,0), (-1,-1), 12),
        ("BOTTOMPADDING", (0,0), (-1,-1), 12),
        ("VALIGN",        (0,0), (-1,-1), "MIDDLE"),
    ]))
    story.append(hdr)
    story.append(Spacer(1, 8))

    # Summary row
    summary = Table([[
        Paragraph(f"Total Deals: {len(results)}", s_chdr),
        Paragraph(f"Successful: {len(ok_res)}", s_chdr),
        Paragraph(f"Errors: {len(err_res)}", s_chdr),
        Paragraph(f"Total Interest: {_fmt_money(total_i)}", s_chdr),
    ]], colWidths=[W/4]*4)
    summary.setStyle(TableStyle([
        ("BACKGROUND",    (0,0), (-1,-1), c_teal),
        ("LEFTPADDING",   (0,0), (-1,-1), 10),
        ("TOPPADDING",    (0,0), (-1,-1), 8),
        ("BOTTOMPADDING", (0,0), (-1,-1), 8),
    ]))
    story.append(summary)
    story.append(Spacer(1, 8))

    # Results table
    headers = ["CUSIP","Deal Name","Client","Method",
               "Accrual\nDays","Comp/Avg\nRate","Ann Rate",
               "Interest Amount","Adj Pay Date","Status"]
    cw = [W*0.07, W*0.16, W*0.10, W*0.14,
          W*0.05, W*0.08, W*0.08,
          W*0.11, W*0.09, W*0.06]
    cw = cw + [W - sum(cw)] if sum(cw) < W else cw

    tbl_data = [[Paragraph(h, s_chdr) for h in headers]]
    for r in results:
        rnd = r.get("rounding_decimals") or 7
        row_color = HexColor("#FEF2F2") if r.get("status") != "OK" else None
        tbl_data.append([
            Paragraph(r.get("cusip",""), s_cell),
            Paragraph(r.get("deal_name","")[:28], s_cell),
            Paragraph(r.get("client_name","")[:18], s_cell),
            Paragraph(r.get("calculation_method","")[:22], s_cell),
            Paragraph(str(r.get("accrual_days","—")), s_cell),
            Paragraph(_fmt_rate(r.get("compounded_rate"), 7), s_cell),
            Paragraph(_fmt_rate(r.get("annualized_rate"), 7), s_cell),
            Paragraph(_fmt_money(r.get("interest_amount")), s_cell),
            Paragraph(_fmt_date(r.get("adjusted_payment_date")), s_cell),
            Paragraph(r.get("status","—"), s_cell),
        ])

    tbl = Table(tbl_data, colWidths=cw[:len(headers)], repeatRows=1)
    ts2 = TableStyle([
        ("BACKGROUND",     (0,0), (-1,0),  c_navy),
        ("FONTSIZE",       (0,0), (-1,-1), 7.5),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [c_white, c_lgrey]),
        ("GRID",           (0,0), (-1,-1), 0.3, c_lgrey),
        ("LEFTPADDING",    (0,0), (-1,-1), 5),
        ("RIGHTPADDING",   (0,0), (-1,-1), 5),
        ("TOPPADDING",     (0,0), (-1,-1), 4),
        ("BOTTOMPADDING",  (0,0), (-1,-1), 4),
        ("VALIGN",         (0,0), (-1,-1), "MIDDLE"),
    ])
    tbl.setStyle(ts2)
    story.append(tbl)
    story.append(Spacer(1, 8))
    story.append(HRFlowable(width=W, thickness=0.5, color=c_lgrey))
    story.append(Spacer(1, 4))
    story.append(Paragraph(
        f"SOFR Interest Calculator  |  Batch ID: {batch_id}  |  {calc_ts}",
        s_foot
    ))

    doc.build(story)
    return fpath
