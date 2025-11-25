import os
import io
import zipfile
import calendar
from datetime import date, datetime

import pandas as pd
import streamlit as st
import gspread
from google.oauth2 import service_account

# ---------- GLOBAL CONFIG ----------
APP_TITLE = "Petty Cash Manager"

ATTACH_DIR = "attachments"
os.makedirs(ATTACH_DIR, exist_ok=True)

COLUMNS = [
    "date", "remark", "category", "mode",
    "cash_in", "cash_out", "attachment_path"
]

FOOD_KEYWORDS = ["breakfast", "lunch", "dinner", "food", "meal", "snacks"]

MONTH_NAMES = {
    1: "January", 2: "February", 3: "March",
    4: "April", 5: "May", 6: "June",
    7: "July", 8: "August", 9: "September",
    10: "October", 11: "November", 12: "December"
}

BASE_CATEGORIES = [
    "Office petty cash",
    "Tea break",
    "Labour Charges",
    "Stationeries",
    "Salary",
    "Water can",
    "Courier services",
    "Food",
    "Other",
]

# ---------- SMALL HELPERS ----------

def get_previous_period(year: int, month: int):
    if month == 1:
        return year - 1, 12
    return year, month - 1


def filter_month_df(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """Filter the full dataframe to only rows in the given year+month."""
    if df.empty:
        return df.copy()
    df = df.copy()
    dt = pd.to_datetime(df["date"], errors="coerce")
    mask = (dt.dt.year == year) & (dt.dt.month == month)
    return df[mask]


# ---------- GOOGLE SHEETS BACKEND ----------

def get_gspread_client():
    """Create a gspread client using service account info from st.secrets['gdrive']."""
    if "gdrive" not in st.secrets:
        st.error("Missing [gdrive] configuration in Streamlit secrets.")
        st.stop()

    info = st.secrets["gdrive"]
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc


def get_worksheet():
    """
    Open the spreadsheet (by URL or id in secrets) and get/create
    a worksheet named 'data' with the correct header row.
    """
    info = st.secrets["gdrive"]
    spreadsheet_url = info.get("spreadsheet_url", "").strip()
    spreadsheet_id = info.get("spreadsheet_id", "").strip()

    if not spreadsheet_url and not spreadsheet_id:
        st.error("Missing spreadsheet_url or spreadsheet_id in [gdrive] secrets.")
        st.stop()

    gc = get_gspread_client()

    try:
        if spreadsheet_url:
            sh = gc.open_by_url(spreadsheet_url)
        else:
            sh = gc.open_by_key(spreadsheet_id)
    except Exception as e:
        st.error(
            "Could not open the Google Sheet.\n\n"
            "Please check:\n"
            "1) The spreadsheet URL/ID is correct.\n"
            "2) The sheet is shared with this service account as Editor:\n"
            f"   {st.secrets['gdrive'].get('client_email', '(service account email)')}\n\n"
            f"Technical details: {e}"
        )
        st.stop()

    try:
        ws = sh.worksheet("data")
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="data", rows="1000", cols="10")
        ws.append_row(COLUMNS)
    return ws


def load_all_data_from_sheet() -> pd.DataFrame:
    """Load ALL rows from 'data' worksheet into a DataFrame."""
    ws = get_worksheet()
    records = ws.get_all_records()  # uses first row as header
    if not records:
        df = pd.DataFrame(columns=COLUMNS)
    else:
        df = pd.DataFrame(records)

    for col in COLUMNS:
        if col not in df.columns:
            df[col] = None

    df["date"] = df["date"].astype(str)
    df.loc[df["date"].isin(["NaT", "nan", "None"]), "date"] = ""

    df["cash_in"] = pd.to_numeric(df.get("cash_in", 0), errors="coerce").fillna(0.0)
    df["cash_out"] = pd.to_numeric(df.get("cash_out", 0), errors="coerce").fillna(0.0)

    df["remark"] = df["remark"].astype(str).fillna("")
    df["category"] = df["category"].astype(str).fillna("Other")
    df["mode"] = df["mode"].astype(str).fillna("Cash")
    df["attachment_path"] = df["attachment_path"].astype(str).fillna("")

    return df


def save_all_data_to_sheet(df_all: pd.DataFrame):
    """Overwrite entire 'data' worksheet with df_all (header + rows)."""
    ws = get_worksheet()
    ws.clear()
    ws.append_row(COLUMNS)
    if not df_all.empty:
        rows = df_all[COLUMNS].astype(str).values.tolist()
        ws.append_rows(rows, value_input_option="USER_ENTERED")


def append_row_to_sheet(row: dict):
    """Append a single row (dict) to the sheet."""
    ws = get_worksheet()
    values = [row.get(col, "") for col in COLUMNS]
    ws.append_row(values, value_input_option="USER_ENTERED")


def delete_month_from_sheet(year: int, month: int):
    """
    Delete all rows in the Google Sheet that belong to the given year+month.
    Keeps other months untouched.
    """
    df_all_latest = load_all_data_from_sheet()
    if df_all_latest.empty:
        return

    dt_all = pd.to_datetime(df_all_latest["date"], errors="coerce")
    mask_month = (dt_all.dt.year == year) & (dt_all.dt.month == month)

    # Keep everything that is NOT this month
    df_kept = df_all_latest[~mask_month].copy()

    # Save back to Google Sheet
    save_all_data_to_sheet(df_kept)


# ---------- CASHBOOK LOGIC ----------

def compute_cashbook(df: pd.DataFrame, opening_balance: float = 0.0):
    """Compute totals and running balance for a month DataFrame."""
    if df.empty:
        return df.copy(), 0.0, 0.0, opening_balance

    df = df.copy()
    df["date_ts"] = pd.to_datetime(df["date"], errors="coerce")
    df["cash_in"] = pd.to_numeric(df["cash_in"], errors="coerce").fillna(0.0)
    df["cash_out"] = pd.to_numeric(df["cash_out"], errors="coerce").fillna(0.0)

    df = df.sort_values("date_ts")

    total_in = df["cash_in"].sum()
    total_out = df["cash_out"].sum()

    df["Balance"] = opening_balance + (df["cash_in"].cumsum() - df["cash_out"].cumsum())
    final_balance = opening_balance + total_in - total_out

    df["Date"] = df["date_ts"].dt.strftime("%d %b %y")

    return df, total_in, total_out, final_balance


def normalize_imported_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    col_map = {
        "date": "date",
        "remark": "remark",
        "narration": "remark",
        "description": "remark",
        "category": "category",
        "mode": "mode",
        "payment mode": "mode",
        "cash in": "cash_in",
        "cash_in": "cash_in",
        "cashin": "cash_in",
        "cash out": "cash_out",
        "cash_out": "cash_out",
        "cashout": "cash_out",
        "attachment_path": "attachment_path",
        "attachment": "attachment_path",
        "file": "attachment_path",
    }

    df = pd.DataFrame(columns=COLUMNS)

    for col in raw_df.columns:
        lc = col.strip().lower()
        if lc in col_map:
            target = col_map[lc]
            df[target] = raw_df[col]

    for col in COLUMNS:
        if col not in df.columns:
            df[col] = None

    dt = pd.to_datetime(df["date"], errors="coerce")
    df["date"] = dt.dt.date.astype(str)
    df.loc[df["date"] == "NaT", "date"] = ""

    df["cash_in"] = pd.to_numeric(df["cash_in"], errors="coerce").fillna(0.0)
    df["cash_out"] = pd.to_numeric(df["cash_out"], errors="coerce").fillna(0.0)
    df["remark"] = df["remark"].astype(str).fillna("")
    df["category"] = df["category"].astype(str).fillna("Other")
    df["mode"] = df["mode"].astype(str).fillna("Cash")
    df["attachment_path"] = df["attachment_path"].astype(str).fillna("")

    return df


# ---------- STREAMLIT UI SETUP ----------

st.set_page_config(page_title=APP_TITLE, layout="wide")
st.title(APP_TITLE)
generated_on = datetime.now().strftime("%d %b %Y, %I:%M %p")
st.caption(f"Generated on {generated_on}")

# ---------- SIDEBAR: PERIOD ----------
with st.sidebar:
    st.header("Period")
    current_year = datetime.now().year
    year = st.number_input("Year", min_value=2020, max_value=2100, value=current_year, step=1)
    month = st.selectbox(
        "Month",
        list(MONTH_NAMES.keys()),
        format_func=lambda m: MONTH_NAMES[m],
        index=datetime.now().month - 1,
    )

    year = int(year)
    month = int(month)
    month_name = MONTH_NAMES[month]

    days_in_month = calendar.monthrange(year, month)[1]
    start_of_month = date(year, month, 1)
    end_of_month = date(year, month, days_in_month)

    st.caption(
        f"{month_name} {year}\n"
        f"Duration: {start_of_month.strftime('%d %b %Y')} - {end_of_month.strftime('%d %b %Y')}"
    )

# ---------- LOAD ALL DATA FROM SHEET ----------
df_all = load_all_data_from_sheet()

# ---------- OPENING BALANCE ----------
prev_year, prev_month = get_previous_period(year, month)
df_prev_month = filter_month_df(df_all, prev_year, prev_month)
prev_cb, _, _, prev_final_balance = compute_cashbook(df_prev_month, opening_balance=0.0)
auto_opening = float(prev_final_balance)

with st.sidebar:
    st.subheader("Opening balance")
    opening_balance = st.number_input(
        "Carried forward",
        value=auto_opening,
        step=100.0,
        help=f"Auto from closing balance of {MONTH_NAMES[prev_month]} {prev_year}. You can edit.",
    )

# ---------- CURRENT MONTH DATA ----------
df_raw = filter_month_df(df_all, year, month)
if not df_raw.empty:
    previous_remarks = sorted(set(r for r in df_raw["remark"].unique() if r.strip()))
    previous_categories = sorted(set(c for c in df_raw["category"].unique() if c.strip()))
else:
    previous_remarks = []
    previous_categories = []

cashbook_df, total_in, total_out, final_balance = compute_cashbook(df_raw, opening_balance)

# ---------- SUMMARY ----------
st.subheader(f"{month_name} {year} — Summary")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Cash in", f"{total_in:,.0f}")
c2.metric("Total Cash out", f"{total_out:,.0f}")
c3.metric("Final Balance", f"{final_balance:,.0f}")
c4.metric("Entries", len(df_raw))

st.markdown("---")

# Danger zone: reset this month only
with st.expander("Danger zone: reset this month", expanded=False):
    st.write(
        f"This will **permanently delete** all entries for "
        f"**{month_name} {year}** from Google Sheets. "
        "Other months will not be touched."
    )
    confirm_reset = st.checkbox(
        f"I understand, delete all entries for {month_name} {year}",
        key="confirm_reset_month",
    )
    if st.button("Delete ALL entries for this month", type="primary"):
        if not confirm_reset:
            st.warning("Tick the confirmation checkbox first.")
        else:
            delete_month_from_sheet(year, month)
            st.success(f"All entries for {month_name} {year} have been deleted.")
            st.rerun()


# ---------- QUICK DUPLICATE ----------
st.subheader("Quick duplicate last entry (this month)")
if df_raw.empty:
    st.caption("No previous entries in this month to duplicate yet.")
else:
    if st.button("Duplicate last entry for today"):
        last = df_raw.copy()
        last["date_ts"] = pd.to_datetime(last["date"], errors="coerce")
        last = last.sort_values("date_ts").iloc[-1]

        tx_date = date.today().isoformat()
        new_row = {
            "date": tx_date,
            "remark": last.get("remark", ""),
            "category": last.get("category", "Other"),
            "mode": last.get("mode", "Cash"),
            "cash_in": float(last.get("cash_in", 0) or 0),
            "cash_out": float(last.get("cash_out", 0) or 0),
            "attachment_path": "",
        }
        append_row_to_sheet(new_row)
        st.success("Last entry duplicated for today!")
        st.rerun()

st.markdown("---")

# ---------- ADD NEW TRANSACTION ----------
st.subheader("Add new transaction")

with st.form("add_entry"):
    col1, col2, col3 = st.columns(3)

    with col1:
        tx_date_obj = st.date_input("Date", value=date.today(), key="date_calendar")
        mode = st.selectbox("Mode", ["Cash", "Bank", "UPI"], index=0, key="mode_select")
        tx_type = st.radio("Type", ["Cash in", "Cash out"], index=0, horizontal=True, key="tx_type")

    with col2:
        if previous_remarks:
            remark_suggestion = st.selectbox(
                "Pick previous remark (optional)",
                [""] + previous_remarks,
                index=0,
                key="remark_suggest",
            )
        else:
            remark_suggestion = ""

        default_remark = remark_suggestion or ""
        remark = st.text_input("Remark", value=default_remark, key="remark_input")

        extra_categories = [c for c in previous_categories if c not in BASE_CATEGORIES]
        category_options = BASE_CATEGORIES + extra_categories
        default_cat = "Other" if "Other" in category_options else category_options[0]

        category_select = st.selectbox(
            "Category",
            category_options,
            index=category_options.index(default_cat),
            key="category_select",
        )

    with col3:
        custom_category = st.text_input("Custom category (optional)", key="custom_category")
        amount = st.number_input("Amount", min_value=0.0, step=50.0, key="amount_input")
        attachment_file = st.file_uploader(
    "Bill / screenshot (optional)",
    type=None,  # allow all file types
    key="attachment_uploader",
)

    submitted = st.form_submit_button("Save transaction")

    if submitted:
        if amount <= 0:
            st.error("Amount must be greater than zero.")
        else:
            cash_in = amount if tx_type == "Cash in" else 0.0
            cash_out = amount if tx_type == "Cash out" else 0.0

            attachment_path = ""
            if attachment_file is not None:
                ext = os.path.splitext(attachment_file.name)[1]
                safe_name = f"{tx_date_obj.strftime('%Y%m%d')}_{remark.replace(' ', '_')}{ext}"
                save_path = os.path.join(ATTACH_DIR, safe_name)
                with open(save_path, "wb") as f:
                    f.write(attachment_file.getbuffer())
                attachment_path = save_path

            final_category = category_select
            remark_lower = remark.lower()
            if any(word in remark_lower for word in FOOD_KEYWORDS):
                final_category = "Food"
            if custom_category.strip():
                final_category = custom_category.strip()

            row = {
                "date": tx_date_obj.isoformat(),
                "remark": remark,
                "category": final_category,
                "mode": mode,
                "cash_in": cash_in,
                "cash_out": cash_out,
                "attachment_path": attachment_path,
            }

            append_row_to_sheet(row)
            st.success("Transaction saved successfully!")

            for key in [
                "date_calendar", "remark_suggest", "remark_input",
                "category_select", "custom_category", "mode_select",
                "tx_type", "amount_input", "attachment_uploader",
            ]:
                if key in st.session_state:
                    del st.session_state[key]

            st.rerun()

st.markdown("---")

# ---------- EDIT TABLE ----------
st.subheader("Edit transactions in table (this month only)")
if df_raw.empty:
    st.caption("No transactions to edit yet.")
else:
    df_editable = df_raw.copy()
    dt = pd.to_datetime(df_editable["date"], errors="coerce")
    df_editable["date"] = dt.dt.date

    editable_cols = ["date", "remark", "category", "mode", "cash_in", "cash_out"]

    edited_df = st.data_editor(
        df_editable[editable_cols],
        num_rows="fixed",
        use_container_width=True,
        key=f"summary_editor_{year}_{month}",
        column_config={
            "date": st.column_config.DateColumn("date", format="YYYY-MM-DD"),
        },
    )

    if st.button("Save changes from table"):
        df_all_latest = load_all_data_from_sheet()
        dt_all = pd.to_datetime(df_all_latest["date"], errors="coerce")
        mask_month = (dt_all.dt.year == year) & (dt_all.dt.month == month)
        month_idx = df_all_latest[mask_month].index

        if len(month_idx) != len(edited_df):
            st.error("Row count mismatch while saving. Try reloading the app and editing again.")
        else:
            for col in editable_cols:
                df_all_latest.loc[month_idx, col] = edited_df[col].values

            dt2 = pd.to_datetime(df_all_latest["date"], errors="coerce")
            df_all_latest["date"] = dt2.dt.date.astype(str)
            df_all_latest.loc[df_all_latest["date"] == "NaT", "date"] = ""

            df_all_latest["cash_in"] = pd.to_numeric(df_all_latest["cash_in"], errors="coerce").fillna(0.0)
            df_all_latest["cash_out"] = pd.to_numeric(df_all_latest["cash_out"], errors="coerce").fillna(0.0)
            df_all_latest["remark"] = df_all_latest["remark"].astype(str).fillna("")
            df_all_latest["category"] = df_all_latest["category"].astype(str).fillna("Other")
            df_all_latest["mode"] = df_all_latest["mode"].astype(str).fillna("Cash")

            save_all_data_to_sheet(df_all_latest)
            st.success("Table changes saved successfully!")
            st.rerun()

st.markdown("---")

# ---------- IMPORT ----------
st.subheader("Import data into this month (Google Sheets backend)")
import_file = st.file_uploader(
    "Upload CSV or Excel (.xlsx)",
    type=["csv", "xlsx"],
    key="import_file",
)
replace_existing = st.checkbox(
    "Replace existing data for this month (otherwise append)",
    value=False,
    key="import_replace",
)

if st.button("Import file now"):
    if import_file is None:
        st.warning("Please choose a file to import.")
    else:
        try:
            if import_file.name.lower().endswith(".csv"):
                raw_import_df = pd.read_csv(import_file)
            else:
                raw_import_df = pd.read_excel(import_file)

            norm_df = normalize_imported_dataframe(raw_import_df)

            if norm_df["date"].isna().all():
                st.error("No valid dates found in imported file. Please check the Date column.")
            else:
                df_all_latest = load_all_data_from_sheet()
                dt_all = pd.to_datetime(df_all_latest["date"], errors="coerce")
                mask_month = (dt_all.dt.year == year) & (dt_all.dt.month == month)

                if replace_existing:
                    df_all_latest = df_all_latest[~mask_month]
                    df_all_new = pd.concat([df_all_latest, norm_df], ignore_index=True)
                else:
                    df_all_new = pd.concat([df_all_latest, norm_df], ignore_index=True)

                save_all_data_to_sheet(df_all_new)
                st.success(f"Imported {len(norm_df)} rows into {month_name} {year}.")
                st.rerun()
        except Exception as e:
            st.error(f"Import failed: {e}")

st.markdown("---")

# ---------- CATEGORY SUMMARY ----------
if not df_raw.empty:
    st.subheader("Category-wise summary (this month)")
    df_cat = df_raw.groupby("category", dropna=False).agg(
        Total_Cash_in=("cash_in", "sum"),
        Total_Cash_out=("cash_out", "sum"),
    ).reset_index().sort_values("Total_Cash_out", ascending=False)
    st.dataframe(df_cat, use_container_width=True, hide_index=True)
    st.markdown("---")

# ---------- TRANSACTIONS + FILTERS ----------
st.subheader(f"{month_name} {year} transactions (with running balance)")
if cashbook_df.empty:
    st.caption("No transactions yet for this month.")
else:
    st.markdown("**Filters**")

    fcol1, fcol2, fcol3 = st.columns(3)
    with fcol1:
        filter_from = st.date_input("From date", value=start_of_month, key="filter_from")
    with fcol2:
        filter_to = st.date_input("To date", value=end_of_month, key="filter_to")
    with fcol3:
        remark_filter = st.text_input("Search in remark", key="filter_remark")

    fcol4, fcol5 = st.columns(2)
    with fcol4:
        cat_options = sorted(cashbook_df["category"].astype(str).unique())
        cat_selected = st.multiselect("Category filter", cat_options, default=cat_options, key="filter_category")
    with fcol5:
        mode_options = sorted(cashbook_df["mode"].astype(str).unique())
        mode_selected = st.multiselect("Mode filter", mode_options, default=mode_options, key="filter_mode")

    filtered_df = cashbook_df.copy()
    filtered_df["date_ts"] = pd.to_datetime(filtered_df["date"], errors="coerce")
    if filter_from:
        filtered_df = filtered_df[filtered_df["date_ts"].dt.date >= filter_from]
    if filter_to:
        filtered_df = filtered_df[filtered_df["date_ts"].dt.date <= filter_to]
    if cat_selected:
        filtered_df = filtered_df[filtered_df["category"].isin(cat_selected)]
    if mode_selected:
        filtered_df = filtered_df[filtered_df["mode"].isin(mode_selected)]
    if remark_filter.strip():
        filtered_df = filtered_df[
            filtered_df["remark"].str.contains(remark_filter.strip(), case=False, na=False)
        ]

    if filtered_df.empty:
        st.warning("No records match the current filters.")
    else:
        f_total_in = filtered_df["cash_in"].sum()
        f_total_out = filtered_df["cash_out"].sum()

        st.caption(
            f"Filtered totals — Cash in: {f_total_in:,.0f} | "
            f"Cash out: {f_total_out:,.0f} "
            f"(Full month final balance: {final_balance:,.0f})"
        )

        display_df = filtered_df.copy()
        display_df["Attachment"] = display_df["attachment_path"].apply(
            lambda x: "📎" if isinstance(x, str) and x.strip() else ""
        )

        display_df = display_df[
            ["Date", "remark", "category", "mode", "cash_in", "cash_out", "Balance", "Attachment"]
        ].rename(
            columns={
                "remark": "Remark",
                "category": "Category",
                "mode": "Mode",
                "cash_in": "Cash in",
                "cash_out": "Cash out",
            }
        )

        st.dataframe(display_df, use_container_width=True, hide_index=True)
        st.markdown(f"**Final Balance (full month): {final_balance:,.0f}**")

st.markdown("---")

# ---------- REPORT & EXPORT ----------
if not df_raw.empty:
    st.subheader("Report & export (this month)")

    report_html = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <title>{APP_TITLE} - {month_name} {year}</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                font-size: 12px;
            }}
            h2, h3 {{
                text-align: center;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            th, td {{
                border: 1px solid #000;
                padding: 4px;
                text-align: center;
            }}
            th {{
                background-color: #f0f0f0;
            }}
            .summary-table {{
                margin-bottom: 16px;
            }}
        </style>
    </head>
    <body>
        <h2>{APP_TITLE}</h2>
        <h3>{month_name} {year}</h3>
        <p><strong>Duration:</strong> {start_of_month.strftime('%d %b %Y')} - {end_of_month.strftime('%d %b %Y')}</p>
        <table class="summary-table">
            <tr>
                <th>Total Cash in</th>
                <th>Total Cash out</th>
                <th>Final Balance</th>
                <th>No. of entries</th>
            </tr>
            <tr>
                <td>{total_in:,.0f}</td>
                <td>{total_out:,.0f}</td>
                <td>{final_balance:,.0f}</td>
                <td>{len(df_raw)}</td>
            </tr>
        </table>
        <table>
            <tr>
                <th>Date</th>
                <th>Remark</th>
                <th>Category</th>
                <th>Mode</th>
                <th>Cash in</th>
                <th>Cash out</th>
                <th>Balance</th>
            </tr>
    """

    for _, row in cashbook_df.sort_values("date_ts").iterrows():
        report_html += f"""
            <tr>
                <td>{row['Date']}</td>
                <td>{row['remark']}</td>
                <td>{row['category']}</td>
                <td>{row['mode']}</td>
                <td>{row['cash_in']}</td>
                <td>{row['cash_out']}</td>
                <td>{row['Balance']}</td>
            </tr>
        """

    report_html += """
        </table>
    </body>
    </html>
    """

    report_bytes = report_html.encode("utf-8")
    st.info("To create a PDF: open the HTML file in your browser → Print → Save as PDF.")
    st.download_button(
        label="Download print-friendly report (HTML)",
        data=report_bytes,
        file_name=f"cash_report_{year}_{month:02d}.html",
        mime="text/html",
    )

    export_df = df_raw.copy()
    export_df["attachment_filename"] = export_df["attachment_path"].apply(
        lambda x: os.path.basename(x) if isinstance(x, str) and x.strip() else ""
    )
    df_to_write = export_df.drop(columns=["attachment_path"])

    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
        sheet_name = f"{month_name[:3]}-{str(year)[-2:]}"
        df_to_write.to_excel(writer, sheet_name=sheet_name, index=False)

        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        filename_col_idx = df_to_write.columns.get_loc("attachment_filename")
        for row_num, filename in enumerate(df_to_write["attachment_filename"], start=1):
            if filename:
                link = f"external:attachments/{filename}"
                worksheet.write_url(row_num, filename_col_idx, link, string=str(filename))

        image_extensions = [".png", ".jpg", ".jpeg"]
        image_col_idx = filename_col_idx + 1
        worksheet.write(0, image_col_idx, "Embedded image")

        thumb_row_height = 80
        thumb_scale = 0.2

        for row_num, (path, filename) in enumerate(
            zip(export_df["attachment_path"], export_df["attachment_filename"]),
            start=1,
        ):
            if isinstance(path, str) and path.strip():
                ext = os.path.splitext(path)[1].lower()
                if ext in image_extensions and os.path.exists(path):
                    worksheet.set_row(row_num, thumb_row_height)
                    worksheet.insert_image(
                        row_num,
                        image_col_idx,
                        path,
                        {"x_scale": thumb_scale, "y_scale": thumb_scale},
                    )

    excel_buffer.seek(0)
    st.download_button(
        label="Download this month as Excel (.xlsx)",
        data=excel_buffer,
        file_name=f"cash_{year}_{month:02d}_petty_cash.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    csv_bytes = df_raw.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download this month as CSV",
        data=csv_bytes,
        file_name=f"cash_{year}_{month:02d}_petty_cash.csv",
        mime="text/csv",
    )

    month_attachment_paths = [
        p for p in df_raw.get("attachment_path", []).tolist()
        if isinstance(p, str) and p.strip() and os.path.exists(p)
    ]
    if month_attachment_paths:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            for p in month_attachment_paths:
                zf.write(p, arcname=os.path.basename(p))
        zip_buffer.seek(0)
        st.download_button(
            label=f"Download {month_name} attachments (ZIP)",
            data=zip_buffer,
            file_name=f"attachments_{year}_{month:02d}.zip",
            mime="application/zip",
        )

st.markdown("---")

# ---------- SMS / WHATSAPP SUMMARY ----------
st.subheader("SMS / WhatsApp monthly summary")
summary_text = (
    f"Petty Cash Summary - {month_name} {year}\n"
    f"Opening balance: {opening_balance:,.0f}\n"
    f"Total cash in: {total_in:,.0f}\n"
    f"Total cash out: {total_out:,.0f}\n"
    f"Final balance: {final_balance:,.0f}\n"
    f"Entries: {len(df_raw)}"
)
st.text_area("Copy & paste this text:", value=summary_text, height=120)

