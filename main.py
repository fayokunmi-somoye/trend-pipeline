import os
from datetime import date
import pandas as pd
from trendspy import Trends
from google.cloud import bigquery
import gspread
from google.oauth2.service_account import Credentials

def fetch_and_store():
    # ─── Configuration ─────────────────────────────────────────
    project_id    = os.environ["PROJECT_ID"]
    dataset_id    = os.environ["DATASET"]
    table_id      = os.environ["TABLE"]
    locale        = os.environ.get("LOCALE", "US")
    today_str     = date.today().isoformat()
    region        = "US"  # change if your BQ dataset is elsewhere

    # Google Sheets config
    SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
    SHEET_NAME     = os.environ.get("SHEET_NAME", "Sheet1")

    # ─── 1) Fetch raw trending searches ────────────────────────
    tr = Trends()
    try:
        trend_list = tr.trending_now(geo=locale)
    except Exception as e:
        print(f"❌ failed to fetch trending_now(): {e}")
        return

    # Build DataFrame of raw results
    rows = []
    for t in trend_list:
        vol = t.volume
        if isinstance(vol, str):
            vol = int(vol.replace(",", ""))
        rows.append({
            "trend_date": today_str,
            "keyword":     t.keyword,
            "value":       vol
        })
    df = pd.DataFrame(rows)

    if df.empty:
        print("⚠️ No trends fetched.")
        return

    print("✅ Raw Trending Searches:\n")
    print(df.to_string(index=False))

    # ─── 2) Stream‑insert into BigQuery ──────────────────────
    bq = bigquery.Client(project=project_id, location=region)
    table_ref = bq.dataset(dataset_id).table(table_id)

    # verify dataset exists
    bq.get_dataset(f"{project_id}.{dataset_id}")

    bq_rows = df.to_dict(orient="records")
    errors = bq.insert_rows_json(table_ref, bq_rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
    print(f"✅ Inserted {len(bq_rows)} rows into {project_id}.{dataset_id}.{table_id}")

    # ─── 3) Append to Google Sheet ───────────────────────────
    creds = Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

    # Prepare values: header + data
    header = ["trend_date", "keyword", "value"]
    values = [header] + df[header].values.tolist()

    # Overwrite existing content
    sheet.clear()
    sheet.update(values)
    print(f"✅ Pushed {len(df)} rows to Google Sheet '{SHEET_NAME}'.")

if __name__ == "__main__":
    fetch_and_store()
