import os
from datetime import date
import pandas as pd
from trendspy import Trends
from google.cloud import bigquery
import gspread
from google.oauth2.service_account import Credentials
from flask import Flask, jsonify, make_response

# Initialize Flask app

app = Flask(__name__)

# Core pipeline logic
def fetch_and_store():
     project_id    = os.environ["PROJECT_ID"]
     dataset_id    = os.environ["DATASET"]
     table_id      = os.environ["TABLE"]
     locale        = os.environ.get("LOCALE", "US")
     today_str     = date.today().isoformat()
     region        = "US"

     SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
     SHEET_NAME     = os.environ.get("SHEET_NAME", "Sheet1")

     # Fetch trending
     tr = Trends()
     try:
         trend_list = tr.trending_now(geo=locale)
     except Exception as e:
         return {"status": "error", "message": f"Failed fetch: {e}"}, 500

     rows = []
     for t in trend_list:
         vol = t.volume
         if isinstance(vol, str):
             vol = int(vol.replace(",", ""))
         rows.append({
             "trend_date": today_str,
             "keyword": t.keyword,
             "value": vol
         })
     df = pd.DataFrame(rows)
     if df.empty:
         return {"status": "warning", "message": "No trends fetched."}, 200

     # BigQuery insert
     bq = bigquery.Client(project=project_id, location=region)
     table_ref = bq.dataset(dataset_id).table(table_id)
     bq.get_dataset(f"{project_id}.{dataset_id}")
     errors = bq.insert_rows_json(table_ref, df.to_dict(orient="records"))
     if errors:
         return {"status": "error", "message": f"BQ insert errors: {errors}"}, 500

     # Google Sheets update
     creds = Credentials.from_service_account_file(
         os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
         scopes=["https://www.googleapis.com/auth/spreadsheets"]
     )
     gc = gspread.authorize(creds)
     sheet = gc.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)
     header = ["trend_date", "keyword", "value"]
     values = [header] + df[header].values.tolist()
     sheet.clear()
     sheet.update(values)

     return {"status": "success", "inserted_rows": len(df)}, 200

# Flask route to trigger pipeline

@app.route("/", methods=["GET"])

def trigger():
    result, status = fetch_and_store()
    response = make_response(jsonify(result), status)
    return response

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
