import requests
import json
import csv
import os
from datetime import datetime, timezone
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =======================
# CONFIG
# =======================

URL = "https://www.ksndmc.org/default.aspx/DailyReport/getLast15MinutesWeather?drpVal=29"

HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "content-type": "application/json; charset=utf-8",
    "x-requested-with": "XMLHttpRequest",
}

# =======================
# PATH HANDLING (CRITICAL FIX)
# =======================

# In GitHub Actions → GITHUB_WORKSPACE is repo root
# Locally → fallback to current working directory
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", Path.cwd()))

DATA_DIR = REPO_ROOT / "Report"
DATA_DIR.mkdir(parents=True, exist_ok=True)

RAW_LOG_FILE = DATA_DIR / "raw_api_log.jsonl"
HISTORY_FILE = DATA_DIR / "temperature_history.csv"
DAILY_SUMMARY_FILE = DATA_DIR / "daily_summary.csv"

# =======================
# FETCH WITH RETRIES
# =======================

def fetch_weather_data():
    session = requests.Session()

    retries = Retry(
        total=5,
        backoff_factor=3,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST"]
    )

    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)

    try:
        response = session.post(
            URL,
            headers=HEADERS,
            timeout=(5, 20)  # connect timeout, read timeout
        )
        response.raise_for_status()

        # KSNDMC returns JSON string inside JSON
        outer = json.loads(response.text)
        payload = json.loads(outer)

        if not isinstance(payload, list):
            raise ValueError("Parsed payload is not a list")

        return payload

    except Exception as e:
        print("⚠️ KSNDMC API unreachable or slow:", str(e))
        return None


# =======================
# RAW LOG STORAGE
# =======================

def append_raw_log(payload):
    record = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "records": payload
    }

    with RAW_LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


# =======================
# HISTORICAL STORAGE
# =======================

def append_temperature_history(payload):
    file_exists = HISTORY_FILE.exists()

    with HISTORY_FILE.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if not file_exists:
            writer.writerow([
                "date",
                "time",
                "district",
                "taluk",
                "station",
                "temperature"
            ])

        for r in payload:
            recorded_dt = datetime.fromisoformat(r["RECORDED_DATE"])

            writer.writerow([
                recorded_dt.date().isoformat(),
                r["RECORDED_TIME"],
                r["DISTRICT"],
                r["TALUKNAME"],
                r["STATION_NAME"],
                r["TEMPERATURE"]
            ])


# =======================
# DAILY MIN / MAX
# =======================

def recompute_daily_summary():
    if not HISTORY_FILE.exists():
        return

    aggregates = {}

    with HISTORY_FILE.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            key = (row["date"], row["station"])
            temp = float(row["temperature"])

            if key not in aggregates:
                aggregates[key] = {"max": temp, "min": temp}
            else:
                aggregates[key]["max"] = max(aggregates[key]["max"], temp)
                aggregates[key]["min"] = min(aggregates[key]["min"], temp)

    with DAILY_SUMMARY_FILE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "date",
            "town",
            "max_temperature",
            "min_temperature"
        ])

        for (date, town), values in sorted(aggregates.items()):
            writer.writerow([
                date,
                town,
                values["max"],
                values["min"]
            ])


# =======================
# MAIN (FAIL-SAFE)
# =======================

def main():
    print(f"Repo root  : {REPO_ROOT}")
    print(f"Report dir: {DATA_DIR}")

    payload = fetch_weather_data()

    if payload is None:
        print("Skipping this run due to API failure")
        return  # Do NOT fail the workflow

    append_raw_log(payload)
    append_temperature_history(payload)
    recompute_daily_summary()

    print(f"Run successful @ {datetime.now(timezone.utc).isoformat()}")
    print(f"Records fetched: {len(payload)}")


if __name__ == "__main__":
    main()
