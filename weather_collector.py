import requests
import json
import csv
from datetime import datetime, timezone
from pathlib import Path

# =======================
# CONFIG
# =======================

URL = "https://www.ksndmc.org/default.aspx/DailyReport/getLast15MinutesWeather?drpVal=29"

HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "content-type": "application/json; charset=utf-8",
    "x-requested-with": "XMLHttpRequest",
}

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

RAW_LOG_FILE = DATA_DIR / "raw_api_log.jsonl"
HISTORY_FILE = DATA_DIR / "temperature_history.csv"
DAILY_SUMMARY_FILE = DATA_DIR / "daily_summary.csv"

# =======================
# FETCH & PARSE
# =======================

def fetch_weather_data():
    response = requests.post(URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    # API returns JSON string inside JSON → decode twice
    outer = json.loads(response.text)
    payload = json.loads(outer)

    if not isinstance(payload, list):
        raise ValueError("Parsed payload is not a list")

    return payload


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
# HISTORICAL CSV STORAGE
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
# DAILY MIN / MAX REPORT
# =======================

def recompute_daily_summary():
    aggregates = {}

    with HISTORY_FILE.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            key = (row["date"], row["station"])
            temp = float(row["temperature"])

            if key not in aggregates:
                aggregates[key] = {
                    "max": temp,
                    "min": temp
                }
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
# MAIN
# =======================

def main():
    payload = fetch_weather_data()

    append_raw_log(payload)
    append_temperature_history(payload)
    recompute_daily_summary()

    print(f"Run successful @ {datetime.now().isoformat()}")
    print(f"Records fetched: {len(payload)}")


if __name__ == "__main__":
    main()
