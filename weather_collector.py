import requests
import json
import csv
from datetime import datetime
from pathlib import Path

URL = "https://www.ksndmc.org/default.aspx/DailyReport/getLast15MinutesWeather?drpVal=29"
HEADERS = {
    "accept": "application/json, text/javascript, */*; q=0.01",
    "content-type": "application/json; charset=utf-8",
    "x-requested-with": "XMLHttpRequest",
}

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

RAW_LOG = DATA_DIR / "raw_api_log.jsonl"
HISTORY = DATA_DIR / "temperature_history.csv"
DAILY = DATA_DIR / "daily_summary.csv"


def fetch_data():
    r = requests.post(URL, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return json.loads(r.text)


def append_raw(payload):
    with RAW_LOG.open("a") as f:
        f.write(json.dumps({
            "fetched_at": datetime.utcnow().isoformat(),
            "payload": payload
        }) + "\n")


def append_history(rows):
    file_exists = HISTORY.exists()
    with HISTORY.open("a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["date", "time", "district", "taluk", "station", "temperature"])
        for r in rows:
            dt = datetime.fromisoformat(r["RECORDED_DATE"])
            writer.writerow([
                dt.date(),
                r["RECORDED_TIME"],
                r["DISTRICT"],
                r["TALUKNAME"],
                r["STATION_NAME"],
                r["TEMPERATURE"]
            ])


def recompute_daily():
    records = {}
    with HISTORY.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["date"], row["station"])
            temp = float(row["temperature"])
            if key not in records:
                records[key] = [temp, temp]
            else:
                records[key][0] = max(records[key][0], temp)
                records[key][1] = min(records[key][1], temp)

    with DAILY.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "town", "max_temperature", "min_temperature"])
        for (date, town), (mx, mn) in records.items():
            writer.writerow([date, town, mx, mn])


def main():
    payload = fetch_data()
    append_raw(payload)
    append_history(payload)
    recompute_daily()


if __name__ == "__main__":
    main()
