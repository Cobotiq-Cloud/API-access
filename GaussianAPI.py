#!/usr/bin/env python3
"""
gausium_task_reports.py
--------------------------------------------------------------------
Retrieve V2 task-reports for a list of robot SNs and re-shape them
into the same column layout as 2025-06.csv.
--------------------------------------------------------------------
Requires:  pip install requests pandas python-dateutil
"""

import os
import time
import json
import math
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from dateutil import tz

# ------------- CONFIG -------------------------------------------------

CLIENT_ID        = "Ij8YFXHbRapgwvNbpoR8Jp7RJv"
CLIENT_SECRET    = "JEegVIfiGmNuUoN6JSuMNfFLrGNUIpJGKTyDO3Erbp68JnxowXg7vmpBPk"
ACCESS_KEY_ID  = "6276595e-4d49-11f0-99de-094b5041c301"   
ACCESS_KEY_SECRET = "57e3438905f8a7c9bd46d8c451515087"    

OPEN_ACCESS_KEY  = os.getenv("OPEN_ACCESS_KEY")          # <-- fill in!
SN_LIST          = [
    "GS438-6160-ACQ-R200",
    "GS401-6120-ACQ-H000",
    "GS142-0230-G1P-P000",
]
DAYS_BACK        = 30                                    # how far to look
PAGE_SIZE        = 200                                   # API max 200
CSV_OUT          = "gausium_reports_last30d.csv"         # set to None to skip

# ------------- CONSTANTS & HELPERS ------------------------------------

API_ROOT   = "https://openapi.gs-robot.com"
UNIT_M2_TO_FT2  = 10.7639
UNIT_L_TO_GAL   = 0.264172

def bearer_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}

def fmt_iso(dt: datetime) -> str:
    """Return an ISO string without microseconds in UTC."""
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def get_token() -> str:
    """Client-Credentials flow: obtain a short-lived access_token."""
    url = f"{API_ROOT}/gas/api/v1alpha1/oauth/token"
    payload = {
        "grant_type": "urn:gaussian:params:oauth:grant-type:open-access-token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "open_access_key": OPEN_ACCESS_KEY,
    }
    r = requests.post(url, json=payload, timeout=15)
    r.raise_for_status()
    return r.json()["access_token"]          # 24 h lifetime per docs:contentReference[oaicite:0]{index=0}

def get_robot_model(token: str, sn: str) -> str:
    """Look-up the model family/type once.  (Keeps the CSV column)."""
    url = f"{API_ROOT}/v1alpha1/robots"
    r = requests.get(url, params={"page":1, "pageSize":500}, headers=bearer_headers(token), timeout=15)
    r.raise_for_status()
    for rob in r.json().get("robots", []):
        if rob["serialNumber"] == sn:
            return rob.get("modelTypeCode") or rob.get("modelFamilyCode")
    return ""

def fetch_task_reports(token: str, sn: str, start_utc: str, end_utc: str) -> list[dict]:
    """Page through the V2 List Robot Task Reports endpoint."""
    out, page = [], 1
    while True:
        url = f"{API_ROOT}/openapi/v2alpha1/robots/{sn}/taskReports"
        q = {
            "page": page,
            "pageSize": PAGE_SIZE,
            "startTimeUtcFloor": start_utc,
            "startTimeUtcUpper": end_utc,
        }
        r = requests.get(url, params=q, headers=bearer_headers(token), timeout=30)
        if r.status_code == 404:      # robot has no reports / wrong SN
            break
        r.raise_for_status()
        data = r.json()
        out.extend(data.get("robotTaskReports", []))
        if page * PAGE_SIZE >= int(data.get("total", 0)):
            break
        page += 1
    return out        # each entry matches the schema in the docs:contentReference[oaicite:1]{index=1}

def seconds_to_hms(sec: int) -> str:
    return time.strftime("%H:%M:%S", time.gmtime(sec))

def transform(report: dict, model: str) -> dict:
    """Map JSON to the CSV schema (units converted)."""
    planned_area_ft2 = (report["plannedCleaningAreaSquareMeter"]
                        * UNIT_M2_TO_FT2 if report.get("plannedCleaningAreaSquareMeter") else None)
    actual_area_ft2  = (report["actualCleaningAreaSquareMeter"]
                        * UNIT_M2_TO_FT2 if report.get("actualCleaningAreaSquareMeter") else None)
    uncleaned_ft2    = (planned_area_ft2 - actual_area_ft2
                        if (planned_area_ft2 and actual_area_ft2) else None)
    return {
        # --- core date/time ---
        "Start DateTime":  report["startTime"],
        "End DateTime":    report["endTime"],
        # --- identifiers ---
        "Map":             ", ".join({st["mapName"] for st in report.get("subTasks", [])}) or "",
        "Task":            report.get("displayName", ""),
        "Robot Name":      report.get("robot", ""),
        "Location":        "",                    # not provided by openapi
        "Account":         "",                    # not provided by openapi
        "S/N":             report["robotSerialNumber"],
        "Machine Model":   model,
        "Supplier":        "Gausium",
        # --- task description ---
        "Cleaning Type":   report.get("cleaningMode", ""),
        "User":            report.get("operator", ""),
        "Completion (%)":  round(report.get("completionPercentage", 0) * 100, 2),
        # --- areas & timing ---
        "Planned Area (ft²)":           round(planned_area_ft2, 2) if planned_area_ft2 else None,
        "Total Time":                   seconds_to_hms(report.get("durationSeconds", 0)),
        "Duration":                     report.get("durationSeconds"),
        "Total Time (h)":              round(report.get("durationSeconds", 0) / 3600, 3),
        "Actual Cleaned (ft²)":         round(actual_area_ft2, 2) if actual_area_ft2 else None,
        "Efficiency (ft²/h)":           round(report.get("efficiencySquareMeterPerHour", 0)
                                             * UNIT_M2_TO_FT2, 3),
        "Water Usage (gal)":            round(report.get("waterConsumptionLiter", 0)
                                             * UNIT_L_TO_GAL, 3),
        # --- batteries & consumables ---
        "Start Battery (%)":            report.get("startBatteryPercentage"),
        "End Battery (%)":              report.get("endBatteryPercentage"),
        "Brush (%)":                    report.get("consumablesResidualPercentage", {}).get("brush"),
        "Filter (%)":                   report.get("consumablesResidualPercentage", {}).get("filter"),
        "Squeegee(%)":                  report.get("consumablesResidualPercentage", {}).get("suctionBlade"),
        # --- polishing ---
        "Planned crystallization area (ft²)": round(report.get("plannedPolishingAreaSquareMeter", 0)
                                                    * UNIT_M2_TO_FT2, 2),
        "Actual crystallization area (ft²)":  round(report.get("actualPolishingAreaSquareMeter", 0)
                                                    * UNIT_M2_TO_FT2, 2),
        # --- misc ---
        "Receive Task Report Time": "",       # API V2 does not surface; left blank
        "Cleaning Mode":          report.get("cleaningMode", ""),
        "Uncleaned Area (ft²)":   round(uncleaned_ft2, 2) if uncleaned_ft2 else None,
        "Plan Running Time (s)":  "",         # not provided
        "Task Type":              "",         # not provided
        "Remarks":                "",
        "Download Link":          report.get("taskReportPngUri", ""),
        "Task status":            report.get("taskEndStatus"),
        "Battery Usage":          (report.get("startBatteryPercentage", 0)
                                   - report.get("endBatteryPercentage", 0)),
    }

# ------------- MAIN FLOW ----------------------------------------------

def main() -> None:
    token = get_token()
    print("✓ obtained access-token")

    utc_now   = datetime.now(timezone.utc)
    start_utc = fmt_iso(utc_now - timedelta(days=DAYS_BACK))
    end_utc   = fmt_iso(utc_now)

    rows = []
    for sn in SN_LIST:
        model = get_robot_model(token, sn)
        reports = fetch_task_reports(token, sn, start_utc, end_utc)
        print(f"• {sn}: fetched {len(reports)} reports")
        rows.extend(transform(r, model) for r in reports)

    df = pd.DataFrame(rows)
    print(f"\nTotal rows: {len(df)}")
    print(df.head())

    if CSV_OUT:
        df.to_csv(CSV_OUT, index=False)
        print(f"\nSaved to {CSV_OUT}")

if __name__ == "__main__":
    main()
