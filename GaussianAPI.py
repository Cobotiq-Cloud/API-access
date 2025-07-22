#!/usr/bin/env python3
"""
gausium_task_reports.py
--------------------------------------------------------------------
Fetch task-reports for every robot SN in a text file, reshape them to
match 2025-06.csv, and write:
  • gausium.csv   (full data)
  • robots_ok.txt (all SNs successfully queried)
  • robots_err.txt(SNs that returned 4xx/5xx)
--------------------------------------------------------------------
Requires:  pip install requests pandas python-dateutil
"""

from __future__ import annotations
import os, time, requests, pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict

# ---------------- CONFIG ---------------------------------------------
SN_FILE           = Path(
    "/Users/chenzaowen/Desktop/API-Access/API-access/SN/accessible_robots.txt"
)
CSV_OUT           = Path("gausium.csv")
TXT_OK            = Path("robots_ok.txt")
TXT_ERR           = Path("robots_err.txt")
DAYS_BACK         = 70           # how far to look back
PAGE_SIZE         = 200          # API max 200

# ---- credentials (move these to env-vars for production) ------------
CLIENT_ID         = "IerEZST93qYQNJ3y1x8nTrWNG3fQ"
CLIENT_SECRET     = "SirRbhEUMPDP8lL0cEIUBOkEzcCp8hH0kHZ4MDIBjq9RtUZS7i6iO8mm1L"
ACCESS_KEY_SECRET = "f14e25fd2498c928f7a4712dd1b53022"     # = open_access_key

# ---------------- CONSTANTS & HELPERS --------------------------------
API_ROOT       = "https://openapi.gs-robot.com"
UNIT_M2_TO_FT2 = 10.7639
UNIT_L_TO_GAL  = 0.264172

def bearer_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}"}

def fmt_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0)\
             .isoformat().replace("+00:00", "Z")

def load_sn_list(path: Path) -> List[str]:
    """Return lines that *look* like serial numbers, skip everything else."""
    out = []
    with path.open(encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if (not line) or line.startswith("#") or ":" in line or " " in line:
                continue
            if line.startswith("GS") and line.count("-") >= 3:
                out.append(line)
            else:
                print(f"⚠️  Unrecognised line skipped: {line}")
    return out

# ---------------- API CALLS ------------------------------------------
def get_token() -> str:
    url = f"{API_ROOT}/gas/api/v1alpha1/oauth/token"
    payload = {
        "grant_type":  "urn:gaussian:params:oauth:grant-type:open-access-token",
        "client_id":   CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "open_access_key": ACCESS_KEY_SECRET,
    }
    r = requests.post(url, json=payload, timeout=15)
    r.raise_for_status()
    return r.json()["access_token"]

def get_robot_model(token: str, sn: str) -> str:
    url = f"{API_ROOT}/v1alpha1/robots"
    r = requests.get(url, params={"page":1, "pageSize":500},
                     headers=bearer_headers(token), timeout=15)
    r.raise_for_status()
    for rob in r.json().get("robots", []):
        if rob["serialNumber"] == sn:
            return rob.get("modelTypeCode") or rob.get("modelFamilyCode") or ""
    return ""

def fetch_task_reports(
    token: str, sn: str, start_utc: str, end_utc: str
) -> List[Dict]:
    out, page = [], 1
    while True:
        url = f"{API_ROOT}/openapi/v2alpha1/robots/{sn}/taskReports"
        q = {"page": page, "pageSize": PAGE_SIZE,
             "startTimeUtcFloor": start_utc, "startTimeUtcUpper": end_utc}
        r = requests.get(url, params=q, headers=bearer_headers(token), timeout=30)
        if r.status_code == 404:        # wrong SN / no reports → harmless
            break
        r.raise_for_status()            # raise for 400/403/500…
        data = r.json()
        out += data.get("robotTaskReports", [])
        if page * PAGE_SIZE >= int(data.get("total", 0)):
            break
        page += 1
    return out

def seconds_to_hms(sec: int) -> str:
    return time.strftime("%H:%M:%S", time.gmtime(sec))

def transform(rep: Dict, model: str) -> Dict:
    plan_ft2 = rep.get("plannedCleaningAreaSquareMeter") or 0
    act_ft2  = rep.get("actualCleaningAreaSquareMeter") or 0
    plan_ft2 *= UNIT_M2_TO_FT2
    act_ft2  *= UNIT_M2_TO_FT2
    unclean  = plan_ft2 - act_ft2 if plan_ft2 and act_ft2 else None
    return {
        "Start DateTime":  rep["startTime"],
        "End DateTime":    rep["endTime"],
        "Map":             ", ".join({s["mapName"] for s in rep.get("subTasks", [])}),
        "Task":            rep.get("displayName", ""),
        "Robot Name":      rep.get("robot", ""),
        "Location":        "", "Account": "",
        "S/N":             rep["robotSerialNumber"],
        "Machine Model":   model, "Supplier": "Gausium",
        "Cleaning Type":   rep.get("cleaningMode", ""),
        "User":            rep.get("operator", ""),
        "Completion (%)":  round(rep.get("completionPercentage", 0)*100, 2),
        "Planned Area (ft²)": round(plan_ft2, 2) or None,
        "Total Time":      seconds_to_hms(rep.get("durationSeconds", 0)),
        "Duration":        rep.get("durationSeconds"),
        "Total Time (h)":  round(rep.get("durationSeconds", 0)/3600, 3),
        "Actual Cleaned (ft²)": round(act_ft2, 2) or None,
        "Efficiency (ft²/h)": round(rep.get("efficiencySquareMeterPerHour", 0)
                                     * UNIT_M2_TO_FT2, 3),
        "Water Usage (gal)": round(rep.get("waterConsumptionLiter", 0)
                                   * UNIT_L_TO_GAL, 3),
        "Start Battery (%)": rep.get("startBatteryPercentage"),
        "End Battery (%)":   rep.get("endBatteryPercentage"),
        "Brush (%)":         rep.get("consumablesResidualPercentage", {}).get("brush"),
        "Filter (%)":        rep.get("consumablesResidualPercentage", {}).get("filter"),
        "Squeegee(%)":       rep.get("consumablesResidualPercentage", {}).get("suctionBlade"),
        "Planned crystallization area (ft²)": round(
            rep.get("plannedPolishingAreaSquareMeter", 0)*UNIT_M2_TO_FT2, 2),
        "Actual crystallization area (ft²)":  round(
            rep.get("actualPolishingAreaSquareMeter", 0)*UNIT_M2_TO_FT2, 2),
        "Receive Task Report Time": "",
        "Cleaning Mode":    rep.get("cleaningMode", ""),
        "Uncleaned Area (ft²)": round(unclean, 2) if unclean else None,
        "Plan Running Time (s)": "", "Task Type": "", "Remarks": "",
        "Download Link":    rep.get("taskReportPngUri", ""),
        "Task status":      rep.get("taskEndStatus"),
        "Battery Usage":    (rep.get("startBatteryPercentage", 0)
                             - rep.get("endBatteryPercentage", 0)),
    }

# ---------------- MAIN ------------------------------------------------
def main() -> None:
    sns = load_sn_list(SN_FILE)
    if not sns:
        print("❌  No serial numbers found – aborting.")
        return

    token      = get_token()
    utc_now    = datetime.now(timezone.utc)
    start_utc  = fmt_iso(utc_now - timedelta(days=DAYS_BACK))
    end_utc    = fmt_iso(utc_now)

    ok, errs, rows = [], [], []

    for sn in sns:
        try:
            model   = get_robot_model(token, sn)
            reports = fetch_task_reports(token, sn, start_utc, end_utc)
            rows.extend(transform(r, model) for r in reports)
            ok.append(sn)
            print(f"• {sn}: fetched {len(reports)} reports")
        except requests.HTTPError as e:
            status = e.response.status_code
            errs.append(f"{sn}  ({status})")
            print(f"❌ {sn} → HTTP {status}")
        except Exception as e:
            errs.append(f"{sn}  (EXCEPTION: {e})")
            print(f"❌ {sn} → {e}")

    # --- save outputs -------------------------------------------------
    if rows:
        pd.DataFrame(rows).to_csv(CSV_OUT, index=False)
        print(f"\nSaved {len(rows)} rows to {CSV_OUT.resolve()}")

    TXT_OK.write_text("\n".join(ok))
    TXT_ERR.write_text("\n".join(errs))
    print(f"👍  OK list  → {TXT_OK.resolve()}  ({len(ok)} robots)")
    print(f"🛑  Error list → {TXT_ERR.resolve()}  ({len(errs)} robots)")

if __name__ == "__main__":
    main()
