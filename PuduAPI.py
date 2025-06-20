#!/usr/bin/env python3
"""
PuduAPI.py
Fetch 30-day cleaning-task reports for SN=866035107050015,
then output a CSV matching the Gaussian schema.
"""

import base64, hashlib, hmac, time, requests, pandas as pd
from urllib.parse import urlparse, urlencode, unquote
from datetime import datetime, timedelta

# ─── 1) CREDENTIALS & CLUSTER ───────────────────────────────────────
API_APP_KEY    = "APIDrdvpicJqA8k54XFSfbYTD8wvk1bOEQnWDxBH"
API_APP_SECRET = "4JBFS7zXnqrmnvzPEICCYCH6lxsete4LAZo6SbxU"
CLUSTER        = "https://csu-open-platform.pudutech.com"  # US cluster

# ─── 2) CORRECTED ENDPOINT PATHS  ───────────────────────────────────
PATH_LIST   = "/pudu-entry/data-board/v1/log/clean_task/query_list"
PATH_DETAIL = "/pudu-entry/data-board/v1/log/clean_task/query"
# PATH_ROBOT  = "/pudu-entry/data-open-platform-service/v1/cleanbot-service/v1/api/open/robot/detail"
PATH_ROBOT = "/pudu-entry/cleanbot-service/v1/api/open/robot/detail"

# ─── 3) ROBOT & WINDOW ─────────────────────────────────────────────
SN        = "866035107050015"
DAYS      = 30
PAGE_LIMIT= 20   # must be 1–20
TZ_OFFSET = 0
CSV_OUT   = f"pudu_{SN}.csv"

# ─── 4) SIGNING UTIL  ───────────────────────────────────────────────
GMT_FMT  = "%a, %d %b %Y %H:%M:%S GMT"
ACCEPT   = "application/json"
CTYPE    = "application/json"

def _canonical(path:str, query:str) -> str:
    if path.startswith(("/release","/test","/prepub")):
        path = "/" + path.lstrip("/").split("/",1)[1]
    if query:
        qs = "&".join(sorted(query.split("&")))
        path += "?" + unquote(qs)
    return path or "/"

def signed_headers(method:str, url:str, body:str="") -> dict:
    u = urlparse(url)
    path = _canonical(u.path, u.query)
    content_md5 = ""
    if method=="POST" and body:
        md5 = hashlib.md5(body.encode()).hexdigest()
        content_md5 = base64.b64encode(md5.encode()).decode()
    xdate = datetime.utcnow().strftime(GMT_FMT)
    sign_str = f"x-date: {xdate}\n{method}\n{ACCEPT}\n{CTYPE}\n{content_md5}\n{path}"
    sig = base64.b64encode(
        hmac.new(API_APP_SECRET.encode(), sign_str.encode(), hashlib.sha1).digest()
    ).decode()
    auth = f'hmac id="{API_APP_KEY}", algorithm="hmac-sha1", headers="x-date", signature="{sig}"'
    return {
        "Host":         u.hostname,
        "Accept":       ACCEPT,
        "Content-Type": CTYPE,
        "x-date":       xdate,
        "Authorization":auth,
    }

def do_get(path:str, params:dict) -> dict:
    qs  = urlencode(sorted(params.items()))
    url = f"{CLUSTER}{path}?{qs}"
    r = requests.get(url, headers=signed_headers("GET", url), timeout=20)
    r.raise_for_status()
    return r.json()

# ─── 5) FETCH PAGED LIST ────────────────────────────────────────────
now_ts   = int(time.time())
start_ts = now_ts - DAYS*24*3600
rows, offset = [], 0

while True:
    payload = {
        "start_time":      start_ts,
        "end_time":        now_ts,
        "offset":          offset,
        "limit":           PAGE_LIMIT,
        "timezone_offset": TZ_OFFSET,
    }
    data = do_get(PATH_LIST, payload)["data"]["list"]
    batch = [r for r in data if r.get("sn")==SN]
    rows.extend(batch)
    if len(data) < PAGE_LIMIT:
        break
    offset += PAGE_LIMIT

print(f"→ fetched {len(rows)} total reports")

# ─── 6) OPTIONAL: FETCH ROBOT META ONCE ────────────────────────────
# try:
#     robot_meta = do_get(PATH_ROBOT, {"sn": SN})["data"]
#     location   = robot_meta.get("shop",{}).get("name","")
#     nickname   = robot_meta.get("nickname","")
#     model      = robot_meta.get("cleanbot",{}).get("clean",{}).get("mode","")
# except:
#     location, nickname, model = "", "", ""


robot_meta = {}               # ensure it's always defined
location, nickname, model = "", "", ""
try:
    robot_meta = do_get(PATH_ROBOT, {"sn": SN})["data"]
    location   = robot_meta.get("shop",{}).get("name","")
    nickname   = robot_meta.get("nickname","")
    model      = robot_meta.get("cleanbot",{}).get("clean",{}).get("mode","")
except Exception as e:
    print(" Warning: could not fetch robot metadata:", e)
    robot_meta = {}           # fallback to empty dict



# ─── 7) OPTIONALLY FETCH DETAIL PER REPORT ─────────────────────────
def get_detail(t):
    return do_get(PATH_DETAIL, {
        "start_time":      t["start_time"],
        "end_time":        t["end_time"],
        "sn":              SN,
        "report_id":       t["report_id"],
        "timezone_offset": TZ_OFFSET,
    })["data"]

# ─── 8) NORMALISE & DUMP CSV ───────────────────────────────────────
M2_FT2, L_GAL = 10.7639, 0.264172
out = []
for t in rows:
    d = get_detail(t)
    planned = t.get("task_area",0)
    actual  = t.get("clean_area",0)
    unclean = max(planned-actual,0)
    out.append({
        "Start DateTime":  datetime.fromtimestamp(t["start_time"]).isoformat()+"Z",
        "End DateTime":    datetime.fromtimestamp(t["end_time"]).isoformat()+"Z",
        "Map":             robot_meta.get("map",{}).get("name",""),
        "Task":            t.get("task_name",""),
        "Robot Name":      nickname,
        "Location":        location,
        "Account":         "",
        "S/N":             SN,
        # if no model code, try to infer from robot nickname
        # e.g. “skywalker” ⇒ “vacuum”
        "Machine Model": (
            model or
            ("vacuum"  if "skywalker" in nickname.lower() else
             "" )
        ),
        "Supplier":        "Pudu",
        "Cleaning Type":   "",
        "User":            "",
        "Completion (%)":  round(d.get("percentage",0),2),
        "Planned Area (ft²)":      round(planned*M2_FT2,2),
        "Total Time":              time.strftime("%H:%M:%S", time.gmtime(t.get("clean_time",0))),
        "Duration":                t.get("clean_time"),
        "Total Time (h)":          round(t.get("clean_time",0)/3600,3),
        "Actual Cleaned (ft²)":    round(actual*M2_FT2,2),
        # Efficiency: actual cleaned area ÷ time (h)
        # clean_area (m²) → ft², then / (clean_time/3600)
        "Efficiency (ft²/h)": round(
            (t.get("clean_area",0) * M2_FT2)
            / (t.get("clean_time",1) / 3600),
            3
        ) if t.get("clean_time",0) > 0 else None,  # :contentReference[oaicite:5]{index=5} :contentReference[oaicite:6]{index=6}
        "Water Usage (gal)":       round(d.get("cost_water",0)/1000*L_GAL,3),
        # Battery: end battery + battery used = start battery
        "End Battery (%)":         d.get("battery"),         # :contentReference[oaicite:7]{index=7}
        "Battery Usage":           d.get("cost_battery"),    # :contentReference[oaicite:8]{index=8}
        "Start Battery (%)":       (
                                       d.get("battery",0)
                                     + d.get("cost_battery",0)
                                   ) if d.get("battery") is not None else None,
        "End Battery (%)":         d.get("battery"),
        "Brush (%)":               "",
        "Filter (%)":              "",
        "Squeegee(%)":             "",
        "Planned crystallization area (ft²)": None,
        "Actual crystallization area (ft²)":  None,
        "Receive Task Report Time": d.get("create_time",""),
        "Cleaning Mode":            "",
        "Uncleaned Area (ft²)":     round(unclean*M2_FT2,2),
        "Plan Running Time (s)":    d.get("remaining_time"),
        "Task Type":                "",
        "Remarks":                  "",
        "Download Link":            "",
        "Task status":              d.get("status"),
        "Battery Usage":            d.get("cost_battery"),
    })

df = pd.DataFrame(out)
df.to_csv(CSV_OUT, index=False)
print(f"→ saved CSV → {CSV_OUT}")
