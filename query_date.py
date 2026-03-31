import json, csv, sys
from pathlib import Path
from datetime import datetime, date
import pandas as pd

BASE        = Path(r"C:\Users\Bhavya Khaitan\BK\Stocks\index_reconstructor")
PARSED_JSON = BASE / "all_circulars_parsed.json"
MEMBERS_DIR = BASE / "current_members"
OUTPUT_DIR  = BASE / "output"

INDEX_CSV = {
    "NIFTY 50":                 "NIFTY_50.csv",
    "NIFTY NEXT 50":            "NIFTY_NEXT_50.csv",
    "NIFTY 100":                "NIFTY_100.csv",
    "NIFTY 500":                "NIFTY_500.csv",
    "NIFTY BANK":               "NIFTY_BANK.csv",
    "NIFTY IT":                 "NIFTY_IT.csv",
    "NIFTY FMCG":               "NIFTY_FMCG.csv",
    "NIFTY AUTO":               "NIFTY_AUTO.csv",
    "NIFTY PHARMA":             "NIFTY_PHARMA.csv",
    "NIFTY ENERGY":             "NIFTY_ENERGY.csv",
    "NIFTY METAL":              "NIFTY_METAL.csv",
    "NIFTY MIDCAP 50":          "NIFTY_MIDCAP_50.csv",
    "NIFTY MIDCAP 150":         "NIFTY_MIDCAP_150.csv",
    "NIFTY SMALLCAP 250":       "NIFTY_SMALLCAP_250.csv",
    "NIFTY FINANCIAL SERVICES": "NIFTY_FINANCIAL_SERVICES.csv",
    "NIFTY PSU BANK":           "NIFTY_PSU_BANK.csv",
    "NIFTY PRIVATE BANK":       "NIFTY_PRIVATE_BANK.csv",
    "NIFTY COMMODITIES":        "NIFTY_COMMODITIES.csv",
    "NIFTY REALTY":             "NIFTY_REALTY.csv",
    "NIFTY INFRASTRUCTURE":     "NIFTY_INFRASTRUCTURE.csv",
    "NIFTY MEDIA":              "NIFTY_MEDIA.csv",
    "NIFTY CPSE":               "NIFTY_CPSE.csv",
    "NIFTY MNC":                "NIFTY_MNC.csv",
    "NIFTY INDIA CONSUMPTION":  "NIFTY_INDIA_CONSUMPTION.csv",
    "NIFTY ALPHA 50":           "NIFTY_ALPHA_50.csv",
    "NIFTY LOW VOLATILITY 50":  "NIFTY_LOW_VOLATILITY_50.csv",
    "NIFTY MANUFACTURING":      "NIFTY_MANUFACTURING.csv",
    "NIFTY INDIA DEFENCE":      "NIFTY_INDIA_DEFENCE.csv",
}

ALIAS = {
    "HDFC":"HDFCBANK","MINDTREE":"LTIM","LTINFOTECH":"LTIM",
    "PVR":"PVRINOX","INOXLEISUR":"PVRINOX","INFRATEL":"BHARTIARTL",
    "ORIENTBANK":"PNB","CORPBANK":"UNIONBANK","SYNDIBANK":"CANARABANK",
    "ALBK":"INDIANB","ALLBANK":"INDIANB","ANDHRBANK":"UNIONBANK",
    "SESAGOA":"VEDL","CAIRN":"VEDL","NIITTECH":"COFORGE",
    "HDFCSTD":"HDFCLIFE","RECLTD":"REC","ACCLTD":"ACC","JSWISPAT":"JSWSTEEL",
}

def normalize(sym):
    s = str(sym).strip().upper()
    return ALIAS.get(s, s)

def load_current_members(index_name):
    csv_file = INDEX_CSV.get(index_name)
    if not csv_file:
        return set()
    path = MEMBERS_DIR / csv_file
    if not path.exists():
        return set()
    members = set()
    with open(path, encoding="utf-8-sig", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for col in ["Symbol","symbol","SYMBOL","ticker","Ticker",
                        "NSE Symbol","NSE_SYMBOL"," SYMBOL"]:
                val = row.get(col,"").strip()
                if val and len(val) >= 2:
                    members.add(normalize(val))
                    break
    return members

def load_circulars():
    with open(PARSED_JSON, encoding="utf-8") as f:
        data = json.load(f)
    valid = []
    for rec in data:
        dt = rec.get("date","")
        if not dt:
            continue
        try:
            d = datetime.strptime(dt[:10],"%Y-%m-%d").date()
            valid.append((d, rec))
        except:
            continue
    valid.sort(key=lambda x: x[0])
    return valid

def reconstruct_index(index_name, circulars):
    current = load_current_members(index_name)
    if not current:
        return {}
    relevant = []
    for d, rec in circulars:
        indices = rec.get("indices",[])
        if index_name in indices or "UNKNOWN" in indices:
            inc = [normalize(s) for s in rec.get("inclusions",[])]
            exc = [normalize(s) for s in rec.get("exclusions",[])]
            if inc or exc:
                relevant.append((d, inc, exc))
    snapshots = {date.today(): frozenset(current)}
    state = set(current)
    for d, inc, exc in reversed(relevant):
        new_state = set(state)
        for s in inc:
            new_state.discard(s)
        for s in exc:
            new_state.add(s)
        state = new_state
        snapshots[d] = frozenset(state)
    return snapshots

def get_on_date(snapshots, query_date):
    result = None
    for sd in sorted(snapshots.keys()):
        if sd <= query_date:
            result = snapshots[sd]
    return result

# ── MAIN ──────────────────────────────────────────────────────────────────────
if len(sys.argv) < 2:
    print("Usage: python query_date.py YYYY-MM-DD")
    print("Example: python query_date.py 2019-03-31")
    sys.exit(1)

query_str = sys.argv[1]
try:
    query_date = datetime.strptime(query_str, "%Y-%m-%d").date()
except:
    print(f"Invalid date format: {query_str}. Use YYYY-MM-DD")
    sys.exit(1)

print(f"\nLoading data...")
circulars = load_circulars()

print(f"Constituents as on {query_date}\n")
print(f"{'Index':<30} {'Count':>6}  Constituents")
print("-" * 120)

rows = []
for idx in INDEX_CSV:
    snapshots = reconstruct_index(idx, circulars)
    members   = get_on_date(snapshots, query_date)
    if members:
        mlist = sorted(members)
        print(f"{idx:<30} {len(mlist):>6}  {', '.join(mlist[:8])}{'...' if len(mlist)>8 else ''}")
        rows.append({"Index": idx, "Count": len(mlist),
                     "Constituents": ", ".join(mlist)})
    else:
        print(f"{idx:<30}      0  NO DATA")
        rows.append({"Index": idx, "Count": 0, "Constituents": "NO DATA"})

# Save to Excel
out = OUTPUT_DIR / f"query_{query_str}.xlsx"
df  = pd.DataFrame(rows)
with pd.ExcelWriter(out, engine="openpyxl") as writer:
    df.to_excel(writer, index=False, sheet_name=f"As_of_{query_str}")
print(f"\nSaved to: {out}")