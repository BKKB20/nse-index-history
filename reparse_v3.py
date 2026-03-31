import re, json, csv
from pathlib import Path
from datetime import datetime
import pdfplumber
from bs4 import BeautifulSoup

BASE     = Path(r"C:\Users\Bhavya Khaitan\BK\Stocks\index_reconstructor")
CACHE    = BASE / "downloaded_circulars"
OUT_FILE = BASE / "all_circulars_parsed_v3.json"

ALIAS = {
    "HDFC":"HDFCBANK","MINDTREE":"LTIM","LTINFOTECH":"LTIM",
    "PVR":"PVRINOX","INOXLEISUR":"PVRINOX","INFRATEL":"BHARTIARTL",
    "ORIENTBANK":"PNB","CORPBANK":"UNIONBANK","SYNDIBANK":"CANARABANK",
    "ALBK":"INDIANB","ALLBANK":"INDIANB","ANDHRBANK":"UNIONBANK",
    "SESAGOA":"VEDL","CAIRN":"VEDL","NIITTECH":"COFORGE",
    "HDFCSTD":"HDFCLIFE","RECLTD":"REC","ACCLTD":"ACC","JSWISPAT":"JSWSTEEL",
    "3IINFOTECH":"3IINFOTECH",
}

# Noise words that look like tickers but aren't
NOISE = {
    "NSE","BSE","SEBI","THE","AND","FOR","FROM","WITH","THAT","THIS",
    "WILL","HAS","ARE","NOT","ALL","NEW","OLD","EFF","WEF","LTD","PVT",
    "INC","PLC","CNX","HEREBY","EFFECTIVE","DATE","CHANGE","CONSTITUENTS",
    "INDICES","INDEX","TRADING","MEMBER","CIRCULAR","FAOP","FUTURES",
    "OPTIONS","ABOVE","BELOW","STOCK","SYMBOL","NAME","COMPANY","SR",
    "NO","SL","SNO","SERIES","EQ","BE","RE","NA","IL","SA","CO","OF",
    "IS","IN","BY","TO","AT","AN","AS","ON","OR","IF","IT","DO","GO",
    "UP","INCLUDED","EXCLUDED","FOLLOWING","COMPANIES","BEING","DETAILS",
    "MEMBERS","REQUESTED","REFER","PRESS","RELEASE","ISSUED","RESPECT",
    "REPLACEMENTS","CLOSE","DEPARTMENT","DOWNLOAD","REF","NATIONAL",
    "EXCHANGE","INDIA","LIMITED","PAGE","SECURITIES","LISTED","MARKET",
    "EQUITY","BEHALF","VICE","PRESIDENT","ASSOCIATE","TOLL","FREE",
    "EMAIL","OPTION","DVR","SCRIP","SCRIPS","ACCOUNT","AFTER","AGE",
    "GE","PSE","AIL","STER","HCL","PTC","TULIP","FSL","CMC","LITL",
    "INFRASTRUCTURE","INFIBEAM","INDIGO","NBCC","SUZLON","STAR",
    "GLOBAL","BRIDGE","NOIDA","RAYON","FASHIONS","MCLEOD","RUSSEL",
    "JAGRAN","PRAKASHAN","UTTAM","SUGAR","MILLS","ENTERTAINMENT",
    "NETWORK","SHIPPING","VARUN","AMTEK","GEOJIT","FINANCIAL",
    "SERVICES","VARUN","STERLITE","INDUSTRIES","PETROLEUM",
}

# All index name patterns — order matters, more specific first
INDEX_PATTERNS = [
    ("NIFTY NEXT 50",           [r"nifty\s*next\s*50", r"nifty\s*junior", r"cnx\s*nifty\s*junior"]),
    ("NIFTY MIDCAP 50",         [r"nifty\s*midcap\s*select", r"nifty\s*midcap\s*50",
                                  r"nifty\s*mid\s*cap\s*50", r"cnx\s*midcap\b"]),
    ("NIFTY MIDCAP 150",        [r"nifty\s*midcap\s*150"]),
    ("NIFTY SMALLCAP 250",      [r"nifty\s*smallcap\s*250"]),
    ("NIFTY FINANCIAL SERVICES",[r"nifty\s*financial\s*services", r"nifty\s*fin\s*serv"]),
    ("NIFTY PSU BANK",          [r"nifty\s*psu\s*bank", r"cnx\s*psu\s*bank"]),
    ("NIFTY PRIVATE BANK",      [r"nifty\s*private\s*bank", r"nifty\s*pvt\s*bank"]),
    ("NIFTY BANK",              [r"nifty\s*bank\b", r"bank\s*nifty", r"cnx\s*bank\b"]),
    ("NIFTY IT",                [r"nifty\s*it\b", r"cnx\s*it\b", r"s&p\s*cnx\s*it"]),
    ("NIFTY FMCG",              [r"nifty\s*fmcg", r"cnx\s*fmcg"]),
    ("NIFTY AUTO",              [r"nifty\s*auto\b", r"cnx\s*auto\b"]),
    ("NIFTY PHARMA",            [r"nifty\s*pharma", r"cnx\s*pharma"]),
    ("NIFTY ENERGY",            [r"nifty\s*energy", r"cnx\s*energy"]),
    ("NIFTY METAL",             [r"nifty\s*metal", r"cnx\s*metal"]),
    ("NIFTY COMMODITIES",       [r"nifty\s*commodities", r"cnx\s*commodities"]),
    ("NIFTY REALTY",            [r"nifty\s*realty", r"cnx\s*realty"]),
    ("NIFTY INFRASTRUCTURE",    [r"nifty\s*infra", r"cnx\s*infra"]),
    ("NIFTY MEDIA",             [r"nifty\s*media", r"cnx\s*media"]),
    ("NIFTY CPSE",              [r"nifty\s*cpse"]),
    ("NIFTY MNC",               [r"nifty\s*mnc\b", r"cnx\s*mnc\b"]),
    ("NIFTY INDIA CONSUMPTION", [r"nifty\s*india\s*consumption", r"nifty\s*consumption"]),
    ("NIFTY INDIA DEFENCE",     [r"nifty\s*india\s*defence", r"nifty\s*defence"]),
    ("NIFTY MANUFACTURING",     [r"nifty\s*india\s*manufacturing", r"nifty\s*manufacturing"]),
    ("NIFTY ALPHA 50",          [r"nifty\s*alpha\s*50"]),
    ("NIFTY LOW VOLATILITY 50", [r"nifty\s*low\s*vol"]),
    ("NIFTY 100",               [r"nifty\s*100\b"]),
    ("NIFTY 500",               [r"nifty\s*500\b"]),
    ("NIFTY 50",                [r"nifty\s*50\b", r"s&p\s*cnx\s*nifty", r"cnx\s*nifty\b"]),
]

TICKER_RE = re.compile(r'^[A-Z][A-Z0-9&\-]{1,19}$')

def normalize(sym):
    s = re.sub(r'[^A-Z0-9&\-]', '', str(sym).strip().upper())
    return ALIAS.get(s, s)

def is_ticker(s):
    if not s or s in NOISE: return False
    if not TICKER_RE.match(s): return False
    if len(s) < 2 or len(s) > 20: return False
    return True

def match_index(line_lower):
    """Return index name if line matches an index header pattern."""
    for idx_name, patterns in INDEX_PATTERNS:
        for pat in patterns:
            if re.search(pat, line_lower):
                return idx_name
    return None

def is_index_header(line):
    """
    Detect index section headers in three formats:
    Format A: 'S&P CNX Nifty Index'  (plain, ends with Index/index)
    Format B: '(1) CNX Nifty Index'  (parenthesized number)
    Format C: '1) Nifty 50'          (number with closing paren)
    Format D: 'NIFTY 50:'            (colon suffix — newer format)
    """
    stripped = line.strip()
    if not stripped:
        return None
    low = stripped.lower()

    # Format D: ends with colon
    if stripped.endswith(':'):
        return match_index(low.rstrip(':'))

    # Format B: starts with (N)
    if re.match(r'^\(\d+\)', stripped):
        return match_index(low)

    # Format C: starts with N)
    if re.match(r'^\d+\)', stripped):
        return match_index(low)

    # Format A: line that IS an index name (short line, no sentence structure)
    # Must be short, no verb words, ends with Index/indices or matches pattern
    if len(stripped.split()) <= 6:
        if re.search(r'(index|indices)$', low) or match_index(low):
            idx = match_index(low)
            if idx:
                return idx

    return None

def extract_ticker_from_row(line):
    """Extract ticker from numbered table row: '1 Company Name SYMBOL'"""
    line = line.strip()
    parts = line.split()
    if not parts:
        return None
    # Must start with a number
    if not parts[0].rstrip('.').isdigit():
        return None
    if len(parts) < 2:
        return None
    # Try last token
    candidate = normalize(parts[-1])
    if is_ticker(candidate):
        return candidate
    # Try second-to-last (sometimes there's a footnote marker)
    if len(parts) >= 3:
        candidate = normalize(parts[-2])
        if is_ticker(candidate):
            return candidate
    return None

def parse_text_per_index(text):
    """Parse circular text into per-index inc/exc dict."""
    results = {}
    lines   = text.split('\n')
    current_index = None
    mode          = None

    for line in lines:
        stripped  = line.strip()
        line_lower = stripped.lower()

        if not stripped:
            continue

        # Check for index section header
        idx = is_index_header(stripped)
        if idx:
            current_index = idx
            mode = None
            if idx not in results:
                results[idx] = {"inclusions": [], "exclusions": []}
            continue

        if current_index is None:
            continue

        # Detect mode
        if any(x in line_lower for x in [
                'being excluded','being removed','is excluded',
                'are excluded','excluded:','exclusion:',
                'scrips are being excluded','company is being excluded',
                'companies are being excluded']):
            mode = 'exc'
            continue

        if any(x in line_lower for x in [
                'being included','being added','is included',
                'are included','included:','inclusion:',
                'scrips are being included','company is being included',
                'companies are being included']):
            mode = 'inc'
            continue

        if mode is None:
            continue

        # Skip header rows
        if any(h in line_lower for h in [
                'sr.','sr no','company name','s.no',
                'symbol','following','regd.','page ']):
            continue

        # Extract ticker from numbered row
        ticker = extract_ticker_from_row(stripped)
        if ticker:
            lst = results[current_index]["inclusions" if mode=='inc' else "exclusions"]
            if ticker not in lst:
                lst.append(ticker)

    return results

def get_text(path, ftype):
    if ftype == 'pdf':
        text = ""
        try:
            with pdfplumber.open(path) as pdf:
                for page in pdf.pages:
                    text += (page.extract_text() or "") + "\n"
        except Exception as e:
            print(f"    PDF error {path.name}: {e}")
        return text
    else:
        try:
            content = path.read_bytes().decode('utf-8', 'ignore')
            return BeautifulSoup(content, 'lxml').get_text(separator='\n')
        except Exception as e:
            print(f"    HTM error {path.name}: {e}")
            return ""

# Load CSV metadata
CSV_FILE = BASE / "circulars_nse" / "circular-FAO-Constituents.csv"
csv_meta = {}
with open(CSV_FILE, encoding='utf-8-sig') as f:
    for row in csv.DictReader(f):
        ref = row["DOWNLOAD REFERENCE NO."].strip()
        m   = re.search(r'(\d+)$', ref)
        if m:
            try:
                dt = datetime.strptime(row["DATE"].strip(), "%B %d, %Y")
                csv_meta[m.group(1)] = {
                    "date": dt.strftime("%Y-%m-%d"),
                    "ref":  ref,
                    "url":  row["LINK"].strip()
                }
            except: pass

# Process all files
results = []
all_files = (
    [(f,'pdf') for f in sorted(CACHE.glob("*.pdf"))
     if "Press release" not in f.name] +
    [(f,'htm') for f in sorted(CACHE.glob("*.htm"))]
)

for path, ftype in all_files:
    m = re.search(r'(\d{4,})', path.name)
    if not m: continue
    fo_num = m.group(1)
    meta   = csv_meta.get(fo_num, {})

    text = get_text(path, ftype)
    if len(text) < 100:
        print(f"  SKIP {path.name}")
        continue

    per_index = parse_text_per_index(text)

    found_any = False
    for idx_name, changes in per_index.items():
        inc = changes["inclusions"]
        exc = changes["exclusions"]
        if not inc and not exc:
            continue
        found_any = True
        results.append({
            "ref":        meta.get("ref", fo_num),
            "date":       meta.get("date", ""),
            "url":        meta.get("url", ""),
            "source":     path.name,
            "index":      idx_name,
            "inclusions": inc,
            "exclusions": exc,
        })
        print(f"  {meta.get('date','?'):12} {idx_name:35s} "
              f"in={inc} ex={exc}")

    if not found_any:
        print(f"  {meta.get('date','?'):12} NO CHANGES — {path.name}")

results.sort(key=lambda x: x.get("date",""))

with open(OUT_FILE,"w") as f:
    json.dump(results, f, indent=2)

print(f"\nSaved {len(results)} records → {OUT_FILE}")
from collections import Counter
counts = Counter(r["index"] for r in results)
print("\nRecords per index:")
for k,v in sorted(counts.items()):
    print(f"  {k:35s} {v}")