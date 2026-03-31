import re, json, csv
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup

BASE  = Path(r"C:\Users\Bhavya Khaitan\BK\Stocks\index_reconstructor")
CACHE = BASE / "downloaded_circulars"

ALIAS = {
    "HDFC":"HDFCBANK","MINDTREE":"LTIM","LTINFOTECH":"LTIM",
    "PVR":"PVRINOX","INOXLEISUR":"PVRINOX","INFRATEL":"BHARTIARTL",
    "ORIENTBANK":"PNB","CORPBANK":"UNIONBANK","SYNDIBANK":"CANARABANK",
    "ALBK":"INDIANB","ALLBANK":"INDIANB","ANDHRBANK":"UNIONBANK",
    "SESAGOA":"VEDL","CAIRN":"VEDL","NIITTECH":"COFORGE",
    "HDFCSTD":"HDFCLIFE","RECLTD":"REC","ACCLTD":"ACC","JSWISPAT":"JSWSTEEL",
}

COMPANY_MAP = {
    "oriental bank":        "ORIENTBANK",
    "jet airways":          "JETAIRWAYS",
    "sterlite industries":  "STER",
    "sterlite":             "STER",
    "reliance petroleum":   "RELPETRO",
    "tech mahindra":        "TECHM",
    "ultratech cement":     "ULTRACEMCO",
    "ultra tech cement":    "ULTRACEMCO",
    "dabur india":          "DABUR",
    "dabur":                "DABUR",
    "ntpc":                 "NTPC",
    "mastek":               "MASTEK",
    "mindtree":             "LTIM",
    "mphasis":              "MPHASIS",
    "larsen":               "LT",
    "l&t":                  "LT",
    "britannia":            "BRITANNIA",
    "container corporation":"CONCOR",
    "reliance capital":     "RELCAPITAL",
    "flextronics":          "FSL",
    "gillette":             "GILLETTE",
    "mangalore refinery":   "MRPL",
    "motor industries":     "MICO",
    "biocon":               "BIOCON",
    "jaiprakash":           "JPASSOCIAT",
    "lupin":                "LUPIN",
    "patni":                "PATNI",
    "hughes software":      "HCLTECH",
    "e-serve":              "ESERVE",
    "procter & gamble":     "PGHH",
    "polaris software":     "POLARIS",
    "bongaigaon":           "BONGREFIN",
    "ibp co":               "IBP",
    "financial technologies":"FINANTECH",
    "tata chemicals":       "TATACHEM",
    "united spirits":       "MCDOWELL-N",
    "india cements":        "INDIACEM",
    "infrastructure dev":   "IDFC",
    "bombay rayon":         "BRFL",
    "noida toll":           "NOIDAT",
    "allcargo":             "ALLCARGO",
    "syndicate bank":       "SYNDIBANK",
    "kotak mahindra":       "KOTAKBANK",
    "hindustan petroleum":  "HINDPETRO",
    "igate":                "IGATE",
    "industrial development bank": "IDBI",
    "shipping corporation": "SCI",
    "mcleod russel":        "MCLEODRUSS",
    "pantaloon":            "PANTALOONR",
    "radico":               "RADICO",
    "trent":                "TRENT",
    "godfrey phillips":     "GODFRYPHLP",
    "sterling biotech":     "STERLINBIO",
    "vst industries":       "VSTIND",
    "hdfc":                 "HDFCBANK",
    "wipro":                "WIPRO",
    "infosys":              "INFY",
    "satyam":               "SATYAMCOMP",
    "hcl tech":             "HCLTECH",
    "hcl technologies":     "HCLTECH",
    "reliance industries":  "RELIANCE",
    "reliance comm":        "RCOM",
    "reliance power":       "RPOWER",
    "tata motors":          "TMPV",
    "tata steel":           "TATASTEEL",
    "tata power":           "TATAPOWER",
    "tata consultancy":     "TCS",
    "sbin":                 "SBIN",
    "state bank":           "SBIN",
    "icici bank":           "ICICIBANK",
    "icici":                "ICICIBANK",
    "axis bank":            "AXISBANK",
    "yes bank":             "YESBANK",
    "punjab national":      "PNB",
    "bank of baroda":       "BANKBARODA",
    "union bank":           "UNIONBANK",
    "canara bank":          "CANBK",
    "bank of india":        "BANKINDIA",
    "dena bank":            "DENABANK",
    "south indian bank":    "SOUTHBANK",
    "federal bank":         "FEDERALBNK",
    "idbi":                 "IDBI",
    "ifci":                 "IFCI",
    "power finance":        "PFC",
    "rural electrif":       "REC",
    "abb":                  "ABB",
    "bhel":                 "BHEL",
    "bharat forge":         "BHARATFORG",
    "siemens":              "SIEMENS",
    "cummins":              "CUMMINSIND",
    "suzlon":               "SUZLON",
    "gmr infra":            "GMRINFRA",
    "mundra port":          "ADANIPORTS",
    "dlf":                  "DLF",
    "unitech":              "UNITECH",
    "hdil":                 "HDIL",
    "ranbaxy":              "RANBAXY",
    "dr. reddy":            "DRREDDY",
    "dr reddy":             "DRREDDY",
    "cipla":                "CIPLA",
    "sun pharma":           "SUNPHARMA",
    "sun pharmaceutical":   "SUNPHARMA",
    "grasim":               "GRASIM",
    "ambuja":               "AMBUJACEM",
    "acc":                  "ACC",
    "shree cement":         "SHREECEM",
    "ultracem":             "ULTRACEMCO",
    "gail":                 "GAIL",
    "ongc":                 "ONGC",
    "oil & natural gas":    "ONGC",
    "oil natural gas":      "ONGC",
    "bharat petroleum":     "BPCL",
    "bpcl":                 "BPCL",
    "indian oil":           "IOC",
    "ioc":                  "IOC",
    "hindustan zinc":       "HINDZINC",
    "hindalco":             "HINDALCO",
    "sail":                 "SAIL",
    "tata steel":           "TATASTEEL",
    "jindal steel":         "JINDALSTEL",
    "nmdc":                 "NMDC",
    "coal india":           "COALINDIA",
    "ntpc":                 "NTPC",
    "power grid":           "POWERGRID",
    "nhpc":                 "NHPC",
    "hero honda":           "HEROMOTOCO",
    "hero motocorp":        "HEROMOTOCO",
    "bajaj auto":           "BAJAJ-AUTO",
    "maruti":               "MARUTI",
    "mahindra":             "M&M",
    "eicher":               "EICHERMOT",
    "ashok leyland":        "ASHOKLEY",
    "tvs motor":            "TVSMOTOR",
    "apollo tyres":         "APOLLOTYRE",
    "mrf":                  "MRF",
    "balkrishna":           "BALKRISIND",
    "asian paints":         "ASIANPAINT",
    "titan":                "TITAN",
    "havells":              "HAVELLS",
    "voltas":               "VOLTAS",
    "crompton":             "CROMPGREAV",
    "bharat electronics":   "BEL",
    "aurobindo":            "AUROPHARMA",
    "wockhardt":            "WOCKPHARMA",
    "ipca":                 "IPCALAB",
    "divis":                "DIVISLAB",
    "torrent pharma":       "TORNTPHARM",
    "zee":                  "ZEEL",
    "sun tv":               "SUNTV",
    "idea cellular":        "IDEA",
    "bharti":               "BHARTIARTL",
    "mtnl":                 "MTNL",
    "tata comm":            "TATACOMM",
    "hexaware":             "HEXAWARE",
    "rolta":                "ROLTA",
    "niit tech":            "COFORGE",
    "infoedge":             "NAUKRI",
    "info edge":            "NAUKRI",
    "just dial":            "JUSTDIAL",
    "page industries":      "PAGEIND",
    "jubilant":             "JUBLFOOD",
    "indusind":             "INDUSINDBK",
    "kotak":                "KOTAKBANK",
    "rcom":                 "RCOM",
    "rpower":               "RPOWER",
}

INDEX_PATTERNS = [
    ("NIFTY NEXT 50",           [r"nifty\s*next\s*50",r"nifty\s*junior",r"cnx\s*nifty\s*junior"]),
    ("NIFTY MIDCAP 50",         [r"nifty\s*midcap\s*select",r"nifty\s*midcap\s*50",
                                  r"nifty\s*mid\s*cap\s*50",r"cnx\s*midcap\b"]),
    ("NIFTY BANK",              [r"cnx\s*bank\b",r"nifty\s*bank\b"]),
    ("NIFTY IT",                [r"cnx\s*it\b",r"nifty\s*it\b",r"s&p\s*cnx\s*it"]),
    ("NIFTY FMCG",              [r"cnx\s*fmcg",r"nifty\s*fmcg"]),
    ("NIFTY AUTO",              [r"cnx\s*auto\b",r"nifty\s*auto\b"]),
    ("NIFTY PHARMA",            [r"cnx\s*pharma",r"nifty\s*pharma"]),
    ("NIFTY ENERGY",            [r"cnx\s*energy",r"nifty\s*energy"]),
    ("NIFTY METAL",             [r"cnx\s*metal",r"nifty\s*metal"]),
    ("NIFTY MNC",               [r"cnx\s*mnc\b",r"nifty\s*mnc\b"]),
    ("NIFTY INFRASTRUCTURE",    [r"cnx\s*infra",r"nifty\s*infra"]),
    ("NIFTY 100",               [r"cnx\s*100\b",r"nifty\s*100\b",r"s&p\s*cnx\s*100"]),
    ("NIFTY 500",               [r"cnx\s*500\b",r"nifty\s*500\b",r"s&p\s*cnx\s*500"]),
    ("NIFTY 50",                [r"s&p\s*cnx\s*nifty\b",r"cnx\s*nifty\b",r"nifty\s*50\b"]),
]

def match_index(text):
    t = text.lower()
    for idx_name, patterns in INDEX_PATTERNS:
        for pat in patterns:
            if re.search(pat, t):
                return idx_name
    return None

def normalize(sym):
    s = re.sub(r'[^A-Z0-9&\-]','',str(sym).strip().upper())
    return ALIAS.get(s,s)

def company_to_sym(name):
    """Convert company name to ticker."""
    name = re.sub(r'\s+',' ', name.strip().lower())
    # Remove common suffixes
    name = re.sub(r'\b(ltd\.?|limited|pvt|private|inc|corp|co\.?)\b','',name).strip()
    for key, sym in COMPANY_MAP.items():
        if key in name:
            return sym
    return None

def extract_sym_from_cells(cells):
    """Try to get ticker from a table row's cells."""
    if not cells:
        return None
    # If there's a Symbol column (cells[1] looks like ticker)
    if len(cells) >= 3:
        cand = normalize(cells[1])
        if re.match(r'^[A-Z][A-Z0-9&\-]{1,15}$', cand) and cand not in ('SR','NO','SL','SNO','COMPANY','NAME','SYMBOL','INDUSTRY'):
            return cand
    # Try company name conversion
    for cell in cells[1:]:
        cell_clean = re.sub(r'[\r\n\t]+',' ', cell).strip()
        if not cell_clean or cell_clean.isdigit():
            continue
        # Try as direct ticker first
        cand = normalize(cell_clean)
        if re.match(r'^[A-Z][A-Z0-9&\-]{1,15}$', cand) and len(cand) <= 15:
            return cand
        # Try company name map
        sym = company_to_sym(cell_clean)
        if sym:
            return sym
    return None

def parse_htm_v2(path):
    """
    Parse HTM by walking through all elements (text + tables) in document order.
    Track current index section and inc/exc mode from text,
    then extract tickers from the immediately following table.
    """
    content = path.read_bytes().decode('utf-8','ignore')
    soup = BeautifulSoup(content,'lxml')

    results = {}
    current_index = None
    mode = None

    # Walk through all elements in document order
    for element in soup.descendants:
        if element.name == 'table':
            # Extract data rows from this table
            if current_index is None or mode is None:
                continue
            rows = element.find_all('tr')
            for row in rows:
                cells = [td.get_text(separator=' ', strip=True)
                         for td in row.find_all(['td','th'])]
                cells = [re.sub(r'\s+',' ',c).strip() for c in cells if c.strip()]
                if not cells:
                    continue
                # Skip header rows
                first = cells[0].lower()
                if any(h in first for h in ['sr','no.','symbol','company']):
                    continue
                # Skip if first cell is not a number
                if not cells[0].rstrip('.').isdigit():
                    continue
                sym = extract_sym_from_cells(cells)
                if sym:
                    n = normalize(sym)
                    if n and len(n)>=2:
                        if current_index not in results:
                            results[current_index] = {"inclusions":[],"exclusions":[]}
                        lst = results[current_index]["inclusions" if mode=='inc' else "exclusions"]
                        if n not in lst:
                            lst.append(n)

        elif hasattr(element,'get_text') and element.name in ['p','b','strong','span','td','div','h1','h2','h3','h4','font']:
            text = element.get_text(separator=' ',strip=True)
            text = re.sub(r'\s+',' ',text).strip()
            if not text or len(text) < 3:
                continue
            text_lower = text.lower()

            # Check for index section header
            # Patterns: "(1) S&P CNX Nifty Index", "1) Nifty 50", plain index name
            idx = None
            for pattern in [r'^\(\d+\)\s*(.+)',r'^\d+\)\s*(.+)',r'^\d+\.\s*(.+)']:
                m = re.match(pattern, text, re.IGNORECASE)
                if m:
                    idx = match_index(m.group(1))
                    break
            if not idx and len(text.split()) <= 10:
                idx = match_index(text)

            if idx:
                current_index = idx
                mode = None
                if idx not in results:
                    results[idx] = {"inclusions":[],"exclusions":[]}
                continue

            # Detect mode
            if current_index:
                if any(x in text_lower for x in
                       ['being excluded','being removed','is excluded',
                        'are excluded','excluded:','exclusion']):
                    mode = 'exc'
                elif any(x in text_lower for x in
                         ['being included','being added','is included',
                          'are included','included:','inclusion']):
                    mode = 'inc'

    return results

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

results = []
for path in sorted(CACHE.glob("*.htm")):
    m = re.search(r'(\d{4,})', path.name)
    if not m: continue
    fo_num = m.group(1)
    meta   = csv_meta.get(fo_num, {})

    per_index = parse_htm_v2(path)

    found = False
    for idx_name, changes in per_index.items():
        inc = changes["inclusions"]
        exc = changes["exclusions"]
        if not inc and not exc:
            continue
        found = True
        results.append({
            "ref":        meta.get("ref", fo_num),
            "date":       meta.get("date",""),
            "url":        meta.get("url",""),
            "source":     path.name,
            "index":      idx_name,
            "inclusions": inc,
            "exclusions": exc,
        })
        print(f"  {meta.get('date','?'):12} {idx_name:35s} in={inc} ex={exc}")

    if not found:
        print(f"  {meta.get('date','?'):12} NO CHANGES — {path.name}")

results.sort(key=lambda x: x.get("date",""))

out = BASE / "htm_circulars_parsed_v2.json"
with open(out,"w") as f:
    json.dump(results, f, indent=2)

print(f"\nSaved {len(results)} HTM records → {out.name}")
from collections import Counter
counts = Counter(r["index"] for r in results)
print("\nRecords per index:")
for k,v in sorted(counts.items()):
    print(f"  {k:35s} {v}")