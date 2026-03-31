# NSE Index History Reconstruction Pipeline

Reconstructs the complete historical constituent lists for 28 NSE indices from 2004 to 2026 by reverse-engineering NSE reconstitution circulars — eliminating the survivorship bias that makes most retail backtesting results wrong.

## The Problem

Every backtesting tool uses today's index composition applied to the past. If you backtest NIFTY 50 from 2010, you're investing in 2010 using stocks that were only added in 2020. Your returns look better than they actually would have been. This is survivorship bias, and it silently corrupts every result.

No free tool exists that gives you the actual constituents of Indian indices on any historical date.

## What This Builds

A complete historical database answering: *"What stocks were in NIFTY 50 on 15 March 2014?"*

- 28 NSE indices reconstructed (NIFTY 50, NIFTY NEXT 50, NIFTY BANK, NIFTY IT, NIFTY MIDCAP 50, and 23 more)
- 2004–2026 coverage using ~100 NSE PDF and HTM circulars
- SQLite database + Excel output (snapshot, flat-list, ticker × date wide-format)
- Date query tool: `python query_date.py 2019-03-31` → Excel with all 28 indices as of that date

## How It Works

NSE publishes reconstitution circulars whenever a stock is added or removed from an index. Starting from today's known composition and walking backwards through every circular:
```
composition_before = composition_after + removed_stocks − added_stocks
```

The pipeline handles 4 different circular formats NSE has used since 2004, scanned PDF limitations, 80+ corporate action mappings (mergers, renames, delistings like HDFC→HDFCBANK, PVR+INOX→PVRINOX), and cross-index contamination between sections of the same circular.

## Files

| File | Purpose |
|------|---------|
| `config.py` | Index metadata, expected counts, 80+ ticker alias mappings |
| `reparse_v3.py` | PDF circular parser — per-index section extraction |
| `reparse_htm_v2.py` | HTM circular parser for 2004–2009 data |
| `merge_and_reconstruct.py` | Core reconstruction engine |
| `database.py` | SQLite storage, query, and export layer |
| `query_date.py` | CLI date query tool |
| `current_members/` | 28 CSVs — current NSE constituents as of March 2026 |
| `NSE_INDEX_HISTORY_FINAL_V3.xlsx` | Final output — all 28 indices, all dates |

## Output

The Excel file contains:
- **FLAT_LIST** — one row per date/index/symbol, best for analysis
- **WIDE_FORMAT** — ticker × date matrix (0/1), ready for backtesting
- **TODAY** — all 28 indices side by side as of March 2026
- **SUMMARY** — snapshot count and status per index

## Known Limitations

- 3 circulars from 2018–2019 are scanned image PDFs — unprocessed, OCR needed
- Pre-2010 accuracy limited by company-name-to-ticker mapping completeness
- BSE indices (Sensex, BSE 500) not yet included

## Built By

Bhavya Khaitan — Second Year B.Tech Textile Technology, VJTI Mumbai
