"""
database.py
───────────
SQLite-backed storage for all historical index snapshots and audit trails.

Schema:
  snapshots   — one row per (index, date, stock) membership
  circulars   — parsed circular metadata
  audit_steps — reconstruction steps
"""

import sqlite3
import json
import logging
from datetime import date
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

DDL = """
CREATE TABLE IF NOT EXISTS snapshots (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    index_name    TEXT    NOT NULL,
    as_of_date    TEXT    NOT NULL,
    stock         TEXT    NOT NULL,
    source_file   TEXT,
    warnings      TEXT,   -- JSON array
    created_at    TEXT    DEFAULT (datetime('now')),
    UNIQUE(index_name, as_of_date, stock)
);

CREATE TABLE IF NOT EXISTS circulars (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    index_name     TEXT    NOT NULL,
    effective_date TEXT,
    source_file    TEXT    NOT NULL,
    inclusions     TEXT,   -- JSON array
    exclusions     TEXT,   -- JSON array
    parse_confidence REAL,
    warnings       TEXT,   -- JSON array
    created_at     TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS audit_steps (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    index_name      TEXT    NOT NULL,
    step_number     INTEGER NOT NULL,
    effective_date  TEXT,
    source_file     TEXT,
    added_back      TEXT,   -- JSON array
    removed         TEXT,   -- JSON array
    constituents    TEXT,   -- JSON array
    expected_size   INTEGER,
    is_balanced     INTEGER,
    is_size_ok      INTEGER,
    warnings        TEXT,   -- JSON array
    created_at      TEXT    DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_snap_index_date
    ON snapshots(index_name, as_of_date);
CREATE INDEX IF NOT EXISTS idx_snap_stock
    ON snapshots(stock);
CREATE INDEX IF NOT EXISTS idx_circ_index
    ON circulars(index_name, effective_date);
"""


class ReconstructionDB:
    def __init__(self, db_path: str = "reconstruction.db"):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def connect(self):
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(DDL)
        self._conn.commit()
        logger.info(f"Connected to DB: {self.db_path}")

    def close(self):
        if self._conn:
            self._conn.close()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *_):
        self.close()

    # ── write ──────────────────────────────────────────────────────────────────
    def upsert_snapshots(self, snapshots: list):
        """Bulk insert/replace snapshot records."""
        rows = []
        for snap in snapshots:
            for stock in snap.constituents:
                rows.append((
                    snap.index_name,
                    snap.as_of_date.isoformat() if snap.as_of_date else None,
                    stock,
                    snap.circular_ref,
                    json.dumps(snap.warnings),
                ))
        self._conn.executemany(
            """INSERT OR REPLACE INTO snapshots
               (index_name, as_of_date, stock, source_file, warnings)
               VALUES (?, ?, ?, ?, ?)""",
            rows,
        )
        self._conn.commit()
        logger.info(f"Upserted {len(rows)} snapshot rows")

    def upsert_circulars(self, records: list):
        """Bulk insert parsed circular records."""
        rows = []
        for r in records:
            rows.append((
                r.index_name,
                r.effective_date.isoformat() if r.effective_date else None,
                r.source_file,
                json.dumps(r.inclusions),
                json.dumps(r.exclusions),
                r.parse_confidence,
                json.dumps(r.warnings),
            ))
        self._conn.executemany(
            """INSERT OR IGNORE INTO circulars
               (index_name, effective_date, source_file, inclusions,
                exclusions, parse_confidence, warnings)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        self._conn.commit()
        logger.info(f"Upserted {len(rows)} circular records")

    def upsert_audit_steps(self, index_name: str, steps: list):
        rows = []
        for s in steps:
            rows.append((
                index_name,
                s.step_number,
                s.effective_date.isoformat() if s.effective_date else None,
                s.source_file,
                json.dumps(s.added_back),
                json.dumps(s.removed),
                json.dumps(s.constituents_after),
                s.expected_size,
                int(s.is_balanced),
                int(s.is_size_ok),
                json.dumps(s.warnings),
            ))
        self._conn.executemany(
            """INSERT OR REPLACE INTO audit_steps
               (index_name, step_number, effective_date, source_file,
                added_back, removed, constituents, expected_size,
                is_balanced, is_size_ok, warnings)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        self._conn.commit()

    # ── read ───────────────────────────────────────────────────────────────────
    def get_constituents(
        self, index_name: str, query_date: date
    ) -> list[str]:
        """
        Return constituents as of query_date (latest snapshot ≤ query_date).
        """
        cur = self._conn.execute(
            """SELECT stock FROM snapshots
               WHERE index_name = ?
                 AND as_of_date <= ?
               ORDER BY as_of_date DESC
               LIMIT 1""",
            (index_name, query_date.isoformat()),
        )
        # get the actual snapshot date first
        cur2 = self._conn.execute(
            """SELECT MAX(as_of_date) FROM snapshots
               WHERE index_name = ? AND as_of_date <= ?""",
            (index_name, query_date.isoformat()),
        )
        snap_date = cur2.fetchone()[0]
        if not snap_date:
            return []

        cur3 = self._conn.execute(
            """SELECT stock FROM snapshots
               WHERE index_name = ? AND as_of_date = ?
               ORDER BY stock""",
            (index_name, snap_date),
        )
        return [row[0] for row in cur3.fetchall()]

    def get_all_dates(self, index_name: str) -> list[str]:
        cur = self._conn.execute(
            """SELECT DISTINCT as_of_date FROM snapshots
               WHERE index_name = ?
               ORDER BY as_of_date""",
            (index_name,),
        )
        return [row[0] for row in cur.fetchall()]

    def get_stock_history(self, stock: str) -> list[dict]:
        """Return all index memberships for a given stock."""
        cur = self._conn.execute(
            """SELECT index_name, as_of_date FROM snapshots
               WHERE stock = ?
               ORDER BY index_name, as_of_date""",
            (stock.upper(),),
        )
        return [{'index': r[0], 'date': r[1]} for r in cur.fetchall()]

    def get_anomalies(self, index_name: str = None) -> list[dict]:
        """Return all audit steps that have warnings."""
        if index_name:
            cur = self._conn.execute(
                """SELECT index_name, effective_date, step_number,
                          warnings, is_balanced, is_size_ok
                   FROM audit_steps
                   WHERE index_name = ? AND warnings != '[]'
                   ORDER BY effective_date""",
                (index_name,),
            )
        else:
            cur = self._conn.execute(
                """SELECT index_name, effective_date, step_number,
                          warnings, is_balanced, is_size_ok
                   FROM audit_steps
                   WHERE warnings != '[]'
                   ORDER BY index_name, effective_date"""
            )
        rows = []
        for r in cur.fetchall():
            rows.append({
                'index': r[0], 'date': r[1], 'step': r[2],
                'warnings': json.loads(r[3]),
                'balanced': bool(r[4]), 'size_ok': bool(r[5]),
            })
        return rows

    def export_to_csv(self, out_dir: str):
        """Export snapshots table to CSV files, one per index."""
        import pandas as pd
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        cur = self._conn.execute(
            "SELECT DISTINCT index_name FROM snapshots ORDER BY index_name"
        )
        indices = [r[0] for r in cur.fetchall()]

        for idx in indices:
            cur2 = self._conn.execute(
                """SELECT as_of_date, stock FROM snapshots
                   WHERE index_name = ?
                   ORDER BY as_of_date, stock""",
                (idx,),
            )
            rows = cur2.fetchall()
            df = pd.DataFrame(rows, columns=['date', 'stock'])
            safe_name = idx.replace(' ', '_').replace('/', '_')
            path = Path(out_dir) / f"{safe_name}.csv"
            df.to_csv(path, index=False)
            logger.info(f"Exported {idx} → {path}")

    def export_wide_csv(self, index_name: str, out_path: str):
        """
        Export a wide-format CSV: rows = dates, columns = stocks, values = 0/1.
        """
        import pandas as pd
        dates = self.get_all_dates(index_name)
        matrix = {}
        all_stocks = set()

        for d in dates:
            cur = self._conn.execute(
                "SELECT stock FROM snapshots WHERE index_name=? AND as_of_date=?",
                (index_name, d),
            )
            stocks = [r[0] for r in cur.fetchall()]
            matrix[d] = stocks
            all_stocks.update(stocks)

        all_stocks = sorted(all_stocks)
        rows = []
        for d, stocks in matrix.items():
            stock_set = set(stocks)
            row = {'date': d}
            row.update({s: 1 if s in stock_set else 0 for s in all_stocks})
            rows.append(row)

        df = pd.DataFrame(rows).set_index('date').sort_index()
        df.to_csv(out_path)
        logger.info(f"Wide CSV exported → {out_path}")
