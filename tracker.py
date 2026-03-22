"""
PropFlow -- Deal Tracker
SQLite-backed history. Every deal seen is logged. Supports status tracking
so you can record what you've reviewed, contacted, passed on, or closed.

Usage:
  python tracker.py log [N]                           # show last N deals (default 50)
  python tracker.py new                               # show only unreviewed deals
  python tracker.py summary                           # stats by market + status
  python tracker.py status <address_fragment> <status> [notes]
  python tracker.py search <keyword>

Valid statuses: new | reviewed | contacted | passed | under_contract | closed
"""

import sqlite3
import os
import sys
from datetime import datetime

DB_PATH  = os.getenv("TRACKER_DB", "deals.db")
STATUSES = {"new", "reviewed", "contacted", "passed", "under_contract", "closed"}


def _connect():
    return sqlite3.connect(DB_PATH)


def init_db():
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS deals (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                address         TEXT NOT NULL,
                city            TEXT,
                state           TEXT,
                ask_price       REAL,
                arv             REAL,
                mao             REAL,
                est_rehab       REAL,
                spread_pct      REAL,
                score           INTEGER,
                distress_type   TEXT,
                dom             INTEGER,
                beds            INTEGER,
                baths           REAL,
                sqft            INTEGER,
                price_per_sqft  REAL,
                year_built      INTEGER,
                source          TEXT,
                url             TEXT,
                status          TEXT DEFAULT 'new',
                notes           TEXT DEFAULT '',
                date_found      TEXT,
                date_updated    TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_address ON deals(LOWER(address))")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_score   ON deals(score DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_status  ON deals(status)")


def get_seen_addresses() -> set[str]:
    """Return all addresses already in the DB (lowercase, stripped)."""
    init_db()
    with _connect() as conn:
        rows = conn.execute("SELECT address FROM deals").fetchall()
    return {r[0].lower().strip() for r in rows}


def log_deals(deals: list) -> int:
    """
    Insert new deals into the DB, skipping any address already tracked.
    Returns count of newly added rows.
    """
    init_db()
    added = 0
    with _connect() as conn:
        existing = {r[0].lower().strip()
                    for r in conn.execute("SELECT address FROM deals").fetchall()}
        for d in deals:
            key = d.address.lower().strip()
            if not key or key in existing:
                continue
            conn.execute("""
                INSERT INTO deals
                  (address, city, state, ask_price, arv, mao, est_rehab, spread_pct,
                   score, distress_type, dom, beds, baths, sqft, price_per_sqft,
                   year_built, source, url, status, date_found, date_updated)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'new',?,?)
            """, (
                d.address, d.city, d.state, d.ask_price, d.arv,
                getattr(d, "mao",        0.0),
                getattr(d, "est_rehab",  0.0),
                d.spread_pct, d.score, d.distress_type, d.dom,
                d.beds, d.baths, d.sqft, d.price_per_sqft,
                getattr(d, "year_built", 0),
                d.source, d.url,
                d.date_found, datetime.now().isoformat(),
            ))
            existing.add(key)
            added += 1
    return added


def update_status(fragment: str, status: str, notes: str = ""):
    """Update status (and optional notes) for any deal whose address contains `fragment`."""
    if status not in STATUSES:
        print(f"Invalid status '{status}'. Valid: {', '.join(sorted(STATUSES))}")
        return
    init_db()
    with _connect() as conn:
        result = conn.execute(
            "UPDATE deals SET status=?, notes=?, date_updated=? WHERE LOWER(address) LIKE ?",
            (status, notes, datetime.now().isoformat(), f"%{fragment.lower()}%"),
        )
        if result.rowcount == 0:
            print(f"No deal found matching '{fragment}'")
        else:
            print(f"Updated {result.rowcount} deal(s) → {status}")


def print_log(limit: int = 50, status_filter: str = None, search: str = None):
    init_db()
    clauses, params = [], []

    if status_filter:
        clauses.append("status = ?")
        params.append(status_filter)
    if search:
        clauses.append("(LOWER(address) LIKE ? OR LOWER(city) LIKE ? OR LOWER(distress_type) LIKE ?)")
        kw = f"%{search.lower()}%"
        params.extend([kw, kw, kw])

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)

    with _connect() as conn:
        rows = conn.execute(
            f"""SELECT score, status, address, city, state,
                       ask_price, mao, est_rehab, distress_type, dom, date_found
                FROM deals {where}
                ORDER BY score DESC LIMIT ?""",
            params,
        ).fetchall()

    if not rows:
        print("No deals found.")
        return

    print(f"\n{'SCR':<5} {'STATUS':<14} {'ADDRESS':<32} {'CITY':<14} {'ST':<3} "
          f"{'ASK':>9} {'MAO':>9} {'REHAB':>8} {'DISTRESS'}")
    print("-" * 125)
    for r in rows:
        score, status, addr, city, state, ask, mao, rehab, distress, dom, found = r
        flag  = "[G]" if score >= 70 else ("[Y]" if score >= 50 else "[ ]")
        smark = {"new": "  NEW", "reviewed": "  rev", "contacted": "  [called]",
                 "passed": "  ✗", "under_contract": "  [locked]", "closed": "  OK"}.get(status, status)
        print(
            f"{flag}{score:<3} {smark:<13} {addr[:30]:<32} {city[:12]:<14} {state:<3} "
            f"${ask:>8,.0f} ${mao:>8,.0f} ${rehab:>7,.0f}  {distress}"
        )
    print(f"\n{len(rows)} deals shown")


def print_summary():
    init_db()
    with _connect() as conn:
        total    = conn.execute("SELECT COUNT(*) FROM deals").fetchone()[0]
        avg_sc   = conn.execute("SELECT AVG(score) FROM deals").fetchone()[0] or 0
        by_status = conn.execute(
            "SELECT status, COUNT(*) FROM deals GROUP BY status ORDER BY COUNT(*) DESC"
        ).fetchall()
        by_market = conn.execute(
            """SELECT city, state, COUNT(*) AS cnt, AVG(score), MAX(score)
               FROM deals GROUP BY city, state ORDER BY cnt DESC LIMIT 12"""
        ).fetchall()
        top_deals = conn.execute(
            "SELECT score, address, city, state, ask_price, mao, distress_type "
            "FROM deals WHERE status='new' ORDER BY score DESC LIMIT 5"
        ).fetchall()

    print(f"\n{'='*55}")
    print(f"PropFlow -- Summary")
    print(f"{'='*55}")
    print(f"Total tracked: {total}   Avg score: {avg_sc:.1f}\n")

    print("Status breakdown:")
    for status, count in by_status:
        bar = "█" * min(count, 30)
        print(f"  {status:<16} {count:>4}  {bar}")

    print("\nTop markets (all-time):")
    print(f"  {'CITY':<15} {'ST':<4} {'#':>5} {'AVG':>7} {'MAX':>7}")
    print("  " + "-" * 42)
    for city, state, cnt, avg, mx in by_market:
        print(f"  {city:<15} {state:<4} {cnt:>5} {avg:>7.1f} {mx:>7}")

    if top_deals:
        print("\nTop unreviewed deals:")
        print(f"  {'SCR':<5} {'ADDRESS':<32} {'CITY':<14} {'ASK':>9} {'MAO':>9}")
        print("  " + "-" * 75)
        for sc, addr, city, state, ask, mao, distress in top_deals:
            flag = "[G]" if sc >= 70 else "[Y]"
            print(f"  {flag}{sc:<3} {addr[:30]:<32} {city[:12]:<14} ${ask:>8,.0f} ${mao:>8,.0f}")


if __name__ == "__main__":
    args = sys.argv[1:]

    if not args or args[0] == "log":
        limit = int(args[1]) if len(args) > 1 and args[1].isdigit() else 50
        print_log(limit=limit)
    elif args[0] == "new":
        print_log(status_filter="new")
    elif args[0] == "summary":
        print_summary()
    elif args[0] == "search":
        if len(args) < 2:
            print("Usage: python tracker.py search <keyword>")
        else:
            print_log(search=args[1])
    elif args[0] == "status":
        if len(args) < 3:
            print("Usage: python tracker.py status <address_fragment> <status> [notes]")
        else:
            notes = " ".join(args[3:]) if len(args) > 3 else ""
            update_status(args[1], args[2], notes)
    else:
        print(__doc__)
