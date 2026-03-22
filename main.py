"""
DealSniper — Main Runner
Usage:
  python main.py          # run scan, push to Google Sheet
  python main.py test     # run scan, print top 20, don't push
  python main.py sheet    # just show the Google Sheet URL
"""

import sys
import time
from datetime import datetime
from scraper import run_scan
from sheets import init_sheet, push_deals, get_sheet_url


def main(dry_run: bool = False):
    print(f"\n{'='*55}")
    print(f"DealSniper — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}")
    print(f"Scanning {20} markets across Redfin + HUD...\n")

    start = time.time()
    deals = run_scan(min_score=40)
    elapsed = round(time.time() - start, 1)

    print(f"\nScan complete in {elapsed}s — {len(deals)} deals found\n")

    if not deals:
        print("No deals found above minimum score.")
        return

    # Print top 20
    print(f"{'RANK':<5} {'SCORE':<7} {'ADDRESS':<35} {'CITY':<15} {'ST':<4} {'ASK':>9} {'SPREAD':>8} {'DISTRESS'}")
    print("-" * 110)
    for i, d in enumerate(deals[:20], 1):
        flag = "🟢" if d.score >= 70 else ("🟡" if d.score >= 50 else "⬜")
        print(
            f"{i:<5} {flag} {d.score:<4} "
            f"{d.address[:33]:<35} {d.city[:13]:<15} {d.state:<4} "
            f"${d.ask_price:>8,.0f} {d.spread_pct:>6.1f}%  {d.distress_type}"
        )

    if dry_run:
        print("\n[dry run] Skipping Google Sheet push.")
        return

    print(f"\nPushing to Google Sheet...")
    try:
        init_sheet()
        added = push_deals(deals)
        url   = get_sheet_url()
        print(f"\n✅ Done — {added} new deals added")
        if url:
            print(f"📊 Sheet: {url}")
    except RuntimeError as e:
        print(f"\n⚠️  Sheet push skipped: {e}")
        print("Run 'python main.py test' to scan without pushing.")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        main(dry_run=True)
    elif len(sys.argv) > 1 and sys.argv[1] == "sheet":
        print(get_sheet_url())
    else:
        main()
