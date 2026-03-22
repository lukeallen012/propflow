"""
PropFlow -- Main Runner

Usage:
  python main.py                         # scan all markets, push to Google Sheet, log deals
  python main.py --test                  # scan + print, skip sheet push
  python main.py --new-only              # show only deals not seen in previous scans
  python main.py --top 10                # show top N deals (default 20)
  python main.py --min-score 60          # raise score threshold (default 40)
  python main.py --city Dallas           # scan one city only
  python main.py --save                  # save results to deals_YYYY-MM-DD.json
  python main.py sheet                   # print Google Sheet URL
"""

import sys
import json
import time
import argparse
from datetime import date, datetime

from scraper import run_scan
from sheets  import init_sheet, push_deals, get_sheet_url
from tracker import init_db, get_seen_addresses, log_deals, print_summary


# --- Display ------------------------------------------------------------------

def _flag(score: int) -> str:
    return "[G]" if score >= 70 else ("[Y]" if score >= 50 else "[ ]")


def print_deals(deals, top_n: int, seen_addresses: set = None):
    seen_addresses = seen_addresses or set()

    print(f"\n{'#':<4} {'SCR':<5} {'NEW':<4} {'ADDRESS':<32} {'CITY':<14} {'ST':<3} "
          f"{'ASK':>9} {'MAO':>9} {'REHAB':>8} {'SPRD':>6}  DISTRESS")
    print("-" * 130)

    for i, d in enumerate(deals[:top_n], 1):
        is_new = d.address.lower().strip() not in seen_addresses
        new_tag = " *" if is_new else "  "
        print(
            f"{i:<4} {_flag(d.score)}{d.score:<3} {new_tag:<4}"
            f"{d.address[:30]:<32} {d.city[:12]:<14} {d.state:<3} "
            f"${d.ask_price:>8,.0f} ${d.mao:>8,.0f} ${d.est_rehab:>7,.0f} "
            f"{d.spread_pct:>5.1f}%  {d.distress_type}"
        )


# --- Save ---------------------------------------------------------------------

def save_json(deals, top_n: int):
    filename = f"deals_{date.today().isoformat()}.json"
    payload  = [
        {
            "rank":           i + 1,
            "score":          d.score,
            "address":        d.address,
            "city":           d.city,
            "state":          d.state,
            "ask_price":      d.ask_price,
            "arv":            d.arv,
            "mao":            d.mao,
            "est_rehab":      d.est_rehab,
            "spread_pct":     d.spread_pct,
            "spread_dollar":  d.spread_dollar,
            "distress_type":  d.distress_type,
            "dom":            d.dom,
            "beds":           d.beds,
            "baths":          d.baths,
            "sqft":           d.sqft,
            "price_per_sqft": d.price_per_sqft,
            "year_built":     d.year_built,
            "source":         d.source,
            "url":            d.url,
            "date_found":     d.date_found,
        }
        for i, d in enumerate(deals[:top_n])
    ]
    with open(filename, "w") as f:
        json.dump(payload, f, indent=2)
    print(f" Saved {len(payload)} deals → {filename}")


# --- CLI ----------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="PropFlow -- real estate deal scanner")
    parser.add_argument("command",     nargs="?", default="run",
                        help="'run' (default), 'test', 'sheet', or 'summary'")
    parser.add_argument("--test",      action="store_true", help="Dry run -- skip sheet push")
    parser.add_argument("--new-only",  action="store_true", help="Show only deals not seen before")
    parser.add_argument("--top",       type=int, default=20,  metavar="N",
                        help="How many deals to display (default: 20)")
    parser.add_argument("--min-score", type=int, default=40,  metavar="N",
                        help="Minimum deal score (default: 40)")
    parser.add_argument("--city",      type=str, default=None, metavar="CITY",
                        help="Scan a single city only")
    parser.add_argument("--save",      action="store_true", help="Save results to JSON")
    return parser.parse_args()


def main():
    args = parse_args()

    # Legacy / convenience positional commands
    if args.command == "sheet":
        print(get_sheet_url() or "SPREADSHEET_ID not set in .env")
        return
    if args.command == "summary":
        print_summary()
        return
    if args.command == "test":
        args.test = True

    dry_run   = args.test
    top_n     = args.top
    min_score = args.min_score

    print(f"\n{'='*60}")
    print(f"PropFlow -- {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")
    if args.city:
        print(f"City filter : {args.city.strip().title()}")
    print(f"Min score   : {min_score}  |  Show top : {top_n}")
    print()

    # Load previously seen addresses so we can flag new ones
    init_db()
    seen = get_seen_addresses()

    start = time.time()
    deals = run_scan(min_score=min_score, city_filter=args.city)
    elapsed = round(time.time() - start, 1)

    new_count  = sum(1 for d in deals if d.address.lower().strip() not in seen)
    print(f"\nScan complete in {elapsed}s")
    print(f"  {len(deals)} deals >= score {min_score}  ({new_count} new *)\n")

    if not deals:
        print("No deals found above minimum score.")
        return

    display_deals = deals
    if args.new_only:
        display_deals = [d for d in deals if d.address.lower().strip() not in seen]
        print(f"Showing {len(display_deals)} new deals only (--new-only)\n")

    print_deals(display_deals, top_n, seen)

    # Always log every qualifying deal to the tracker
    added = log_deals(deals)
    if added:
        print(f"\n[+] {added} new deals logged to tracker (deals.db)")

    if args.save:
        save_json(display_deals, top_n)

    if dry_run:
        print("\n[dry run] Skipping Google Sheet push.")
        return

    print(f"\nPushing to Google Sheet...")
    try:
        init_sheet()
        pushed = push_deals(deals)
        url    = get_sheet_url()
        print(f"\nOK {pushed} new deals added to sheet")
        if url:
            print(f" {url}")
    except RuntimeError as e:
        print(f"\nWARN️  Sheet push skipped: {e}")
        print("    Run with --test to scan without pushing.")


if __name__ == "__main__":
    main()
