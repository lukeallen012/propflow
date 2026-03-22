"""
PropFlow — Daily Scheduler
Runs a full scan automatically on a configurable schedule and pushes results
to Google Sheet + tracker DB.

Usage:
  python scheduler.py                    # run daily at 07:00 (default)
  python scheduler.py --time 08:30       # custom daily time (24h HH:MM)
  python scheduler.py --now              # run immediately, then keep scheduling
  python scheduler.py --interval 12h    # run every N hours instead of daily
  python scheduler.py --dry-run         # schedule but skip sheet push (test mode)
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime

try:
    import schedule
except ImportError:
    print("Missing 'schedule' package. Run: pip install schedule")
    sys.exit(1)


def _run(dry_run: bool = False):
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M')}] PropFlow scheduled scan starting...")
    cmd = [sys.executable, "main.py", "--save"]
    if dry_run:
        cmd.append("--test")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"[scheduler] Scan exited with code {result.returncode}")
    else:
        print(f"[{datetime.now().strftime('%H:%M')}] Scan complete.")


def _parse_interval(s: str) -> int:
    """Parse '6h', '30m', '2' → minutes."""
    s = s.strip().lower()
    if s.endswith("h"):
        return int(s[:-1]) * 60
    if s.endswith("m"):
        return int(s[:-1])
    return int(s) * 60  # default treat bare number as hours


def main():
    parser = argparse.ArgumentParser(description="PropFlow scheduler")
    parser.add_argument("--time",     default="07:00",
                        help="Daily run time in HH:MM (default: 07:00)")
    parser.add_argument("--interval", default=None, metavar="INTERVAL",
                        help="Run every N hours/minutes instead of daily (e.g. 12h, 6h, 90m)")
    parser.add_argument("--now",      action="store_true",
                        help="Run immediately on start, then keep scheduling")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Skip Google Sheet push (safe for testing)")
    args = parser.parse_args()

    dry = args.dry_run

    if args.interval:
        minutes = _parse_interval(args.interval)
        schedule.every(minutes).minutes.do(_run, dry_run=dry)
        print(f"[scheduler] Running every {minutes} minutes. Press Ctrl+C to stop.")
    else:
        schedule.every().day.at(args.time).do(_run, dry_run=dry)
        print(f"[scheduler] Running daily at {args.time}. Press Ctrl+C to stop.")

    if args.now:
        _run(dry_run=dry)

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
