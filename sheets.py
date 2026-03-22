"""
DealSniper — Google Sheets Integration
Pushes deals to a Google Sheet, color codes by score, avoids duplicates.

Setup:
1. Go to console.cloud.google.com
2. Create project → Enable Google Sheets API + Google Drive API
3. Create Service Account → download JSON key → save as credentials.json
4. Share your Google Sheet with the service account email
5. Add SPREADSHEET_ID to .env
"""

import os
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SPREADSHEET_ID  = os.getenv("SPREADSHEET_ID", "")
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")

HEADERS = [
    "Address", "City", "State", "Ask Price", "Est. ARV",
    "Spread $", "Spread %", "Score", "Distress Type",
    "DOM", "Beds", "Baths", "Sq Ft", "$/SqFt",
    "Status", "Source", "URL", "Date Found",
]

# Score thresholds for color coding
COLOR_GREEN  = {"red": 0.714, "green": 0.843, "blue": 0.659}  # 70+ = hot
COLOR_YELLOW = {"red": 1.0,   "green": 0.949, "blue": 0.8}    # 50-69 = review
COLOR_WHITE  = {"red": 1.0,   "green": 1.0,   "blue": 1.0}    # <50 = pass


def _get_service():
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
    except ImportError:
        raise RuntimeError(
            "Missing google libraries. Run: pip install google-api-python-client google-auth"
        )

    if not os.path.exists(CREDENTIALS_FILE):
        raise RuntimeError(
            f"Google credentials file not found: {CREDENTIALS_FILE}\n"
            "See sheets.py docstring for setup instructions."
        )

    creds = Credentials.from_service_account_file(
        CREDENTIALS_FILE,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return build("sheets", "v4", credentials=creds)


def init_sheet():
    """
    Create header row + freeze it + bold headers.
    Safe to call multiple times — checks if headers already exist.
    """
    svc    = _get_service()
    sheet  = svc.spreadsheets()

    # Read first row
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="Sheet1!A1:R1",
    ).execute()
    existing = result.get("values", [[]])
    if existing and existing[0] == HEADERS:
        return  # already initialized

    # Write headers
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range="Sheet1!A1:R1",
        valueInputOption="RAW",
        body={"values": [HEADERS]},
    ).execute()

    # Bold + freeze header row, set column widths
    sheet.batchUpdate(
        spreadsheetId=SPREADSHEET_ID,
        body={"requests": [
            # Freeze row 1
            {"updateSheetProperties": {
                "properties": {"sheetId": 0, "gridProperties": {"frozenRowCount": 1}},
                "fields": "gridProperties.frozenRowCount",
            }},
            # Bold headers
            {"repeatCell": {
                "range": {"sheetId": 0, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }},
        ]},
    ).execute()
    print("[sheets] Sheet initialized.")


def _get_existing_addresses(sheet) -> set[str]:
    """Read all addresses already in the sheet to avoid duplicates."""
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range="Sheet1!A2:A",
    ).execute()
    rows = result.get("values", [])
    return {r[0].lower().strip() for r in rows if r}


def push_deals(deals) -> int:
    """
    Append new deals to the sheet. Skips duplicates (by address).
    Color codes rows by score. Returns count of new rows added.
    """
    if not SPREADSHEET_ID:
        print("[sheets] SPREADSHEET_ID not set — skipping push.")
        return 0

    svc   = _get_service()
    sheet = svc.spreadsheets()

    existing = _get_existing_addresses(sheet)

    new_rows = []
    for d in deals:
        if d.address.lower().strip() in existing:
            continue
        new_rows.append([
            d.address,
            d.city,
            d.state,
            d.ask_price,
            round(d.arv, 0),
            round(d.spread_dollar, 0),
            f"{d.spread_pct:.1f}%",
            d.score,
            d.distress_type,
            d.dom,
            d.beds,
            d.baths,
            d.sqft,
            d.price_per_sqft,
            d.status,
            d.source,
            d.url,
            d.date_found,
        ])

    if not new_rows:
        print("[sheets] No new deals to push.")
        return 0

    # Append rows
    append_result = sheet.values().append(
        spreadsheetId=SPREADSHEET_ID,
        range="Sheet1!A2",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": new_rows},
    ).execute()

    updated_range = append_result["updates"]["updatedRange"]
    # Parse start row from range like "Sheet1!A102:R110"
    try:
        start_row = int(updated_range.split("!")[1].split(":")[0][1:])
    except Exception:
        start_row = 2

    # Color code each new row by score
    color_requests = []
    for i, row in enumerate(new_rows):
        score     = row[7]  # index 7 = Score column
        row_index = start_row + i - 1  # 0-indexed

        if score >= 70:
            bg = COLOR_GREEN
        elif score >= 50:
            bg = COLOR_YELLOW
        else:
            bg = COLOR_WHITE

        color_requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": 0,
                    "startRowIndex": row_index,
                    "endRowIndex":   row_index + 1,
                },
                "cell": {"userEnteredFormat": {"backgroundColor": bg}},
                "fields": "userEnteredFormat.backgroundColor",
            }
        })

    if color_requests:
        sheet.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"requests": color_requests},
        ).execute()

    print(f"[sheets] Pushed {len(new_rows)} new deals.")
    return len(new_rows)


def get_sheet_url() -> str:
    return f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}" if SPREADSHEET_ID else ""
