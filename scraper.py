"""
DealSniper — Property Scraper
Sources: Redfin (CSV API), HUD Homestore (free API), RapidAPI Zillow (free tier)
"""

import urllib.request
import urllib.parse
import json
import csv
import os
import io
import time
from datetime import date, datetime
from dotenv import load_dotenv
from scorer import score_deal, Deal

load_dotenv()

RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY", "")

# Sun Belt + Midwest markets hedge funds actively buy in
TARGET_MARKETS = [
    ("Atlanta",       "GA"), ("Charlotte",    "NC"), ("Dallas",       "TX"),
    ("Houston",       "TX"), ("Phoenix",       "AZ"), ("Tampa",         "FL"),
    ("Jacksonville",  "FL"), ("Orlando",       "FL"), ("Nashville",     "TN"),
    ("Memphis",       "TN"), ("Indianapolis",  "IN"), ("Columbus",      "OH"),
    ("Kansas City",   "MO"), ("San Antonio",   "TX"), ("Las Vegas",     "NV"),
    ("Raleigh",       "NC"), ("Birmingham",    "AL"), ("Huntsville",    "AL"),
    ("St. Louis",     "MO"), ("Cleveland",     "OH"),
]


def _http_get(url: str, headers: dict = None, timeout: int = 15):
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json,text/html,*/*",
                **(headers or {}),
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except Exception as e:
        print(f"  [scraper] HTTP error {url[:80]}: {e}")
        return None


# ─── REDFIN ───────────────────────────────────────────────────────────────────

REDFIN_CSV_URL = (
    "https://www.redfin.com/stingray/api/gis-csv"
    "?al=1&market=atlanta&num_homes=100&ord=redfin-recommended-asc"
    "&page_number=1&sf=1,2,3,5,6,7&status=9&uipt=1&v=8"
    "&min_price={min_price}&max_price={max_price}"
    "&region_id={region_id}&region_type=6"
)

REDFIN_REGION_IDS = {
    # city → redfin region_id (metro area)
    "Atlanta":      "14368",
    "Charlotte":    "15143",
    "Dallas":       "15099",
    "Houston":      "10640",
    "Phoenix":      "14683",
    "Tampa":        "14961",
    "Jacksonville": "15380",
    "Orlando":      "14770",
    "Nashville":    "14650",
    "Memphis":      "14502",
    "Indianapolis": "14289",
    "Columbus":     "15178",
    "Kansas City":  "15364",
    "San Antonio":  "14892",
    "Las Vegas":    "14382",
    "Raleigh":      "14786",
    "Birmingham":   "14962",
    "St. Louis":    "14941",
    "Cleveland":    "14082",
}


def scrape_redfin(city: str, state: str, min_price: int = 80000, max_price: int = 350000) -> list[Deal]:
    region_id = REDFIN_REGION_IDS.get(city)
    if not region_id:
        return []

    url = (
        f"https://www.redfin.com/stingray/api/gis-csv"
        f"?al=1&num_homes=100&ord=days-on-market-desc"
        f"&page_number=1&sf=1&status=9&uipt=1&v=8"
        f"&min_price={min_price}&max_price={max_price}"
        f"&region_id={region_id}&region_type=6"
        f"&hoa=0"  # no HOA filter helps find SFR
    )

    raw = _http_get(url, headers={"Accept": "text/csv"})
    if not raw:
        return []

    text = raw.decode("utf-8", errors="replace")
    # Redfin prepends "Download initiated:" line — skip it
    lines = text.splitlines()
    csv_start = next((i for i, l in enumerate(lines) if l.startswith("ADDRESS")), 0)
    csv_text = "\n".join(lines[csv_start:])

    deals = []
    try:
        reader = csv.DictReader(io.StringIO(csv_text))
        for row in reader:
            try:
                ask   = float(str(row.get("PRICE", "0")).replace("$", "").replace(",", "") or 0)
                arv   = float(str(row.get("PRICE", "0")).replace("$", "").replace(",", "") or 0)  # will override with Zestimate if available
                beds  = int(float(str(row.get("BEDS", "0") or 0)))
                baths = float(str(row.get("BATHS", "0") or 0))
                sqft  = int(float(str(row.get("SQUARE FEET", "0")).replace(",", "") or 0))
                dom   = int(float(str(row.get("DAYS ON MARKET", "0") or 0)))
                addr  = row.get("ADDRESS", "").strip()
                city_ = row.get("CITY",    city).strip()
                state_= row.get("STATE OR PROVINCE", state).strip()
                url_  = row.get("URL (SEE https://www.redfin.com/buy-a-home/comparative-market-analysis for info on how redfin determines its estimates)", "").strip()
                prop_type = row.get("PROPERTY TYPE", "SFR")

                if ask <= 0 or not addr:
                    continue

                # Estimate ARV as 120% of ask (rough proxy — will refine with comps later)
                # In reality hedge funds want 70% LTV or better
                arv_est = ask * 1.20

                flags = []
                if "Foreclosure" in row.get("STATUS", ""):
                    flags.append("pre-foreclosure")
                if dom >= 60:
                    flags.append(f"long DOM")

                score, distress = score_deal(ask, arv_est, beds, sqft, dom, flags, prop_type)

                price_per_sqft = round(ask / sqft, 2) if sqft > 0 else 0.0

                deals.append(Deal(
                    address=addr,
                    city=city_,
                    state=state_,
                    ask_price=ask,
                    arv=arv_est,
                    spread_pct=round((arv_est - ask) / arv_est * 100, 1),
                    spread_dollar=round(arv_est - ask, 0),
                    score=score,
                    distress_type=distress,
                    dom=dom,
                    beds=beds,
                    baths=baths,
                    sqft=sqft,
                    price_per_sqft=price_per_sqft,
                    source="Redfin",
                    url=f"https://www.redfin.com{url_}" if url_ and not url_.startswith("http") else url_,
                    date_found=date.today().isoformat(),
                ))
            except Exception:
                continue
    except Exception as e:
        print(f"  [redfin] CSV parse error for {city}: {e}")

    return deals


# ─── HUD HOMESTORE ────────────────────────────────────────────────────────────

def scrape_hud(state: str) -> list[Deal]:
    """
    HUD REO homes — actual government foreclosures, always priced below market.
    https://www.hudhomestore.gov/
    """
    url = (
        f"https://www.hudhomestore.gov/HudHomes/ListingHudHomes.aspx"
        f"?selectedState={state}&priceLow=50000&priceHigh=350000"
        f"&bedsMin=3&sqftMin=1000"
    )

    # HUD has a JSON API endpoint used by their mobile app
    api_url = (
        f"https://www.hudhomestore.gov/HudHomes/SearchResults.aspx"
        f"?selectedState={state}&priceLow=50000&priceHigh=350000"
        f"&bedsMin=3&sqftMin=1000&pageNum=1&pageSize=50"
        f"&sortBy=price&sortAscend=true"
    )

    raw = _http_get(api_url)
    if not raw:
        return []

    deals = []
    try:
        data = json.loads(raw)
        listings = data.get("listingHudHomes", data.get("properties", []))
        for prop in listings:
            try:
                ask   = float(prop.get("listPrice", 0) or prop.get("price", 0))
                arv   = ask * 1.25   # HUD homes typically 20-30% below market
                beds  = int(prop.get("bedrooms", 0) or 0)
                baths = float(prop.get("bathrooms", 0) or 0)
                sqft  = int(prop.get("squareFeet", 0) or prop.get("sqft", 0) or 0)
                addr  = prop.get("address", "").strip()
                city_ = prop.get("city", "").strip()
                st    = prop.get("state", state).strip()
                case_ = prop.get("caseNumber", "")

                if ask <= 0 or not addr:
                    continue

                score, distress = score_deal(ask, arv, beds, sqft, 0, ["HUD", "REO"], "SFR")
                price_per_sqft  = round(ask / sqft, 2) if sqft > 0 else 0.0

                deals.append(Deal(
                    address=addr,
                    city=city_,
                    state=st,
                    ask_price=ask,
                    arv=arv,
                    spread_pct=round((arv - ask) / arv * 100, 1),
                    spread_dollar=round(arv - ask, 0),
                    score=score,
                    distress_type=distress,
                    dom=0,
                    beds=beds,
                    baths=baths,
                    sqft=sqft,
                    price_per_sqft=price_per_sqft,
                    source="HUD",
                    url=f"https://www.hudhomestore.gov/Listing/PropertyDetails.aspx?caseNumber={case_}",
                    date_found=date.today().isoformat(),
                ))
            except Exception:
                continue
    except Exception as e:
        print(f"  [hud] Parse error for {state}: {e}")

    return deals


# ─── ZILLOW via RapidAPI ──────────────────────────────────────────────────────

def scrape_zillow(city: str, state: str) -> list[Deal]:
    """Zillow via RapidAPI free tier. Requires RAPIDAPI_KEY in .env."""
    if not RAPIDAPI_KEY:
        return []

    location = f"{city}, {state}"
    url = (
        f"https://zillow-com1.p.rapidapi.com/propertyExtendedSearch"
        f"?location={urllib.parse.quote(location)}"
        f"&home_type=Houses&minPrice=80000&maxPrice=350000&bedsMin=3"
        f"&daysOn=90"  # listed 90+ days = more motivated
    )

    raw = _http_get(url, headers={
        "X-RapidAPI-Key":  RAPIDAPI_KEY,
        "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com",
    })
    if not raw:
        return []

    deals = []
    try:
        data  = json.loads(raw)
        props = data.get("props", [])
        for prop in props:
            try:
                ask        = float(prop.get("price", 0) or 0)
                zestimate  = float(prop.get("zestimate", 0) or ask * 1.15)
                arv        = zestimate if zestimate > ask else ask * 1.15
                beds       = int(prop.get("bedrooms", 0) or 0)
                baths      = float(prop.get("bathrooms", 0) or 0)
                sqft       = int(prop.get("livingArea", 0) or 0)
                dom        = int(prop.get("daysOnZillow", 0) or 0)
                addr       = prop.get("address", "").strip()
                city_      = prop.get("city",    city).strip()
                state_     = prop.get("state",   state).strip()
                zpid       = prop.get("zpid", "")
                prop_type  = prop.get("homeType", "SFR")

                if ask <= 0 or not addr:
                    continue

                flags = []
                if prop.get("foreclosureTypes"):
                    flags.append("pre-foreclosure")
                if dom >= 60:
                    flags.append("long DOM")
                if prop.get("priceReduction"):
                    flags.append("price cut")

                score, distress = score_deal(ask, arv, beds, sqft, dom, flags, prop_type)
                price_per_sqft  = round(ask / sqft, 2) if sqft > 0 else 0.0

                deals.append(Deal(
                    address=addr,
                    city=city_,
                    state=state_,
                    ask_price=ask,
                    arv=arv,
                    spread_pct=round((arv - ask) / arv * 100, 1),
                    spread_dollar=round(arv - ask, 0),
                    score=score,
                    distress_type=distress,
                    dom=dom,
                    beds=beds,
                    baths=baths,
                    sqft=sqft,
                    price_per_sqft=price_per_sqft,
                    source="Zillow",
                    url=f"https://www.zillow.com/homes/{zpid}_zpid/" if zpid else "",
                    date_found=date.today().isoformat(),
                ))
            except Exception:
                continue
    except Exception as e:
        print(f"  [zillow] Parse error for {city}: {e}")

    return deals


# ─── MAIN SCAN ────────────────────────────────────────────────────────────────

def run_scan(min_score: int = 40, max_per_source: int = 5) -> list[Deal]:
    """
    Scan all target markets across all sources.
    Returns deals sorted by score descending.
    """
    all_deals: list[Deal] = []
    seen_addresses: set[str] = set()

    states_done_hud: set[str] = set()

    for city, state in TARGET_MARKETS:
        print(f"  Scanning {city}, {state}...", end=" ")
        city_deals = []

        # Redfin
        rf = scrape_redfin(city, state)
        city_deals.extend(rf)
        time.sleep(0.5)  # be polite

        # HUD — once per state
        if state not in states_done_hud:
            hud = scrape_hud(state)
            city_deals.extend(hud)
            states_done_hud.add(state)
            time.sleep(0.3)

        # Zillow (if API key set)
        if RAPIDAPI_KEY:
            zl = scrape_zillow(city, state)
            city_deals.extend(zl)
            time.sleep(0.5)

        # Deduplicate by address
        for d in city_deals:
            key = d.address.lower().strip()
            if key and key not in seen_addresses:
                seen_addresses.add(key)
                all_deals.append(d)

        above = sum(1 for d in city_deals if d.score >= min_score)
        print(f"{len(city_deals)} found, {above} above {min_score} score")

    # Sort by score
    all_deals.sort(key=lambda d: d.score, reverse=True)
    return all_deals
