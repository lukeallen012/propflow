"""
PropFlow -- Property Scraper
Source: Zillow via RapidAPI ("Zillow.Com Realtime Scraper")
  Host: real-estate101.p.rapidapi.com

Setup:
  1. Sign up free at https://rapidapi.com
  2. Search "Zillow.Com Realtime Scraper" -> Subscribe (free tier)
  3. Copy your API key -> add to .env as RAPIDAPI_KEY=your_key_here
"""

import urllib.request
import urllib.parse
import json
import os
import time
from datetime import date
from dotenv import load_dotenv
from scorer import score_deal, Deal, est_rehab, calc_mao

load_dotenv()

RAPIDAPI_KEY  = os.getenv("RAPIDAPI_KEY", "")
ZILLOW_HOST        = "real-estate101.p.rapidapi.com"
ZILLOW_SEARCH      = f"https://{ZILLOW_HOST}/api/search"
ZILLOW_MAPBOUNDS   = f"https://{ZILLOW_HOST}/api/search/bymapbounds"

TARGET_MARKETS = [
    ("Atlanta",       "GA"), ("Charlotte",    "NC"), ("Dallas",       "TX"),
    ("Houston",       "TX"), ("Phoenix",       "AZ"), ("Tampa",         "FL"),
    ("Jacksonville",  "FL"), ("Orlando",       "FL"), ("Nashville",     "TN"),
    ("Memphis",       "TN"), ("Indianapolis",  "IN"), ("Columbus",      "OH"),
    ("Kansas City",   "MO"), ("San Antonio",   "TX"), ("Las Vegas",     "NV"),
    ("Raleigh",       "NC"), ("Birmingham",    "AL"), ("St. Louis",     "MO"),
    ("Cleveland",     "OH"), ("Cincinnati",    "OH"),
]

# Approximate metro bounding boxes: (north, south, east, west)
CITY_BOUNDS = {
    "Atlanta":      (33.93, 33.54, -84.10, -84.65),
    "Charlotte":    (35.40, 35.07, -80.67, -80.99),
    "Dallas":       (33.08, 32.55, -96.46, -97.09),
    "Houston":      (30.11, 29.52, -95.01, -95.79),
    "Phoenix":      (33.75, 33.28, -111.75, -112.43),
    "Tampa":        (28.07, 27.80, -82.26, -82.77),
    "Jacksonville": (30.54, 30.10, -81.33, -81.91),
    "Orlando":      (28.72, 28.37, -81.12, -81.55),
    "Nashville":    (36.40, 36.00, -86.52, -87.00),
    "Memphis":      (35.30, 35.00, -89.75, -90.22),
    "Indianapolis": (39.95, 39.63, -85.94, -86.37),
    "Columbus":     (40.16, 39.86, -82.80, -83.22),
    "Kansas City":  (39.20, 38.84, -94.35, -94.86),
    "San Antonio":  (29.65, 29.21, -98.24, -98.73),
    "Las Vegas":    (36.40, 36.01, -114.97, -115.41),
    "Raleigh":      (35.93, 35.65, -78.51, -78.82),
    "Birmingham":   (33.65, 33.40, -86.61, -86.98),
    "St. Louis":    (38.77, 38.48, -90.10, -90.50),
    "Cleveland":    (41.64, 41.35, -81.51, -81.89),
    "Cincinnati":   (39.33, 39.03, -84.29, -84.72),
}

PROP_TYPE_MAP = {
    "SINGLE_FAMILY": "SFR",
    "TOWNHOUSE": "SFR",
    "MANUFACTURED": "SFR",
    "CONDO": "Condo",
    "MULTI_FAMILY": "Multi",
    "LOT": "Land",
}


def _http_get(url: str, headers: dict = None, timeout: int = 15):
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
                **(headers or {}),
            },
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except Exception as e:
        print(f"  [scraper] HTTP error: {e}")
        return None


def _parse_prop(prop: dict, city: str, state: str) -> Deal | None:
    """Parse one property dict from the Zillow Realtime Scraper bymapbounds response."""
    try:
        ask        = float(prop.get("unformattedPrice", 0) or 0)
        zestimate  = float(prop.get("zestimate", 0) or 0)
        arv        = zestimate if zestimate > ask else ask * 1.15
        beds       = int(prop.get("beds", 0) or 0)
        baths      = float(prop.get("baths", 0) or 0)
        sqft       = int(prop.get("area", 0) or prop.get("livingArea", 0) or 0)
        dom        = int(prop.get("daysOnZillow", 0) or 0)
        zpid       = str(prop.get("id", ""))
        detail_url = prop.get("detailUrl", "")
        prop_type  = PROP_TYPE_MAP.get(prop.get("homeType", ""), "SFR")

        addr_obj = prop.get("address", {})
        addr     = addr_obj.get("street", "").strip()
        city_    = addr_obj.get("city", city).strip()
        state_   = addr_obj.get("state", state).strip()

        if ask <= 0 or not addr:
            return None

        # Distress signals from listing subtype and days on market
        flags = []
        sub = prop.get("listingSubType", {})
        if sub.get("is_foreclosure") or sub.get("is_bankOwned"):
            flags.append("pre-foreclosure")
        if sub.get("is_auction"):
            flags.append("auction")
        if prop.get("isPriceReduced") or prop.get("priceReduction"):
            flags.append("price cut")
        if dom >= 90:
            flags.append("long DOM")
        elif dom >= 60:
            flags.append("long DOM")

        score, distress = score_deal(ask, arv, beds, sqft, dom, flags, prop_type)
        price_per_sqft  = round(ask / sqft, 2) if sqft > 0 else 0.0
        rehab           = est_rehab(sqft, flags)
        mao             = calc_mao(arv, rehab)

        return Deal(
            address=addr,
            city=city_,
            state=state_,
            ask_price=ask,
            arv=arv,
            spread_pct=round((arv - ask) / arv * 100, 1) if arv > ask else 0.0,
            spread_dollar=round(arv - ask, 0),
            score=score,
            distress_type=distress,
            dom=dom,
            beds=beds,
            baths=baths,
            sqft=sqft,
            price_per_sqft=price_per_sqft,
            est_rehab=rehab,
            mao=mao,
            source="Zillow",
            url=detail_url,
            date_found=date.today().isoformat(),
        )
    except Exception:
        return None


def scrape_city(city: str, state: str) -> list[Deal]:
    """
    Scan one city via Zillow /api/search (bylocation).
    Falls back to bymapbounds if /api/search returns nothing.
    """
    if not RAPIDAPI_KEY:
        return []

    # Primary: /api/search with location string
    location = f"{city.lower().replace(' ', '-')}-{state.lower()}"
    url = (
        f"{ZILLOW_SEARCH}"
        f"?location={urllib.parse.quote(location)}"
        f"&isSingleFamily=true&isTownhouse=true"
        f"&minPrice=80000&maxPrice=350000&beds=3&page=1"
    )

    raw = _http_get(url, headers={
        "x-rapidapi-key":  RAPIDAPI_KEY,
        "x-rapidapi-host": ZILLOW_HOST,
    })
    if not raw:
        return []

    deals = _fetch_and_parse(raw, city, state)

    # Fallback to bymapbounds if bylocation returned nothing
    if not deals:
        bounds = CITY_BOUNDS.get(city)
        if bounds:
            north, south, east, west = bounds
            url2 = (
                f"{ZILLOW_MAPBOUNDS}"
                f"?north={north}&south={south}&east={east}&west={west}"
                f"&status_type=ForSale&home_type=Houses"
                f"&price_min=80000&price_max=350000&beds_min=3&page=1"
            )
            raw2 = _http_get(url2, headers={
                "x-rapidapi-key":  RAPIDAPI_KEY,
                "x-rapidapi-host": ZILLOW_HOST,
            })
            if raw2:
                deals = _fetch_and_parse(raw2, city, state)

    return deals


def _fetch_and_parse(raw: bytes, city: str, state: str) -> list[Deal]:
    deals = []
    try:
        data = json.loads(raw)
        msg = (data.get("message", "") or data.get("error", "") or "") if isinstance(data, dict) else ""
        if msg and any(w in msg.lower() for w in ("subscribe", "quota", "rate", "limit", "unauthoriz")):
            print(f"  [zillow] API issue: {msg[:100]}")
            return []
        props = data.get("results", []) if isinstance(data, dict) else []
        for prop in props:
            deal = _parse_prop(prop, city, state)
            if deal:
                deals.append(deal)
    except Exception as e:
        print(f"  [zillow] Parse error for {city}: {e}")
    return deals


# --- MAIN SCAN ----------------------------------------------------------------

def run_scan(min_score: int = 40, city_filter: str = None) -> list[Deal]:
    """
    Scan all target markets. Returns deals scored >= min_score, sorted by score.
    """
    if not RAPIDAPI_KEY:
        print()
        print("  !! No RAPIDAPI_KEY set -- no data sources available !!")
        print("  To get listings:")
        print("    1. Sign up free at https://rapidapi.com")
        print("    2. Search 'Zillow.Com Realtime Scraper' -> Subscribe (free tier)")
        print("    3. Copy your API key -> add to .env: RAPIDAPI_KEY=your_key")
        print()
        return []

    all_deals: list[Deal] = []
    seen_addresses: set[str] = set()

    markets = TARGET_MARKETS
    if city_filter:
        markets = [(c, s) for c, s in TARGET_MARKETS if c.lower() == city_filter.lower()]
        if not markets:
            print(f"  [warn] '{city_filter}' not in TARGET_MARKETS -- scanning all")
            markets = TARGET_MARKETS

    for city, state in markets:
        print(f"  Scanning {city}, {state}...", end=" ", flush=True)

        city_deals = scrape_city(city, state)
        time.sleep(0.4)  # stay under rate limit

        for d in city_deals:
            key = d.address.lower().strip()
            if key and key not in seen_addresses:
                seen_addresses.add(key)
                all_deals.append(d)

        above = sum(1 for d in city_deals if d.score >= min_score)
        print(f"{len(city_deals)} found, {above} >= score {min_score}")

    all_deals.sort(key=lambda d: d.score, reverse=True)
    return [d for d in all_deals if d.score >= min_score]
