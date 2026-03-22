"""
PropFlow — Deal Scorer
Scores properties 0-100 based on hedge fund buy box criteria.
Equity spread (40pts) + distress signals (30pts) + property fit (30pts)
"""

from dataclasses import dataclass, field
from typing import Literal


@dataclass
class Deal:
    address:       str
    city:          str
    state:         str
    ask_price:     float
    arv:           float          # comp-based (median $/sqft) or Zestimate
    spread_pct:    float          # (arv - ask) / arv * 100
    spread_dollar: float          # arv - ask
    score:         int            # 0-100
    distress_type: str            # pre-foreclosure, price cut, REO, long DOM, etc.
    dom:           int            # days on market
    beds:          int
    baths:         float
    sqft:          int
    price_per_sqft: float
    year_built:    int   = 0
    est_rehab:     float = 0.0   # rough rehab cost estimate
    mao:           float = 0.0   # max allowable offer (ARV×70% − rehab)
    status:        str   = "New"
    source:        str   = ""
    url:           str   = ""
    date_found:    str   = ""


def score_deal(
    ask_price:     float,
    arv:           float,
    beds:          int,
    sqft:          int,
    dom:           int,
    distress_flags: list[str],    # e.g. ["pre-foreclosure", "price cut"]
    property_type: str = "SFR",
) -> tuple[int, str]:
    """
    Returns (score 0-100, distress_type string).
    """
    if arv <= 0 or ask_price <= 0:
        return 0, "unknown"

    spread_pct = (arv - ask_price) / arv * 100
    score = 0
    reasons = []

    # ── Equity spread (0-40 pts) ──────────────────────────────────────────────
    # Negative spread (ask > ARV) = overpriced, no equity points awarded
    if spread_pct >= 30:
        score += 40
    elif spread_pct >= 20:
        score += 30
    elif spread_pct >= 15:
        score += 20
    elif spread_pct >= 10:
        score += 10
    elif spread_pct <= 0:
        score += 0   # overpriced relative to comps

    # ── Distress signals (0-30 pts) ───────────────────────────────────────────
    distress_score = 0
    for flag in distress_flags:
        f = flag.lower()
        if "pre-foreclosure" in f or "foreclosure" in f:
            distress_score += 20
            reasons.append("pre-foreclosure")
        elif "reo" in f or "bank" in f or "owned" in f:
            distress_score += 15
            reasons.append("REO")
        elif "short sale" in f:
            distress_score += 12
            reasons.append("short sale")
        elif "price" in f and ("cut" in f or "reduc" in f or "drop" in f):
            distress_score += 10
            reasons.append("price cut")
        elif "hud" in f:
            distress_score += 15
            reasons.append("HUD")
        elif "auction" in f:
            distress_score += 15
            reasons.append("auction")

    if dom >= 90:
        distress_score += 15
        reasons.append(f"{dom}d on market")
    elif dom >= 60:
        distress_score += 10
        reasons.append(f"{dom}d on market")

    score += min(distress_score, 30)

    # ── Property fit (0-30 pts) ───────────────────────────────────────────────
    if beds >= 3:
        score += 10
    if property_type.upper() in ("SFR", "SINGLE FAMILY", "HOUSE"):
        score += 10
    if 80_000 <= ask_price <= 350_000:
        score += 10

    distress_label = ", ".join(reasons) if reasons else ("long DOM" if dom >= 60 else "none")
    return min(score, 100), distress_label


def est_rehab(sqft: int, distress_flags: list[str], year_built: int = 0) -> float:
    """
    Rough rehab cost estimate for a single-family home.
      Light distress (price cut, long DOM): ~$20/sqft
      Heavy distress (foreclosure, REO, HUD, auction): ~$35/sqft
    Age penalty added for old systems (electrical, plumbing, HVAC).
    """
    flags_str = " ".join(distress_flags).lower()
    heavy = any(w in flags_str for w in ("foreclosure", "reo", "hud", "auction"))
    psf   = 35 if heavy else 20
    base  = sqft * psf if sqft >= 500 else 15_000

    if year_built > 0:
        if year_built < 1960:
            base += 25_000
        elif year_built < 1980:
            base += 15_000
        elif year_built < 1990:
            base += 8_000

    return round(base, 0)


def calc_mao(arv: float, rehab: float, margin: float = 0.70) -> float:
    """
    Max Allowable Offer = ARV × 70% − rehab.
    Standard wholesale / fix-and-flip formula. Returns 0 if negative.
    """
    return max(0.0, round(arv * margin - rehab, 0))
