"""Automated Deep-Dive Scoring — Phase 6 of the SEC Filing Intelligence Pipeline.

Reads top-N candidates from scr_discovery_flags, runs structured deep-dive
scoring across Modules 2, 3, 5, 6, 8, and 9 from the user's framework, and
outputs PASS / WATCHLIST / KILL verdicts.

CLI:
    python -m sec_filing_intelligence.deep_dive --run              # Top 50 deep-dives
    python -m sec_filing_intelligence.deep_dive --run --dry-run    # Skip report send
    python -m sec_filing_intelligence.deep_dive --ticker OPXS      # Single ticker
    python -m sec_filing_intelligence.deep_dive --latest            # Show latest verdicts
"""

import argparse
import csv
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from .config import (
    DB_PATH,
    DEEP_DIVE_ANALOG_STRONG,
    DEEP_DIVE_ANALOG_WEAK,
    DEEP_DIVE_MOAT_GATE,
    DEEP_DIVE_MOAT_STRONG,
    DEEP_DIVE_TOP_N,
    DISCOVERY_OUTPUT_DIR,
    TABLE_DEEP_DIVE_RESULTS,
    TABLE_DISCOVERY_FLAGS,
    TABLE_KILL_LIST,
)
from .db import get_connection, run_migration
from .utils import get_logger

logger = get_logger("deep_dive", "screener_deep_dive.log")


# ── Data Collection ─────────────────────────────────────────────────────────


def _collect_ticker_data(ticker: str) -> dict:
    """Collect all available data for a deep-dive candidate.

    Pulls from existing DB tables first, then does a fault-tolerant yfinance
    live lookup for supplemental pricing/financial data.

    Args:
        ticker: Stock ticker symbol.

    Returns:
        Dict with all available data fields for scoring.
    """
    data = {"ticker": ticker}

    with get_connection() as conn:
        # scr_discovery_flags (latest scan)
        row = conn.execute(
            f"""SELECT * FROM {TABLE_DISCOVERY_FLAGS}
                WHERE ticker = ?
                ORDER BY scan_date DESC LIMIT 1""",
            (ticker,),
        ).fetchone()
        if row:
            data["discovery_score"] = row["composite_score"]
            data["moat_types"] = row["moat_types"] or ""
            data["keywords_matched"] = row["keywords_matched"] or ""
            data["company_name"] = row["company_name"] or ""
            data["flag_count"] = row["flag_count"] or 0
            data["moat_type_count"] = row["moat_type_count"] or 0
            # Phase 2+ columns (may not exist in older scans)
            for col in ("sector", "market_cap", "analyst_count",
                        "gov_contracts", "customer_mentions", "self_claim_count"):
                try:
                    data[col] = row[col]
                except (IndexError, KeyError):
                    pass

        # scr_text_search_hits — all keyword hits
        hits = conn.execute(
            """SELECT keyword, keyword_layer, moat_type
               FROM scr_text_search_hits
               WHERE ticker = ?
               ORDER BY scan_date DESC""",
            (ticker,),
        ).fetchall()
        data["keyword_hits"] = [
            {"keyword": h["keyword"], "layer": h["keyword_layer"], "moat_type": h["moat_type"]}
            for h in hits
        ]

        # scr_moat_signals
        moats = conn.execute(
            """SELECT moat_type, signal_source, signal_strength, evidence
               FROM scr_moat_signals
               WHERE ticker = ?
               ORDER BY scan_date DESC""",
            (ticker,),
        ).fetchall()
        data["moat_signals"] = [dict(m) for m in moats]

        # scr_universe
        uni = conn.execute(
            "SELECT company_name, exchange, sector, industry FROM scr_universe WHERE ticker = ?",
            (ticker,),
        ).fetchone()
        if uni:
            if not data.get("company_name"):
                data["company_name"] = uni["company_name"] or ""
            data["exchange"] = uni["exchange"]
            data["sector"] = data.get("sector") or uni["sector"]
            data["industry"] = uni["industry"]

        # scr_fundamentals (latest)
        try:
            fund = conn.execute(
                """SELECT revenue_ttm, revenue_growth_yoy, gross_margin, operating_margin,
                          cash_and_equivalents, total_debt, debt_to_equity,
                          cash_runway_quarters, shares_outstanding,
                          institutional_ownership_pct
                   FROM scr_fundamentals
                   WHERE ticker = ?
                   ORDER BY date DESC LIMIT 1""",
                (ticker,),
            ).fetchone()
        except Exception:
            # Column may not exist in older schemas — fall back to subset
            fund = conn.execute(
                """SELECT revenue_ttm, revenue_growth_yoy, gross_margin, operating_margin,
                          cash_and_equivalents, total_debt, debt_to_equity,
                          cash_runway_quarters, shares_outstanding
                   FROM scr_fundamentals
                   WHERE ticker = ?
                   ORDER BY date DESC LIMIT 1""",
                (ticker,),
            ).fetchone()
        if fund:
            data["revenue"] = fund["revenue_ttm"]
            data["revenue_growth"] = fund["revenue_growth_yoy"]
            data["gross_margin"] = fund["gross_margin"]
            data["operating_margin"] = fund["operating_margin"]
            data["cash"] = fund["cash_and_equivalents"]
            data["total_debt"] = fund["total_debt"]
            data["debt_to_equity"] = fund["debt_to_equity"]
            data["cash_runway"] = fund["cash_runway_quarters"]
            data["shares_outstanding"] = fund["shares_outstanding"]
            try:
                data["institutional_ownership"] = fund["institutional_ownership_pct"]
            except (IndexError, KeyError):
                pass

        # scr_insider_transactions (last 180 days)
        cutoff = (date.today() - timedelta(days=180)).isoformat()
        insiders = conn.execute(
            """SELECT insider_name, insider_title, transaction_type,
                      shares, total_value, is_open_market, filing_date
               FROM scr_insider_transactions
               WHERE ticker = ? AND filing_date >= ?
               ORDER BY filing_date DESC""",
            (ticker, cutoff),
        ).fetchall()
        data["insider_transactions"] = [dict(i) for i in insiders]

        # scr_patent_summary
        pat = conn.execute(
            "SELECT total_patents, active_patents, primary_technology_area FROM scr_patent_summary WHERE ticker = ?",
            (ticker,),
        ).fetchone()
        if pat:
            data["patent_count"] = pat["total_patents"] or 0
            data["active_patents"] = pat["active_patents"] or 0
            data["technology_area"] = pat["primary_technology_area"]
        else:
            data["patent_count"] = 0
            data["active_patents"] = 0

        # scr_price_metrics
        pm = conn.execute(
            """SELECT current_price, all_time_high, high_52w, low_52w,
                      pct_from_ath, pct_from_52w_high,
                      avg_volume_30d, avg_volume_10d, float_shares,
                      short_interest_pct
               FROM scr_price_metrics WHERE ticker = ?""",
            (ticker,),
        ).fetchone()
        if pm:
            data["current_price"] = pm["current_price"]
            data["all_time_high"] = pm["all_time_high"]
            data["high_52w"] = pm["high_52w"]
            data["low_52w"] = pm["low_52w"]
            data["pct_from_ath"] = pm["pct_from_ath"]
            data["pct_from_52w_high"] = pm["pct_from_52w_high"]
            data["avg_volume_30d"] = pm["avg_volume_30d"]
            data["avg_volume_10d"] = pm["avg_volume_10d"]
            data["float_shares"] = pm["float_shares"]
            data["short_pct"] = pm["short_interest_pct"]

        # scr_analyst_coverage (latest)
        ac = conn.execute(
            """SELECT analyst_count FROM scr_analyst_coverage
               WHERE ticker = ?
               ORDER BY date DESC LIMIT 1""",
            (ticker,),
        ).fetchone()
        if ac:
            data["analyst_count"] = data.get("analyst_count") or ac["analyst_count"]

        # scr_ampx_red_flags (going concern)
        rf = conn.execute(
            """SELECT flag_type FROM scr_ampx_red_flags
               WHERE ticker = ?
               ORDER BY scan_date DESC LIMIT 5""",
            (ticker,),
        ).fetchall()
        data["red_flags"] = [r["flag_type"] for r in rf]

    # yfinance live lookup (fault-tolerant)
    data = _yfinance_enrich(ticker, data)

    return data


def _yfinance_enrich(ticker: str, data: dict) -> dict:
    """Enrich data dict with live yfinance info. Fault-tolerant: never raises."""
    try:
        import yfinance as yf

        t = yf.Ticker(ticker)
        info = t.info or {}

        # Fill in gaps only — don't overwrite DB data with potentially stale yf data
        if not data.get("current_price"):
            data["current_price"] = info.get("currentPrice") or info.get("regularMarketPrice")
        if not data.get("market_cap"):
            mc = info.get("marketCap")
            if mc:
                data["market_cap"] = mc / 1_000_000  # Store in millions
        if not data.get("shares_outstanding"):
            data["shares_outstanding"] = info.get("sharesOutstanding")
        if not data.get("float_shares"):
            data["float_shares"] = info.get("floatShares")
        if not data.get("revenue_growth"):
            data["revenue_growth"] = info.get("revenueGrowth")
            if data.get("revenue_growth"):
                data["revenue_growth"] = data["revenue_growth"] * 100  # pct
        if not data.get("gross_margin"):
            data["gross_margin"] = info.get("grossMargins")
        if not data.get("operating_margin"):
            data["operating_margin"] = info.get("operatingMargins")
        if not data.get("cash"):
            data["cash"] = info.get("totalCash")
        if not data.get("total_debt"):
            data["total_debt"] = info.get("totalDebt")
        if not data.get("debt_to_equity"):
            de = info.get("debtToEquity")
            if de is not None:
                data["debt_to_equity"] = de / 100.0  # yf returns as pct
        if not data.get("analyst_count"):
            data["analyst_count"] = info.get("numberOfAnalystOpinions")
        if not data.get("institutional_ownership"):
            data["institutional_ownership"] = info.get("heldPercentInstitutions")
        if not data.get("short_pct"):
            data["short_pct"] = info.get("shortPercentOfFloat")

    except Exception as e:
        logger.debug("yfinance enrichment failed for %s: %s", ticker, e)

    return data


# ── Module 2: Moat Assessment (GATE — 0-25) ─────────────────────────────────


def _score_moat(data: dict) -> dict:
    """Score moat across 5 sub-dimensions (0-5 each, total 0-25).

    2A: Proprietary IP (patent count)
    2B: Switching Costs (keyword signals)
    2C: Network Effects (keyword signals)
    2D: Regulatory Barrier (keyword + moat type signals)
    2E: Cost Advantage (infrastructure + manufacturing signals)

    Returns dict with total score, sub-scores, pass/fail, and evidence.
    """
    keywords_lower = (data.get("keywords_matched") or "").lower()
    moat_types = (data.get("moat_types") or "").lower()
    keyword_hits = data.get("keyword_hits", [])
    keyword_set = {h["keyword"].lower() for h in keyword_hits}
    gov_contracts = data.get("gov_contracts") or 0

    evidence = []

    # 2A: Proprietary IP
    patent_count = data.get("patent_count", 0) or 0
    if patent_count >= 50:
        score_2a = 5
    elif patent_count >= 31:
        score_2a = 4
    elif patent_count >= 16:
        score_2a = 3
    elif patent_count >= 6:
        score_2a = 2
    elif patent_count >= 1:
        score_2a = 1
    else:
        score_2a = 0
    if patent_count > 0:
        evidence.append(f"2A: {patent_count} patents -> {score_2a}/5")
    else:
        evidence.append("2A: 0 patents (UNVERIFIED if not in patent DB)")

    # 2B: Switching Costs
    score_2b = 0
    if any(k in keyword_set for k in ("switching costs",)):
        score_2b = max(score_2b, 4)
        evidence.append("2B: 'switching costs' keyword hit")
    if any(k in keyword_set for k in ("mission-critical", "embedded in")):
        score_2b = max(score_2b, 3)
        evidence.append("2B: mission-critical/embedded keyword")
    if gov_contracts > 0:
        score_2b = min(score_2b + 1, 5)
        evidence.append("2B: +1 gov contract switching cost")

    # 2C: Network Effects
    score_2c = 0
    if any(k in keyword_set for k in ("network effect",)):
        score_2c = max(score_2c, 4)
        evidence.append("2C: 'network effect' keyword hit")
    if any(k in keyword_set for k in ("proprietary dataset",)):
        score_2c = max(score_2c, 3)
        evidence.append("2C: 'proprietary dataset' keyword hit")

    # 2D: Regulatory Barrier
    score_2d = 0
    if "regulatory" in moat_types:
        score_2d = max(score_2d, 2)
    itar_kw = {"itar", "nrc licensed", "fcc license", "faa certified"}
    if keyword_set & itar_kw:
        score_2d = max(score_2d, 4)
        evidence.append(f"2D: regulatory keyword {keyword_set & itar_kw}")
    if any(k in keyword_set for k in ("sole source",)) and gov_contracts > 0:
        score_2d = 5
        evidence.append("2D: sole source + gov contract -> 5")
    if any(k in keyword_set for k in ("security clearance",)):
        score_2d = max(score_2d, 4)
        evidence.append("2D: security clearance -> 4")

    # 2E: Cost Advantage
    score_2e = 0
    if "infrastructure" in moat_types:
        score_2e = max(score_2e, 2)
    if any(k in keyword_set for k in ("only manufacturer", "only facility")):
        score_2e = max(score_2e, 4)
        evidence.append("2E: only manufacturer/facility -> 4")
    if any(k in keyword_set for k in ("vertically integrated",)):
        score_2e = max(score_2e, 3)
        evidence.append("2E: vertically integrated -> 3")
    if any(k in keyword_set for k in ("only domestic", "only u.s.")):
        score_2e = 5
        evidence.append("2E: only domestic/U.S. -> 5")

    total = score_2a + score_2b + score_2c + score_2d + score_2e
    gate_pass = total >= DEEP_DIVE_MOAT_GATE

    return {
        "moat_score": total,
        "moat_max": 25,
        "gate_pass": gate_pass,
        "sub_scores": {
            "2A_ip": score_2a,
            "2B_switching": score_2b,
            "2C_network": score_2c,
            "2D_regulatory": score_2d,
            "2E_cost": score_2e,
        },
        "evidence": evidence,
    }


# ── Module 3: Analog Pattern Match (0-8 per analog) ─────────────────────────


def _score_analogs(data: dict) -> dict:
    """Score ticker against 4 analog DNA patterns (AMPX, RKLB, ASTS, NBIS).

    Each analog has 8 binary criteria. Best match = highest count.
    5+/8 = strong match, 4/8 = weak match.

    Returns dict with per-analog scores and best match info.
    """
    keywords_lower = (data.get("keywords_matched") or "").lower()
    moat_types = (data.get("moat_types") or "").lower()
    keyword_hits = data.get("keyword_hits", [])
    keyword_set = {h["keyword"].lower() for h in keyword_hits}

    pct_ath = data.get("pct_from_ath") or 0
    revenue_growth = data.get("revenue_growth") or 0
    de = data.get("debt_to_equity")
    analyst_count = data.get("analyst_count") or 0
    customer_mentions = data.get("customer_mentions") or 0
    gov_contracts = data.get("gov_contracts") or 0
    moat_type_count = data.get("moat_type_count") or 0
    sector = (data.get("sector") or "").lower()
    industry = (data.get("industry") or "").lower()
    patent_count = data.get("patent_count") or 0
    inst_own = data.get("institutional_ownership") or 0
    market_cap = data.get("market_cap") or 0
    cash = data.get("cash") or 0
    cash_runway = data.get("cash_runway") or 0
    revenue = data.get("revenue") or 0

    analogs = {}

    # AMPX DNA (post-hype washout)
    ampx = 0
    if pct_ath and pct_ath <= -80:
        ampx += 1
    if customer_mentions and customer_mentions > 0:
        ampx += 1
    if revenue_growth and revenue_growth > 100:
        ampx += 1
    if moat_type_count >= 3:
        ampx += 1
    if keyword_set & {"production", "capacity", "production ramp", "capacity expansion complete"}:
        ampx += 1
    if de is not None and de <= 0.3:
        ampx += 1
    if analyst_count <= 2:
        ampx += 1
    if de is not None and de == 0 and cash_runway and cash_runway > 6:
        ampx += 1
    analogs["AMPX"] = ampx

    # RKLB DNA (sole alternative)
    rklb = 0
    if keyword_set & {"sole source", "only provider"}:
        rklb += 1
    if revenue and revenue > 0:
        rklb += 1
    if keyword_set & {"proprietary technology"} and keyword_set & {"patent portfolio"}:
        rklb += 1
    if "vertically integrated" in keyword_set:
        rklb += 1
    if gov_contracts and gov_contracts > 0:
        rklb += 1
    if moat_type_count >= 2 and "government" in moat_types:
        rklb += 1
    if any(s in sector for s in ("defense", "industrials")) or any(
        s in industry for s in ("defense", "space", "aerospace")
    ):
        rklb += 1
    if inst_own and inst_own < 0.40:
        rklb += 1
    analogs["RKLB"] = rklb

    # ASTS DNA (patent fortress moonshot)
    asts = 0
    if patent_count > 30:
        asts += 1
    if keyword_set & {"fcc license", "spectrum"}:
        asts += 1
    if customer_mentions and customer_mentions >= 3:
        asts += 1
    if "regulatory" in moat_types:
        asts += 1
    if market_cap and market_cap > 500:
        asts += 1
    if revenue_growth and revenue_growth > 200:
        asts += 1
    # Criterion 7: dated catalyst — hard to auto-detect, default 0
    if cash and cash > 500_000_000:
        asts += 1
    analogs["ASTS"] = asts

    # NBIS DNA (AI/infra picks-and-shovels)
    nbis = 0
    if "infrastructure" in moat_types:
        nbis += 1
    if revenue and revenue > 100_000_000:
        nbis += 1
    if "supply_chain" in moat_types:
        nbis += 1
    sector_match = any(
        s in sector for s in ("semiconductor", "technology", "industrial")
    ) or any(s in industry for s in ("semiconductor", "technology", "industrial"))
    if sector_match:
        nbis += 1
    if analyst_count and analyst_count < 5:
        nbis += 1
    if revenue_growth and revenue_growth > 30:
        nbis += 1
    if moat_type_count >= 3:
        nbis += 1
    if keyword_set & {"capacity constrained", "limited availability"}:
        nbis += 1
    analogs["NBIS"] = nbis

    best_name = max(analogs, key=analogs.get)
    best_score = analogs[best_name]

    if best_score < DEEP_DIVE_ANALOG_WEAK:
        best_name = "NONE"

    return {
        "analogs": analogs,
        "best_analog": best_name,
        "best_analog_score": best_score,
        "strong_match": best_score >= DEEP_DIVE_ANALOG_STRONG,
        "weak_match": best_score >= DEEP_DIVE_ANALOG_WEAK,
    }


# ── Module 5: Balance Sheet (pass/fail) ─────────────────────────────────────


def _score_balance_sheet(data: dict) -> dict:
    """Check balance sheet health: cash runway, debt, going concern.

    Returns dict with pass/fail, flags list, and key metrics.
    """
    flags = []
    hard_kill = False

    cash_runway = data.get("cash_runway")
    de = data.get("debt_to_equity")
    current_ratio = data.get("current_ratio")
    red_flags = data.get("red_flags", [])

    # Going concern = HARD KILL
    if any("going_concern" in f for f in red_flags):
        flags.append("HARD KILL: going concern language in 10-K")
        hard_kill = True

    # Cash runway check
    if cash_runway is not None and cash_runway < 6:
        flags.append(f"FAIL: cash runway {cash_runway:.1f} quarters < 6")
        hard_kill = True
    elif cash_runway is not None:
        flags.append(f"OK: cash runway {cash_runway:.1f} quarters")

    # Debt check
    if de is not None and de > 1.0:
        flags.append(f"RED FLAG: D/E ratio {de:.2f} > 1.0")
    elif de is not None:
        flags.append(f"OK: D/E ratio {de:.2f}")

    # Current ratio
    if current_ratio is not None and current_ratio < 1.5:
        flags.append(f"YELLOW FLAG: current ratio {current_ratio:.2f} < 1.5")

    return {
        "pass": not hard_kill,
        "flags": flags,
        "cash_runway": cash_runway,
        "de_ratio": de,
    }


# ── Module 6: Insider Forensics (pass/fail) ─────────────────────────────────


def _score_insider(data: dict) -> dict:
    """Analyze insider transactions for buy/sell signals.

    Returns dict with pass/fail, CEO buying flag, cluster selling flag.
    """
    transactions = data.get("insider_transactions", [])

    buy_count = 0
    sell_count = 0
    ceo_buying = False
    distinct_sellers_60d = set()
    cutoff_60d = (date.today() - timedelta(days=60)).isoformat()

    for t in transactions:
        tx_type = (t.get("transaction_type") or "").lower()
        is_open = t.get("is_open_market", 0)
        title = (t.get("insider_title") or "").lower()
        filing_date = t.get("filing_date") or ""
        name = t.get("insider_name") or ""

        if tx_type == "purchase" and is_open:
            buy_count += 1
            if "ceo" in title or "chief executive" in title:
                ceo_buying = True
        elif tx_type == "sale" or tx_type == "sell":
            sell_count += 1
            if filing_date >= cutoff_60d:
                # C-suite check
                c_suite = any(
                    role in title
                    for role in ("ceo", "cfo", "coo", "cto", "chief", "president", "director")
                )
                if c_suite:
                    distinct_sellers_60d.add(name)

    cluster_selling = len(distinct_sellers_60d) >= 3
    hard_kill = cluster_selling

    return {
        "pass": not hard_kill,
        "ceo_buying": ceo_buying,
        "cluster_selling": cluster_selling,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "distinct_sellers_60d": len(distinct_sellers_60d),
    }


# ── Module 8: Washout & Phase Assessment ────────────────────────────────────


def _score_washout(data: dict) -> dict:
    """Assess washout phase (1-5) based on price, volume, and analyst coverage.

    Phase 1: Capitulation (deep crash, no coverage)
    Phase 2: Base building (stabilizing, minimal coverage)
    Phase 3: Early recognition (analysts arriving, volume picking up)
    Phase 4: Growth confirmation (consensus building)
    Phase 5: Fully discovered (broad coverage, near highs)

    Returns dict with phase number, name, and entry_ok flag.
    """
    pct_ath = data.get("pct_from_ath") or 0
    pct_52w = data.get("pct_from_52w_high") or 0
    analyst_count = data.get("analyst_count") or 0
    vol_10d = data.get("avg_volume_10d") or 0
    vol_30d = data.get("avg_volume_30d") or 0

    # Start with price-based phase
    if pct_ath and pct_ath <= -80:
        phase = 1
    elif pct_52w and pct_52w <= -60:
        phase = 1
    elif pct_52w and pct_52w <= -30:
        phase = 2
    elif pct_52w and pct_52w > -30:
        phase = 4
    else:
        phase = 3  # default if no price data

    # Adjust by analyst coverage
    if analyst_count == 0:
        phase = min(phase, 2)
    elif analyst_count <= 3:
        phase = min(phase, 3)
    elif analyst_count > 8:
        phase = max(phase, 5)

    # Volume trend
    volume_increasing = False
    if vol_10d and vol_30d and vol_30d > 0:
        volume_increasing = (vol_10d / vol_30d) > 1.2

    phase_names = {
        1: "Capitulation",
        2: "Base Building",
        3: "Early Recognition",
        4: "Growth Confirmation",
        5: "Fully Discovered",
    }

    return {
        "phase": phase,
        "phase_name": phase_names.get(phase, "Unknown"),
        "entry_ok": phase <= 3,
        "volume_increasing": volume_increasing,
    }


# ── Module 9: Hard Disqualifiers ────────────────────────────────────────────


def _check_disqualifiers(data: dict, insider_result: dict) -> dict:
    """Check 8 hard disqualifiers (some auto-detected, some UNVERIFIED).

    Returns dict with pass/fail, kills list, and unverified list.
    """
    kills = []
    unverified = []

    # 1. Acquisition-driven growth
    unverified.append("acquisition_driven_growth")

    # 2. Going concern
    red_flags = data.get("red_flags", [])
    if any("going_concern" in f for f in red_flags):
        kills.append("going_concern")

    # 3. Clustered selling (from Module 6)
    if insider_result.get("cluster_selling"):
        kills.append("clustered_insider_selling")

    # 4. Dilution >30%
    # Check scr_fundamentals_xbrl for share count change if available
    try:
        with get_connection() as conn:
            xbrl = conn.execute(
                """SELECT shares_outstanding_change_2yr_pct
                   FROM scr_fundamentals_xbrl
                   WHERE ticker = ?
                   ORDER BY fiscal_year DESC LIMIT 1""",
                (data["ticker"],),
            ).fetchone()
            if xbrl and xbrl["shares_outstanding_change_2yr_pct"] is not None:
                if xbrl["shares_outstanding_change_2yr_pct"] > 30:
                    kills.append(f"dilution_{xbrl['shares_outstanding_change_2yr_pct']:.0f}pct")
    except Exception:
        pass

    # 5. Float < 7M shares
    float_shares = data.get("float_shares")
    if float_shares is not None and float_shares < 7_000_000:
        kills.append(f"float_too_small_{float_shares/1e6:.1f}M")

    # 6. Revenue-per-share declining — hard to verify without history
    unverified.append("revenue_per_share_declining")

    # 7. Promotional CEO
    unverified.append("promotional_ceo")

    # 8. Auditor downgrade
    unverified.append("auditor_downgrade")

    return {
        "pass": len(kills) == 0,
        "kills": kills,
        "unverified": unverified,
    }


# ── Module 10: Expected Value Estimation ─────────────────────────────────────


def _estimate_expected_value(data: dict, moat: dict, analogs: dict,
                             washout: dict) -> dict:
    """Estimate probability-weighted expected value based on available signals.

    Uses moat strength, analog match, phase, and fundamentals to construct
    rough bull/base/bear scenarios. NOT a precise valuation — a scoring heuristic.
    """
    moat_score = moat.get("total", 0)
    best_analog_score = analogs.get("best_analog_score", 0)
    phase = washout.get("phase", 3)
    revenue_growth = data.get("revenue_growth") or 0
    de = data.get("debt_to_equity")
    pct_from_high = data.get("pct_from_52w_high") or 0

    # Bull case probability: higher moat + stronger analog + earlier phase = higher P(bull)
    p_bull = min(0.05 * moat_score + 0.03 * best_analog_score + 0.05 * max(0, 4 - phase), 0.50)
    # Bear case probability: inverse of bull signals
    p_bear = max(0.10, 0.40 - 0.02 * moat_score - 0.02 * best_analog_score)
    p_base = 1.0 - p_bull - p_bear

    # Return estimates based on phase and crash depth
    if pct_from_high and pct_from_high <= -70:
        bull_return = 5.0  # 500% from deep washout
        base_return = 0.5  # 50% recovery
        bear_return = -0.5  # another 50% down
    elif pct_from_high and pct_from_high <= -40:
        bull_return = 3.0
        base_return = 0.3
        bear_return = -0.4
    else:
        bull_return = 2.0
        base_return = 0.1
        bear_return = -0.3

    # Boost bull if strong revenue growth
    if revenue_growth > 100:
        bull_return *= 1.3
    elif revenue_growth > 50:
        bull_return *= 1.1

    # Penalize if heavy debt
    if de is not None and de > 5:
        bear_return *= 1.5

    ev = p_bull * bull_return + p_base * base_return + p_bear * bear_return
    p_2x = p_bull * (1.0 if bull_return >= 2.0 else 0.5)

    return {
        "expected_value": round(ev * 100, 1),  # as percentage
        "p_bull": round(p_bull * 100, 1),
        "p_base": round(p_base * 100, 1),
        "p_bear": round(p_bear * 100, 1),
        "bull_return": round(bull_return * 100, 0),
        "base_return": round(base_return * 100, 0),
        "bear_return": round(bear_return * 100, 0),
        "p_2x": round(p_2x * 100, 1),
        "ev_pass": ev > 0.30 and p_2x > 0.15,
    }


# ── Verdict Logic ───────────────────────────────────────────────────────────


def _compute_verdict(
    moat: dict,
    analogs: dict,
    balance: dict,
    insider: dict,
    washout: dict,
    disqualifiers: dict,
) -> dict:
    """Compute final PASS / WATCHLIST / KILL verdict.

    Decision tree:
    1. Any hard disqualifier -> KILL
    2. Moat score < gate (8/25) -> KILL
    3. Phase > 3 -> WATCHLIST at best
    4. Clustered insider selling -> KILL
    5. Strong analog (5+/8) + moat >= 12/25 + no kills -> PASS
    6. Weak analog (4+/8) + moat >= 8/25 -> WATCHLIST
    7. Everything else -> KILL

    Returns dict with verdict, reason, and all module results.
    """
    reasons = []

    # Check hard disqualifiers first
    if not disqualifiers["pass"]:
        return {
            "verdict": "KILL",
            "reason": f"Hard disqualifier: {', '.join(disqualifiers['kills'])}",
        }

    # Moat gate
    if not moat["gate_pass"]:
        return {
            "verdict": "KILL",
            "reason": f"Moat score {moat['moat_score']}/25 < gate {DEEP_DIVE_MOAT_GATE}",
        }

    # Clustered selling
    if insider.get("cluster_selling"):
        return {
            "verdict": "KILL",
            "reason": "Clustered C-suite selling (3+ in 60 days)",
        }

    # Balance sheet failure
    if not balance["pass"]:
        return {
            "verdict": "KILL",
            "reason": f"Balance sheet fail: {'; '.join(balance['flags'])}",
        }

    # Phase check
    phase_cap = None
    if not washout["entry_ok"]:
        phase_cap = "WATCHLIST"
        reasons.append(f"Phase {washout['phase']} ({washout['phase_name']}) - late entry")

    # Analog + moat scoring
    best_analog = analogs["best_analog_score"]
    moat_score = moat["moat_score"]

    if best_analog >= DEEP_DIVE_ANALOG_STRONG and moat_score >= DEEP_DIVE_MOAT_STRONG:
        verdict = "PASS" if phase_cap is None else phase_cap
        reasons.append(
            f"Strong analog {analogs['best_analog']} ({best_analog}/8) + "
            f"moat {moat_score}/25"
        )
    elif best_analog >= DEEP_DIVE_ANALOG_WEAK and moat_score >= DEEP_DIVE_MOAT_GATE:
        verdict = "WATCHLIST"
        reasons.append(
            f"Weak analog {analogs['best_analog']} ({best_analog}/8) + "
            f"moat {moat_score}/25"
        )
    else:
        verdict = "KILL"
        reasons.append(
            f"No strong analog match (best: {analogs['best_analog']} {best_analog}/8, "
            f"moat {moat_score}/25)"
        )

    return {
        "verdict": verdict,
        "reason": "; ".join(reasons),
    }


# ── Build Summary ───────────────────────────────────────────────────────────


def _build_summary(data: dict, moat: dict, analogs: dict, balance: dict,
                   insider: dict, washout: dict, disqualifiers: dict,
                   verdict_result: dict) -> str:
    """Build a one-line summary for DB storage and reporting."""
    parts = [
        f"Verdict={verdict_result['verdict']}",
        f"Moat={moat['moat_score']}/25",
        f"Analog={analogs['best_analog']}({analogs['best_analog_score']}/8)",
        f"Phase={washout['phase']}({washout['phase_name']})",
    ]
    if insider["ceo_buying"]:
        parts.append("CEO-BUY")
    if insider["cluster_selling"]:
        parts.append("CLUSTER-SELL")
    if not balance["pass"]:
        parts.append("BS-FAIL")
    if disqualifiers["kills"]:
        parts.append(f"DISQ={','.join(disqualifiers['kills'])}")
    parts.append(f"Reason: {verdict_result['reason']}")
    return " | ".join(parts)


# ── Single Ticker Deep-Dive ─────────────────────────────────────────────────


def run_single_deep_dive(ticker: str) -> dict:
    """Run deep-dive analysis on a single ticker.

    Args:
        ticker: Stock ticker symbol.

    Returns:
        Dict with all module results, verdict, and summary.
    """
    run_migration()

    logger.info("Running deep-dive for %s", ticker)
    data = _collect_ticker_data(ticker)

    moat = _score_moat(data)
    analogs = _score_analogs(data)
    balance = _score_balance_sheet(data)
    insider = _score_insider(data)
    washout = _score_washout(data)
    disqualifiers = _check_disqualifiers(data, insider)
    verdict_result = _compute_verdict(moat, analogs, balance, insider, washout, disqualifiers)
    ev = _estimate_expected_value(data, moat, analogs, washout)
    summary = _build_summary(data, moat, analogs, balance, insider, washout, disqualifiers, verdict_result)

    return {
        "ticker": ticker,
        "data": data,
        "moat": moat,
        "analogs": analogs,
        "balance_sheet": balance,
        "insider": insider,
        "washout": washout,
        "disqualifiers": disqualifiers,
        "verdict": verdict_result["verdict"],
        "reason": verdict_result["reason"],
        "expected_value": ev,
        "summary": summary,
    }


# ── Batch Deep-Dive Runner ──────────────────────────────────────────────────


def run_deep_dives(top_n: int = DEEP_DIVE_TOP_N, dry_run: bool = False) -> dict:
    """Run deep-dive analysis on top N discovery candidates.

    1. Load latest discovery flags from DB
    2. Sort by score desc, take top_n
    3. Score all modules per ticker, compute verdict
    4. Store results in scr_deep_dive_results
    5. Format scan report
    6. Write CSV

    Args:
        top_n: Number of top candidates to deep-dive.
        dry_run: If True, skip report send.

    Returns:
        Summary dict with counts per verdict, csv_path, and results.
    """
    run_migration()

    scan_date = date.today().isoformat()

    # 1. Load latest discovery flags
    with get_connection() as conn:
        latest_row = conn.execute(
            f"SELECT MAX(scan_date) AS latest FROM {TABLE_DISCOVERY_FLAGS}"
        ).fetchone()
        if not latest_row or not latest_row["latest"]:
            logger.warning("No discovery results found — run discovery first")
            return {"error": "No discovery results found"}

        latest_scan = latest_row["latest"]

        # Filter out kill list tickers
        rows = conn.execute(
            f"""SELECT ticker, composite_score, company_name
                FROM {TABLE_DISCOVERY_FLAGS}
                WHERE scan_date = ?
                  AND ticker NOT IN (SELECT ticker FROM {TABLE_KILL_LIST})
                ORDER BY composite_score DESC
                LIMIT ?""",
            (latest_scan, top_n),
        ).fetchall()

    if not rows:
        logger.warning("No candidates found for deep-dive")
        return {"error": "No candidates found"}

    tickers = [(r["ticker"], r["composite_score"], r["company_name"]) for r in rows]
    logger.info("Running deep-dives on %d candidates (discovery scan %s)", len(tickers), latest_scan)

    # 2. Run deep-dives
    results = []
    for i, (ticker, disc_score, company) in enumerate(tickers, 1):
        logger.info("[%d/%d] Deep-diving %s (score %.1f)", i, len(tickers), ticker, disc_score)
        try:
            result = run_single_deep_dive(ticker)
            result["discovery_score"] = disc_score
            result["company_name"] = company
            results.append(result)
        except Exception as e:
            logger.error("Deep-dive failed for %s: %s", ticker, e)
            results.append({
                "ticker": ticker,
                "verdict": "ERROR",
                "reason": str(e),
                "discovery_score": disc_score,
            })

    # 3. Store results
    _store_deep_dive_results(results, scan_date)

    # 4. Format report
    report = _format_deep_dive_report(results, scan_date)
    if dry_run:
        print(report)
    else:
        logger.info("Deep-dive report:\n%s", report)

    # 5. Write CSV
    csv_path = _write_deep_dive_csv(results, scan_date)

    # 6. Summary
    verdicts = {"PASS": 0, "WATCHLIST": 0, "KILL": 0, "ERROR": 0}
    for r in results:
        v = r.get("verdict", "ERROR")
        verdicts[v] = verdicts.get(v, 0) + 1

    summary = {
        "scan_date": scan_date,
        "discovery_scan_date": latest_scan,
        "total_analyzed": len(results),
        "verdicts": verdicts,
        "csv_path": str(csv_path),
        "results": results,
    }

    logger.info("Deep-dive complete: %s", {k: v for k, v in summary.items() if k != "results"})
    return summary


# ── Storage ─────────────────────────────────────────────────────────────────


def _store_deep_dive_results(results: list[dict], scan_date: str) -> int:
    """Store deep-dive results in scr_deep_dive_results table.

    Args:
        results: List of deep-dive result dicts.
        scan_date: ISO date string.

    Returns:
        Number of rows stored.
    """
    stored = 0
    with get_connection() as conn:
        for r in results:
            ticker = r.get("ticker", "")
            verdict = r.get("verdict", "ERROR")
            moat = r.get("moat", {})
            analogs = r.get("analogs", {})
            balance = r.get("balance_sheet", {})
            insider = r.get("insider", {})
            washout = r.get("washout", {})
            disq = r.get("disqualifiers", {})

            conn.execute(
                f"""INSERT OR REPLACE INTO {TABLE_DEEP_DIVE_RESULTS}
                    (ticker, scan_date, verdict, moat_score, moat_details,
                     best_analog, analog_match_count, balance_sheet_pass,
                     insider_pass, phase, phase_name, disqualifier_kills,
                     discovery_score, summary)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ticker,
                    scan_date,
                    verdict,
                    moat.get("moat_score"),
                    json.dumps(moat.get("sub_scores")) if moat.get("sub_scores") else None,
                    analogs.get("best_analog"),
                    analogs.get("best_analog_score"),
                    1 if balance.get("pass") else 0,
                    1 if insider.get("pass") else 0,
                    washout.get("phase"),
                    washout.get("phase_name"),
                    json.dumps(disq.get("kills")) if disq.get("kills") else None,
                    r.get("discovery_score"),
                    r.get("summary"),
                ),
            )
            stored += 1
        conn.commit()

    logger.info("Stored %d deep-dive results for %s", stored, scan_date)
    return stored


# ── Reporting ───────────────────────────────────────────────────────────────


def _format_deep_dive_report(results: list[dict], scan_date: str) -> str:
    """Format deep-dive results as a formatted report."""
    lines = [
        f"DEEP-DIVE VERDICTS ({scan_date})",
        f"Analyzed: {len(results)} candidates",
        "",
    ]

    # Count verdicts
    verdicts = {"PASS": [], "WATCHLIST": [], "KILL": [], "ERROR": []}
    for r in results:
        v = r.get("verdict", "ERROR")
        verdicts.setdefault(v, []).append(r)

    lines.append(
        f"PASS: {len(verdicts['PASS'])} | "
        f"WATCHLIST: {len(verdicts['WATCHLIST'])} | "
        f"KILL: {len(verdicts['KILL'])} | "
        f"ERROR: {len(verdicts['ERROR'])}"
    )
    lines.append("")

    # PASS section
    if verdicts["PASS"]:
        lines.append("--- PASS ---")
        for r in sorted(verdicts["PASS"], key=lambda x: x.get("discovery_score", 0), reverse=True):
            moat = r.get("moat", {})
            analogs = r.get("analogs", {})
            ev = r.get("expected_value", {})
            lines.append(
                f"  {r['ticker']} | "
                f"Moat {moat.get('moat_score', '?')}/25 | "
                f"{analogs.get('best_analog', '?')} DNA {analogs.get('best_analog_score', '?')}/8 | "
                f"EV: +{ev.get('expected_value', 0):.0f}% | "
                f"P(2x): {ev.get('p_2x', 0):.0f}%"
            )
        lines.append("")

    # WATCHLIST section
    if verdicts["WATCHLIST"]:
        lines.append("--- WATCHLIST ---")
        for r in sorted(verdicts["WATCHLIST"], key=lambda x: x.get("discovery_score", 0), reverse=True)[:15]:
            moat = r.get("moat", {})
            analogs = r.get("analogs", {})
            lines.append(
                f"  {r['ticker']} | "
                f"Moat {moat.get('moat_score', '?')}/25 | "
                f"{analogs.get('best_analog', '?')} DNA {analogs.get('best_analog_score', '?')}/8 | "
                f"{r.get('reason', '')[:60]}"
            )
        if len(verdicts["WATCHLIST"]) > 15:
            lines.append(f"  +{len(verdicts['WATCHLIST']) - 15} more in CSV")
        lines.append("")

    # KILL summary (just count + top reasons)
    if verdicts["KILL"]:
        lines.append(f"--- KILL ({len(verdicts['KILL'])}) ---")
        reason_counts: dict[str, int] = {}
        for r in verdicts["KILL"]:
            reason = r.get("reason", "unknown")[:40]
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        for reason, count in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            lines.append(f"  {count}x: {reason}")

    return "\n".join(lines)


def _write_deep_dive_csv(results: list[dict], scan_date: str) -> Path:
    """Write deep-dive results to CSV file.

    Args:
        results: List of deep-dive result dicts.
        scan_date: ISO date string.

    Returns:
        Path to the written CSV file.
    """
    out_dir = DISCOVERY_OUTPUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"deep-dive-{scan_date}.csv"

    fieldnames = [
        "ticker", "company_name", "verdict", "discovery_score",
        "moat_score", "moat_2A_ip", "moat_2B_switching", "moat_2C_network",
        "moat_2D_regulatory", "moat_2E_cost",
        "best_analog", "analog_score",
        "AMPX_dna", "RKLB_dna", "ASTS_dna", "NBIS_dna",
        "balance_pass", "insider_pass", "ceo_buying",
        "phase", "phase_name",
        "disqualifier_kills", "reason",
        "expected_value_pct", "p_bull", "p_base", "p_bear",
        "bull_return", "base_return", "bear_return", "p_2x",
    ]

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in sorted(results, key=lambda x: (
            {"PASS": 0, "WATCHLIST": 1, "KILL": 2, "ERROR": 3}.get(x.get("verdict", "ERROR"), 9),
            -(x.get("discovery_score") or 0),
        )):
            moat = r.get("moat", {})
            sub = moat.get("sub_scores", {})
            analogs = r.get("analogs", {})
            analog_map = analogs.get("analogs", {})
            balance = r.get("balance_sheet", {})
            insider = r.get("insider", {})
            washout = r.get("washout", {})
            disq = r.get("disqualifiers", {})

            writer.writerow({
                "ticker": r.get("ticker", ""),
                "company_name": r.get("company_name", ""),
                "verdict": r.get("verdict", "ERROR"),
                "discovery_score": r.get("discovery_score", 0),
                "moat_score": moat.get("moat_score", 0),
                "moat_2A_ip": sub.get("2A_ip", 0),
                "moat_2B_switching": sub.get("2B_switching", 0),
                "moat_2C_network": sub.get("2C_network", 0),
                "moat_2D_regulatory": sub.get("2D_regulatory", 0),
                "moat_2E_cost": sub.get("2E_cost", 0),
                "best_analog": analogs.get("best_analog", ""),
                "analog_score": analogs.get("best_analog_score", 0),
                "AMPX_dna": analog_map.get("AMPX", 0),
                "RKLB_dna": analog_map.get("RKLB", 0),
                "ASTS_dna": analog_map.get("ASTS", 0),
                "NBIS_dna": analog_map.get("NBIS", 0),
                "balance_pass": 1 if balance.get("pass") else 0,
                "insider_pass": 1 if insider.get("pass") else 0,
                "ceo_buying": 1 if insider.get("ceo_buying") else 0,
                "phase": washout.get("phase", ""),
                "phase_name": washout.get("phase_name", ""),
                "disqualifier_kills": ",".join(disq.get("kills", [])),
                "reason": r.get("reason", ""),
                "expected_value_pct": r.get("expected_value", {}).get("expected_value", ""),
                "p_bull": r.get("expected_value", {}).get("p_bull", ""),
                "p_base": r.get("expected_value", {}).get("p_base", ""),
                "p_bear": r.get("expected_value", {}).get("p_bear", ""),
                "bull_return": r.get("expected_value", {}).get("bull_return", ""),
                "base_return": r.get("expected_value", {}).get("base_return", ""),
                "bear_return": r.get("expected_value", {}).get("bear_return", ""),
                "p_2x": r.get("expected_value", {}).get("p_2x", ""),
            })

    logger.info("CSV written: %s", csv_path)
    return csv_path


# ── Latest Results Query ────────────────────────────────────────────────────


def get_latest_deep_dive_results(top_n: int = 50) -> list[dict]:
    """Query scr_deep_dive_results for the latest scan's results.

    Args:
        top_n: Max rows to return.

    Returns:
        List of dicts ordered by verdict priority then discovery_score.
    """
    run_migration()

    with get_connection() as conn:
        row = conn.execute(
            f"SELECT MAX(scan_date) AS latest FROM {TABLE_DEEP_DIVE_RESULTS}"
        ).fetchone()
        if not row or not row["latest"]:
            return []

        latest = row["latest"]
        rows = conn.execute(
            f"""SELECT ticker, scan_date, verdict, moat_score, moat_details,
                       best_analog, analog_match_count, balance_sheet_pass,
                       insider_pass, phase, phase_name, disqualifier_kills,
                       discovery_score, summary
                FROM {TABLE_DEEP_DIVE_RESULTS}
                WHERE scan_date = ?
                ORDER BY
                    CASE verdict
                        WHEN 'PASS' THEN 0
                        WHEN 'WATCHLIST' THEN 1
                        WHEN 'KILL' THEN 2
                        ELSE 3
                    END,
                    discovery_score DESC
                LIMIT ?""",
            (latest, top_n),
        ).fetchall()

    return [dict(r) for r in rows]


# ── CLI ─────────────────────────────────────────────────────────────────────


def _cli():
    parser = argparse.ArgumentParser(
        description="Automated Deep-Dive Scoring for Discovery Pipeline"
    )
    parser.add_argument("--run", action="store_true", help="Run deep-dives on latest discovery top N")
    parser.add_argument("--dry-run", action="store_true", help="Print report to stdout instead of logging")
    parser.add_argument("--ticker", type=str, help="Run single ticker deep-dive")
    parser.add_argument("--latest", action="store_true", help="Show latest deep-dive verdicts")
    parser.add_argument("--top", type=int, default=DEEP_DIVE_TOP_N, help="Number of candidates")

    args = parser.parse_args()

    if args.ticker:
        result = run_single_deep_dive(args.ticker.upper())
        print(f"\n{'=' * 60}")
        print(f"DEEP-DIVE: {result['ticker']}")
        print(f"{'=' * 60}")
        print(f"Verdict: {result['verdict']}")
        print(f"Reason: {result['reason']}")
        print()
        moat = result.get("moat", {})
        print(f"Moat Score: {moat.get('moat_score', 0)}/25 (gate={'PASS' if moat.get('gate_pass') else 'FAIL'})")
        for k, v in moat.get("sub_scores", {}).items():
            print(f"  {k}: {v}/5")
        print()
        analogs = result.get("analogs", {})
        print(f"Best Analog: {analogs.get('best_analog')} ({analogs.get('best_analog_score')}/8)")
        for name, score in analogs.get("analogs", {}).items():
            print(f"  {name}: {score}/8")
        print()
        balance = result.get("balance_sheet", {})
        print(f"Balance Sheet: {'PASS' if balance.get('pass') else 'FAIL'}")
        for flag in balance.get("flags", []):
            print(f"  {flag}")
        print()
        insider = result.get("insider", {})
        print(f"Insider: {'PASS' if insider.get('pass') else 'FAIL'} (buys={insider.get('buy_count')}, sells={insider.get('sell_count')})")
        if insider.get("ceo_buying"):
            print("  CEO BUYING")
        print()
        washout = result.get("washout", {})
        print(f"Phase: {washout.get('phase')} ({washout.get('phase_name')})")
        print()
        disq = result.get("disqualifiers", {})
        if disq.get("kills"):
            print(f"Disqualifiers: {', '.join(disq['kills'])}")
        if disq.get("unverified"):
            print(f"Unverified: {', '.join(disq['unverified'])}")

    elif args.run:
        result = run_deep_dives(top_n=args.top, dry_run=args.dry_run)
        if "error" in result:
            print(f"Error: {result['error']}")
            sys.exit(1)
        verdicts = result.get("verdicts", {})
        print(f"\nDeep-dive complete: {result.get('total_analyzed', 0)} analyzed")
        print(f"  PASS: {verdicts.get('PASS', 0)}")
        print(f"  WATCHLIST: {verdicts.get('WATCHLIST', 0)}")
        print(f"  KILL: {verdicts.get('KILL', 0)}")
        print(f"  CSV: {result.get('csv_path', 'N/A')}")

    elif args.latest:
        results = get_latest_deep_dive_results(top_n=args.top)
        if not results:
            print("No deep-dive results found. Run --run first.")
            sys.exit(0)
        print(f"\nLatest Deep-Dive Results ({results[0]['scan_date']}):")
        print(f"{'Ticker':<8} {'Verdict':<10} {'Moat':>5} {'Analog':<6} {'Match':>5} {'Phase':>5} {'Score':>7} Summary")
        print("-" * 90)
        for r in results:
            print(
                f"{r['ticker']:<8} {r['verdict']:<10} "
                f"{r['moat_score'] or 0:>5} "
                f"{r['best_analog'] or '':>6} "
                f"{r['analog_match_count'] or 0:>5} "
                f"{r['phase'] or 0:>5} "
                f"{r['discovery_score'] or 0:>7.1f} "
                f"{(r['summary'] or '')[:50]}"
            )

    else:
        parser.print_help()


if __name__ == "__main__":
    _cli()
