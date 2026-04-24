"""SEC Filing Intelligence — configuration constants."""

import os
from pathlib import Path

# ── Repo root (3 levels up from scripts/screener/config.py) ─────────────────
REPO_ROOT = Path(__file__).parent.parent.parent

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH = Path(os.environ.get("SFI_DB_PATH", str(Path(__file__).parent.parent.parent / "data" / "sec_filings.db")))

# ── Screener table names (all scr_ prefixed, 23 total) ───────────────────────
TABLE_UNIVERSE = "scr_universe"
TABLE_FUNDAMENTALS = "scr_fundamentals"
TABLE_PRICE_METRICS = "scr_price_metrics"
TABLE_PATENTS = "scr_patents"
TABLE_PATENT_SUMMARY = "scr_patent_summary"
TABLE_INSIDER_TRANSACTIONS = "scr_insider_transactions"
TABLE_FILING_SIGNALS = "scr_filing_signals"
TABLE_ANALYST_COVERAGE = "scr_analyst_coverage"
TABLE_SUPPLY_CHAIN = "scr_supply_chain"
TABLE_FORCING_FUNCTIONS = "scr_forcing_functions"
TABLE_CATALYSTS = "scr_catalysts"
TABLE_SCORES = "scr_scores"
TABLE_SCORE_HISTORY = "scr_score_history"
TABLE_ANOMALIES = "scr_anomalies"
TABLE_SIGNAL_SPIKES = "scr_signal_spikes"
TABLE_WATCHLIST = "scr_watchlist"
TABLE_KILL_LIST = "scr_kill_list"
TABLE_SECTOR_THEMES = "scr_sector_themes"
TABLE_AMPX_PROFILE = "scr_ampx_profile"
TABLE_USER_FEEDBACK = "scr_user_feedback"

# ── Read-only shared tables (SELECT only, never write) ───────────────────────
SHARED_TABLE_PRICE_HISTORY = "price_history"
SHARED_TABLE_WATCHLIST = "watchlist"
SHARED_TABLE_STOCKS = "stocks"
SHARED_TABLE_FUNDAMENTALS = "fundamentals"
SHARED_TABLE_EARNINGS = "earnings_calendar"

# ── SEC EDGAR API ─────────────────────────────────────────────────────────────
EDGAR_BASE_URL = "https://efts.sec.gov/LATEST"
EDGAR_SUBMISSIONS_URL = "https://data.sec.gov/submissions"
EDGAR_FILINGS_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
EDGAR_FULL_TEXT_SEARCH_URL = f"{EDGAR_BASE_URL}/search-index"
EDGAR_RATE_LIMIT_RPS = 10  # SEC fair access: max 10 requests/second
EDGAR_USER_AGENT = os.environ.get("EDGAR_USER_AGENT", "SECFilingIntelligence research@example.com")
EDGAR_REQUEST_TIMEOUT = 30  # seconds

# ── Insider Tracking ──────────────────────────────────────────────────────────
INSIDER_LOOKBACK_DAYS = 180  # Look back 6 months for insider transactions
INSIDER_CLUSTER_WINDOW_DAYS = 90  # Cluster = 3+ insiders buying within this window
INSIDER_MIN_TRANSACTION_VALUE = 10_000  # Ignore tiny transactions

# ── Price Analysis ────────────────────────────────────────────────────────────
ATH_LOOKBACK_YEARS = 5  # Years of price history for ATH calculation
VOLUME_MA_PERIOD = 50  # 50-day moving average for volume comparison
WASHOUT_RECOVERY_PERIOD_DAYS = 252  # ~1 year of trading days for 52-week low
PRICE_BATCH_DELAY = 0.5  # Seconds between yfinance fetches
PRICE_LOG_FILE = "screener_price.log"

# ── Washout Detector Thresholds ───────────────────────────────────────────────
WASHOUT_ATH_DROP_PCT = -70  # Crashed 70%+ from ATH
WASHOUT_ABOVE_52W_LOW_MULT = 1.1  # 10% above 52-week low (stabilizing)
WASHOUT_MIN_AVG_VOLUME = 50_000  # Not illiquid

# ── Filing Scanner ────────────────────────────────────────────────────────────
FILING_TYPES = ["10-K", "10-Q", "8-K"]
FILING_LOOKBACK_DAYS = 365  # Look back 1 year for filings
GROWTH_KEYWORDS = [
    "significant growth",
    "new contract",
    "strategic partnership",
    "expansion",
    "accelerating revenue",
    "record revenue",
    "backlog increase",
    "new product launch",
    "market share gain",
]
RISK_KEYWORDS = [
    "going concern",
    "material weakness",
    "restatement",
    "covenant violation",
    "liquidity risk",
]

# Themed keyword groups for supply-chain / catalyst filing search (EFTS)
FILING_KEYWORDS = {
    "defense_contract": [
        "Department of Defense contract", "SBIR Phase", "defense prime",
        "NDAA compliant", "Blue UAS", "ITAR", "government contract awarded",
        "Pentagon", "DARPA", "AFRL contract",
    ],
    "nuclear_supply": [
        "nuclear qualified", "ASME N-stamp", "nuclear grade",
        "NRC license", "HALEU", "small modular reactor", "SMR",
        "nuclear safety related", "10 CFR Part 50",
    ],
    "production_expansion": [
        "production facility expansion", "manufacturing capacity",
        "first production shipment", "factory opening", "production ramp",
        "capacity expansion complete", "new production line",
    ],
    "government_funding": [
        "DOE loan", "Department of Energy grant", "DPA Title III",
        "Section 45X credit", "IRA manufacturing credit",
        "CHIPS Act funding", "critical minerals designation",
    ],
    "ai_infrastructure": [
        "data center cooling", "liquid cooling system",
        "800G transceiver", "1.6T optical", "GPU cluster",
        "power delivery unit", "busbar", "AI inference",
    ],
    "customer_validation": [
        "production order", "series production", "volume manufacturing",
        "supply agreement", "offtake agreement", "multi-year contract",
        "sole source", "preferred supplier",
    ],
    "rare_earth_critical": [
        "rare earth processing", "critical mineral", "graphite anode",
        "lithium refining", "cobalt free", "domestic supply chain",
        "China dependency", "reshoring",
    ],
}

# ── Patent Tracker ────────────────────────────────────────────────────────
PATENTSVIEW_BASE_URL = "https://search.patentsview.org/api/v1"
PATENTSVIEW_RATE_LIMIT_RPS = 1  # Conservative (API allows 45/min)
PATENT_LIFETIME_YEARS = 20  # Standard US utility patent term
PATENT_MAX_PAGES = 10  # Safety cap on paginated results (1000 patents)

CONTRACT_MFG_KEYWORDS = [
    "contract manufacturer",
    "build-to-print",
    "manufacture to customer specifications",
    "contract manufacturing",
    "outsourced manufacturing",
]

EXCLUSIVE_LICENSE_KEYWORDS = [
    "exclusive license",
    "exclusively licensed",
]

NATIONAL_LABS = [
    "Argonne", "Brookhaven", "Fermilab", "Idaho National",
    "Lawrence Berkeley", "Lawrence Livermore", "Los Alamos",
    "Oak Ridge", "Pacific Northwest", "Sandia", "NREL",
    "MIT Lincoln", "Jet Propulsion", "NASA",
]

# ── Catalyst Tracker ──────────────────────────────────────────────────────────
CATALYST_HORIZON_DAYS = 90  # Track catalysts within this window
CATALYST_TYPES = [
    "earnings",
    "fda_approval",
    "contract_award",
    "product_launch",
    "analyst_day",
    "conference",
    "spinoff",
    "merger",
]

# ── Fundamentals Collection ───────────────────────────────────────────────────
FUNDAMENTALS_BATCH_SIZE = 20  # Tickers per yfinance batch
FUNDAMENTALS_BATCH_DELAY = 1.0  # Seconds between batches
FUNDAMENTALS_STALE_DAYS = 7  # Re-fetch after this many days
FUNDAMENTALS_LOG_FILE = "screener_fundamentals.log"

# ── Universe Refresh (EDGAR full scan) ───────────────────────────────────────
REFRESH_BATCH_SIZE = 20       # Smaller batches to avoid yfinance rate limits
REFRESH_BATCH_DELAY = 3.0     # Seconds between batches (longer for full scan)
CACHE_STALE_DAYS = 7          # Skip re-enriching tickers updated within this window

# ── Reporting ─────────────────────────────────────────────────────────────────
REPORT_TOP_N_CANDIDATES = 10  # Report top N candidates by score
REPORT_MIN_SCORE = 6.0  # Minimum score to report a candidate

# ---- AMPX Rules Screener (added 2026-04-14) ----
# Table names
TABLE_AMPX_RULES_SCORES = "scr_ampx_rules_scores"
TABLE_AMPX_RED_FLAGS = "scr_ampx_red_flags"
TABLE_AMPX_RUN_LOG = "scr_ampx_run_log"

# Filter gates
AMPX_MIN_MARKET_CAP_M = 30      # $30M floor (excludes micro-shells)
AMPX_MAX_MARKET_CAP_M = 500     # $500M ceiling (small-cap universe)
AMPX_MIN_CRASH_52W_PCT = -60    # <= -60% from 52w high OR
AMPX_MIN_CRASH_ATH_PCT = -70    # <= -70% from ATH (scr_price_metrics stores NEGATIVE values)
# Floor applied to BOTH paths of the crash gate. An ATH-only qualifier sitting
# near its 52w high is a slow grinder, not a washout — require active selling
# pressure (≥30% 52w decline) to match the AMPX-pattern setup.
AMPX_MIN_52W_DECLINE_PCT = -30
AMPX_MIN_LIQUIDITY_USD = 500_000  # $500K/day notional floor (avg_volume * current_price)

# Target sectors (STRICT match — 'Unknown' excluded)
AMPX_TARGET_SECTORS = (
    "Technology", "Industrials", "Energy",
    "Basic Materials", "Communication Services",
)

# Insider buying dimension (AMPX-specific — does NOT share RADAR's constants)
AMPX_INSIDER_LOOKBACK_DAYS = 90
AMPX_INSIDER_CLUSTER_WINDOW_DAYS = 30
AMPX_INSIDER_CLUSTER_MIN_COUNT = 2

# Priority industry keywords (matched via word-boundary regex, NOT substring)
AMPX_PRIORITY_INDUSTRIES = (
    "semiconductor", "solar", "nuclear", "fusion", "hydrogen",
    "geothermal", "carbon capture", "drone", "defense", "aerospace",
    "battery", "lithium", "uranium", "cooling", "data center",
    "satellite", "sensor", "robotics", "AI", "rare earth",
    "mineral", "cybersecurity", "quantum", "space", "munitions",
    "radar", "sonar", "submarine", "genomics", "gene therapy",
    "EV charging", "autonomous vehicle", "3D printing",
    "additive manufacturing",
    "communications", "networking", "edge AI", "inference",
)

# Going-concern regex (any match = hard kill)
AMPX_GOING_CONCERN_PATTERNS = (
    r"substantial doubt.{0,60}(going concern|ability to continue)",
    r"(conditions|circumstances|factors).{0,40}(raise|indicate).{0,40}substantial doubt",
    r"significant uncertainty.{0,40}(going concern|ability to continue|operate)",
    r"may not be able to continue as a going concern",
)

# Output paths
AMPX_OUTPUT_DIR = str(Path(__file__).parent.parent.parent / "output" / "ampx_rules")
# Configure output limits via CLI flags

# ---- EDGAR XBRL fundamentals (added 2026-04-14) ----
TABLE_FUNDAMENTALS_XBRL = "scr_fundamentals_xbrl"

# Per-call rate limit. SEC fair-access ceiling = 10 req/s; we leave headroom.
XBRL_RATE_LIMIT_SLEEP_SEC = 0.1
XBRL_BATCH_SIZE = 50
XBRL_BATCH_ERROR_THRESHOLD = 0.6  # if >60% of batch errors, halve batch & sleep 30s
XBRL_BATCH_BACKOFF_SEC = 30
XBRL_RUNWAY_CAP_QUARTERS = 40  # display-hygiene cap matching ampx_rules.py:369

# AMPX-specific: dilution drag tiering on 2yr share count change percentage.
# R7 refinement of R6's binary >80% → -1.0 threshold. The binary threshold
# created a cliff where 79% dilution scored identically to 5% dilution.
# Tiered version catches moderate diluters (VUZI 25%, ARBE 40%, PDYN 78%)
# that previously escaped penalty unscathed.
#
# Scoring:
#   dilution_pct < AMPX_DILUTION_TIER_MILD_PCT (10%)      → d12 = 0.0
#   10 ≤ dilution_pct < AMPX_DILUTION_TIER_MODERATE_PCT   → d12 = -0.25
#   50 ≤ dilution_pct < AMPX_DILUTION_TIER_HEAVY_PCT      → d12 = -0.5
#   100 ≤ dilution_pct (AMPG/ABAT territory)              → d12 = -1.0
# Plus unchanged RS-mask path: dilution_pct < 0 AND rs_count ≥ 1 → -1.0.
AMPX_DILUTION_TIER_MILD_PCT = 10.0
AMPX_DILUTION_TIER_MODERATE_PCT = 50.0
AMPX_DILUTION_TIER_HEAVY_PCT = 100.0

# Back-compat: keep the old name pointing at the HEAVY tier. Some test fixtures
# and legacy callers reference AMPX_DILUTION_DRAG_PCT; those paths now compare
# against the >=100% cutoff which is the closest equivalent of the old -1.0
# threshold (was 80, now 100 — the 80-99% zone drops from -1.0 to -0.5).
AMPX_DILUTION_DRAG_PCT = AMPX_DILUTION_TIER_HEAVY_PCT

# Source switch for the AMPX scorer. Three modes:
#   - "yfinance": yf only, no XBRL. Legacy pre-Round-5 behavior.
#   - "yfinance_xbrl_fallback": yf primary, XBRL rescues null-yf tickers.
#     Safe first deployment — delivers the XBRL migration's original goal
#     (the ~300 tickers yfinance missed enter scoring) with zero regression
#     on the ~1,000 tickers where yfinance already produces usable data.
#   - "edgar_xbrl": XBRL primary, yf fallback. Gated on a 10-K-based accuracy
#     study (see docs/ampx/xbrl-validation-2026-04-14.md follow-ups). Not
#     active until we've verified XBRL wins the majority of yf divergences.
FUNDAMENTALS_SOURCE = "yfinance_xbrl_fallback"

# ── Asymmetry Scanner ────────────────────────────────────────────────────────
TABLE_THESIS_HITS = "scr_thesis_hits"
THESIS_SEEDS_DIR = REPO_ROOT / "docs" / "ampx" / "thesis_seeds"
THESIS_OUTPUT_DIR = REPO_ROOT / "output" / "asymmetry"
FILINGS_DIR = REPO_ROOT / "data" / "filings"

# Quality multiplier for composite scoring
QUALITY_MULTIPLIER = {
    ("tier1", True): 1.0,    # tier1 + moat >= 0.2
    ("tier1", False): 0.85,  # tier1 + moat < 0.2
    ("tier2", True): 0.7,
    ("tier2", False): 0.7,
    ("tier3", True): 0.4,
    ("tier3", False): 0.4,
}
MOAT_QUALITY_THRESHOLD = 0.2

# Exposure-mode calibration
EXPOSURE_KEYWORD_HITS_PER_10K_CHARS = 1.0  # 1 hit per 10K chars ~ 10% exposure

# ── Multibagger Discovery Pipeline (Phase 1) ────────────────────────────────
TABLE_DISCOVERY_FLAGS = "scr_discovery_flags"
TABLE_TEXT_SEARCH_HITS = "scr_text_search_hits"
TABLE_MOAT_SIGNALS = "scr_moat_signals"

DISCOVERY_OUTPUT_DIR = REPO_ROOT / "output" / "discovery"
# Configure output limits via CLI flags
DISCOVERY_FILING_FORMS = "10-K,20-F,40-F"  # 10-K (US), 20-F (foreign), 40-F (Canadian cross-listed)

# Layer 2A — Hard keywords: strong sole-source / monopoly language in 10-K
DISCOVERY_HARD_KEYWORDS = [
    "sole source",
    "only provider",
    "exclusive license",
    "no competitor",
    "sole supplier",
    "only licensed",
    "government-granted",
]

# Layer 2B — Soft keywords: moat indicators that need context
DISCOVERY_SOFT_KEYWORDS = [
    "only company",
    "only manufacturer",
    "proprietary technology",
    "no alternative",
    "limited suppliers",
    "significant barriers",
    "exclusive agreement",
    "patented process",
    "irreplaceable",
    "mission-critical",
    "classified program",
    "exclusive rights",
    "no substitute",
    "critical to national",
    "only facility",
    "only plant",
    "years to replicate",
    "sole supplier to",
    "critical component",
    "no alternative supplier",
    "single source of supply",
    "indefinite delivery",
    "sole source justification",
    "proprietary dataset",
    "network effect",
    "switching costs",
    # Phase 5 keyword discovery (from winners' 10-K analysis)
    "trade secret",
    "ITAR",
    "security clearance",
    "competitive advantage",
    "national security",
    "vertically integrated",
    "limited number of",
    "barriers to entry",
    "defense industrial base",
    "export control",
    "patent portfolio",
    "no direct competitor",
    "first commercial",
    "only commercial",
    "FCC license",
    "NRC licensed",
    "FAA certified",
    "capacity constrained",
    # Phase 7 keyword expansion (wider funnel)
    "only qualified",
    "only certified",
    "only domestic source",
    "only domestic producer",
    "only domestic manufacturer",
    "no viable alternative",
    "program of record",
    "take-or-pay",
    "long-term supply agreement",
    "multi-year supply",
    "minimum purchase commitment",
    "supply shortage",
    "reshoring",
    "critical national security",
    "domestic production capability",
    "other transaction authority",
    "directed energy",
    "orphan drug",
    "regulatory exclusivity",
    # Phase 8 — v3 non-defense moat expansion
    # Layer A: Network & Platform Moats
    "installed base",
    "platform ecosystem",
    "marketplace liquidity",
    "two-sided marketplace",
    "developer ecosystem",
    "API integrations",
    "platform lock-in",
    "ecosystem partners",
    "marketplace network",
    "flywheel effect",
    "platform adoption",
    "embedded in customer",
    # Layer B: Switching Cost Moats
    "high switching costs",
    "multi-year contract",
    "implementation timeline",
    "migration cost",
    "deeply integrated",
    "mission-critical system",
    "long-term customer",
    "customer retention rate",
    "renewal rate",
    "contracted revenue",
    # Layer C: Data & IP Moats
    "proprietary database",
    "unique dataset",
    "only comprehensive",
    "data advantage",
    "curated dataset",
    "proprietary algorithm",
    "machine learning model",
    "training data",
    "data moat",
    "information advantage",
    # Layer D: Regulatory Capture (non-defense)
    "licensed facility",
    "permitted operator",
    "franchise agreement",
    "regulated monopoly",
    "certificate of need",
    "gaming license",
    "spectrum license",
    "rate base",
    "regulated utility",
    "exclusive franchise",
    # Layer E: Supply Chain Qualification
    "qualified supplier",
    "sole qualified",
    "single qualified source",
    "qualification process",
    "years to qualify",
    "approved vendor list",
    "customer qualification",
    "requalification",
]

# Keyword → moat type mapping (a keyword can map to multiple types)
# Types: regulatory, technology, infrastructure, network, supply_chain, government
DISCOVERY_KEYWORD_MOAT_MAP = {
    "exclusive license": ["regulatory"],
    "only licensed": ["regulatory"],
    "government-granted": ["regulatory"],
    "proprietary technology": ["technology"],
    "patented process": ["technology"],
    "significant barriers": ["technology"],
    "years to replicate": ["technology", "infrastructure"],
    "only facility": ["infrastructure"],
    "only plant": ["infrastructure"],
    "only manufacturer": ["infrastructure"],
    "proprietary dataset": ["network"],
    "network effect": ["network"],
    "switching costs": ["network"],
    "sole source": ["supply_chain", "regulatory"],
    "only provider": ["supply_chain"],
    "sole supplier": ["supply_chain"],
    "no competitor": ["supply_chain"],
    "sole supplier to": ["supply_chain"],
    "critical component": ["supply_chain"],
    "no alternative supplier": ["supply_chain"],
    "no alternative": ["supply_chain"],
    "single source of supply": ["supply_chain"],
    "limited suppliers": ["supply_chain"],
    "no substitute": ["supply_chain"],
    "irreplaceable": ["supply_chain"],
    "mission-critical": ["supply_chain"],
    "indefinite delivery": ["government"],
    "sole source justification": ["government"],
    "classified program": ["government"],
    "exclusive agreement": ["government", "regulatory"],
    "exclusive rights": ["regulatory"],
    "critical to national": ["government"],
    "only company": ["technology"],
    # Phase 5 keyword discovery mappings
    "trade secret": ["technology"],
    "ITAR": ["government", "regulatory"],
    "security clearance": ["government"],
    "competitive advantage": ["technology"],
    "national security": ["government"],
    "vertically integrated": ["infrastructure"],
    "limited number of": ["supply_chain"],
    "barriers to entry": ["technology"],
    "defense industrial base": ["government"],
    "export control": ["government", "regulatory"],
    "patent portfolio": ["technology"],
    "no direct competitor": ["supply_chain"],
    "first commercial": ["technology"],
    "only commercial": ["supply_chain", "infrastructure"],
    "FCC license": ["regulatory"],
    "NRC licensed": ["regulatory"],
    "FAA certified": ["regulatory"],
    "capacity constrained": ["infrastructure"],
    # Phase 7 keyword expansion mappings
    "only qualified": ["supply_chain", "regulatory"],
    "only certified": ["supply_chain", "regulatory"],
    "only domestic source": ["supply_chain", "infrastructure"],
    "only domestic producer": ["supply_chain", "infrastructure"],
    "only domestic manufacturer": ["supply_chain", "infrastructure"],
    "no viable alternative": ["supply_chain"],
    "program of record": ["government"],
    "take-or-pay": ["supply_chain"],
    "long-term supply agreement": ["supply_chain"],
    "multi-year supply": ["supply_chain"],
    "minimum purchase commitment": ["supply_chain"],
    "supply shortage": ["supply_chain", "infrastructure"],
    "reshoring": ["infrastructure", "government"],
    "critical national security": ["government"],
    "domestic production capability": ["infrastructure", "government"],
    "other transaction authority": ["government"],
    "directed energy": ["technology", "government"],
    "orphan drug": ["regulatory"],
    "regulatory exclusivity": ["regulatory"],
    # Phase 8 — v3 non-defense moat mappings
    # Layer A: Network & Platform
    "installed base": ["platform"],
    "platform ecosystem": ["platform"],
    "marketplace liquidity": ["platform"],
    "two-sided marketplace": ["platform", "network"],
    "developer ecosystem": ["platform"],
    "API integrations": ["platform", "switching_cost"],
    "platform lock-in": ["platform", "switching_cost"],
    "ecosystem partners": ["platform"],
    "marketplace network": ["platform", "network"],
    "flywheel effect": ["platform", "network"],
    "platform adoption": ["platform"],
    "embedded in customer": ["switching_cost"],
    # Layer B: Switching Costs
    "high switching costs": ["switching_cost"],
    "multi-year contract": ["switching_cost"],
    "implementation timeline": ["switching_cost"],
    "migration cost": ["switching_cost"],
    "deeply integrated": ["switching_cost"],
    "mission-critical system": ["switching_cost", "supply_chain"],
    "long-term customer": ["switching_cost"],
    "customer retention rate": ["switching_cost"],
    "renewal rate": ["switching_cost"],
    "contracted revenue": ["switching_cost"],
    # Layer C: Data & IP
    "proprietary database": ["data", "network"],
    "unique dataset": ["data"],
    "only comprehensive": ["data"],
    "data advantage": ["data"],
    "curated dataset": ["data"],
    "proprietary algorithm": ["data", "technology"],
    "machine learning model": ["data", "technology"],
    "training data": ["data"],
    "data moat": ["data"],
    "information advantage": ["data"],
    # Layer D: Regulatory Capture (non-defense)
    "licensed facility": ["regulated_non_defense"],
    "permitted operator": ["regulated_non_defense"],
    "franchise agreement": ["regulated_non_defense"],
    "regulated monopoly": ["regulated_non_defense", "regulatory"],
    "certificate of need": ["regulated_non_defense", "regulatory"],
    "gaming license": ["regulated_non_defense", "regulatory"],
    "spectrum license": ["regulated_non_defense", "regulatory"],
    "rate base": ["regulated_non_defense"],
    "regulated utility": ["regulated_non_defense"],
    "exclusive franchise": ["regulated_non_defense", "regulatory"],
    # Layer E: Qualified Supplier
    "qualified supplier": ["qualified_supplier", "supply_chain"],
    "sole qualified": ["qualified_supplier", "supply_chain"],
    "single qualified source": ["qualified_supplier", "supply_chain"],
    "qualification process": ["qualified_supplier"],
    "years to qualify": ["qualified_supplier", "technology"],
    "approved vendor list": ["qualified_supplier"],
    "customer qualification": ["qualified_supplier"],
    "requalification": ["qualified_supplier"],
}

# Layer 2C — Structural signal thresholds
DISCOVERY_MAX_ANALYST_COUNT = 3
DISCOVERY_INSIDER_CLUSTER_MIN = 2
DISCOVERY_INSIDER_CLUSTER_VALUE_MIN = 100_000
DISCOVERY_INSIDER_WINDOW_DAYS = 30
DISCOVERY_MAX_REVENUE_M = 200
DISCOVERY_MIN_REVENUE_GROWTH_PCT = 30
DISCOVERY_MIN_PATENT_COUNT_3YR = 10
DISCOVERY_MIN_GOV_REVENUE_PCT = 30

# Scoring weights for composite
DISCOVERY_WEIGHT_FLAG_COUNT = 2.0
DISCOVERY_WEIGHT_MOAT_DIVERSITY = 3.0
DISCOVERY_WEIGHT_HARD_KEYWORD = 1.5
DISCOVERY_WEIGHT_SOFT_KEYWORD = 1.0
DISCOVERY_WEIGHT_STRUCTURAL = 1.0

# Phase 2: Sector scoring based on SIC codes from EFTS
# SIC ranges for sector classification
DISCOVERY_SECTOR_MAP = {
    # Defense / Aerospace — HIGH PRIORITY
    "defense": [(3760, 3769), (3812, 3812), (3489, 3489), (3761, 3761), (3769, 3769)],
    # Semiconductors / Electronics — HIGH PRIORITY
    "semiconductor": [(3674, 3674), (3672, 3672), (3679, 3679)],
    # Industrial / Manufacturing — HIGH PRIORITY
    "industrial": [(3400, 3499), (3500, 3599), (3600, 3699)],
    # Energy / Mining / Critical Minerals — HIGH PRIORITY
    "energy": [(1000, 1499), (1300, 1389), (2911, 2911), (4911, 4991)],
    # Technology / Software — MEDIUM PRIORITY
    "technology": [(3570, 3579), (3670, 3679), (7370, 7379), (3825, 3829)],
    # Space / Satellites — HIGH PRIORITY
    "space": [(3764, 3764), (3812, 3812)],
    # Pharma / Biotech — LOW PRIORITY (high noise)
    "pharma": [(2830, 2836), (2860, 2869), (5122, 5122), (8731, 8734)],
    # Biotech — LOW PRIORITY
    "biotech": [(2836, 2836), (8731, 8731)],
}

# Score multipliers per sector category
DISCOVERY_SECTOR_SCORE = {
    "defense": 5.0,
    "semiconductor": 4.0,
    "industrial": 3.0,
    "energy": 4.0,
    "technology": 3.0,
    "space": 5.0,
    "pharma": -3.0,     # Penalty — high noise from FDA patent language
    "biotech": -2.0,    # Smaller penalty — some biodefense is interesting
    "unknown": 0.0,
}

# Market cap scoring (for tickers in our universe)
DISCOVERY_MCAP_SCORES = {
    "nano": 3.0,      # $20M - $100M
    "micro": 4.0,     # $100M - $500M — sweet spot
    "small": 3.0,     # $500M - $2B
    "mid": 1.0,       # $2B - $10B
    "large": -2.0,    # >$10B — already discovered
}

# Bonus for being in existing thesis system
DISCOVERY_THESIS_MATCH_BONUS = 2.0

# Phase 3: Fundamentals-based scoring adjustments
# "Already discovered" penalty — ALL THREE must be true to trigger
DISCOVERY_ALREADY_DISCOVERED_MIN_CAP = 10_000_000_000  # $10B
DISCOVERY_ALREADY_DISCOVERED_MIN_ANALYSTS = 10
DISCOVERY_ALREADY_DISCOVERED_MAX_PCT_FROM_HIGH = -10  # within 10% of 52w high
DISCOVERY_ALREADY_DISCOVERED_PENALTY = -5.0

# Debt scoring — only penalize UNFUNDED leverage
DISCOVERY_UNFUNDED_DEBT_DE_THRESHOLD = 5.0  # D/E > 5
DISCOVERY_UNFUNDED_DEBT_RUNWAY_THRESHOLD = 4  # AND cash runway < 4 quarters
DISCOVERY_UNFUNDED_DEBT_PENALTY = -4.0

# Debt-free bonus
DISCOVERY_DEBT_FREE_DE_THRESHOLD = 0.3
DISCOVERY_DEBT_FREE_BONUS = 2.0

# Revenue decline penalty
DISCOVERY_REVENUE_DECLINE_PENALTY = -3.0

# Under-followed bonus (for enrichment scoring)
DISCOVERY_ZERO_ANALYST_BONUS = 3.0
DISCOVERY_LOW_ANALYST_BONUS = 1.5  # 1-3 analysts

# Phase 4: USAspending.gov contract validation
USASPENDING_API_URL = "https://api.usaspending.gov/api/v2"
USASPENDING_SEARCH_YEARS = 3  # Look back 3 years for contracts
USASPENDING_TOP_CONTRACTS = 5  # Check competition data for top 5 contracts
USASPENDING_BATCH_DELAY = 1.0  # Seconds between API calls
USASPENDING_MAX_TICKERS = 100  # Max tickers to check (top N by score)

# Score bonuses for government contract validation
DISCOVERY_GOV_SOLE_SOURCE_BONUS = 5.0    # Confirmed sole-source (extent_competed B/C/G/NDO)
DISCOVERY_GOV_SINGLE_BIDDER_BONUS = 3.0  # Competed but only 1 offer received
DISCOVERY_GOV_RESTRICTED_BONUS = 2.0     # extent_competed D (excluded sources before competing)
DISCOVERY_GOV_CONTRACT_BONUS = 1.0       # Has ANY government contract

# Phase 4: Customer 10-K cross-validation
DISCOVERY_CUSTOMER_XVAL_MAX_TICKERS = 75  # Check top N tickers
DISCOVERY_CUSTOMER_XVAL_BONUS = 4.0       # Mentioned in another company's 10-K
DISCOVERY_CUSTOMER_XVAL_MULTI_BONUS = 6.0 # Mentioned by 3+ different companies

# Phase 5: Historical tracking
TABLE_DISCOVERY_HISTORY = "scr_discovery_history"
DISCOVERY_HISTORY_MIN_SCORE_DELTA = 3.0  # Flag tickers with score change >= this
DISCOVERY_HISTORY_NEW_TICKER_BONUS = 2.0  # Bonus for first-time appearances

# Phase 5b: 10-K context analysis
DISCOVERY_CONTEXT_MAX_TICKERS = 50  # Analyze top N tickers
DISCOVERY_CONTEXT_SELF_CLAIM_BONUS = 2.0   # Keyword in Business Description (Item 1)
DISCOVERY_CONTEXT_RISK_FACTOR_PENALTY = -1.0  # Keyword in Risk Factors (Item 1A)
DISCOVERY_CONTEXT_MDA_BONUS = 1.0  # Keyword in MD&A (Item 7)

# Phase 6: Automated Deep-Dive
TABLE_DEEP_DIVE_RESULTS = "scr_deep_dive_results"
DEEP_DIVE_TOP_N = 50
DEEP_DIVE_MOAT_GATE = 8  # Minimum moat score out of 25
DEEP_DIVE_ANALOG_STRONG = 5  # Strong analog match threshold (out of 8)
DEEP_DIVE_ANALOG_WEAK = 4  # Weak analog match threshold
DEEP_DIVE_MOAT_STRONG = 12  # Strong moat for PASS verdict

# ── Forward Moat Scanner ─────────────────────────────────────────────────────
TABLE_FORWARD_MOAT_SCORES = "scr_forward_moat_scores"
TABLE_FORWARD_MOAT_HISTORY = "scr_forward_moat_history"

FORWARD_MOAT_SCAN_TOP_N = 500
FORWARD_MOAT_DEEPDIVE_TOP_N = 50
FORWARD_MOAT_MIN_SCORE_DEEPDIVE = 15
FORWARD_MOAT_MIN_SIGNAL_SCORE = 4
FORWARD_MOAT_OUTPUT_DIR = REPO_ROOT / "output" / "discovery"
# Configure output limits via CLI flags

# Backlog signal
FORWARD_BACKLOG_KEYWORDS = ("backlog", "order book", "unfunded orders", "contract awards")
FORWARD_BACKLOG_GROWTH_THRESHOLDS = [(1.0, 8), (0.5, 6), (0.25, 4)]

# Partnership signal
FORWARD_PARTNERSHIP_KEYWORDS = (
    "strategic investment", "joint development", "collaboration agreement",
    "license agreement", "strategic partnership", "equity investment",
    "joint venture", "co-development",
)
FORWARD_PARTNERSHIP_MISMATCH_RATIO = 10

# Institutional holders to exclude from partnership detection — these hold shares
# in every company and are not strategic partners.
FORWARD_PARTNERSHIP_EXCLUDE_INSTITUTIONAL = {
    "JPM", "C", "GS", "MS", "BLK", "BRK-B", "V", "MA",
    "WFC", "BAC", "USB", "PNC", "SCHW", "ICE", "CME",
    "MCO", "SPGI", "MMC", "AON", "CB",
}

# Fortune 200 tickers for partnership mismatch detection (top US companies by market cap)
FORWARD_PARTNERSHIP_FORTUNE_200 = (
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "BRK-B", "LLY", "AVGO", "JPM",
    "V", "TSLA", "WMT", "UNH", "XOM", "MA", "PG", "JNJ", "COST", "HD",
    "ABBV", "ORCL", "MRK", "BAC", "CRM", "CVX", "NFLX", "AMD", "KO", "TMO",
    "PEP", "LIN", "DIS", "ABT", "ADBE", "WFC", "PM", "MCD", "CSCO", "ACN",
    "IBM", "GE", "NOW", "ISRG", "QCOM", "TXN", "CAT", "INTU", "VZ", "AMGN",
    "AMAT", "GS", "AXP", "MS", "BLK", "PFE", "RTX", "LOW", "BKNG", "T",
    "HON", "UNP", "SPGI", "SCHW", "NEE", "DE", "C", "BA", "BMY", "PLD",
    "SYK", "LRCX", "CB", "ADP", "MDT", "GILD", "FI", "VRTX", "BSX", "MMC",
    "TMUS", "MU", "SO", "KLAC", "ETN", "AMT", "CI", "SHW", "CME", "TJX",
    "ZTS", "PNC", "ICE", "MCO", "USB", "WM", "EQIX", "GD", "NOC", "LMT",
    "CL", "EMR", "APH", "AON", "ITW", "MMM", "F", "GM", "CVS", "COP",
    "SLB", "OXY", "PSX", "VLO", "MPC", "DVN", "FANG", "EOG", "HES", "BKR",
    "DOW", "DD", "FMC", "ALB", "LYB", "ECL", "IFF", "PPG", "NEM", "FCX",
    "INTC", "HPQ", "DELL", "WDC", "STX", "KEYS", "ANSS", "CDNS", "SNPS", "MRVL",
    "PANW", "FTNT", "CRWD", "ZS", "NET", "DDOG", "SNOW", "PLTR", "U", "SHOP",
    "SQ", "PYPL", "COIN", "ROKU", "SNAP", "PINS", "UBER", "LYFT", "ABNB", "DASH",
    "LHX", "HII", "TXT", "ERJ", "KTOS", "AVAV", "RKLB",
    "VOD", "ORAN",
)

# New TAM keywords
FORWARD_TAM_KEYWORDS = (
    "eVTOL", "autonomous vehicle", "quantum computing", "fusion energy",
    "small modular reactor", "direct-to-device", "foundry services", "reshoring",
    "CHIPS Act", "Inflation Reduction Act", "AUKUS", "NDAA compliant",
    "gene therapy", "CRISPR", "solid state battery", "silicon anode",
    "carbon capture", "green hydrogen", "hypersonic", "space debris",
    "digital twin", "synthetic biology", "neuromorphic", "edge AI",
    "generative AI", "large language model", "6G", "advanced air mobility",
    "nuclear renaissance", "critical minerals", "rare earth processing",
    "vertical farm", "lab grown", "longevity", "GLP-1",
)

# Capex inflection
FORWARD_CAPEX_GROWTH_MIN = 0.30
FORWARD_CAPEX_ATH_CRASH_MIN = -0.50

# Technology milestone keywords
FORWARD_MILESTONE_KEYWORDS = (
    "world's first", "first ever", "successfully demonstrated",
    "achieved certification", "FDA approved", "FAA certified",
    "FCC licensed", "completed Phase", "record performance",
    "first commercial", "first flight", "first delivery",
    "granted patent", "breakthrough",
)

# Market cap ceiling — excludes large-caps from a small-cap screen
FORWARD_MOAT_MAX_MARKET_CAP = 5_000_000_000  # $5B ceiling

# Biotech/pharma milestone keywords that are "table stakes" not real signals
FORWARD_MILESTONE_BIOTECH_NOISE = {"FDA approved", "completed Phase"}

# Sectors treated as biotech/pharma for milestone noise reduction
FORWARD_MILESTONE_BIOTECH_SECTORS = {"biotech", "pharma", "healthcare",
                                     "Healthcare", "Biotechnology",
                                     "Health Care", "Pharmaceuticals"}

# Deep-dive gates (modified for forward moat tickers)
FORWARD_DEEPDIVE_MIN_RUNWAY_Q = 4
FORWARD_DEEPDIVE_MAX_DILUTION_PCT = 0.30

# 10-K section patterns (regex for section headers)
DISCOVERY_10K_SECTIONS = {
    "business": r"(?:ITEM|Item)\s*1[.\s]",           # Item 1 or Item 1.
    "risk_factors": r"(?:ITEM|Item)\s*1A[.\s]",       # Item 1A
    "properties": r"(?:ITEM|Item)\s*2[.\s]",          # Item 2
    "mda": r"(?:ITEM|Item)\s*7[.\s]",                 # Item 7 MD&A
    "financials": r"(?:ITEM|Item)\s*8[.\s]",          # Item 8
}
