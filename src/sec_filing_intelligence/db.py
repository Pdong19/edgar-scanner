"""Centralized DB connection manager for SEC Filing Intelligence tables.

All writes go to scr_ prefixed tables only. Read-only access to shared pipeline
tables (price_history, watchlist, stocks, fundamentals, earnings_calendar).

RADAR architecture: Every active, non-killed ticker gets fully scored on all
dimensions. No mechanical elimination by score thresholds. Three detection modes
(pattern match, anomaly, signal spike) surface interesting candidates.
"""

import sqlite3
from contextlib import contextmanager

from .config import DB_PATH


@contextmanager
def get_connection():
    """Context manager for SQLite connections with WAL mode."""
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


_migrated = False


def run_migration():
    """Create all 24 scr_ tables if they don't exist, then seed reference data.

    CRITICAL: This function ONLY creates scr_ prefixed tables.
    It never modifies any existing pipeline table.
    """
    global _migrated
    if _migrated:
        return
    with get_connection() as conn:
        # Migrate legacy scr_universe (old 5-col schema → new 16-col)
        uni_cols = {r[1] for r in conn.execute("PRAGMA table_info(scr_universe)").fetchall()}
        if uni_cols and "is_active" not in uni_cols:
            count = conn.execute("SELECT COUNT(*) FROM scr_universe").fetchone()[0]
            if count == 0:
                conn.execute("DROP TABLE scr_universe")
        # Migrate legacy scr_supply_chain (old schema without customer_ticker)
        sc_cols = {r[1] for r in conn.execute("PRAGMA table_info(scr_supply_chain)").fetchall()}
        if sc_cols and "customer_ticker" not in sc_cols:
            count = conn.execute("SELECT COUNT(*) FROM scr_supply_chain").fetchone()[0]
            if count == 0:
                conn.execute("DROP TABLE scr_supply_chain")
            else:
                # Table has data — add missing columns via ALTER TABLE
                _new_sc_cols = {
                    "customer_ticker": "TEXT",
                    "relationship_type": "TEXT NOT NULL DEFAULT 'supplier'",
                    "is_bottleneck": "INTEGER DEFAULT 0",
                    "source_filing": "TEXT",
                    "confidence": "REAL DEFAULT 0.5",
                    "first_seen": "TEXT",
                    "last_confirmed": "TEXT",
                }
                for col_name, col_def in _new_sc_cols.items():
                    if col_name not in sc_cols:
                        conn.execute(f"ALTER TABLE scr_supply_chain ADD COLUMN {col_name} {col_def}")
        conn.executescript(_DDL)
        # Migrate legacy column name: updated_at → last_updated
        pm_cols = {r[1] for r in conn.execute("PRAGMA table_info(scr_price_metrics)").fetchall()}
        if "updated_at" in pm_cols and "last_updated" not in pm_cols:
            conn.execute("ALTER TABLE scr_price_metrics RENAME COLUMN updated_at TO last_updated")
        # Add dim12_dilution_drag column to existing scr_ampx_rules_scores tables
        ampx_cols = {r[1] for r in conn.execute("PRAGMA table_info(scr_ampx_rules_scores)").fetchall()}
        if ampx_cols and "dim12_dilution_drag" not in ampx_cols:
            conn.execute(
                "ALTER TABLE scr_ampx_rules_scores ADD COLUMN dim12_dilution_drag REAL DEFAULT 0"
            )

        # --- Form 4 poller migration (2026-04-15) ---
        it_cols = {
            r[1] for r in conn.execute("PRAGMA table_info(scr_insider_transactions)").fetchall()
        }
        if it_cols and "transaction_date" not in it_cols:
            conn.execute("ALTER TABLE scr_insider_transactions ADD COLUMN transaction_date TEXT")
        if it_cols and "accession_number" not in it_cols:
            conn.execute("ALTER TABLE scr_insider_transactions ADD COLUMN accession_number TEXT")
        if it_cols and "is_amended" not in it_cols:
            conn.execute(
                "ALTER TABLE scr_insider_transactions ADD COLUMN is_amended INTEGER DEFAULT 0"
            )
        if it_cols and "amendment_accession" not in it_cols:
            conn.execute("ALTER TABLE scr_insider_transactions ADD COLUMN amendment_accession TEXT")

        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_insider_dedup
              ON scr_insider_transactions (ticker, accession_number, insider_name, transaction_date)
        """)

        # Ensure scr_universe.cik exists (required by form4_parser.cik_to_ticker fallback)
        uni_cols_now = {r[1] for r in conn.execute("PRAGMA table_info(scr_universe)").fetchall()}
        if "cik" not in uni_cols_now:
            conn.execute("ALTER TABLE scr_universe ADD COLUMN cik TEXT")

        # --- scr_universe column migration (market_cap, avg_volume) ---
        # ampx_rules.py queries u.market_cap and u.avg_volume but the old DDL
        # only had market_cap_m. Add both columns if they don't exist yet.
        uni_cols_now = {r[1] for r in conn.execute("PRAGMA table_info(scr_universe)").fetchall()}
        if "market_cap" not in uni_cols_now:
            conn.execute("ALTER TABLE scr_universe ADD COLUMN market_cap REAL")
        if "avg_volume" not in uni_cols_now:
            conn.execute("ALTER TABLE scr_universe ADD COLUMN avg_volume REAL")

        # --- scr_fundamentals column migration ---
        # fundamentals.py UPSERT writes columns that the old DDL didn't declare.
        # Add them so existing DBs don't break on INSERT.
        fund_cols = {r[1] for r in conn.execute("PRAGMA table_info(scr_fundamentals)").fetchall()}
        _new_fund_cols = {
            "revenue_quarterly": "REAL",
            "market_cap": "REAL",
            "institutional_ownership": "REAL",
            "fcf_growth_yoy": "REAL",
            "current_ratio": "REAL",
            "roe": "REAL",
            "data_source": "TEXT DEFAULT 'yfinance'",
            "raw_json": "TEXT",
        }
        for col_name, col_def in _new_fund_cols.items():
            if col_name not in fund_cols:
                conn.execute(f"ALTER TABLE scr_fundamentals ADD COLUMN {col_name} {col_def}")

        # Legacy accession backfill from sec_url. Match the NNNNNNNNNN-NN-NNNNNN format.
        # Example sec_url: .../Archives/edgar/data/320193/000032019325000001/0000320193-25-000001-index.htm
        # Extract the hyphenated accession: 0000320193-25-000001.
        # NOTE: production has `sec_url`, but the current _DDL emits `source_url`
        # (two historically divergent schemas). Pick whichever URL column is present.
        import re
        it_cols_after = {
            r[1] for r in conn.execute("PRAGMA table_info(scr_insider_transactions)").fetchall()
        }
        url_col = "sec_url" if "sec_url" in it_cols_after else (
            "source_url" if "source_url" in it_cols_after else None
        )
        if url_col is not None:
            legacy = conn.execute(
                f"SELECT id, {url_col} AS url FROM scr_insider_transactions "
                f"WHERE accession_number IS NULL AND {url_col} IS NOT NULL"
            ).fetchall()
            accession_re = re.compile(r"(\d{10}-\d{2}-\d{6})")
            for row in legacy:
                match = accession_re.search(row["url"])
                if match:
                    conn.execute(
                        "UPDATE scr_insider_transactions SET accession_number = ? WHERE id = ?",
                        (match.group(1), row["id"]),
                    )

        # --- Drop legacy UNIQUE(ticker, filing_date, insider_name, transaction_type, shares) ---
        # The new intended dedup is idx_insider_dedup on (ticker, accession_number,
        # insider_name, transaction_date). The legacy 5-tuple UNIQUE silently rejected
        # Form 4/A amendments where shares were unchanged (common case).
        #
        # SQLite doesn't support DROP CONSTRAINT, so we rebuild the table.
        table_sql_row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='scr_insider_transactions'"
        ).fetchone()
        if table_sql_row and "UNIQUE(ticker, filing_date, insider_name, transaction_type, shares)" in table_sql_row[0]:
            # 1. Preserve existing columns (whatever they are — handles sec_url/source_url
            #    and shares_owned_after/ownership_after column-name drift).
            existing_cols = [
                r[1] for r in conn.execute("PRAGMA table_info(scr_insider_transactions)").fetchall()
            ]
            col_list = ", ".join(existing_cols)

            # 2. Build a new CREATE TABLE matching the existing shape but WITHOUT
            #    the legacy UNIQUE. Generate it dynamically so it preserves every
            #    column definition (type, defaults, NOT NULL).
            original_sql = table_sql_row[0]
            # Remove the UNIQUE(...) line. Match the exact constraint (allow whitespace
            # and optional trailing comma).
            rebuild_sql = re.sub(
                r",\s*UNIQUE\s*\(\s*ticker\s*,\s*filing_date\s*,\s*insider_name\s*,\s*transaction_type\s*,\s*shares\s*\)",
                "",
                original_sql,
            )
            # Rename the table in the CREATE so we can do old → new → rename swap.
            rebuild_sql = rebuild_sql.replace(
                "CREATE TABLE scr_insider_transactions",
                "CREATE TABLE scr_insider_transactions_new",
                1,
            )

            # 3. Execute the rebuild.
            conn.execute(rebuild_sql)
            conn.execute(
                f"INSERT INTO scr_insider_transactions_new ({col_list}) "
                f"SELECT {col_list} FROM scr_insider_transactions"
            )
            conn.execute("DROP TABLE scr_insider_transactions")
            conn.execute(
                "ALTER TABLE scr_insider_transactions_new RENAME TO scr_insider_transactions"
            )

            # 4. Re-create idx_insider_dedup on the new table (DROP TABLE above
            #    also removed its indexes — recreate explicitly).
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_insider_dedup
                  ON scr_insider_transactions (ticker, accession_number, insider_name, transaction_date)
            """)

        # Seed only if empty
        if conn.execute("SELECT COUNT(*) FROM scr_ampx_profile").fetchone()[0] == 0:
            _seed_ampx_profile_rows(conn)
        if conn.execute("SELECT COUNT(*) FROM scr_kill_list").fetchone()[0] == 0:
            _seed_kill_list_rows(conn)
        conn.commit()
    _migrated = True


def get_active_tickers() -> list[str]:
    """Return active, non-killed tickers from scr_universe."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT ticker FROM scr_universe WHERE is_active = 1 AND is_killed = 0 ORDER BY ticker"
        ).fetchall()
    return [r["ticker"] for r in rows]


# ── DDL for all 24 tables ────────────────────────────────────────────────────

_DDL = """

-- 1. scr_universe — Broad scanning universe
CREATE TABLE IF NOT EXISTS scr_universe (
    ticker TEXT PRIMARY KEY,
    company_name TEXT,
    cik TEXT,
    exchange TEXT,
    sector TEXT,
    industry TEXT,
    market_cap_m REAL,
    market_cap REAL,
    avg_volume REAL,
    ipo_date TEXT,
    is_spac INTEGER DEFAULT 0,
    is_former_spac INTEGER DEFAULT 0,
    is_tradeable_fidelity INTEGER DEFAULT 1,
    is_tradeable_robinhood INTEGER DEFAULT 1,
    is_active INTEGER DEFAULT 1,
    is_killed INTEGER DEFAULT 0,
    kill_reason TEXT,
    last_updated TEXT,
    added_date TEXT DEFAULT (date('now'))
);

-- 2. scr_fundamentals — Quarterly financial snapshot
CREATE TABLE IF NOT EXISTS scr_fundamentals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    revenue_ttm REAL,
    revenue_prev_year REAL,
    revenue_quarterly REAL,
    revenue_growth_yoy REAL,
    revenue_growth_qoq REAL,
    gross_margin REAL,
    operating_margin REAL,
    net_margin REAL,
    cash_and_equivalents REAL,
    total_debt REAL,
    debt_to_equity REAL,
    shares_outstanding REAL,
    institutional_ownership_pct REAL,
    institutional_ownership REAL,
    market_cap REAL,
    quarterly_burn_rate REAL,
    cash_runway_quarters REAL,
    free_cash_flow REAL,
    fcf_growth_yoy REAL,
    current_ratio REAL,
    roe REAL,
    capex_to_revenue REAL,
    rd_to_revenue REAL,
    data_source TEXT DEFAULT 'yfinance',
    raw_json TEXT,
    last_updated TEXT,
    UNIQUE(ticker, date)
);

-- 3. scr_price_metrics — Price-derived screening metrics
CREATE TABLE IF NOT EXISTS scr_price_metrics (
    ticker TEXT PRIMARY KEY,
    current_price REAL,
    all_time_high REAL,
    ath_date TEXT,
    high_52w REAL,
    low_52w REAL,
    pct_from_ath REAL,
    pct_from_52w_high REAL,
    avg_volume_30d REAL,
    avg_volume_10d REAL,
    volume_ratio REAL,
    float_shares REAL,
    short_interest_pct REAL,
    last_updated TEXT
);

-- 4. scr_patents — Individual patent records
CREATE TABLE IF NOT EXISTS scr_patents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    patent_number TEXT NOT NULL,
    title TEXT,
    grant_date TEXT,
    expiration_date TEXT,
    technology_area TEXT,
    is_active INTEGER DEFAULT 1,
    source TEXT DEFAULT 'USPTO',
    last_updated TEXT,
    UNIQUE(ticker, patent_number)
);

-- 5. scr_patent_summary — Aggregated patent metrics
CREATE TABLE IF NOT EXISTS scr_patent_summary (
    ticker TEXT PRIMARY KEY,
    total_patents INTEGER,
    active_patents INTEGER,
    patents_last_3yr INTEGER,
    primary_technology_area TEXT,
    has_exclusive_license INTEGER DEFAULT 0,
    exclusive_license_source TEXT,
    ip_section_summary TEXT,
    estimated_replication_years REAL,
    is_contract_manufacturer INTEGER DEFAULT 0,
    contract_mfg_evidence TEXT,
    last_updated TEXT
);

-- 6. scr_insider_transactions — SEC Form 4 data
CREATE TABLE IF NOT EXISTS scr_insider_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    filing_date TEXT NOT NULL,
    insider_name TEXT,
    insider_title TEXT,
    transaction_type TEXT NOT NULL,
    shares INTEGER,
    price_per_share REAL,
    total_value REAL,
    ownership_after INTEGER,
    is_open_market INTEGER DEFAULT 0,
    source_url TEXT
);

-- 7. scr_filing_signals — SEC filing keyword hits
CREATE TABLE IF NOT EXISTS scr_filing_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    filing_date TEXT NOT NULL,
    filing_type TEXT,
    keyword_matched TEXT NOT NULL,
    keyword_category TEXT,
    context_snippet TEXT,
    filing_url TEXT,
    relevance_score REAL DEFAULT 0.5,
    last_updated TEXT,
    UNIQUE(ticker, filing_date, keyword_matched)
);

-- 8. scr_analyst_coverage — Analyst tracking
CREATE TABLE IF NOT EXISTS scr_analyst_coverage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    analyst_count INTEGER,
    avg_price_target REAL,
    consensus_rating TEXT,
    coverage_change INTEGER,
    initiations_90d INTEGER,
    last_updated TEXT,
    UNIQUE(ticker, date)
);

-- 9. scr_supply_chain — Supplier/customer relationships
CREATE TABLE IF NOT EXISTS scr_supply_chain (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    supplier_ticker TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    customer_ticker TEXT,
    relationship_type TEXT NOT NULL,
    is_bottleneck INTEGER DEFAULT 0,
    source_filing TEXT,
    confidence REAL DEFAULT 0.5,
    first_seen TEXT,
    last_confirmed TEXT,
    UNIQUE(supplier_ticker, customer_ticker, relationship_type)
);

-- 10. scr_forcing_functions — Regulatory/physics/geo forcing functions
CREATE TABLE IF NOT EXISTS scr_forcing_functions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    category TEXT,
    description TEXT,
    effective_date TEXT,
    deadline_date TEXT,
    affected_tickers TEXT,
    demand_created TEXT,
    competitor_timeline_years REAL,
    status TEXT DEFAULT 'active',
    source TEXT,
    added_date TEXT DEFAULT (date('now'))
);

-- 11. scr_catalysts — Upcoming events
CREATE TABLE IF NOT EXISTS scr_catalysts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    catalyst_type TEXT NOT NULL,
    description TEXT,
    expected_date TEXT,
    date_confidence TEXT,
    potential_impact TEXT,
    source TEXT,
    is_resolved INTEGER DEFAULT 0,
    resolved_outcome TEXT,
    added_date TEXT DEFAULT (date('now'))
);

-- 12. scr_scores — Full dimensional scores per ticker (THE CORE TABLE)
CREATE TABLE IF NOT EXISTS scr_scores (
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,

    -- DIMENSION GROUP A: TECHNOLOGY MOAT
    d_proprietary_tech REAL DEFAULT 0,
    d_replication_timeline REAL DEFAULT 0,
    d_forcing_function REAL DEFAULT 0,
    d_bottleneck_position REAL DEFAULT 0,
    d_government_moat REAL DEFAULT 0,

    -- DIMENSION GROUP B: FINANCIAL
    d_washout REAL DEFAULT 0,
    d_revenue_inflection REAL DEFAULT 0,
    d_cash_runway REAL DEFAULT 0,
    d_balance_sheet REAL DEFAULT 0,
    d_insider_signal REAL DEFAULT 0,

    -- DIMENSION GROUP C: MARKET STRUCTURE
    d_small_float REAL DEFAULT 0,
    d_undiscovered REAL DEFAULT 0,
    d_narrative_proximity REAL DEFAULT 0,
    d_acquisition_target REAL DEFAULT 0,
    d_capital_efficiency REAL DEFAULT 0,

    -- COMPOSITES (for Mode 1 pattern matching)
    group_a_score REAL DEFAULT 0,
    group_b_score REAL DEFAULT 0,
    group_c_score REAL DEFAULT 0,
    composite_score REAL DEFAULT 0,
    ampx_similarity REAL DEFAULT 0,

    -- METADATA
    dimensions_with_data INTEGER DEFAULT 0,
    dimensions_manual INTEGER DEFAULT 0,
    data_completeness REAL DEFAULT 0,
    last_scored TEXT,
    notes TEXT,

    PRIMARY KEY(ticker, date)
);

-- 13. scr_score_history — Track score evolution over time
CREATE TABLE IF NOT EXISTS scr_score_history (
    ticker TEXT NOT NULL,
    date TEXT NOT NULL,
    composite_score REAL,
    group_a_score REAL,
    group_b_score REAL,
    group_c_score REAL,
    ampx_similarity REAL,
    PRIMARY KEY(ticker, date)
);

-- 14. scr_anomalies — Mode 2: Statistical outlier detections
CREATE TABLE IF NOT EXISTS scr_anomalies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    detected_date TEXT DEFAULT (date('now')),
    anomaly_type TEXT NOT NULL,
    dimensions_involved TEXT,
    dimension_values TEXT,
    rarity_score REAL,
    universe_percentile REAL,
    description TEXT,
    is_reviewed INTEGER DEFAULT 0,
    review_outcome TEXT,
    UNIQUE(ticker, detected_date, anomaly_type, dimensions_involved)
);

-- 15. scr_signal_spikes — Mode 3: Rate-of-change detections
CREATE TABLE IF NOT EXISTS scr_signal_spikes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    detected_date TEXT DEFAULT (date('now')),
    spike_type TEXT NOT NULL,
    previous_value REAL,
    current_value REAL,
    change_magnitude REAL,
    change_pct REAL,
    description TEXT,
    is_reviewed INTEGER DEFAULT 0,
    review_outcome TEXT,
    UNIQUE(ticker, detected_date, spike_type, description)
);

-- 16. scr_watchlist — Human-curated candidates
CREATE TABLE IF NOT EXISTS scr_watchlist (
    ticker TEXT PRIMARY KEY,
    source_mode TEXT,
    first_qualified_date TEXT,
    current_score REAL,
    peak_score REAL,
    peak_score_date TEXT,
    status TEXT DEFAULT 'watching',
    thesis_summary TEXT,
    added_date TEXT DEFAULT (date('now'))
);

-- 17. scr_kill_list — Analyzed and rejected tickers
CREATE TABLE IF NOT EXISTS scr_kill_list (
    ticker TEXT PRIMARY KEY,
    company_name TEXT,
    kill_date TEXT DEFAULT (date('now')),
    kill_reason TEXT,
    scan_version TEXT,
    can_resurrect INTEGER DEFAULT 0,
    resurrect_condition TEXT
);

-- 18. scr_sector_themes — Active macro themes
CREATE TABLE IF NOT EXISTS scr_sector_themes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    theme_name TEXT UNIQUE NOT NULL,
    description TEXT,
    status TEXT DEFAULT 'active',
    related_tickers TEXT,
    policy_drivers TEXT,
    forcing_functions TEXT,
    added_date TEXT DEFAULT (date('now')),
    last_updated TEXT
);

-- 19. scr_ampx_profile — Reference profile for similarity scoring
CREATE TABLE IF NOT EXISTS scr_ampx_profile (
    dimension TEXT PRIMARY KEY,
    value REAL NOT NULL,
    weight REAL DEFAULT 1.0,
    notes TEXT
);

-- 20. scr_user_feedback — Learning from human decisions
CREATE TABLE IF NOT EXISTS scr_user_feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    date TEXT DEFAULT (date('now')),
    source_mode TEXT,
    decision TEXT,
    reason TEXT,
    dimensions_that_mattered TEXT
);

-- 21. scr_ampx_rules_scores — AMPX Rules Screener per-ticker scores (added 2026-04-14)
CREATE TABLE IF NOT EXISTS scr_ampx_rules_scores (
    ticker TEXT NOT NULL,
    scan_date TEXT NOT NULL,
    score REAL NOT NULL,
    score_details TEXT,
    dim1_crash REAL, dim2_revgrowth REAL, dim3_debt REAL, dim4_runway REAL,
    dim5_float REAL, dim6_institutional REAL, dim7_analyst REAL, dim8_priority_industry REAL,
    dim9_short REAL, dim10_leaps REAL, dim11_insider REAL,
    dim12_dilution_drag REAL DEFAULT 0,  -- XBRL-only drag, -1.0 when 2yr dilution > AMPX_DILUTION_DRAG_PCT
    has_going_concern INTEGER DEFAULT 0,
    PRIMARY KEY (ticker, scan_date)
);
CREATE INDEX IF NOT EXISTS idx_ampx_rules_scores_date
    ON scr_ampx_rules_scores(scan_date);
CREATE INDEX IF NOT EXISTS idx_ampx_rules_scores_score
    ON scr_ampx_rules_scores(score DESC);

-- 22. scr_ampx_red_flags — AMPX Rules Screener red-flag hits (added 2026-04-14)
CREATE TABLE IF NOT EXISTS scr_ampx_red_flags (
    ticker TEXT NOT NULL,
    scan_date TEXT NOT NULL,
    flag_type TEXT NOT NULL,
    matched_phrase TEXT,
    PRIMARY KEY (ticker, scan_date, flag_type)
);

-- 23. scr_ampx_run_log — AMPX Rules Screener per-run metrics (added 2026-04-14)
CREATE TABLE IF NOT EXISTS scr_ampx_run_log (
    run_date TEXT PRIMARY KEY,
    duration_sec INTEGER,
    rows_in INTEGER,
    rows_scored INTEGER,
    red_flagged_n INTEGER,
    errors_n INTEGER,
    peak_rss_mb INTEGER
);

-- 24. scr_fundamentals_xbrl — EDGAR XBRL fundamentals (parallel to scr_fundamentals; added 2026-04-14)
CREATE TABLE IF NOT EXISTS scr_fundamentals_xbrl (
    ticker TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    fiscal_year INTEGER NOT NULL,
    period_end_date TEXT NOT NULL,
    revenue REAL,
    revenue_prev_year REAL,
    revenue_growth_yoy REAL,
    gross_profit REAL,
    operating_income REAL,
    net_income REAL,
    cash_and_equivalents REAL,
    long_term_debt REAL,
    short_term_debt REAL,
    total_debt REAL,
    total_assets REAL,
    total_liabilities REAL,
    stockholders_equity REAL,
    debt_to_equity REAL,
    shares_outstanding REAL,
    shares_outstanding_1yr_ago REAL,
    shares_outstanding_2yr_ago REAL,
    shares_outstanding_change_1yr_pct REAL,
    shares_outstanding_change_2yr_pct REAL,
    operating_cash_flow REAL,
    capex REAL,
    free_cash_flow REAL,
    quarterly_burn REAL,                  -- annual op_cf / 4 if negative, else NULL
    cash_runway_quarters REAL,            -- cash / abs(quarterly_burn) capped at 40, or NULL
    data_quality_flags TEXT,              -- JSON list of concerns e.g. ["no_debt_reported","incomplete_history"]
    source TEXT DEFAULT 'edgar_xbrl',
    PRIMARY KEY (ticker, fiscal_year)
);
CREATE INDEX IF NOT EXISTS idx_xbrl_ticker ON scr_fundamentals_xbrl(ticker);
CREATE INDEX IF NOT EXISTS idx_xbrl_fetched ON scr_fundamentals_xbrl(fetched_at);

-- 25. scr_patent_cpcs — Structured CPC classifications per patent (added 2026-04-15)
-- Replaces the lossy technology_area-as-first-CPC-title pattern. Each patent has
-- 2-5 CPCs; preserving the full list enables thesis-driven moat scoring via
-- prefix-match queries (e.g., nuclear thesis: cpc_group_id LIKE 'G21%').
CREATE TABLE IF NOT EXISTS scr_patent_cpcs (
    ticker TEXT NOT NULL,
    patent_number TEXT NOT NULL,
    cpc_group_id TEXT NOT NULL,
    cpc_group_title TEXT,
    rank INTEGER DEFAULT 0,
    PRIMARY KEY (ticker, patent_number, cpc_group_id)
);
CREATE INDEX IF NOT EXISTS idx_scr_patent_cpcs_cpc_group_id
    ON scr_patent_cpcs(cpc_group_id);

-- 26. scr_thesis_hits — Asymmetry scanner output per (ticker, thesis, scan) (added 2026-04-15)
-- One row per ticker scored by a given thesis on a given scan date. Composite
-- score combines moat/demand/valuation components per the thesis config.
CREATE TABLE IF NOT EXISTS scr_thesis_hits (
    ticker TEXT NOT NULL,
    thesis_id TEXT NOT NULL,
    scan_date TEXT NOT NULL,
    tier TEXT,                     -- tier1 / tier2 / tier3 (deepest matched)
    moat_score REAL,               -- CPC match fraction OR alt-signal score, 0..1
    demand_stage TEXT,             -- pre_qualification / qualified / early_ramp / full_scale
    asymmetry_multiple REAL,       -- probability-weighted fwd_value / current_ev
    composite_score REAL,          -- weighted combination per thesis config
    PRIMARY KEY (ticker, thesis_id, scan_date)
);
CREATE INDEX IF NOT EXISTS idx_scr_thesis_hits_date
    ON scr_thesis_hits(scan_date);
CREATE INDEX IF NOT EXISTS idx_scr_thesis_hits_ticker
    ON scr_thesis_hits(ticker);
CREATE INDEX IF NOT EXISTS idx_scr_thesis_hits_thesis
    ON scr_thesis_hits(thesis_id);

-- v_cross_thesis_overlap — the highest-signal view of the whole system.
-- Surfaces tickers appearing in 2+ theses on the same scan date. Sort by
-- thesis_count desc then avg_score desc to prioritise convergence candidates.
CREATE VIEW IF NOT EXISTS v_cross_thesis_overlap AS
SELECT
    ticker,
    scan_date,
    COUNT(DISTINCT thesis_id) AS thesis_count,
    GROUP_CONCAT(DISTINCT thesis_id) AS theses,
    AVG(composite_score) AS avg_score,
    MAX(composite_score) AS peak_score
FROM scr_thesis_hits
GROUP BY ticker, scan_date
HAVING thesis_count >= 2;

-- 27. scr_form4_poll_log — per-run observability row for the Form 4 atom poller
CREATE TABLE IF NOT EXISTS scr_form4_poll_log (
    poll_ts TEXT NOT NULL,
    source TEXT NOT NULL,
    filings_fetched INTEGER DEFAULT 0,
    filings_in_universe INTEGER DEFAULT 0,
    filings_skipped_no_cik INTEGER DEFAULT 0,
    new_transactions_inserted INTEGER DEFAULT 0,
    amendments_resolved INTEGER DEFAULT 0,
    errors_n INTEGER DEFAULT 0,
    duration_s INTEGER DEFAULT 0,
    notes TEXT DEFAULT 'ok',
    PRIMARY KEY (poll_ts, source)
);

-- 28. scr_text_search_hits — Raw EFTS keyword hits for discovery
CREATE TABLE IF NOT EXISTS scr_text_search_hits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    scan_date TEXT NOT NULL,
    keyword TEXT NOT NULL,
    keyword_layer TEXT NOT NULL,
    moat_type TEXT,
    filing_date TEXT,
    filing_type TEXT,
    filing_url TEXT,
    cik TEXT,
    company_name TEXT,
    relevance_score REAL DEFAULT 0.5,
    UNIQUE(ticker, scan_date, keyword, filing_date)
);
CREATE INDEX IF NOT EXISTS idx_text_search_hits_ticker ON scr_text_search_hits(ticker);
CREATE INDEX IF NOT EXISTS idx_text_search_hits_scan ON scr_text_search_hits(scan_date);

-- 29. scr_discovery_flags — Per-ticker discovery summary
CREATE TABLE IF NOT EXISTS scr_discovery_flags (
    ticker TEXT NOT NULL,
    scan_date TEXT NOT NULL,
    company_name TEXT,
    cik TEXT,
    layer_2a INTEGER DEFAULT 0,
    layer_2b INTEGER DEFAULT 0,
    layer_2c INTEGER DEFAULT 0,
    flag_count INTEGER DEFAULT 0,
    keywords_matched TEXT,
    structural_signals TEXT,
    moat_types TEXT,
    moat_type_count INTEGER DEFAULT 0,
    composite_score REAL DEFAULT 0,
    sector TEXT,
    rank INTEGER,
    PRIMARY KEY (ticker, scan_date)
);
CREATE INDEX IF NOT EXISTS idx_discovery_flags_score
    ON scr_discovery_flags(scan_date, composite_score DESC);

-- 30. scr_moat_signals — Individual moat type signals per ticker
CREATE TABLE IF NOT EXISTS scr_moat_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    scan_date TEXT NOT NULL,
    moat_type TEXT NOT NULL,
    signal_source TEXT NOT NULL,
    signal_strength REAL DEFAULT 0.5,
    evidence TEXT,
    UNIQUE(ticker, scan_date, moat_type, signal_source)
);
CREATE INDEX IF NOT EXISTS idx_moat_signals_ticker ON scr_moat_signals(ticker);
CREATE INDEX IF NOT EXISTS idx_moat_signals_scan ON scr_moat_signals(scan_date);

-- 31. scr_deep_dive_results — Automated deep-dive verdicts per discovery scan
CREATE TABLE IF NOT EXISTS scr_deep_dive_results (
    ticker TEXT NOT NULL,
    scan_date TEXT NOT NULL,
    verdict TEXT NOT NULL,
    moat_score INTEGER,
    moat_details TEXT,
    best_analog TEXT,
    analog_match_count INTEGER,
    balance_sheet_pass INTEGER,
    insider_pass INTEGER,
    phase INTEGER,
    phase_name TEXT,
    disqualifier_kills TEXT,
    discovery_score REAL,
    summary TEXT,
    PRIMARY KEY (ticker, scan_date)
);
CREATE INDEX IF NOT EXISTS idx_deep_dive_date ON scr_deep_dive_results(scan_date);

-- 32. scr_discovery_history — Weekly score snapshots for trend tracking
CREATE TABLE IF NOT EXISTS scr_discovery_history (
    ticker TEXT NOT NULL,
    scan_date TEXT NOT NULL,
    composite_score REAL,
    flag_count INTEGER,
    moat_type_count INTEGER,
    sector TEXT,
    gov_sole_source INTEGER DEFAULT 0,
    customer_mentions INTEGER DEFAULT 0,
    PRIMARY KEY (ticker, scan_date)
);
CREATE INDEX IF NOT EXISTS idx_discovery_history_date ON scr_discovery_history(scan_date);

-- 33. scr_forward_moat_scores — Forward Moat Scanner per-ticker signal scores
CREATE TABLE IF NOT EXISTS scr_forward_moat_scores (
    ticker TEXT NOT NULL,
    scan_date TEXT NOT NULL,
    sig_backlog REAL DEFAULT 0,
    sig_segment_crossover REAL DEFAULT 0,
    sig_partnership_mismatch REAL DEFAULT 0,
    sig_new_tam REAL DEFAULT 0,
    sig_capex_inflection REAL DEFAULT 0,
    sig_tech_milestone REAL DEFAULT 0,
    forward_score REAL DEFAULT 0,
    backlog_current TEXT,
    backlog_prior TEXT,
    backlog_growth_pct REAL,
    partnership_names TEXT,
    partnership_verified INTEGER DEFAULT 0,
    new_tam_keywords TEXT,
    capex_growth_pct REAL,
    rd_growth_pct REAL,
    milestone_keywords TEXT,
    forward_verdict TEXT,
    forward_verdict_reason TEXT,
    PRIMARY KEY (ticker, scan_date)
);

-- 34. scr_forward_moat_history — Weekly forward + moat score snapshots
CREATE TABLE IF NOT EXISTS scr_forward_moat_history (
    ticker TEXT NOT NULL,
    scan_date TEXT NOT NULL,
    forward_score REAL DEFAULT 0,
    moat_score REAL DEFAULT 0,
    combined_rank INTEGER,
    PRIMARY KEY (ticker, scan_date)
);

"""


# ── Seed data ─────────────────────────────────────────────────────────────────

def _seed_ampx_profile_rows(conn):
    """Seed the AMPX reference vector (scores at $0.61, September 2024)."""
    rows = [
        ('d_proprietary_tech', 0.8, 1.5, 'Silicon nanowire anode — proprietary chemistry, 20+ patents'),
        ('d_replication_timeline', 0.7, 1.3, 'Materials science — 3-5 year replication minimum'),
        ('d_forcing_function', 0.6, 1.5, 'Section 1709 + defense drone demand emerging but not yet law'),
        ('d_bottleneck_position', 0.5, 1.2, 'Only commercial SiNW anode producer but not yet sole-source'),
        ('d_government_moat', 0.3, 1.0, 'Some defense contracts but not ITAR/nuclear-level barrier'),
        ('d_washout', 1.0, 1.5, '97.7% decline from $26 ATH to $0.61. Maximum washout.'),
        ('d_revenue_inflection', 0.7, 1.5, 'Revenue 167% YoY from tiny $9M base. Growth rate was the signal.'),
        ('d_cash_runway', 0.6, 1.3, '~$73M cash, ~$35M/yr burn = ~2 year runway. Tight but survivable.'),
        ('d_balance_sheet', 0.9, 1.2, 'Zero debt. Critical for surviving the trough.'),
        ('d_insider_signal', 0.5, 1.0, 'No selling at lows but no major open-market purchases either.'),
        ('d_small_float', 0.8, 1.3, '~115M shares, ~$70M market cap. Tiny float amplified buying.'),
        ('d_undiscovered', 0.9, 1.4, 'Zero analyst coverage at the low. Nobody was watching.'),
        ('d_narrative_proximity', 0.7, 1.3, 'Drone + eVTOL battery supplier — picks and shovels for hot sectors.'),
        ('d_acquisition_target', 0.4, 0.8, 'Possible but not primary thesis. Tech too strategic to sell cheap.'),
        ('d_capital_efficiency', 0.5, 0.8, 'Was capex-heavy before Korean alliance pivot. Mixed at the time.'),
    ]
    conn.executemany(
        """INSERT OR REPLACE INTO scr_ampx_profile
           (dimension, value, weight, notes) VALUES (?, ?, ?, ?)""",
        rows,
    )


def _seed_kill_list_rows(conn):
    """Seed the kill list with tickers from scans I-IV and current/recent positions."""
    kills = [
        # ── Scan I: Large-cap pipeline blacklist (too efficient, no multi-bagger upside) ──
        ('CAT', 'Caterpillar', 'Large-cap industrial, fully valued, 0% signal WR', 'scan-I'),
        ('GILD', 'Gilead Sciences', 'Mature pharma, flat growth, 0% signal WR', 'scan-I'),
        ('JNJ', 'Johnson & Johnson', 'Mega-cap healthcare, litigation risk, 0% signal WR', 'scan-I'),
        ('CMCSA', 'Comcast', 'Mature cable/media, cord-cutting headwind, 0% signal WR', 'scan-I'),
        ('PG', 'Procter & Gamble', 'Mega-cap consumer staple, no growth catalyst', 'scan-I'),
        ('PM', 'Philip Morris', 'Tobacco declining, regulatory risk, no moat thesis', 'scan-I'),
        ('MCD', "McDonald's", 'Mega-cap fast food, fully valued, no tech edge', 'scan-I'),
        ('PFE', 'Pfizer', 'Revenue cliff post-COVID, pipeline uncertainty', 'scan-I'),
        ('LMT', 'Lockheed Martin', 'Mega-cap defense, low growth, too large for multi-bagger', 'scan-I'),
        ('SPY', 'SPDR S&P 500 ETF', 'ETF — not a stock, cannot be a multi-bagger', 'scan-I'),
        # ── Scan II: Mega-cap tech (>$500B, already discovered) ──
        ('AAPL', 'Apple', 'Mega-cap, $3T+ mkt cap, fully discovered', 'scan-II'),
        ('MSFT', 'Microsoft', 'Mega-cap, $3T+ mkt cap, fully discovered', 'scan-II'),
        ('GOOGL', 'Alphabet', 'Mega-cap, $2T+ mkt cap, fully discovered', 'scan-II'),
        ('GOOG', 'Alphabet Class C', 'Mega-cap duplicate, same as GOOGL', 'scan-II'),
        ('AMZN', 'Amazon', 'Mega-cap, $2T+ mkt cap, fully discovered', 'scan-II'),
        ('NVDA', 'NVIDIA', 'Mega-cap, $2T+ mkt cap, fully discovered', 'scan-II'),
        ('META', 'Meta Platforms', 'Mega-cap, $1.5T+ mkt cap, fully discovered', 'scan-II'),
        ('TSLA', 'Tesla', 'Mega-cap, $800B+ mkt cap, fully discovered', 'scan-II'),
        ('AVGO', 'Broadcom', 'Large-cap semi, $700B+ mkt cap, fully discovered', 'scan-II'),
        ('COST', 'Costco', 'Mega-cap retail, 50+ P/E, no tech moat', 'scan-II'),
        # ── Scan III: Mature industrials / utilities / REITs (no multi-bagger DNA) ──
        ('XOM', 'Exxon Mobil', 'Mega-cap oil, commodity-driven, no tech moat', 'scan-III'),
        ('CVX', 'Chevron', 'Mega-cap oil, commodity-driven, no tech moat', 'scan-III'),
        ('WMT', 'Walmart', 'Mega-cap retail, razor-thin margins, fully valued', 'scan-III'),
        ('JPM', 'JPMorgan Chase', 'Mega-cap bank, regulatory heavy, no multi-bagger path', 'scan-III'),
        ('V', 'Visa', 'Mega-cap payments, fully valued at 30x earnings', 'scan-III'),
        ('MA', 'Mastercard', 'Mega-cap payments, same as V thesis', 'scan-III'),
        ('UNH', 'UnitedHealth', 'Mega-cap insurance, regulatory risk, fully valued', 'scan-III'),
        ('HD', 'Home Depot', 'Mega-cap retail, cyclical housing, no tech moat', 'scan-III'),
        ('KO', 'Coca-Cola', 'Mega-cap consumer staple, 1-2% growth, fully valued', 'scan-III'),
        ('PEP', 'PepsiCo', 'Mega-cap consumer staple, same as KO thesis', 'scan-III'),
        ('DIS', 'Walt Disney', 'Large-cap media, streaming losses, legacy headwinds', 'scan-III'),
        ('NFLX', 'Netflix', 'Large-cap streaming, high valuation, no tech moat for screener', 'scan-III'),
        # ── Scan IV: Current/recent positions (separate tracking) ──
        ('AMPX', 'Amprius Technologies', 'Current/recent position — separate tracking', 'scan-IV'),
        ('OKLO', 'Oklo Inc', 'Current/recent position — separate tracking', 'scan-IV'),
        ('RKLB', 'Rocket Lab USA', 'Current/recent position — separate tracking', 'scan-IV'),
        ('LUNR', 'Intuitive Machines', 'Current/recent position — separate tracking', 'scan-IV'),
        ('ASTS', 'AST SpaceMobile', 'Current/recent position — separate tracking', 'scan-IV'),
        ('ACHR', 'Archer Aviation', 'Current/recent position — separate tracking', 'scan-IV'),
        ('JOBY', 'Joby Aviation', 'Current/recent position — separate tracking', 'scan-IV'),
        ('IONQ', 'IonQ Inc', 'Current/recent position — separate tracking', 'scan-IV'),
        ('SMR', 'NuScale Power', 'Current/recent position — separate tracking', 'scan-IV'),
        ('PLTR', 'Palantir Technologies', 'Current/recent position — separate tracking', 'scan-IV'),
        ('ALAB', 'Astera Labs', 'Current/recent position — separate tracking', 'scan-IV'),
    ]
    conn.executemany(
        """INSERT OR IGNORE INTO scr_kill_list
           (ticker, company_name, kill_reason, scan_version)
           VALUES (?, ?, ?, ?)""",
        kills,
    )
