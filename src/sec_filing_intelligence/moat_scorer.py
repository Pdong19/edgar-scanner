"""CPC-match moat scorer for the SEC Filing Intelligence asymmetry scanner.

Given a ticker and a thesis config, returns the fraction of the ticker's
active patents whose CPCs match any of the thesis's configured prefixes.
Score in [0.0, 1.0].

This is the CPC-mode moat score. Theses with only `alternative_moat_signals`
get 0.0 here; the runner uses a separate alt-signal path for those.

Query pattern uses LIKE 'prefix%' which SQLite can answer via the
idx_scr_patent_cpcs_cpc_group_id index (prefix-anchored LIKE is index-usable).
A patent matching multiple thesis prefixes is counted ONCE (DISTINCT on
patent_number) to prevent score inflation.
"""

from .db import get_connection


def score_moat(ticker: str, cpc_prefixes: list[str]) -> float:
    """Return CPC-match moat score in [0.0, 1.0] for this ticker.

    Args:
        ticker: Stock ticker symbol.
        cpc_prefixes: CPC classification prefixes to match against (e.g., ["G21", "H01M"]).

    Returns 0.0 when:
    - ticker has no patents
    - ticker has patents but none match the CPC prefixes
    - cpc_prefixes is empty
    """
    prefixes = cpc_prefixes
    if not prefixes:
        return 0.0

    like_clauses = " OR ".join(["c.cpc_group_id LIKE ?"] * len(prefixes))
    like_params = [f"{p}%" for p in prefixes]

    with get_connection() as conn:
        total = conn.execute(
            "SELECT COUNT(DISTINCT patent_number) FROM scr_patents "
            "WHERE ticker=? AND is_active=1",
            (ticker,),
        ).fetchone()[0]
        if total == 0:
            return 0.0

        matches = conn.execute(
            f"""SELECT COUNT(DISTINCT c.patent_number)
                FROM scr_patent_cpcs c
                JOIN scr_patents p USING (ticker, patent_number)
                WHERE c.ticker=? AND p.is_active=1 AND ({like_clauses})""",
            (ticker, *like_params),
        ).fetchone()[0]

    return matches / total
