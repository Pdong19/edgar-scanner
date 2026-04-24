"""
Multi-Bagger Stock Screener Module

This module is completely independent of the signal pipeline. It shares the same
SQLite database file but uses only scr_ prefixed tables for writes. It reads from
price_history, watchlist, stocks, fundamentals, and earnings_calendar as read-only
data sources. It never writes to signals, signal_outcomes, paper_trades, or any
table used by the trading pipeline.

Isolation contract:
- All screener tables use the 'scr_' prefix
- No imports from scripts/ (main pipeline modules)
- Read-only access to shared tables (SELECT only, no INSERT/UPDATE/DELETE)
- Independent cron schedule, independent configuration
- Does not affect signal generation, reconciliation, or paper trading
"""
