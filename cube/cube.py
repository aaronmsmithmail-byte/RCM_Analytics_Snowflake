from cube import config

@config('database_type')
def database_type(ctx: dict) -> str:
    return 'duckdb'

@config('database_url')
def database_url(ctx: dict) -> str:
    return 'duckdb:/cube/data/rcm_analytics.db'
