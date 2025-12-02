from src.config.supabase_client import get_symbols, has_price_data, insert_prices, get_cftc_contracts, insert_cftc_report
from src.tradingview.fetch_ohlc import fetch_ohlc
from src.utils.supabase_utils import ohlc_to_supabase_records
from src.cftc.cot_report import fetch_cftc_report
import math
import datetime
import pandas as pd


symbols_df = get_symbols(timeframe_filter="1W")

for _, row in symbols_df.iterrows():
    symbol_timeframe_id = row["id"]
    exchange_name = row["exchange"]
    symbol_code = row["symbol"]
    timeframe = row["timeframe"]

    candles_to_fetch = 5 if has_price_data(symbol_timeframe_id) else 20000
    tradingview_symbol = f"{exchange_name}:{symbol_code}"

    print(f"\n▶ Fetching {tradingview_symbol} | {timeframe} | {candles_to_fetch} candles")

    try:
        df_ohlc = fetch_ohlc(
            symbol=tradingview_symbol,
            timeframe=timeframe,
            candles=candles_to_fetch
        )

        print(df_ohlc.head())

        # --- Chuyển sang format Supabase ---
        records = ohlc_to_supabase_records(symbol_timeframe_id, df_ohlc)

        # insert lên Supabase, tránh trùng
        insert_prices(records)

    except Exception as e:
        print(f"❌ Failed to fetch {tradingview_symbol}: {e}")

print("🔹 Fetching CFTC contracts...")
df_cftc_contracts = get_cftc_contracts()

print("🔹 Fetching COT report...")
current_year = datetime.datetime.now().year
df_cftc_report = fetch_cftc_report(current_year)

print("🔹 Preprocessing and merging data...")
df_cftc_report = df_cftc_report.rename(columns={
    'CFTC Contract Market Code': 'market_code',
    'As of Date in Form YYYY-MM-DD': 'report_date',
    'Open Interest (All)': 'open_interest_all',
    'Noncommercial Positions-Long (All)': 'noncommercial_long',
    'Noncommercial Positions-Short (All)': 'noncommercial_short',
    'Commercial Positions-Long (All)': 'commercial_long',
    'Commercial Positions-Short (All)': 'commercial_short',
    'Nonreportable Positions-Long (All)': 'nonreportable_long',
    'Nonreportable Positions-Short (All)': 'nonreportable_short',
    '% of OI-Commercial-Long (All)': 'pct_oi_commercial_long',
    '% of OI-Commercial-Short (All)': 'pct_oi_commercial_short',
    '% of OI-Noncommercial-Long (All)': 'pct_oi_noncommercial_long',
    '% of OI-Noncommercial-Short (All)': 'pct_oi_noncommercial_short',
})

# Merge contracts + reports
df_merged = pd.merge(
    df_cftc_report,
    df_cftc_contracts.rename(columns={'id': 'cftc_contract_id'}),
    how='left',
    on='market_code'
)

df_final = df_merged[
    ['cftc_contract_id', 'report_date', 'open_interest_all',
        'noncommercial_long', 'noncommercial_short',
        'commercial_long', 'commercial_short',
        'nonreportable_long', 'nonreportable_short',
        'pct_oi_commercial_long', 'pct_oi_commercial_short',
        'pct_oi_noncommercial_long', 'pct_oi_noncommercial_short']
].dropna(subset=['cftc_contract_id'])
batch_size = 500
records = df_final.to_dict(orient="records")
total = len(records)
num_batches = math.ceil(total / batch_size)

success_count = 0
fail_count = 0
print(df_final)

success_count, fail_count = insert_cftc_report(records, batch_size=batch_size)

print(f"\n🎉 Done! Uploaded: {success_count}, Failed: {fail_count}")
