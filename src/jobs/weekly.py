from config.supabase_client import get_symbols

from src.config.supabase_client import get_symbols, has_price_data, insert_prices
from src.tradingview.fetch_ohlc import fetch_ohlc
from src.utils.supabase_utils import ohlc_to_supabase_records
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