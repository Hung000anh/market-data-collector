from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import pandas as pd

def ohlc_to_supabase_records(symbol_timeframe_id, df_ohlc):
    records = []

    for _, row in df_ohlc.iterrows():
        ts = row["timestamp"]

        # --- Nếu là pd.Timestamp thì lấy .timestamp() ---
        if isinstance(ts, pd.Timestamp):
            ts = ts.timestamp()   # float seconds

        record = {
            "symbol_timeframe_id": symbol_timeframe_id,
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row.get("volume", 0),
            "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc)
                        .astimezone(ZoneInfo("America/New_York"))
                        .strftime("%Y-%m-%dT%H:%M:%SZ")
        }

        records.append(record)

    return records