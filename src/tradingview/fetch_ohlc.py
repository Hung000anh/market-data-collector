import pandas as pd
from tradingview_websocket import TradingViewWebSocket
from src.config.supabase_client import get_symbols


def to_dataframe(data):
    records = []
    for item in data:
        ts, op, hi, lo, cl, vol = item['v']
        records.append({
            "timestamp": ts,
            "open": op,
            "high": hi,
            "low": lo,
            "close": cl,
            "volume": vol
        })
    
    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")  # unix → datetime
    return df

def fetch_ohlc(symbol: str, timeframe: str, candles: int = 100):
    ws = TradingViewWebSocket(symbol, timeframe, candles)
    ws.connect()
    ws.run()
    df = to_dataframe(ws.result_data)
    return df

if __name__ == "__main__":
    # Lấy danh sách các cặp 1D
    df_symbols = get_symbols(timeframe_filter='1D')

    # Loop fetch OHLC cho tất cả
    for _, row in df_symbols.iterrows():
        exchange = row['exchange']
        symbol = row['symbol']
        timeframe = row['timeframe']
        
        tv_symbol = f"{exchange}:{symbol}"  # nối chuỗi đúng định dạng TradingView
        
        print(f"Fetching {tv_symbol} {timeframe}...")
        try:
            df_ohlc = fetch_ohlc(tv_symbol, timeframe)
            print(df_ohlc.head())
        except Exception as e:
            print(f"Failed to fetch {tv_symbol}: {e}")