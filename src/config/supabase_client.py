import os
import pandas as pd
import requests
import json
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_CLIENT_ANON_KEY = os.getenv("SUPABASE_CLIENT_ANON_KEY") 
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
def get_symbols(timeframe_filter=None):
    """
    Lấy danh sách symbols từ Supabase, join nhiều bảng: symbols, exchanges, timeframes.
    """
    url = f"{SUPABASE_URL}/rest/v1/symbol_timeframes"
    
    # Join nested: symbols -> exchanges, timeframes
    params = {
        "select": "id,symbols(symbol,exchanges(name)),timeframes(name)"
    }

    headers = {
        "apikey": SUPABASE_CLIENT_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_CLIENT_ANON_KEY}",
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(f"Error fetching symbols: {response.text}")

    data = response.json()

    if timeframe_filter:
    # Lọc theo timeframe
        data = [item for item in data if item['timeframes']['name'] == timeframe_filter]

    # Chuyển sang DataFrame “flatten”
    records = []
    for item in data:
        records.append({
            "id": item['id'],
            "exchange": item['symbols']['exchanges']['name'],
            "symbol": item['symbols']['symbol'],
            "timeframe": item['timeframes']['name']
        })

    df = pd.DataFrame(records)
    return df

def has_price_data(symbol_timeframe_id: int) -> bool:
    """
    Kiểm tra xem symbol_timeframe_id có dữ liệu trong bảng 'prices' hay không.
    Trả về True nếu đã có dữ liệu.
    """
    url = f"{SUPABASE_URL}/rest/v1/prices"

    params = {
        "symbol_timeframe_id": f"eq.{symbol_timeframe_id}",
        "select": "id",
        "limit": 1
    }

    headers = {
        "apikey": SUPABASE_CLIENT_ANON_KEY,
        "Authorization": f"Bearer {SUPABASE_CLIENT_ANON_KEY}"
    }

    response = requests.get(url, params=params, headers=headers)

    if response.status_code != 200:
        print(f"⚠️ Supabase error: {response.text}")
        return False

    return len(response.json()) > 0

def insert_prices(records, batch_size=1000):
    """
    Gửi dữ liệu OHLC lên Supabase, tránh trùng lặp dựa trên `symbol_timeframe_id` + `timestamp`.
    Sử dụng upsert với `on_conflict`.
    """
    url = f"{SUPABASE_URL}/rest/v1/prices?on_conflict=symbol_timeframe_id,timestamp"
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"  # upsert theo PK hoặc unique constraint
    }

    # Chia nhỏ batch để tránh payload quá lớn
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]

        try:
            resp = requests.post(url, headers=headers, data=json.dumps(batch))
            if resp.status_code in [200, 201]:
                print(f"✅ Inserted batch {i//batch_size + 1} ({len(batch)} rows)")
            else:
                print(f"❌ Failed batch {i//batch_size + 1}: {resp.status_code} - {resp.text}")
        except Exception as e:
            print(f"❌ Exception in batch {i//batch_size + 1}: {e}")

if __name__ == "__main__":
    df = get_symbols(timeframe_filter='1D')
    print(df)
