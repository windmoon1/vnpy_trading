"""
Script 07 (V2.0): Download Share Capital History (Incremental Update)
---------------------------------------------------------------------
目标: 增量下载股票股本变动历史 (share_capital)。
策略:
  1. 查询数据库中该股票已有的最新变动日期 (date)。
  2. 设定 API 的 start_date 为该最新日期的前一天 (安全回溯)。
  3. 仅下载新增的记录。
"""

import akshare as ak
import pandas as pd
import time
from datetime import datetime, timedelta
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne

# ==========================================
# 配置项 (Configuration)
# ==========================================
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
COLLECTION_NAME = "share_capital"
# 首次下载的起始日期
INITIAL_START_DATE = "19900101"

def get_db():
    """获取数据库连接"""
    client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    return client[DB_NAME]

def get_stock_list() -> list:
    """获取待下载的股票列表 (从本地 stock_info 获取)"""
    db = get_db()
    # 优先从 stock_info 获取 A 股/北交所代码
    cursor = db["stock_info"].find(
        {"category": {"$in": ["STOCK_A", "STOCK_BJ", "UNKNOWN_A"]}},
        {"symbol": 1}
    )
    symbols = [doc["symbol"] for doc in cursor]
    return sorted(list(set(symbols)))

def get_last_recorded_date(symbol: str, db) -> str:
    """
    [NEW] 查询数据库中该股票股本变动的最新日期，并返回下一天的 YYYYMMDD 格式。
    """
    doc = db[COLLECTION_NAME].find_one(
        {"symbol": symbol},
        sort=[("date", -1)],
        projection={"date": 1}
    )

    if doc and 'date' in doc:
        # DB 存储格式是 YYYY-MM-DD
        latest_dt = datetime.strptime(doc['date'], "%Y-%m-%d")
        # 安全起见，从最新记录的**当天**开始重新下载（让 upsert 覆盖重复记录）
        return latest_dt.strftime("%Y%m%d")

    # 如果没有记录，返回全局起始日期
    return INITIAL_START_DATE

def download_and_save(symbol: str, db):
    """
    下载单个股票的股本变动并存入 MongoDB (增量模式)
    """

    # 1. 获取增量起始日期
    start_date_str = get_last_recorded_date(symbol, db)

    # 如果最新日期是今天，则无需更新
    today_str = datetime.now().strftime("%Y%m%d")
    if start_date_str == today_str:
        return 0

    try:
        # 2. 接口调用 (使用增量起始日期)
        current_date = today_str
        df = ak.stock_share_change_cninfo(
            symbol=symbol,
            start_date=start_date_str, # ✅ 使用增量日期
            end_date=current_date
        )

        if df is None or df.empty:
            return 0

        # 3. 字段映射和清洗
        rename_map = {
            '变动日期': 'date',
            '总股本': 'total_shares',
            '已流通股份': 'float_shares',
            '变动原因': 'change_reason'
        }

        if not set(rename_map.keys()).issubset(df.columns):
            return 0

        df = df.rename(columns=rename_map)

        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        def clean_shares(val):
            if pd.isna(val) or val == '':
                return 0.0
            try:
                return float(val) * 10000 # 万股 -> 股
            except:
                return 0.0

        df['total_shares'] = df['total_shares'].apply(clean_shares)
        df['float_shares'] = df['float_shares'].apply(clean_shares)

        # 4. 构造写入操作 (Upsert)
        requests = []
        for _, row in df.iterrows():
            filter_doc = {
                "symbol": symbol,
                "date": row["date"]
            }
            update_doc = {
                "$set": {
                    "total_shares": row["total_shares"],
                    "float_shares": row["float_shares"],
                    "change_reason": row["change_reason"],
                    "updated_at": datetime.now()
                }
            }
            requests.append(UpdateOne(filter_doc, update_doc, upsert=True))

        if requests:
            db[COLLECTION_NAME].bulk_write(requests)
            return len(requests)

        return 0

    except Exception as e:
        # print(f"Error {symbol}: {e}")
        return 0

def run():
    print("🚀 启动 [A股股本变动下载器] (增量 V2.0)...")

    db = get_db()
    symbols = get_stock_list()
    print(f"📊 目标股票数量: {len(symbols)}")

    if not symbols:
        return

    # 简单进度条
    pbar = tqdm(symbols)
    for symbol in pbar:
        # 检查是否需要跳过（如果是最新日期则不显示）
        start_date_check = get_last_recorded_date(symbol, db)
        today_str = datetime.now().strftime("%Y%m%d")

        if start_date_check == today_str:
            pbar.set_description(f"跳过 {symbol} (已最新)")
            continue

        pbar.set_description(f"下载 {symbol} (Start: {start_date_check})")
        download_and_save(symbol, db)
        time.sleep(0.1)

    print("\n✅ 增量下载完成。")

if __name__ == "__main__":
    run()