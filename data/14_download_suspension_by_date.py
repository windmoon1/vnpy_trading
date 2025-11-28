# data/14_download_suspension_by_date.py

import akshare as ak
import pandas as pd
from pymongo import MongoClient, UpdateOne, ASCENDING
from datetime import datetime, timedelta
from tqdm import tqdm
import time

# ---------------- Configuration ----------------
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"

# 存每天原始数据的集合 (临时/缓冲)
COL_DAILY_RAW = "suspension_daily_raw"
# 最终存入的目标集合 (与 ST 状态共存)
COL_TARGET = "stock_status_history"

# 起始日期 (东财数据大约从 2005 年开始比较全)
START_DATE = "20050101"


# -----------------------------------------------

def get_db():
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    return client[DB_NAME]


def get_trading_calendar(db):
    """获取交易日历列表"""
    print("📅 Loading Trading Calendar...")
    # 优先用交易日历表，没有则用指数兜底
    col = db["trade_date_hist"] if "trade_date_hist" in db.list_collection_names() else db["index_daily"]

    query = {}
    field = "trade_date" if col.name == "trade_date_hist" else "date"
    if col.name == "index_daily":
        query = {"symbol": "sh000001"}
        # 兼容你的 datetime 字段
        if db[col.name].find_one(query, {"datetime": 1}):
            field = "datetime"

    cursor = col.find(query, {field: 1, "_id": 0}).sort(field, ASCENDING)
    dates = []
    for doc in cursor:
        d_val = doc.get(field)
        if d_val:
            dates.append(pd.to_datetime(d_val))

    # 过滤 2005 年以后的日期
    dates = sorted(list(set(dates)))
    dates = [d for d in dates if d >= pd.Timestamp(START_DATE)]

    print(f"✅ Calendar Ready: {len(dates)} days from {dates[0].date()} to {dates[-1].date()}")
    return dates


def download_daily_suspensions(db, dates):
    """
    Step 1: 按日期下载并存入 suspension_daily_raw
    """
    collection = db[COL_DAILY_RAW]
    # 建索引方便去重
    collection.create_index([("date", ASCENDING), ("symbol", ASCENDING)], unique=True)

    print(f"🚀 Starting download for {len(dates)} days...")

    # 找出已经下载过的日期，支持断点续传
    existing_dates = collection.distinct("date")
    existing_dates_set = set([d.strftime("%Y%m%d") for d in existing_dates])

    download_list = [d for d in dates if d.strftime("%Y%m%d") not in existing_dates_set]
    print(f"   Skipping {len(existing_dates_set)} days, remaining {len(download_list)} days.")

    pbar = tqdm(download_list)
    for dt in pbar:
        date_str = dt.strftime("%Y%m%d")
        pbar.set_description(f"Downloading {date_str}")

        try:
            # 调用接口
            df = ak.stock_tfp_em(date=date_str)

            if df is None or df.empty:
                # 即使为空也记录一条"空记录"，防止下次重复请求（可选）
                continue

            # 数据清洗
            ops = []
            for _, row in df.iterrows():
                symbol = str(row['代码'])
                name = str(row['名称'])
                reason = str(row['停牌原因']) if '停牌原因' in row else ""

                # 时间字段处理
                suspend_time = row.get('停牌时间')  # 可能是 datetime 或 str
                resumption_time = row.get('预计复牌时间')

                doc = {
                    "date": dt,  # 这里的 date 是"查询日期"，即公告发布日
                    "symbol": symbol,
                    "name": name,
                    "reason": reason,
                    "suspend_at": str(suspend_time) if pd.notna(suspend_time) else None,
                    "resume_at": str(resumption_time) if pd.notna(resumption_time) else None,
                    "raw_source": "ak.stock_tfp_em"
                }

                ops.append(
                    UpdateOne(
                        {"date": dt, "symbol": symbol},
                        {"$set": doc},
                        upsert=True
                    )
                )

            if ops:
                collection.bulk_write(ops)

        except Exception as e:
            pbar.write(f"⚠️ Error on {date_str}: {e}")
            # 遇到网络错误稍微停一下
            time.sleep(1)

        # 礼貌限流
        time.sleep(0.1)

    print("✅ Step 1: Download Completed.")


def aggregate_to_stock_history(db):
    """
    Step 2: 将每日散点数据聚合为以股票为维度的事件列表
    """
    print("\n🔄 Step 2: Aggregating data to [stock_status_history]...")
    source_col = db[COL_DAILY_RAW]
    target_col = db[COL_TARGET]

    # 1. 获取所有涉及的股票
    symbols = source_col.distinct("symbol")
    print(f"   Found {len(symbols)} stocks with suspension records.")

    ops = []
    pbar = tqdm(symbols)

    for symbol in pbar:
        # 获取该股票的所有记录，按日期排序
        cursor = source_col.find({"symbol": symbol}).sort("date", ASCENDING)
        records = list(cursor)

        if not records:
            continue

        # 转换格式
        suspension_list = []
        for r in records:
            # 清洗一下日期
            try:
                start_dt = pd.to_datetime(r['suspend_at']) if r.get('suspend_at') else r['date']
                end_dt = pd.to_datetime(r['resume_at']) if r.get('resume_at') else None

                item = {
                    "start": start_dt,
                    "reason": r.get('reason', '')
                }
                if end_dt:
                    item['end'] = end_dt

                suspension_list.append(item)
            except:
                continue

        # 简单的去重逻辑（因为有些长停牌可能每天都在榜单上）
        # 这里我们暂且全部存入，DataLoader 读取时可以用区间合并逻辑
        # 或者只存 unique 的 start_date

        if suspension_list:
            ops.append(
                UpdateOne(
                    {"symbol": symbol},
                    {"$set": {
                        "suspensions_em": suspension_list,  # 使用新字段避免覆盖之前的 inference 结果，方便对比
                        "suspensions_source": "eastmoney_api",
                        "updated_at": datetime.now()
                    }},
                    upsert=True
                )
            )

        if len(ops) >= 500:
            target_col.bulk_write(ops)
            ops = []

    if ops:
        target_col.bulk_write(ops)

    print("✅ Aggregation Completed.")


if __name__ == "__main__":
    db_client = get_db()

    # 1. 获取日历
    calendar_dates = get_trading_calendar(db_client)

    # 2. 下载数据 (耗时较长，支持断点)
    download_daily_suspensions(db_client, calendar_dates)

    # 3. 聚合入库
    # aggregate_to_stock_history(db_client)