# data/14_compute_suspensions.py
import numpy as np
import pandas as pd
from pymongo import MongoClient, UpdateOne, ASCENDING
from datetime import datetime
from tqdm import tqdm

# ---------------- Configuration ----------------
MONGO_HOST = "localhost"
MONGO_PORT = 27017

# 数据库配置 (根据你的实际情况调整)
# 假设你的日历可能在 vnpy_stock 或 vnpy_master，这里我会都试一下
DB_STOCK_NAME = "vnpy_stock"
DB_MASTER_NAME = "vnpy_master"

# 集合名称配置
COL_CALENDAR = "trade_date_hist"  # 或 "trading_calendar"，脚本会自动尝试
COL_INDEX = "index_daily"
COL_STOCK = "stock_daily"
COL_STATUS = "stock_status_history"


# 字段配置
# 日历表通常只有一个 date 字段
# 指数/股票表里可能是 date 或 datetime
# -----------------------------------------------

def get_db_client():
    return MongoClient(MONGO_HOST, MONGO_PORT)


def load_calendar_from_collection(client, db_name, col_name, date_field="date"):
    """尝试从指定库表加载日历"""
    try:
        db = client[db_name]
        if col_name not in db.list_collection_names():
            return None

        print(f"   Trying [{db_name}.{col_name}]...")
        cursor = db[col_name].find({}, {date_field: 1, "_id": 0}).sort(date_field, ASCENDING)
        dates = [x.get(date_field) for x in cursor if x.get(date_field)]

        if dates:
            return pd.to_datetime(dates).sort_values().unique()
    except Exception as e:
        print(f"   ⚠️ Error reading {db_name}.{col_name}: {e}")
    return None


def get_master_calendar(client):
    """
    智能获取基准交易日历
    优先级: 交易日历表 > 上证指数
    """
    print("📅 Initializing Master Calendar...")

    # 1. 尝试从 vnpy_stock 或 vnpy_master 读取交易日历
    # 常见的集合名: trade_date_hist (AKShare默认), trading_calendar (vnpy默认)
    candidates = [
        (DB_STOCK_NAME, "trade_date_hist", "trade_date"),  # AKShare tool 也就是 09号脚本通常存为 trade_date
        (DB_STOCK_NAME, "trading_calendar", "date"),
        (DB_MASTER_NAME, "trading_calendar", "date"),
    ]

    for db_name, col_name, date_field in candidates:
        dates = load_calendar_from_collection(client, db_name, col_name, date_field)
        if dates is not None and len(dates) > 0:
            print(f"✅ Loaded Master Calendar from [{db_name}.{col_name}]. Total: {len(dates)}")
            return dates

    # 2. Fallback: 使用上证指数
    print("⚠️ No standalone calendar found. Fallback to Index (sh000001)...")
    db = client[DB_STOCK_NAME]
    cursor = db[COL_INDEX].find({"symbol": "sh000001"}, {"datetime": 1, "_id": 0}).sort("datetime", ASCENDING)
    dates = [x.get("datetime") for x in cursor]

    if dates:
        dt_index = pd.to_datetime(dates).sort_values().unique()
        print(f"✅ Loaded Index Calendar (sh000001). Total: {len(dt_index)}")
        return dt_index

    raise RuntimeError("❌ CRITICAL: Could not generate Master Calendar! No calendar table and no index data found.")


def compute_suspensions(client, master_calendar):
    """
    核心计算逻辑
    """
    db = client[DB_STOCK_NAME]
    collection_status = db[COL_STATUS]

    print("Fetching stock list...")
    stocks = db[COL_STOCK].distinct("symbol")
    stocks.sort()

    print(f"🚀 Analyzing {len(stocks)} stocks for suspension gaps...")

    ops = []

    # 预计算: 将 master_calendar 转为 numpy array 以加速搜索
    # 确保是 datetime64[ns] 类型
    master_arr = master_calendar.values.astype('datetime64[D]')

    pbar = tqdm(stocks)
    for symbol in pbar:
        try:
            # 1. 获取个股所有交易日期
            # 兼容 date 和 datetime 字段
            projection = {"date": 1, "datetime": 1, "_id": 0}
            cursor = db[COL_STOCK].find({"symbol": symbol}, projection)

            stock_dates = []
            for doc in cursor:
                # 优先取 datetime (字符串), 其次取 date (datetime obj)
                d = doc.get("datetime") or doc.get("date")
                if d:
                    stock_dates.append(d)

            if not stock_dates:
                continue

            # 转为 datetime64[D]
            actual_dates = pd.to_datetime(stock_dates).values.astype('datetime64[D]')
            actual_dates.sort()

            # 2. 确定生命周期 (上市日 ~ 退市日/最新数据日)
            min_date = actual_dates[0]
            max_date = actual_dates[-1]

            # 3. 截取理论应有的交易日 (Expected)
            # 在 Master 中找到 min_date 和 max_date 的位置
            # searchsorted: find indices where elements should be inserted to maintain order
            start_idx = np.searchsorted(master_arr, min_date)
            end_idx = np.searchsorted(master_arr, max_date, side='right')

            expected_slice = master_arr[start_idx:end_idx]

            # 4. 计算差集 (Suspensions = Expected - Actual)
            # np.setdiff1d 返回在 expected 中但不在 actual 中的元素
            susp_dates_arr = np.setdiff1d(expected_slice, actual_dates)

            if len(susp_dates_arr) == 0:
                continue

            # 5. 将离散日期合并为区间
            intervals = []

            # 技巧: 寻找连续的索引
            # 首先找到 susp_dates 在 master_arr 中的索引位置
            # 比如 master 是 [1,2,3,4,5], susp 是 [2,3,5]
            # indices 是 [1,2,4]
            # diff 是 [1, 2] -> 2不等于1，说明断开了

            # 这里我们用一个简单的循环来合并区间，虽然不是最快但逻辑最清晰
            # 将 numpy datetime64 转回 pandas Timestamp 以便提取 .date()
            susp_dates_pd = pd.to_datetime(susp_dates_arr)

            if len(susp_dates_pd) > 0:
                current_start = susp_dates_pd[0]
                current_end = susp_dates_pd[0]

                # 获取 master 中对应的索引，用于判断"是否紧邻的交易日"
                # isin 遮罩
                mask = np.isin(master_arr, susp_dates_arr)
                susp_indices = np.where(mask)[0]  # master 中的索引位置

                # 分组: 如果 index 是连续的 (diff==1)，则属于同一波停牌
                # 使用 shift 比较
                if len(susp_indices) > 0:
                    # group_id 会在索引不连续时增加
                    # [1, 2, 4, 5] -> diff -> [1, 2, 1] -> diff!=1 -> [False, True, False] -> cumsum -> [0, 1, 1, 1] ...
                    # 更简单的方法: x - i (如果连续，这个差值是常数)
                    groups = susp_indices - np.arange(len(susp_indices))

                    # 遍历分组
                    unique_groups = np.unique(groups)
                    for g in unique_groups:
                        group_indices = susp_indices[groups == g]
                        # 映射回日期
                        start_dt = pd.to_datetime(master_arr[group_indices[0]])
                        end_dt = pd.to_datetime(master_arr[group_indices[-1]])

                        intervals.append({
                            "start": start_dt,
                            "end": end_dt,
                            "reason": "Missing Data (Inferred)"
                        })

            # 6. 存入数据库
            if intervals:
                ops.append(
                    UpdateOne(
                        {"symbol": symbol},
                        {"$set": {
                            "suspensions": intervals,
                            "suspension_source": "calendar_inference",
                            "updated_at": datetime.now()
                        }},
                        upsert=True
                    )
                )

        except Exception as e:
            pbar.write(f"⚠️ Error {symbol}: {e}")
            continue

        if len(ops) >= 1000:
            collection_status.bulk_write(ops)
            ops = []

    if ops:
        collection_status.bulk_write(ops)

    print(f"\n✅ Done! Processed {len(stocks)} stocks.")


if __name__ == "__main__":
    client = get_db_client()
    master_cal = get_master_calendar(client)

    if master_cal is not None:
        compute_suspensions(client, master_cal)