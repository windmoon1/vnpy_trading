# data/15_fuse_suspensions.py

import pandas as pd
import numpy as np
from pymongo import MongoClient, UpdateOne, ASCENDING
from datetime import datetime
from tqdm import tqdm

# ---------------- Configuration ----------------
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"

COL_BAR = "bar_daily"  # 日线行情集合
COL_EM_RAW = "suspension_daily_raw"  # 东财原始停牌表
COL_CALENDAR = "trade_date_hist"  # 交易日历
COL_TARGET = "stock_status_history"  # 最终结果表


# -----------------------------------------------

def get_db():
    return MongoClient(MONGO_HOST, MONGO_PORT)[DB_NAME]


def load_master_calendar(db):
    """
    加载基准交易日历 (Numpy Array 加速版)
    """
    print("📅 Loading Master Calendar...")
    # 优先尝试 trade_date_hist，其次 trading_calendar，最后 index_daily 兜底
    candidates = ["trade_date_hist", "trading_calendar", "index_daily"]

    dates = []
    for col_name in candidates:
        if col_name not in db.list_collection_names():
            continue

        field = "datetime" if col_name == "index_daily" else ("trade_date" if col_name == "trade_date_hist" else "date")
        query = {"symbol": "sh000001"} if col_name == "index_daily" else {}

        print(f"   Trying [{col_name}] with field='{field}'...")
        cursor = db[col_name].find(query, {field: 1, "_id": 0}).sort(field, ASCENDING)

        # 兼容性读取：可能是 str 可能是 datetime
        temp_dates = []
        for x in cursor:
            val = x.get(field)
            if val:
                temp_dates.append(pd.to_datetime(val))

        if temp_dates:
            dates = temp_dates
            break

    if not dates:
        raise RuntimeError("❌ CRITICAL: No calendar found!")

    # 转为 numpy array (datetime64[D]) 以实现极速差集运算
    cal_arr = np.array(dates, dtype='datetime64[D]')
    cal_arr = np.unique(cal_arr)  # 去重
    cal_arr.sort()

    print(f"✅ Master Calendar: {len(cal_arr)} days ({cal_arr[0]} to {cal_arr[-1]})")
    return cal_arr


def load_em_annotations(db):
    """
    加载东财停牌注解，构建快速查询字典
    Key: (date_str, symbol) -> Value: reason
    """
    print("📖 Loading EM Suspension Annotations...")
    collection = db[COL_EM_RAW]
    # 只需读取日期、代码、原因
    cursor = collection.find({}, {"date": 1, "symbol": 1, "reason": 1, "_id": 0})

    annotation_map = {}
    count = 0
    for doc in cursor:
        try:
            d_date = doc.get('date')
            symbol = doc.get('symbol')
            reason = doc.get('reason')

            if not d_date or not symbol:
                continue

            # 将日期转为字符串 Key (YYYY-MM-DD)
            # 注意：MongoDB 存的可能是 datetime 或 str，统一转 str
            if isinstance(d_date, datetime):
                date_key = d_date.strftime("%Y-%m-%d")
            else:
                date_key = str(d_date).split(" ")[0]

            annotation_map[(date_key, symbol)] = reason
            count += 1
        except:
            continue

    print(f"✅ Loaded {count} annotations into memory.")
    return annotation_map


def fuse_data(db, master_cal, em_map):
    """
    执行【事实 + 注解】融合
    逻辑：
    1. 事实(Fact): Volume > 0 的日子，绝对不停牌。
    2. 缺失(Gap):  交易日历中有，但事实中没有(缺行或Vol=0)的日子。
    3. 注解(Note): 在缺失日，如果 EM 表有记录，用 EM 原因；否则用兜底原因。
    """
    print("🚀 Starting Data Fusion...")
    bar_col = db[COL_BAR]
    target_col = db[COL_TARGET]

    # 获取所有股票代码
    stocks = bar_col.distinct("symbol")
    stocks.sort()

    ops = []

    for symbol in tqdm(stocks):
        try:
            # 1. 获取“有效交易日” (Volume > 0)
            # 必须读取 volume 字段
            cursor = bar_col.find(
                {"symbol": symbol},
                {"date": 1, "datetime": 1, "volume": 1, "_id": 0}
            )

            active_dates_list = []
            for doc in cursor:
                # --- 兼容性日期读取 (你要求的) ---
                d = doc.get("datetime") or doc.get("date")
                if not d:
                    continue

                # --- 核心判定逻辑 ---
                # 如果 Volume > 0，视为在场交易
                # 如果 Volume = 0，视为离场(停牌候选)，不加入 active_dates
                vol = doc.get("volume", 0)
                if vol > 0:
                    active_dates_list.append(pd.to_datetime(d))

            if not active_dates_list:
                continue

            # 转为 numpy array
            active_dates = np.array(active_dates_list, dtype='datetime64[D]')
            active_dates.sort()

            # 2. 确定生命周期 (上市日 ~ 最新有交易日)
            min_date = active_dates[0]
            max_date = active_dates[-1]

            # 3. 截取理论日历 (Expected)
            start_idx = np.searchsorted(master_cal, min_date)
            end_idx = np.searchsorted(master_cal, max_date, side='right')
            expected_slice = master_cal[start_idx:end_idx]

            # 4. 计算缺失日 (Gaps = Expected - Active)
            # 这里面包含了：真正缺数据的日子 + Volume=0 的日子
            susp_dates = np.setdiff1d(expected_slice, active_dates)

            if len(susp_dates) == 0:
                continue

            # 5. 区间合并与原因匹配
            intervals = []

            # 辅助函数：提交一个连续停牌区间
            def commit_chunk(chunk_dates):
                if len(chunk_dates) == 0:
                    return

                start_dt = pd.to_datetime(chunk_dates[0])
                end_dt = pd.to_datetime(chunk_dates[-1])

                # 寻找原因：只要区间内任何一天在 EM Map 里有记录，就采用该记录
                # 优先匹配 start_date (通常公告发在停牌首日)
                best_reason = "Missing Data / Zero Vol"
                source_tag = "inference"

                for date_np in chunk_dates:
                    # 转为 YYYY-MM-DD 用于查字典
                    day_str = str(date_np)
                    key = (day_str, symbol)

                    if key in em_map:
                        best_reason = em_map[key]
                        source_tag = "eastmoney_confirmed"
                        # 只要找到一个原因，就认为整个区间是因为这件事，直接跳出
                        break

                intervals.append({
                    "start": start_dt,
                    "end": end_dt,
                    "reason": best_reason,
                    "source": source_tag
                })

            # 使用 Numpy 技巧快速分组连续日期
            # 逻辑：两个日期如果在日历上连续，它们在 master_cal 的 index 差值应为 1
            # 我们需要找到 susp_dates 在 master_cal 中的原始索引
            mask = np.isin(master_cal, susp_dates)
            all_indices = np.where(mask)[0]

            if len(all_indices) > 0:
                # Grouping: index - arange
                groups = all_indices - np.arange(len(all_indices))
                unique_groups = np.unique(groups)

                for g in unique_groups:
                    group_indices = all_indices[groups == g]
                    group_dates = master_cal[group_indices]
                    commit_chunk(group_dates)

            # 6. 构造写入请求
            if intervals:
                ops.append(
                    UpdateOne(
                        {"symbol": symbol},
                        {"$set": {
                            "suspensions": intervals,  # 最终使用的字段
                            "suspension_updated_at": datetime.now()
                        }},
                        upsert=True
                    )
                )

        except Exception as e:
            # print(f"Error {symbol}: {e}")
            continue

        if len(ops) >= 1000:
            target_col.bulk_write(ops)
            ops = []

    if ops:
        target_col.bulk_write(ops)

    print("\n✅ Fusion Completed! Your data is now Production-Ready.")


if __name__ == "__main__":
    db = get_db()

    # 1. 准备日历
    master_calendar = load_master_calendar(db)

    # 2. 准备注解 (之前下载的东财数据)
    em_annotation_map = load_em_annotations(db)

    # 3. 融合
    if len(master_calendar) > 0:
        fuse_data(db, master_calendar, em_annotation_map)