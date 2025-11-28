"""
Script 12 (Part 2 - V6.0): Download Industry Index Bars (Smart Update)
----------------------------------------------------------------------
目标: 下载/补全 [行业指数] 日线行情 (SW & EM)
修复: 解决"存在即跳过"导致的数据停更问题。
逻辑:
  1. 获取 Symbol。
  2. 查 DB 中该 Symbol 的最新日期 (last_db_date)。
  3. 如果 last_db_date < 昨天: 启动下载。
  4. 采用 upsert 模式写入，自动去重。
"""

import akshare as ak
import pandas as pd
import time
import random
import sys
import os
import datetime
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne

# 引入工具
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from utils.network_guard import NetworkGuard
from utils.fix_akshare import apply_patches

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
MAX_RETRIES = 3

# 日期阈值：如果数据库最新日期晚于此日期，视为"足够新"，跳过下载
# 这里设为昨天，保证每天运行都能下到最新的
YESTERDAY = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client[DB_NAME]

def get_db_latest_date(symbol):
    """查询数据库中该标的的最新日期"""
    doc = db["index_daily"].find_one(
        {"symbol": symbol},
        sort=[("datetime", -1)],
        projection={"datetime": 1}
    )
    return doc["datetime"] if doc else None

def retry_action(func, *args, **kwargs):
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if "Length mismatch" in str(e) or "char 0" in str(e): return pd.DataFrame()
            if attempt == MAX_RETRIES - 1: print(f"   ❌ {kwargs.get('symbol')} 失败: {e}")
            time.sleep(random.uniform(2, 5))
            NetworkGuard.rotate_identity()

# =========================================================================
# 1. 申万行业 (SW)
# =========================================================================
def get_sw_list():
    print("📡 [SW] 拉取申万行业列表...")
    full_list = []
    try:
        df1 = ak.sw_index_first_info()
        for _, row in df1.iterrows():
            full_list.append({"code": str(row['行业代码']).split(".")[0], "name": row['行业名称']})
        time.sleep(1)
        df2 = ak.sw_index_second_info()
        for _, row in df2.iterrows():
            full_list.append({"code": str(row['行业代码']).split(".")[0], "name": row['行业名称']})
    except Exception as e:
        print(f"❌ [SW] 列表获取失败: {e}")
        return []

    seen = set()
    unique = []
    for x in full_list:
        if x['code'] not in seen:
            unique.append(x)
            seen.add(x['code'])
    return unique

def save_sw_bars(symbol, name):
    # 智能跳过逻辑
    last_date = get_db_latest_date(symbol)
    if last_date and last_date >= YESTERDAY:
        return "SKIPPED"

    df = retry_action(ak.index_hist_sw, symbol=symbol)
    if df is None or df.empty: return "EMPTY"

    # 简单清洗
    df.rename(columns={
        "日期": "date", "开盘": "open", "最高": "high", "最低": "low", "收盘": "close", "成交量": "volume",
        "date": "date", "open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume"
    }, inplace=True)

    ops = []
    for _, row in df.iterrows():
        try:
            date_str = str(row["date"])[:10]
            # 只写入比数据库新的数据 (如果是全量覆盖也可以，upsert会处理)
            # 为了简单，我们直接 upsert 所有数据，MongoDB 会处理重复
            doc = {
                "symbol": symbol,
                "exchange": "INDEX",
                "datetime": date_str,
                "interval": "d",
                "category": "INDUSTRY_SW",
                "name": name,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"])
            }
            ops.append(UpdateOne({"symbol": symbol, "datetime": date_str}, {"$set": doc}, upsert=True))
        except: continue

    if ops:
        db["index_daily"].bulk_write(ops, ordered=False)
        return "UPDATED"
    return "EMPTY"

# =========================================================================
# 2. 东财行业 (EM)
# =========================================================================
def get_em_list():
    print("📡 [EM] 拉取东财行业列表...")
    try:
        df = ak.stock_board_industry_name_em()
        return [{"code": str(row["板块代码"]), "name": row["板块名称"]} for _, row in df.iterrows()]
    except Exception as e:
        print(f"❌ [EM] 列表获取失败: {e}")
        return []

def save_em_bars(symbol, name):
    # 智能跳过逻辑
    last_date = get_db_latest_date(symbol)
    if last_date and last_date >= YESTERDAY:
        return "SKIPPED"

    df = retry_action(ak.stock_board_industry_hist_em, symbol=name)
    if df is None or df.empty: return "EMPTY"

    rename_map = {
        "日期": "date", "开盘": "open", "最高": "high", "最低": "low", "收盘": "close", "成交量": "volume",
        "成交额": "turnover", "换手率": "turnover_rate", "振幅": "amplitude", "涨跌幅": "change_pct"
    }
    cols = {k: v for k, v in rename_map.items() if k in df.columns}
    df.rename(columns=cols, inplace=True)

    ops = []
    for _, row in df.iterrows():
        try:
            date_str = str(row["date"])[:10]
            doc = {
                "symbol": symbol,
                "exchange": "INDEX",
                "datetime": date_str,
                "interval": "d",
                "category": "INDUSTRY_EM",
                "name": name,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"])
            }
            if "turnover" in row: doc["turnover"] = float(row["turnover"])
            if "turnover_rate" in row: doc["turnover_rate"] = float(row["turnover_rate"])
            if "amplitude" in row: doc["amplitude"] = float(row["amplitude"])
            if "change_pct" in row: doc["change_pct"] = float(row["change_pct"])

            ops.append(UpdateOne({"symbol": symbol, "datetime": date_str}, {"$set": doc}, upsert=True))
        except: continue

    if ops:
        db["index_daily"].bulk_write(ops, ordered=False)
        return "UPDATED"
    return "EMPTY"

def run_job():
    print(f"🚀 启动 [行业指数] 智能修复任务 (Target Date >= {YESTERDAY})...")
    apply_patches()
    NetworkGuard.install()

    # 1. 修复申万
    sw_list = get_sw_list()
    pbar_sw = tqdm(sw_list, desc="SW Index")
    for item in pbar_sw:
        pbar_sw.set_description(f"SW: {item['name']}")
        status = save_sw_bars(item['code'], item['name'])
        if status == "UPDATED":
            # 申万接口容易封，多睡会
            time.sleep(random.uniform(1.5, 3.0))

    # 2. 修复东财
    em_list = get_em_list()
    pbar_em = tqdm(em_list, desc="EM Index")
    for item in pbar_em:
        pbar_em.set_description(f"EM: {item['name']}")
        status = save_em_bars(item['code'], item['name'])
        if status == "UPDATED":
            time.sleep(random.uniform(2.0, 4.0))

    print("\n✅ 任务结束。")

if __name__ == "__main__":
    try:
        run_job()
    except KeyboardInterrupt:
        print("\n🛑 用户停止。")