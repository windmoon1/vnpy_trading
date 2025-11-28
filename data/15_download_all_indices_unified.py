"""
Script 15 (V11.0): Download All Indices (Robust & Normalized)
-------------------------------------------------------------
目标: [清库重置版] 统一下载 [宽基]、[行业]、[概念] 三类指数。
架构: Code-First + Unified Schema + Normalized Symbols

改进:
  1. [Normalize] 行业/概念代码强制添加 'BK' 前缀 (如 BK0475)，确保 DB 主键统一。
  2. [Index] 自动创建 MongoDB 索引，加速断点查询。
  3. [Feedback] 进度条显式展示 Skip 数量。

Schema:
  symbol, date, category, name,
  open, high, low, close, volume,
  turnover, turnover_rate, amplitude, change_pct
"""

import akshare as ak
import pandas as pd
import time
import random
import sys
import os
import datetime
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne, ASCENDING

# 引入工具
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from utils.network_guard import NetworkGuard
from utils.fix_akshare import apply_patches

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
MAX_RETRIES = 5

START_DATE = "19900101"
END_DATE = datetime.datetime.now().strftime("%Y%m%d")

BENCHMARKS = [
    ("sh000001", "上证指数"), ("sz399001", "深证成指"), ("sz399006", "创业板指"),
    ("sh000300", "沪深300"), ("sh000905", "中证500"), ("sh000852", "中证1000"),
    ("sh000688", "科创50"), ("sh000016", "上证50"),
    ("sh000985", "中证全指"), ("sz899050", "北证50"),
]

client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client[DB_NAME]

def ensure_indexes():
    """创建索引加速查询"""
    print("🔨 正在优化数据库索引...")
    db["index_daily"].create_index([("symbol", ASCENDING), ("datetime", -1)])
    db["index_daily"].create_index([("category", ASCENDING)])

def normalize_bk_code(code: str) -> str:
    """标准化板块代码: 0475 -> BK0475"""
    code = str(code).strip()
    if not code.startswith("BK"):
        return f"BK{code}"
    return code

def get_db_latest_date(symbol):
    """查询数据库最新日期"""
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
            err_msg = str(e)
            if "Length mismatch" in err_msg or "char 0" in err_msg: return pd.DataFrame()
            if "ProxyError" in err_msg or "ConnectionPool" in err_msg:
                time.sleep(random.uniform(3, 8))
                NetworkGuard.rotate_identity()
            if attempt == MAX_RETRIES - 1: pass
            time.sleep(random.uniform(1, 3))
    return None

def standardize_columns(df):
    if df is None or df.empty: return None
    rename_map = {
        "date": "date", "amount": "turnover",
        "日期": "date", "开盘": "open", "最高": "high", "最低": "low", "收盘": "close",
        "成交量": "volume", "成交额": "turnover", "换手率": "turnover_rate",
        "涨跌幅": "change_pct", "振幅": "amplitude"
    }
    cols_to_rename = {k: v for k, v in rename_map.items() if k in df.columns}
    df.rename(columns=cols_to_rename, inplace=True)

    for col in ["turnover_rate", "change_pct"]:
        if col not in df.columns: df[col] = 0.0

    if "amplitude" not in df.columns:
        df = df.sort_values("date").reset_index(drop=True)
        pre_close = df["close"].shift(1)
        amplitude = (df["high"] - df["low"]) / pre_close * 100
        df["amplitude"] = amplitude.fillna(0.0)

    required = ["date", "open", "high", "low", "close", "volume", "turnover", "turnover_rate", "change_pct", "amplitude"]
    for col in required:
        if col not in df.columns: return None
    return df[required]

def process_one_symbol(store_symbol, query_symbol, name, category, fetch_func, **kwargs):
    """
    :param store_symbol: 存库代码 (BK0475)
    :param query_symbol: 查询代码 (BK0475 / 0475 / sh000300)
    """

    # 1. 断点跳过
    last_date = get_db_latest_date(store_symbol)
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    if last_date and last_date >= yesterday:
        return "SKIPPED"

    # 2. 下载
    df = retry_action(fetch_func, symbol=query_symbol, **kwargs)

    # 3. 标准化
    df = standardize_columns(df)
    if df is None or df.empty: return "EMPTY"

    # 4. 入库
    ops = []
    for _, row in df.iterrows():
        try:
            date_str = str(row["date"])[:10]
            doc = {
                "symbol": store_symbol,
                "exchange": "INDEX",
                "datetime": date_str,
                "interval": "d",
                "category": category,
                "name": name,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
                "turnover": float(row["turnover"]),
                "turnover_rate": float(row["turnover_rate"]),
                "change_pct": float(row["change_pct"]),
                "amplitude": float(row["amplitude"])
            }
            ops.append(UpdateOne({"symbol": store_symbol, "datetime": date_str}, {"$set": doc}, upsert=True))
        except: continue

    if ops:
        db["index_daily"].bulk_write(ops, ordered=False)
        return "UPDATED"
    return "EMPTY"

def run_unified_job():
    print("🚀 启动 [全指数] 统一下载任务 (V11.0 Robust)...")
    ensure_indexes()

    apply_patches()
    NetworkGuard.install()

    # --- 1. 宽基 ---
    print(f"\n📊 [1/3] 宽基指数 ({len(BENCHMARKS)} 个)...")
    for symbol, name in tqdm(BENCHMARKS, desc="Benchmark"):
        process_one_symbol(
            store_symbol=symbol, query_symbol=symbol, name=name, category="BENCHMARK",
            fetch_func=ak.stock_zh_index_daily_em
        )

    # --- 2. 行业 ---
    print("\n📊 [2/3] 行业板块 (Normalized BK)...")
    try:
        em_ind_df = ak.stock_board_industry_name_em()
        # 标准化: 存库用 BKxxxx, 查询用 BKxxxx (AKShare 支持)
        em_ind_list = [{"code": normalize_bk_code(r["板块代码"]), "name": r["板块名称"]} for _, r in em_ind_df.iterrows()]

        pbar = tqdm(em_ind_list, desc="Industry")
        stats = {"skip": 0, "upd": 0}

        for item in pbar:
            status = process_one_symbol(
                store_symbol=item['code'], query_symbol=item['code'],
                name=item['name'], category="INDUSTRY",
                fetch_func=ak.stock_board_industry_hist_em,
                start_date=START_DATE, end_date=END_DATE
            )
            if status == "UPDATED":
                stats["upd"] += 1
                time.sleep(random.uniform(0.5, 1.5))
            elif status == "SKIPPED":
                stats["skip"] += 1

            pbar.set_postfix(skip=stats['skip'], upd=stats['upd'])

    except Exception as e: print(f"❌ 行业失败: {e}")

    # --- 3. 概念 ---
    print("\n📊 [3/3] 概念板块 (Normalized BK)...")
    try:
        try:
            em_con_df = retry_action(ak.stock_board_concept_name_em)
            if em_con_df is None: raise Exception("API Error")
            em_con_list = [{"code": normalize_bk_code(r["板块代码"]), "name": r["板块名称"]} for _, r in em_con_df.iterrows()]
        except:
            print("   ⚠️ 切换本地缓存...")
            cursor = db["index_info"].find({"category": "CONCEPT"}, {"name": 1, "symbol": 1})
            em_con_list = [{"code": normalize_bk_code(d["symbol"]), "name": d["name"]} for d in cursor]

        pbar = tqdm(em_con_list, desc="Concept")
        stats = {"skip": 0, "upd": 0}

        for item in pbar:
            status = process_one_symbol(
                store_symbol=item['code'], query_symbol=item['code'],
                name=item['name'], category="CONCEPT",
                fetch_func=ak.stock_board_concept_hist_em,
                start_date=START_DATE, end_date=END_DATE, period="daily"
            )
            if status == "UPDATED":
                stats["upd"] += 1
                time.sleep(random.uniform(1.0, 3.0))
            elif status == "SKIPPED":
                stats["skip"] += 1

            pbar.set_postfix(skip=stats['skip'], upd=stats['upd'])

    except Exception as e: print(f"❌ 概念失败: {e}")

    print("\n✨ 任务完成。")

if __name__ == "__main__":
    try:
        run_unified_job()
    except KeyboardInterrupt:
        print("\n🛑 用户停止。")