"""
Script 12 (Part 1 - V6.0): Download Concept Index Bars (Final Clean)
--------------------------------------------------------------------
目标: 下载 [概念板块] 的日线行情 (index_daily)
修复:
  1. [Critical] 显式传入 start_date="19900101" 和 end_date=Today。
     解决 AKShare 默认参数只返回 2022 年数据的严重 Bug。
  2. [Fields] 确保存入 turnover (成交额) 等关键字段。
  3. [Reset] 建议先清空 category="CONCEPT" 的旧数据再运行。

逻辑:
  - 全量下载模式 (因为之前的数据都不完整)。
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

# 🔥 核心修正: 显式定义全量时间范围
START_DATE = "19900101"
END_DATE = datetime.datetime.now().strftime("%Y%m%d")

client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client[DB_NAME]

def get_concept_list():
    """从本地 index_info 获取概念列表"""
    cursor = db["index_info"].find({"category": "CONCEPT"}, {"name": 1, "symbol": 1})
    concepts = [{"code": d["symbol"], "name": d["name"]} for d in cursor]
    return concepts

def check_is_downloaded(symbol):
    """
    检查是否已下载 (粗略检查)
    由于我们刚清空了数据库，这个检查在第一轮运行时主要起到断点续传的作用
    (万一网络断了，重启脚本时跳过已完成的)
    """
    return db["index_daily"].find_one({"symbol": symbol}, {"_id": 1}) is not None

def retry_action(func, *args, **kwargs):
    """通用重试装饰器"""
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # 忽略数据为空导致的格式错误 (空壳板块)
            if "Length mismatch" in str(e) or "char 0" in str(e):
                return pd.DataFrame()

            if attempt == MAX_RETRIES - 1:
                print(f"   ❌ {kwargs.get('symbol')} 最终失败: {e}")
                return None

            time.sleep(random.uniform(2, 5))
            NetworkGuard.rotate_identity()
    return None

def fetch_and_save_bars(symbol, name):
    """下载并存储单个指数"""

    # 断点续传: 如果这次运行中已经下过了，就跳过
    if check_is_downloaded(symbol):
        return "SKIPPED"

    # 1. 下载 (显式传入时间参数)
    try:
        df = retry_action(
            ak.stock_board_concept_hist_em,
            symbol=name,
            period="daily",
            start_date=START_DATE,
            end_date=END_DATE,
            adjust=""
        )
    except Exception:
        return "FAILED"

    if df is None or df.empty:
        return "EMPTY"

    # 2. 字段清洗与映射
    rename_map = {
        "日期": "date",
        "开盘": "open",
        "最高": "high",
        "最低": "low",
        "收盘": "close",
        "成交量": "volume",
        "成交额": "turnover",
        "换手率": "turnover_rate",
        "振幅": "amplitude",
        "涨跌幅": "change_pct",
        "涨跌额": "change_amt"
    }

    available_cols = set(df.columns)
    valid_rename = {k: v for k, v in rename_map.items() if k in available_cols}
    df = df.rename(columns=valid_rename)

    # 3. 批量写入
    ops = []
    for _, row in df.iterrows():
        try:
            date_str = str(row["date"])[:10]
            doc = {
                "symbol": symbol,
                "exchange": "INDEX",
                "datetime": date_str,
                "interval": "d",
                "category": "CONCEPT",
                "name": name,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
            }
            if "turnover" in row: doc["turnover"] = float(row["turnover"])
            if "turnover_rate" in row: doc["turnover_rate"] = float(row["turnover_rate"])
            if "amplitude" in row: doc["amplitude"] = float(row["amplitude"])
            if "change_pct" in row: doc["change_pct"] = float(row["change_pct"])

            ops.append(UpdateOne(
                {"symbol": symbol, "datetime": date_str},
                {"$set": doc},
                upsert=True
            ))
        except Exception:
            continue

    if ops:
        db["index_daily"].bulk_write(ops, ordered=False)
        return "UPDATED"
    return "EMPTY"

def run_job():
    print(f"🚀 启动 [概念板块] 重新下载任务 (V6.0 Clean)...")
    print(f"   📅 强制时间范围: {START_DATE} -> {END_DATE}")

    apply_patches()
    NetworkGuard.install()

    concept_list = get_concept_list()
    if not concept_list:
        print("⚠️ 未找到概念列表。")
        return

    print(f"📊 任务队列: {len(concept_list)} 个板块")

    pbar = tqdm(concept_list, desc="Concept")
    stats = {"skipped": 0, "updated": 0, "empty": 0, "failed": 0}

    for item in pbar:
        code = item['code']
        name = item['name']

        pbar.set_description(f"Get: {name}")

        status = fetch_and_save_bars(code, name)

        if status == "SKIPPED":
            stats["skipped"] += 1
        elif status == "UPDATED":
            stats["updated"] += 1
            # 只有真正请求了网络才需要 sleep
            time.sleep(random.uniform(1.0, 3.0))
        elif status == "EMPTY":
            stats["empty"] += 1
        else:
            stats["failed"] += 1

        pbar.set_postfix(new=stats["updated"], skip=stats["skipped"])

    print("\n" + "="*40)
    print(f"✅ 下载完成。")
    print(f"   📥 成功入库: {stats['updated']}")
    print(f"   ⏭️ 跳过(已存): {stats['skipped']}")
    print(f"   ⚪ 无数据:     {stats['empty']}")

if __name__ == "__main__":
    try:
        run_job()
    except KeyboardInterrupt:
        print("\n🛑 用户停止。")