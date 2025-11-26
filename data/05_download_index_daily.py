"""
脚本 05: 核心指数日线下载器 (v2.1 - 单位修正版)
---------------------------------------
更新日志:
- [Fix] 统一量纲: 成交量 (Volume) 自动乘以 100 (手 -> 股)。
- [Feat] 断点续传 + 智能重试 + 随机延迟 (继承自 v2.0)。
"""
import os
import time
import random
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from pymongo import UpdateOne, MongoClient
from vnpy.trader.constant import Exchange, Interval
import akshare as ak

# --- 🛡️ 直连补丁 ---
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['all_proxy'] = ''
os.environ['NO_PROXY'] = '*'

# --- 配置 ---
START_DATE = "19900101"
MAX_RETRIES = 5
RETRY_DELAY = 30
NORMAL_DELAY = (30, 60)

# --- 核心指数清单 ---
INDEX_CONFIG = {
    "000001": (Exchange.SSE, "上证指数", "sh000001"),
    "399001": (Exchange.SZSE, "深证成指", "sz399001"),
    "000300": (Exchange.SSE,  "沪深300", "sh000300"),
    "000905": (Exchange.SSE,  "中证500", "sh000905"),
    "000852": (Exchange.SSE,  "中证1000", "sh000852"),
    "399006": (Exchange.SZSE, "创业板指", "sz399006"),
    "000688": (Exchange.SSE,  "科创50",   "sh000688"),
    "000016": (Exchange.SSE,  "上证50",   "sh000016"),
    "000985": (Exchange.SSE,  "中证全指", "sh000985"),
}

# --- 数据库 ---
CLIENT = MongoClient("localhost", 27017)
col_index = CLIENT["vnpy_stock"]["index_daily"]
col_info = CLIENT["vnpy_stock"]["index_info"]

def get_downloaded_symbols():
    try:
        return set(col_index.distinct("symbol"))
    except:
        return set()

def save_index_data(symbol, exchange, name, df):
    if df.empty: return 0

    updates = []
    for _, row in df.iterrows():
        try:
            date_val = row['date']
            if isinstance(date_val, str):
                dt = datetime.strptime(date_val.split()[0], "%Y-%m-%d")
            else:
                dt = date_val

            # 🚨 核心修正: 东财 Volume 单位为手，需转为股 (x100)
            vol_hand = float(row['volume'])
            vol_share = vol_hand * 100

            doc = {
                "symbol": symbol,
                "exchange": exchange.value,
                "interval": Interval.DAILY.value,
                "datetime": dt,
                "open_price": float(row['open']),
                "high_price": float(row['high']),
                "low_price": float(row['low']),
                "close_price": float(row['close']),
                "volume": vol_share,          # ✅ 已修正为股
                "turnover": float(row['amount']),
                "gateway_name": "AKSHARE_EM_INDEX"
            }

            filter_doc = {
                "symbol": symbol,
                "exchange": exchange.value,
                "interval": Interval.DAILY.value,
                "datetime": dt
            }
            updates.append(UpdateOne(filter_doc, {"$set": doc}, upsert=True))
        except Exception:
            continue

    if updates:
        col_index.bulk_write(updates)
        col_info.update_one(
            {"symbol": symbol},
            {"$set": {
                "symbol": symbol,
                "exchange": exchange.value,
                "name": name,
                "category": "BENCHMARK"
            }},
            upsert=True
        )
        return len(updates)
    return 0

def fetch_with_retry(api_symbol, name):
    for attempt in range(MAX_RETRIES):
        try:
            df = ak.stock_zh_index_daily_em(symbol=api_symbol)
            return df
        except Exception as e:
            print(f"\n⚠️  [{name}] 下载受阻 (第 {attempt+1}/{MAX_RETRIES} 次): {e}")
            if attempt < MAX_RETRIES - 1:
                print(f"⏳ 触发熔断保护，冷却 {RETRY_DELAY} 秒后重试...")
                time.sleep(RETRY_DELAY)
            else:
                print(f"❌ [{name}] 彻底失败，跳过。")
                raise e

def run():
    print("🚀 启动 [指数数据下载器 v2.1] (单位: 股 | 智能抗封锁)...")

    done_set = get_downloaded_symbols()
    print(f"📚 数据库已收录: {len(done_set)} 个指数 (将跳过)")

    tasks = []
    for symbol, meta in INDEX_CONFIG.items():
        if symbol in done_set:
            continue
        tasks.append((symbol, meta))

    if not tasks:
        print("✨ 所有指数数据已就绪，无需下载。")
        return

    print(f"🎯 本次待下载: {len(tasks)} 个")
    print("-" * 60)

    pbar = tqdm(tasks, unit="idx")
    for symbol, (exchange, name, api_symbol) in pbar:
        pbar.set_description(f"下载 {name}")
        try:
            df = fetch_with_retry(api_symbol, name)
            save_index_data(symbol, exchange, name, df)
        except Exception:
            continue

        time.sleep(random.uniform(*NORMAL_DELAY))

    print("\n✨ 任务全部完成！(Database: vnpy_stock.index_daily)")

if __name__ == "__main__":
    run()