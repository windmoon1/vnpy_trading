"""
Script 16 (V2.0): Download Index Components & Sync Meta (Ultimate Fix)
----------------------------------------------------------------------
目标:
  1. 下载 [宽基]、[行业]、[概念] 的最新成分股。
  2. [FIX] 修复概念成分股因 patch 导致的代码/名称传参错误。
  3. [NEW] 根据 index_daily 同步补全 index_info 表。

逻辑:
  - 概念/行业接口: 统一传 BK 代码 (适配 fix_akshare 补丁)。
  - 宽基接口: 增加新浪源兜底。
"""

import akshare as ak
import pandas as pd
import time
import random
import datetime
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne

# 引入工具
import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from utils.network_guard import NetworkGuard
from utils.fix_akshare import apply_patches

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
TODAY = datetime.datetime.now().strftime("%Y-%m-%d")

# 宽基映射: {指数名称: (API代码, 存库Symbol)}
BENCHMARK_MAP = {
    "上证指数": ("000001", "sh000001"),
    "深证成指": ("399001", "sz399001"), # 需用 sina 接口
    "创业板指": ("399006", "sz399006"), # 需用 sina 接口
    "沪深300": ("000300", "sh000300"),
    "中证500": ("000905", "sh000905"),
    "中证1000": ("000852", "sh000852"),
    "科创50":   ("000688", "sh000688"),
    "上证50":   ("000016", "sh000016"),
    "中证全指": ("000985", "sh000985"),
    "北证50":   ("899050", "sz899050"),
}

client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client[DB_NAME]

def format_stock_symbol(symbol):
    """标准化股票代码"""
    s = str(symbol).strip()
    if len(s) == 6:
        if s.startswith(('6')): return f"{s}.SH"
        if s.startswith(('0', '3')): return f"{s}.SZ"
        if s.startswith(('4', '8')): return f"{s}.BJ"
    return s

def save_components(db_symbol, index_name, category, component_list, weights=None):
    if not component_list: return

    doc = {
        "index_symbol": db_symbol,
        "index_name": index_name,
        "date": TODAY,
        "category": category,
        "components": component_list,
        "count": len(component_list),
        "weights": weights if weights else {}
    }

    db["index_components"].update_one(
        {"index_symbol": db_symbol, "date": TODAY},
        {"$set": doc},
        upsert=True
    )

# =========================================================================
# 0. 元数据同步 (Sync Info) - 新增功能
# =========================================================================
def sync_index_info():
    print(f"\n🔄 [0/3] 同步 index_info 元数据...")

    # 从 index_daily 聚合所有现存的指数
    pipeline = [
        {"$group": {
            "_id": "$symbol",
            "name": {"$first": "$name"},
            "category": {"$first": "$category"}
        }}
    ]
    cursor = db["index_daily"].aggregate(pipeline)

    ops = []
    for doc in cursor:
        symbol = doc["_id"]
        name = doc.get("name", symbol)
        category = doc.get("category", "UNKNOWN")

        # 构造 info 文档
        info_doc = {
            "symbol": symbol,
            "name": name,
            "category": category,
            "source": "EM" if "BK" in symbol else "EXCHANGE"
        }
        ops.append(UpdateOne({"symbol": symbol}, {"$set": info_doc}, upsert=True))

    if ops:
        db["index_info"].bulk_write(ops, ordered=False)
        print(f"   ✅ 已同步 {len(ops)} 条指数元数据到 index_info")
    else:
        print("   ⚠️ index_daily 为空，无法同步。")

# =========================================================================
# 1. 宽基指数成分股
# =========================================================================
def download_benchmark_components():
    print(f"\n📊 [1/3] 宽基指数成分股...")

    for name, (api_code, db_symbol) in tqdm(BENCHMARK_MAP.items(), desc="Benchmark"):
        try:
            df = pd.DataFrame()

            # 策略 A: 中证官网 (带权重，质量最高)
            try:
                df = ak.index_stock_cons_weight_csindex(symbol=api_code)
            except: pass

            # 策略 B: 新浪接口 (兜底，专门解决深证成指/创业板指)
            if df.empty:
                try:
                    # 新浪接口通常需要特定的前缀
                    sina_symbol = db_symbol.replace("sh", "").replace("sz", "") # 000001
                    if "sz" in db_symbol: sina_symbol = f"sz{sina_symbol}" # sz399001
                    if "sh" in db_symbol: sina_symbol = f"sh{sina_symbol}"

                    # 简单点，直接试纯数字
                    df = ak.index_stock_cons_sina(symbol=api_code)
                except: pass

            if df.empty:
                # print(f"   ⚠️ {name} 无数据")
                continue

            comps = []
            weights = {}
            for _, row in df.iterrows():
                raw_code = row.get("成分券代码") or row.get("代码")
                if not raw_code: continue

                stock_sym = format_stock_symbol(str(raw_code).zfill(6))
                comps.append(stock_sym)

                w = row.get("权重") or row.get("权重(%)")
                if w: weights[stock_sym] = float(w)

            save_components(db_symbol, name, "BENCHMARK", comps, weights)
            time.sleep(1)

        except Exception as e:
            print(f"   ❌ {name} 失败: {e}")

# =========================================================================
# 2. 行业板块成分股
# =========================================================================
def download_industry_components():
    print(f"\n📊 [2/3] 行业板块成分股...")

    try:
        # 直接从 index_info 读列表 (刚刚同步过，肯定全)
        cursor = db["index_info"].find({"category": "INDUSTRY"})
        tasks = list(cursor)

        for item in tqdm(tasks, desc="Industry"):
            try:
                # 我们的 fix_akshare 补丁让它支持 BK 代码
                # item['symbol'] 是 BK0475
                df = ak.stock_board_industry_cons_em(symbol=item['symbol'])

                comps = []
                for _, row in df.iterrows():
                    raw_code = row.get("代码")
                    if raw_code: comps.append(format_stock_symbol(raw_code))

                save_components(item['symbol'], item['name'], "INDUSTRY", comps)
                time.sleep(random.uniform(0.5, 1.5))
            except: continue

    except Exception as e:
        print(f"❌ 行业错误: {e}")

# =========================================================================
# 3. 概念板块成分股 (FIXED)
# =========================================================================
def download_concept_components():
    print(f"\n📊 [3/3] 概念板块成分股 (Patch Compatible)...")

    try:
        # 从 index_info 读列表
        cursor = db["index_info"].find({"category": "CONCEPT"})
        tasks = list(cursor)

        for item in tqdm(tasks, desc="Concept"):
            try:
                # 🔥 核心修复:
                # 我们的 fix_akshare.py 补丁将 stock_board_concept_cons_em
                # 修改为了直接使用 symbol 参数拼接 URL。
                # 因此，这里【必须】传 BK 代码 (item['symbol'])，而不是中文名！
                # 之前 V1.0 传了 item['name']，导致 URL 变成 fs=b:锂电池 (错误)

                df = ak.stock_board_concept_cons_em(symbol=item['symbol'])

                comps = []
                for _, row in df.iterrows():
                    raw_code = row.get("代码")
                    if raw_code: comps.append(format_stock_symbol(raw_code))

                save_components(item['symbol'], item['name'], "CONCEPT", comps)
                time.sleep(random.uniform(1.0, 2.0))

            except Exception as e:
                # print(f"Err: {item['symbol']} {e}")
                continue

    except Exception as e:
        print(f"❌ 概念错误: {e}")

def run():
    print("🚀 启动 [成分股下载 + 元数据同步] 任务 (V2.0)...")
    apply_patches()
    NetworkGuard.install()

    # 1. 先同步元数据，确保 index_info 有最新数据
    sync_index_info()

    # 2. 下载成分股
    download_benchmark_components()
    download_industry_components()
    download_concept_components()

    print("\n✅ 所有任务完成。")

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\n🛑 用户停止。")