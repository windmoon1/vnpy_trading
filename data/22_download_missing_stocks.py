"""
Script 22: Download Missing Stocks (B-Share & BSE)
--------------------------------------------------
目标: 补全 stock_info 表中缺失的 [北交所] 和 [B股] 基础信息。
原因: 之前的 Script 02 主要针对沪深A股，导致 920xxx, 200xxx 等代码在 stock_info 中缺失。

覆盖范围:
  1. 北交所 (BSE): 43/83/87/92 开头
  2. 上证B股 (SSE): 900 开头
  3. 深证B股 (SZSE): 200 开头
"""

import akshare as ak
import pandas as pd
import datetime
from pymongo import MongoClient, UpdateOne
from tqdm import tqdm

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
COLLECTION = "stock_info"

client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client[DB_NAME]


def save_to_db(df, exchange_str, category_name):
    """通用入库逻辑"""
    if df is None or df.empty:
        print(f"   ⚠️ {category_name} 接口返回为空。")
        return

    print(f"   📊 {category_name}: 获取到 {len(df)} 条记录")

    ops = []
    # 不同的接口返回的列名可能不同，需要分别处理
    # 统一目标: symbol(纯数字), name, exchange, list_date, product="STOCK"

    for _, row in df.iterrows():
        try:
            # 尝试适配列名 (东财接口通常是 '代码', '名称')
            code = str(row.get('代码', '')).strip()
            name = str(row.get('名称', '')).strip()

            if not code: continue

            doc = {
                "symbol": code,
                "name": name,
                "exchange": exchange_str,
                "product": "STOCK",
                "category": category_name,  # 标记来源 (A/B/BSE)
                "updated_at": datetime.datetime.now()
            }

            # 尝试获取上市日期 (如果有)
            if '上市日期' in row:
                doc['list_date'] = str(row['上市日期'])

            ops.append(UpdateOne(
                {"symbol": code},
                {"$set": doc},
                upsert=True
            ))
        except Exception:
            continue

    if ops:
        db[COLLECTION].bulk_write(ops, ordered=False)
        print(f"   ✅ 入库成功: {len(ops)} 条")


def sync_bj_stocks():
    """1. 北交所 (BSE)"""
    print("\n📡 正在拉取 [北交所] 全部股票...")
    try:
        # 接口: stock_bj_a_spot_em (东财北证A股实时行情)
        # 注意: 包含了 920, 83, 43 等
        df = ak.stock_bj_a_spot_em()
        save_to_db(df, "BSE", "STOCK_BJ")
    except Exception as e:
        print(f"   ❌ 北交所下载失败: {e}")


def sync_b_stocks():
    """2. B股 (SH/SZ)"""
    print("\n📡 正在拉取 [B股] 全部股票...")
    try:
        # 接口: stock_zh_b_spot_em (东财B股实时行情)
        df = ak.stock_zh_b_spot_em()

        # B股需要区分交易所: 900->SSE, 200->SZSE
        sse_ops = []
        szse_ops = []

        for _, row in df.iterrows():
            code = str(row['代码'])
            name = row['名称']

            exchange = "UNKNOWN"
            if code.startswith("900"):
                exchange = "SSE"
            elif code.startswith("200"):
                exchange = "SZSE"

            doc = {
                "symbol": code,
                "name": name,
                "exchange": exchange,
                "product": "STOCK",
                "category": "STOCK_B",
                "updated_at": datetime.datetime.now()
            }

            op = UpdateOne({"symbol": code}, {"$set": doc}, upsert=True)
            if exchange == "SSE":
                sse_ops.append(op)
            elif exchange == "SZSE":
                szse_ops.append(op)

        if sse_ops:
            db[COLLECTION].bulk_write(sse_ops, ordered=False)
            print(f"   ✅ 上证B股 (900xxx): {len(sse_ops)} 条")

        if szse_ops:
            db[COLLECTION].bulk_write(szse_ops, ordered=False)
            print(f"   ✅ 深证B股 (200xxx): {len(szse_ops)} 条")

    except Exception as e:
        print(f"   ❌ B股下载失败: {e}")


def verify_fix(codes_to_check):
    """3. 验证修复结果"""
    print("\n🔍 正在验证修复结果...")
    found_count = 0
    for code in codes_to_check:
        # 去掉后缀查
        pure_code = code.split(".")[0]
        doc = db[COLLECTION].find_one({"symbol": pure_code})
        status = "✅ 已存在" if doc else "❌ 仍缺失"
        info = f"({doc['exchange']} - {doc['name']})" if doc else ""
        print(f"   - {code:<10}: {status} {info}")
        if doc: found_count += 1

    print(f"\n✨ 修复率: {found_count}/{len(codes_to_check)}")


if __name__ == "__main__":
    # 执行同步
    sync_bj_stocks()
    sync_b_stocks()

    # 验证你刚才提到的几个问题代码
    check_list = ['200488.SZ', '920553.BJ', '920394.BJ', '920075.BJ']
    verify_fix(check_list)