"""
脚本 08: 财务数据输入审计工具 (Dump ALL Raw Data for Manual Audit)
------------------------------------------------------------------
目标: 无过滤地打印最近 N 期财务报告中的所有字段，供用户手动核对。
"""
import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING, DESCENDING
import numpy as np
from typing import Dict, Any

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
CLIENT = MongoClient(MONGO_HOST, MONGO_PORT)
DB = CLIENT[DB_NAME]

# 集合定义 (与 01_init_db_architecture.py 保持一致)
COL_BARS = DB["bar_daily"]
COL_CAPITAL = DB["share_capital"]
COL_INCOME = DB["finance_income"]
COL_BALANCE = DB["finance_balance"]

TEST_SYMBOLS = ["600519", "601398"]

def get_latest_market_data(symbol: str) -> dict:
    """获取最新价格和股本数据"""

    # 1. 最新价格
    latest_bar = COL_BARS.find_one({"symbol": symbol}, sort=[("datetime", DESCENDING)])

    # 2. 最新股本
    latest_capital = COL_CAPITAL.find_one({"symbol": symbol}, sort=[("date", DESCENDING)])

    result = {}
    if latest_bar:
        result['latest_date'] = latest_bar['datetime'].strftime('%Y-%m-%d')
        result['close_price'] = latest_bar['close_price']

    if latest_capital:
        result['total_shares'] = latest_capital['total_shares']
        result['float_shares'] = latest_capital['float_shares']

    return result

def dump_all_fields_for_audit(symbol: str, num_reports: int = 5):
    """【核心功能】: 遍历最新的 N 期资产负债表和利润表，打印所有字段"""

    print("=" * 80)
    print(f"| 🔎 原始数据审计开始: {symbol} (最近 {num_reports} 期)")
    print("=" * 80)

    # 1. 市场数据快照
    market_data = get_latest_market_data(symbol)
    print("\n--- 市场与股本数据 (最新快照) ---")
    print(f"  最新收盘日: {market_data.get('latest_date', 'N/A')}")
    print(f"  最新收盘价: {market_data.get('close_price', 'N/A'):,.2f} 元")
    print(f"  总股本 (亿股): {market_data.get('total_shares', 0) / 1e8:,.4f}")
    print(f"  流通股本 (亿股): {market_data.get('float_shares', 0) / 1e8:,.4f}")


    # 2. 资产负债表 (BALANCE) - 打印所有字段
    print("\n\n--- 原始【资产负债表】数据转储 (最新至旧) ---")
    balance_cursor = COL_BALANCE.find({"symbol": symbol}).sort([("report_date", DESCENDING)]).limit(num_reports)

    for i, doc in enumerate(balance_cursor):
        report_date = doc.get('report_date').strftime('%Y-%m-%d')
        pub_date = doc.get('publish_date').strftime('%Y-%m-%d')
        print(f"\n  📝 第 {i+1} 期 (报告期: {report_date} | 公告日: {pub_date})")
        print("  " + "-" * 78)

        # 遍历文档中的所有字段
        for k, v in doc.items():
            if k in ['_id', 'symbol', 'exchange', 'gateway_name']:
                continue

            # 格式化输出大数字，便于阅读
            v_str = f"{v:,.0f} 元" if isinstance(v, (int, float)) else str(v)
            print(f"    - {k:<35}: {v_str}")


    # 3. 利润表 (INCOME) - 打印所有字段
    print("\n\n--- 原始【利润表】数据转储 (最新至旧) ---")
    income_cursor = COL_INCOME.find({"symbol": symbol}).sort([("report_date", DESCENDING)]).limit(num_reports)

    for i, doc in enumerate(income_cursor):
        report_date = doc.get('report_date').strftime('%Y-%m-%d')
        pub_date = doc.get('publish_date').strftime('%Y-%m-%d')
        print(f"\n  📝 第 {i+1} 期 (报告期: {report_date} | 公告日: {pub_date})")
        print("  " + "-" * 78)

        # 遍历文档中的所有字段
        for k, v in doc.items():
            if k in ['_id', 'symbol', 'exchange', 'gateway_name']:
                continue

            v_str = f"{v:,.0f} 元" if isinstance(v, (int, float)) else str(v)
            print(f"    - {k:<35}: {v_str}")

    print("=" * 80)
    print(f"| 原始数据转储完毕: {symbol} ")
    print("=" * 80)

def run():
    print("🚀 启动 [财务数据输入审计工具]...")
    for symbol in TEST_SYMBOLS:
        audit_stock = DB["stock_info"].find_one({"symbol": symbol})
        if audit_stock:
            dump_all_fields_for_audit(symbol, 5)
        else:
            print(f"⚠️ 警告: 未在 stock_info 集合中找到 {symbol} 的信息。")

    print("\n✨ 审计数据输出完毕，请手动核对所有字段。")

if __name__ == "__main__":
    run()