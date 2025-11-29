"""
Script 09: 原始财务数据提取器 (Raw Financial Data Dumper)
-------------------------------------------------------
目标: 忠实打印茅台(600519)和工行(601398)的近6期财务数据。
用途: 供用户手工计算 TTM 和 PB，以核对数据源准确性。
"""
import pandas as pd
from datetime import datetime
from pymongo import MongoClient, DESCENDING

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
CLIENT = MongoClient(MONGO_HOST, MONGO_PORT)
DB = CLIENT[DB_NAME]

# 需要核对的股票
TARGETS = {
    "600519": "贵州茅台",
    "601398": "工商银行"
}

# 关键字段映射 (我们会尝试读取这些字段)
FIELDS_INCOME = [
    "净利润",
    "归属于母公司所有者的净利润",
    "归属于母公司股东的净利润",
    "营业总收入",
    "营业收入"
]

FIELDS_BALANCE = [
    "归属于母公司股东权益合计",
    "归属于母公司股东的权益",
    "所有者权益合计",
    "其他权益工具"  # 银行股核心差异点
]

def dump_stock_data(symbol, name):
    print(f"\n{'='*80}")
    print(f"🔎 股票: {symbol} | {name}")
    print(f"{'='*80}")

    # 1. 获取最新行情和股本 (作为乘数)
    bar = DB["bar_daily"].find_one({"symbol": symbol}, sort=[("datetime", DESCENDING)])
    cap = DB["share_capital"].find_one({"symbol": symbol}, sort=[("date", DESCENDING)])

    close_price = bar['close_price'] if bar else 0
    date_str = bar['datetime'].strftime("%Y-%m-%d") if bar else "N/A"

    # 注意：这里我们直接打印数据库里的原始股本，不做任何假设
    total_shares = cap['total_shares'] if cap else 0
    float_shares = cap['float_shares'] if cap else 0

    print(f"\n[1. 市场快照] (基准日: {date_str})")
    print(f"   - 收盘价: {close_price} 元")
    print(f"   - 总股本: {total_shares:,.0f} 股 ({total_shares/1e8:.2f} 亿)")
    print(f"   - 流通股: {float_shares:,.0f} 股 ({float_shares/1e8:.2f} 亿)")
    print(f"   ----------------------------------------------------")
    print(f"   => 总市值 (计算值): {close_price * total_shares / 1e8:,.2f} 亿元")

    # 2. 获取近 6 期 利润表 (用于手动算 PE TTM)
    print(f"\n[2. 利润表 - 原始数据] (用于计算 PE/PS TTM)")
    print(f"   {'报告期':<12} | {'公告日':<12} | {'归母净利润 (元)':<20} | {'营业总收入 (元)':<20}")
    print(f"   {'-'*75}")

    # 构造查询投影
    proj_inc = {"report_date": 1, "publish_date": 1, "_id": 0}
    for f in FIELDS_INCOME: proj_inc[f] = 1

    cursor_inc = DB["finance_income"].find({"symbol": symbol}, proj_inc).sort("report_date", DESCENDING).limit(6)

    reports_inc = []
    for doc in cursor_inc:
        r_date = doc.get("report_date").strftime("%Y-%m-%d") if doc.get("report_date") else "N/A"
        p_date = doc.get("publish_date").strftime("%Y-%m-%d") if doc.get("publish_date") else "N/A"

        # 智能查找净利润 (优先归母，没有则取净利润)
        np_val = None
        for f in ["归属于母公司所有者的净利润", "归属于母公司股东的净利润", "净利润"]:
            if doc.get(f) is not None:
                np_val = doc.get(f)
                break

        # 智能查找营收
        rev_val = None
        for f in ["营业总收入", "营业收入"]:
            if doc.get(f) is not None:
                rev_val = doc.get(f)
                break

        print(f"   {r_date:<12} | {p_date:<12} | {str(np_val):<20} | {str(rev_val):<20}")
        reports_inc.append(doc)

    # 3. 获取近 6 期 资产负债表 (用于手动算 PB)
    print(f"\n[3. 资产负债表 - 原始数据] (用于计算 PB/BPS)")
    print(f"   {'报告期':<12} | {'公告日':<12} | {'归母权益 (元)':<20} | {'其他权益工具 (元)':<20}")
    print(f"   {'-'*75}")

    proj_bal = {"report_date": 1, "publish_date": 1, "_id": 0}
    for f in FIELDS_BALANCE: proj_bal[f] = 1

    cursor_bal = DB["finance_balance"].find({"symbol": symbol}, proj_bal).sort("report_date", DESCENDING).limit(6)

    for doc in cursor_bal:
        r_date = doc.get("report_date").strftime("%Y-%m-%d") if doc.get("report_date") else "N/A"
        p_date = doc.get("publish_date").strftime("%Y-%m-%d") if doc.get("publish_date") else "N/A"

        # 智能查找归母权益
        eq_val = None
        for f in ["归属于母公司股东权益合计", "归属于母公司股东的权益", "所有者权益合计"]:
            if doc.get(f) is not None:
                eq_val = doc.get(f)
                break

        other_eq = doc.get("其他权益工具", 0) # 永续债

        print(f"   {r_date:<12} | {p_date:<12} | {str(eq_val):<20} | {str(other_eq):<20}")

    print("\n   >>> 手工计算提示 <<<")
    print("   1. PE(TTM) = 总市值 / (本期Q3累计净利 + 去年年报净利 - 去年同期Q3累计净利)")
    print("   2. PB(LF)  = 总市值 / (最新归母权益 - 其他权益工具[仅银行])")

if __name__ == "__main__":
    for s, n in TARGETS.items():
        dump_stock_data(s, n)