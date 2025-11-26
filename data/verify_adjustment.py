"""
脚本: 前复权价格三重验证 (Final Triple Check)
--------------------------------------------
目标: 验证 [Raw Price / Factor] 是否等于 [Sina Direct QFQ Price]。
"""
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
import numpy as np
import akshare as ak

# --- 目标配置 ---
TARGET_SYMBOL = "000001"
TARGET_DATE = datetime(2006, 1, 4) # 第一个交易日
SYMBOL_SINA = "sz000001" # 接口需要前缀

# --- 数据库连接 ---
CLIENT = MongoClient("localhost", 27017)
DB = CLIENT["vnpy_stock"]
COL_BAR = DB["bar_daily"]
COL_ADJ = DB["adjust_factor"]

def get_direct_qfq_price(symbol_sina, date):
    """
    直接从 Sina 接口下载目标日期的前复权价格 (作为标准答案)
    """
    try:
        date_str = date.strftime('%Y%m%d')
        df = ak.stock_zh_a_daily(
            symbol=symbol_sina,
            start_date=date_str,
            end_date=date_str,
            adjust="qfq" # 请求前复权数据
        )
        if not df.empty:
            return df.iloc[0]['close']
        return None
    except Exception as e:
        print(f"⚠️ 无法获取 Sina QFQ Direct Check Price: {e.__class__.__name__}")
        return None

def calculate_and_compare(symbol: str, date: datetime):
    print(f"🔎 目标: {symbol} 在 {date.strftime('%Y-%m-%d')} 的前复权收盘价")
    print("-" * 60)

    # 1. 获取原始价格 (Raw Price)
    bar_doc = COL_BAR.find_one({'symbol': symbol, 'datetime': date})
    # 2. 获取复权因子 (Factor)
    factor_doc = COL_ADJ.find_one(
        {'symbol': symbol, 'date': {'$lte': date}},
        sort=[('date', -1)]
    )

    if not bar_doc or not factor_doc:
        print("❌ 错误: 数据库中 Raw Price 或 Factor 缺失。请确认脚本 02/05 已跑完。")
        return

    # 3. 核心计算 (修正后的除法逻辑)
    raw_close = bar_doc.get('close_price')
    factor = factor_doc.get('factor')
    price_adj_calculated = raw_close / factor

    # 4. 获取标准答案 (Direct API Fetch)
    direct_qfq_price = get_direct_qfq_price(SYMBOL_SINA, date)

    # 5. 打印对比结果
    print(f"   原始收盘价 (Raw): {raw_close:.4f}")
    print(f"   生效复权因子:    {factor:.8f} (日期: {factor_doc['date'].strftime('%Y-%m-%d')})")
    print("-" * 60)
    print(f"   A. 理论计算价格:   {price_adj_calculated:.4f}  (Raw / Factor)")
    print(f"   B. Sina标准价格:   {direct_qfq_price:.4f}")

    if direct_qfq_price is not None and abs(price_adj_calculated - direct_qfq_price) < 0.001:
        print("\n🎉 🎉 **最终验证：数据完全匹配！**")
        print("   结论：您的 [Raw Data + Factor] 架构正确无误。")
    else:
        print("\n❌ 校验失败：计算价格与标准价格差异过大。")
        print("   请检查是否有精度损失或 Factor 数据问题。")

if __name__ == "__main__":
    calculate_and_compare(TARGET_SYMBOL, TARGET_DATE)