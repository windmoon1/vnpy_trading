"""
Script 29: Verify BSE Adjustment Factor Schema
----------------------------------------------
目标: 校验北交所股票的复权因子数据结构。
用途: 确保 ak.stock_zh_a_info_qfq_factor 返回的字段能正确映射到 adjust_factor 表。
"""

import akshare as ak
import pandas as pd
from pprint import pprint
import datetime
import time
import random

# --- 配置 ---
# 使用已验证成功的查询格式
QUERY_SYMBOL = "bj920832"
PURE_SYMBOL = "920832"

def inspect_factor_schema():
    print(f"🔍 正在请求复权因子数据: {QUERY_SYMBOL}...")

    # --- 增加重试机制 ---
    df = None
    for i in range(3):
        try:
            # 使用 ak.stock_zh_a_daily
            df = ak.stock_zh_a_daily(symbol=QUERY_SYMBOL)
            if df is not None and not df.empty:
                break
            time.sleep(random.uniform(1, 3))
        except Exception:
            time.sleep(random.uniform(1, 3))

    if df is None or df.empty:
        print("❌ API 返回空数据或调用失败。")
        return

    print(f"✅ API 原始字段: {df.columns.tolist()}")

    # 1. 字段映射和抽取 (目标是 {datetime, adjust_factor})

    # 查找因子列: 通常是 'qfq_factor' 或 'factor'
    factor_col = 'qfq_factor' if 'qfq_factor' in df.columns else 'factor'

    if 'date' not in df.columns or factor_col not in df.columns:
        print(f"❌ 警告: 原始数据中缺少核心字段 'date' 或 '{factor_col}'。")
        return

    # 2. 构造最终文档结构 (取最新一行作为示例)
    row = df.iloc[-1]

    final_doc = {
        "symbol": PURE_SYMBOL,
        "datetime": row['date'].isoformat(), # 转换为 ISODate 格式
        "adjust_factor": float(row[factor_col]),
        "source": "AKSHARE_SINA"
    }

    # 打印最终结构
    print("\n=============================================")
    print("⚖️ 目标 DB 结构校验 (Adjustment Factor Schema)")
    print("=============================================")
    pprint(final_doc)

    # 检查核心字段完整性
    print("\n✅ 核心字段映射成功且完整。")

if __name__ == "__main__":
    inspect_factor_schema()