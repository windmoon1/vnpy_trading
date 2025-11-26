"""
Debug Script V2: 智能探测指数接口格式
-------------------------------------------------------
目标:
1. 自动尝试多种代码格式 (如 000300, sh000300) 找出正确的那个。
2. 验证返回数据的字段完整性 (Open/High/Low/Volume)。
3. 确认成交额 (Turnover) 的单位。
"""
import os
import pandas as pd
from datetime import datetime
import akshare as ak

# --- 1. 环境配置 ---
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['all_proxy'] = ''
os.environ['NO_PROXY'] = '*'

# --- 配置 ---
BASE_CODE = "000300"  # 沪深300 基础代码
TEST_NAME = "CSI 300 Index"

def run_test():
    print(f"🔬 开始智能测试: 寻找 [{TEST_NAME}] 的正确代码格式...")

    # 候选列表：优先尝试带 sh/sz 前缀的
    candidates = [
        f"sh{BASE_CODE}",  # 可能性 90%
        f"sz{BASE_CODE}",  # 可能性 5%
        BASE_CODE          # 可能性 5%
    ]

    valid_df = pd.DataFrame()
    correct_symbol = ""

    # 1. 暴力轮询 (Brute-force Check)
    for sym in candidates:
        print(f"   👉 尝试代码: {sym} ...", end="")
        try:
            df = ak.stock_zh_index_daily_em(symbol=sym)
            if not df.empty:
                print(" ✅ 通了!")
                valid_df = df
                correct_symbol = sym
                break
            else:
                print(" ❌ 空数据")
        except Exception as e:
            print(f" ❌ 报错: {e}")

    if valid_df.empty:
        raise ValueError("❌ 所有格式尝试均失败！请检查网络或 AKShare 接口状态。")

    print(f"\n🎉 锁定正确格式: [{correct_symbol}]")

    # 2. 数据质量验证
    print("\n📊 数据概览 (Tail 3):")
    print(valid_df.tail(3))

    # 3. 关键字段检查
    row = valid_df.iloc[-1] # 取最新的一行
    print("\n🔍 字段与数值单位检查 (最新一交易日):")

    # 检查成交额 (Amount)
    # 沪深300 日成交额通常在 2000亿 (2e11) 左右
    amount = float(row['amount']) if 'amount' in row else 0.0

    print(f"   📅 日期: {row['date']}")
    print(f"   💰 成交额 (raw): {amount:,.2f}")

    if amount > 1_000_000_000:
        print("   ✅ 单位判断: [元] (无需乘 10000)")
    elif amount > 10_000:
        print("   ⚠️ 单位判断: [万元] (入库时需 * 10000)")
    else:
        print("   ⚠️ 单位判断: [亿元] (入库时需 * 1亿)")

    # 检查 High/Low
    if float(row['high']) == float(row['close']) and float(row['low']) == float(row['close']):
        print("   ⚠️ 警告: High/Low/Close 数值完全一致，可能是伪造的K线！")
    else:
        print("   ✅ High/Low 数据看起来正常 (有波动)。")

    print("\n✨ 验证结束。请根据上方 [单位判断] 修改 Script 05。")

if __name__ == "__main__":
    run_test()