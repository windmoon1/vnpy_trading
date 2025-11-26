"""
脚本: 689009 复权因子定点查杀 (EastMoney Source)
------------------------------------------------
目标: 验证东财接口是否支持科创板 CDR 的 QFQ Factor。
"""
import os
import time
import requests
from datetime import datetime
from tqdm import tqdm
import akshare as ak
import pandas as pd

# --- 🛡️ 直连补丁 ---
os.environ['http_proxy'] = ''; os.environ['https_proxy'] = ''; os.environ['all_proxy'] = ''; os.environ['NO_PROXY'] = '*'

# --- 配置 ---
TARGET_SYMBOL = "689009"
ADJUST = "qfq-factor" # 核心目标：获取复权因子

def run_factor_check_689009():
    print(f"🔎 正在对 {TARGET_SYMBOL} 进行东财复权因子最终查杀...")

    try:
        # 核心调用: EastMoney 历史行情接口
        # 即使请求因子，我们也要走最稳定的 stock_zh_a_hist 接口
        df = ak.stock_zh_a_hist(
            symbol=TARGET_SYMBOL,
            period="daily",
            start_date="20200101",
            end_date=datetime.now().strftime("%Y%m%d"),
            adjust=ADJUST
        )

        if df.empty:
            print("❌ [失败]: 接口返回空数据。")
            return

        # 1. 检查关键字段
        # 东财返回的因子字段可能不是 qfq_factor，而是 '复权因子' 或其他中文名
        factor_col = None
        for col in df.columns:
            if '复权' in col or 'FACTOR' in col.upper():
                factor_col = col
                break

        if factor_col is None:
            print("❌ [失败]: 接口未返回任何包含 '复权' 或 'factor' 的字段。")
            print(f"   实际列名: {df.columns.tolist()}")
            return

        # 2. 打印成功数据
        print("✅ 接口调用成功！")
        print(f"   数据条数: {len(df)} 条")
        print("\n--- 关键因子数据预览 ---")
        print(df[['日期', factor_col]].head(5))

        # 3. 终极决策
        print(f"\n🎉 结论: 东财能够返回 {factor_col}，我们成功获取到复权因子。")
        print("   下一步：用这个方法来修复我们的脚本 05。")

    except Exception as e:
        print(f"\n❌ [结论]: 发生致命错误: {e.__class__.__name__} - {e}")
        print("   => 提示：该股的复权因子可能需要走付费 API 渠道。")

if __name__ == "__main__":
    run_factor_check_689009()