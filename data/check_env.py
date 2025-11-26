"""
脚本: 复权因子数据源验证 (Adjustment Factor Check)
---------------------------------------------
目标: 验证 ak.stock_zh_a_daily(adjust="qfq-factor") 接口的可用性和数据结构。
"""
import os
import akshare as ak
import pandas as pd
from datetime import datetime

# --- 🛡️ 直连补丁 ---
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['all_proxy'] = ''
os.environ['NO_PROXY'] = '*'

# --- 配置 ---
SYMBOL_CODE = "000001"
SYMBOL_SINA = "sz000001"
START = "20200101"  # 选一个较新的时间段，确保接口活跃


def check_factor_data():
    print(f"🔎 正在验证 [复权因子] 接口...")
    print(f"   标的: {SYMBOL_SINA}")
    print("-" * 50)

    try:
        # 核心调用: 使用特定的 adjust 参数来获取因子
        df = ak.stock_zh_a_daily(
            symbol=SYMBOL_SINA,
            start_date=START,
            end_date=datetime.now().strftime("%Y%m%d"),
            adjust="qfq-factor"
        )

        if df.empty:
            print("❌ 接口返回空数据！")
            return

        # 检查关键列名
        if 'qfq_factor' not in df.columns:
            print("❌ 关键字段 [qfq_factor] 不存在！")
            print(f"   实际列名: {df.columns.tolist()}")
            return

        print("✅ 接口连通性验证成功！")
        print(f"   数据条数: {len(df)} 条复权因子记录。")
        print("   --- 数据采样 (最新5条复权因子) ---")

        # 因子通常在没有公司行动时不变，有分红或拆股时变动
        print(df[['date', 'qfq_factor']].tail(10))

        print("\n🎉 结论: 复权因子数据可获取。我们可以将 Raw Data + Factor 模型投入使用了。")
        return True

    except Exception as e:
        print(f"❌ 最终测试失败: {e}")
        return False


if __name__ == "__main__":
    check_factor_data()