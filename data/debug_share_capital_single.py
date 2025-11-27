"""
Script: Debug Sina Financial Abstract (Single Stock) - V2 (Wide Table Fix)
--------------------------------------------------------------------------
功能:
1. 针对新浪财务摘要的 "宽表结构" (日期做列名) 进行解析。
2. 定位 "总股本" 所在的行，并提取所有季度的数值。
3. 验证提取出的数据是否正确 (单位: 万股)。
"""

import akshare as ak
import pandas as pd
from datetime import datetime
from tabulate import tabulate

# 测试目标: 000005 (已退市)
SYMBOL = "000005"

def debug_sina_wide_format():
    print(f"🔬 [新浪财务摘要] 宽表解析测试: {SYMBOL}")
    print("=" * 60)

    try:
        # 1. 发起请求
        df = ak.stock_financial_abstract(symbol=SYMBOL)

        if df is None or df.empty:
            print("❌ 失败: 返回空数据")
            return

        # 2. 定位 "总股本" 行
        # 新浪返回的列: ['选项', '指标', '20241231', '20240630', ...]
        # 我们需要找到 '指标' 列中包含 '总股本' 的那一行

        indicator_col = '指标'
        if indicator_col not in df.columns:
            print(f"❌ 结构异常: 找不到 '{indicator_col}' 列。当前列: {df.columns.tolist()}")
            return

        # 模糊匹配 "总股本"
        mask = df[indicator_col].astype(str).str.contains("总股本")
        target_rows = df[mask]

        if target_rows.empty:
            print("❌ 找不到包含 '总股本' 的指标行")
            print("   -> 当前指标列表:", df[indicator_col].unique().tolist())
            return

        # 取第一行匹配结果 (通常是 "总股本(万股)")
        row = target_rows.iloc[0]
        metric_name = row[indicator_col]
        print(f"✅ 锁定指标行: {metric_name}")

        # 3. 提取时间序列数据
        # 排除非日期列
        date_cols = [c for c in df.columns if c not in ['选项', '指标', 'index']]

        extracted_data = []

        print("\n🔍 解析明细 (前5条):")
        count = 0
        for date_str in date_cols:
            val = row[date_str]

            # 跳过空值
            if pd.isna(val) or val == '':
                continue

            try:
                # 日期转换: '20241231' -> '2024-12-31'
                dt = datetime.strptime(date_str, "%Y%m%d")
                fmt_date = dt.strftime("%Y-%m-%d")

                # 数值转换: 假设单位是 [万股], 需 * 10000
                shares_float = float(val) * 10000

                extracted_data.append({
                    "date": fmt_date,
                    "total_shares": shares_float
                })

                if count < 5:
                    print(f"   - {date_str} -> {fmt_date} | 原始值: {val} -> {shares_float:,.0f}")
                    count += 1
            except Exception as e:
                # print(f"   ⚠️ 解析错误 {date_str}: {e}")
                continue

        # 4. 最终验证
        print(f"\n📊 提取汇总:")
        print(f"   成功提取记录数: {len(extracted_data)}")
        if extracted_data:
            latest = extracted_data[0] # 也就是原本列中最靠前的日期
            print(f"   最新一期: {latest['date']} | 股本: {latest['total_shares']:,.0f}")

            # 再次确认单位
            if latest['total_shares'] > 1_000_000_000:
                print("   ✅ 单位检查通过: 数值在 [亿] 级别 (已自动修正万股单位)")
            else:
                print("   ⚠️ 单位警告: 数值偏小，请检查原始单位是否不是万股")

    except Exception as e:
        print(f"💥 异常: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_sina_wide_format()