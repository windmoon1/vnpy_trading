"""
脚本 07: 退市数据全量审计 (Data Audit)
------------------------------------------------
目标: 校验退市股票的行情完整性（是否到退市前最后一刻）及复权因子覆盖率。
输出: 控制台摘要 + 详细 CSV 报告 ('delisted_audit_report.csv')
"""
import os
import pandas as pd
from datetime import datetime, timedelta
from pymongo import MongoClient
from tqdm import tqdm
from vnpy.trader.constant import Exchange

# --- 配置 ---
DB_NAME = "vnpy_stock"
# 警告阈值：如果 (退市日期 - 最后K线日期) > 60天，视为疑似缺失（或长期停牌）
GAP_THRESHOLD_DAYS = 60

# 数据库连接
client = MongoClient("localhost", 27017)
db = client[DB_NAME]
col_info = db["stock_info"]
col_bar = db["bar_daily"]
col_adj = db["adjust_factor"]


def get_last_bar_date(symbol):
    """获取数据库中该股票最后一条K线的日期"""
    doc = col_bar.find_one(
        {"symbol": symbol},
        sort=[("datetime", -1)],  # 按时间倒序取第一个
        projection={"datetime": 1}
    )
    return doc["datetime"] if doc else None


def check_adjust_factor(symbol):
    """检查是否有复权因子"""
    # 只要有一条因子记录就算有（通常 akshare 会一次性拉取所有历史因子）
    return col_adj.find_one({"symbol": symbol}, projection={"_id": 1}) is not None


def run_audit():
    print("🚀 启动 [退市股票数据审计]...")

    # 1. 获取所有退市股票名单
    # 注意：我们只关心状态为 DELISTED 的
    cursor = col_info.find({"status": "DELISTED"})
    delisted_stocks = list(cursor)

    if not delisted_stocks:
        print("⚠️ 未在 stock_info 中找到任何状态为 DELISTED 的股票。请先运行脚本 04。")
        return

    print(f"📋 待审计股票数量: {len(delisted_stocks)}")

    results = []

    # 2. 遍历检查
    for stock in tqdm(delisted_stocks, unit="stock"):
        symbol = stock["symbol"]
        name = stock.get("name", "Unknown")
        delisted_str = stock.get("delisted_date", "")

        # 解析退市日期
        delisted_dt = None
        if delisted_str:
            try:
                if isinstance(delisted_str, str):
                    delisted_dt = datetime.strptime(delisted_str, "%Y-%m-%d")
                elif isinstance(delisted_str, datetime):
                    delisted_dt = delisted_str
            except:
                pass

        # Check 1: 最后行情日期
        last_bar_dt = get_last_bar_date(symbol)

        # Check 2: 复权因子
        has_factor = check_adjust_factor(symbol)

        # 判定逻辑
        status = "OK"
        gap_days = -1

        if not last_bar_dt:
            status = "MISSING_BARS"  # 完全没行情
        elif not delisted_dt:
            status = "MISSING_META"  # 元数据里没退市日期，无法校验
        else:
            # 计算差距
            gap = delisted_dt - last_bar_dt
            gap_days = gap.days

            if gap_days > GAP_THRESHOLD_DAYS:
                status = "LARGE_GAP"  # 缺尾部数据 或 停牌
            elif gap_days < -5:
                # 这种情况理论上不该发生（K线日期晚于退市日期），除非借壳或数据源错误
                status = "DATA_CONFLICT"

            if status == "OK" and not has_factor:
                status = "MISSING_FACTOR"  # 行情有，但缺因子

        results.append({
            "symbol": symbol,
            "name": name,
            "status": status,
            "delisted_date": delisted_dt.strftime("%Y-%m-%d") if delisted_dt else "N/A",
            "last_bar_date": last_bar_dt.strftime("%Y-%m-%d") if last_bar_dt else "N/A",
            "gap_days": gap_days if gap_days != -1 else "",
            "has_factor": has_factor
        })

    # 3. 生成报告
    df = pd.DataFrame(results)

    # 统计摘要
    print("\n" + "=" * 40)
    print("📊 审计摘要 (Audit Summary)")
    print("=" * 40)
    summary = df['status'].value_counts()
    print(summary)

    # 导出 CSV
    csv_file = "data/delisted_data_audit.csv"
    df.sort_values(by="status").to_csv(csv_file, index=False, encoding="utf-8-sig")
    print(f"\n📝 详细报告已保存至: {os.path.abspath(csv_file)}")

    # 4. 打印一些典型的问题案例供抽查
    if not df[df['status'] != 'OK'].empty:
        print("\n🔍 异常样本抽查 (Top 5):")
        print(df[df['status'] != 'OK'].head(5).to_string(index=False))

    # 5. 给出建议
    print("\n💡 修复建议:")
    if "MISSING_BARS" in summary:
        print("   - MISSING_BARS: 运行脚本 04 重新下载 (可能当时网络超时跳过了)。")
    if "MISSING_FACTOR" in summary:
        print("   - MISSING_FACTOR: 运行脚本 03 补充下载因子 (可能 AkShare 接口波动)。")
    if "LARGE_GAP" in summary:
        print("   - LARGE_GAP: 正常现象。很多退市股在真正摘牌前会经历数月的停牌整理期。")
        print("     只要 gap_days 不是特别离谱（如 > 365天），通常可以直接使用。")


if __name__ == "__main__":
    run_audit()