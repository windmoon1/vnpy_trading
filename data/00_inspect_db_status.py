"""
Script 00: Database Status Inspector (Health Check)
---------------------------------------------------
功能:
1. 扫描 Script 01 定义的所有数据库和集合。
2. 统计各表的数据量 (Count)。
3. 标识哪些是 "✅ 就绪"，哪些是 "⬜ 空置"。
4. 帮助决策下一步的数据获取优先级。
"""

import pandas as pd
from pymongo import MongoClient
from tabulate import tabulate

# ==========================================
# 配置: 定义我们需要检查的架构 (Sync with Script 01)
# ==========================================
MONGO_HOST = "localhost"
MONGO_PORT = 27017

SCHEMA_CHECKLIST = {
    "vnpy_stock": [
        "stock_info",  # 基础信息 (已完成)
        "bar_daily",  # 日线行情 (已完成)
        "adjust_factor",  # 复权因子 (已完成)
        "share_capital",  # 股本变动 (已完成)
        "finance_balance",  # 资产负债表 (进行中)
        "finance_income",  # 利润表 (进行中)
        "finance_cashflow",  # 现金流量表 (进行中)
        "valuation_daily",  # 每日估值 (待生成)
        "index_daily",  # 指数行情 (Script 05)
        "index_components",  # 指数成分股 (待定)
        "industry_history",  # 行业分类历史 (待定)
        "analysis_limit_up",  # 涨停分析 (待定)
        "analysis_limit_down"  # 跌停分析 (待定)
    ],
    "vnpy_master": [
        "trading_calendar",  # 交易日历 (重要!)
        "exchange_rate"  # 汇率 (可选)
    ],
    "vnpy_etf": [
        "etf_info",  # ETF列表
        "bar_daily"  # ETF行情
    ],
    "vnpy_future": [
        "bar_daily",  # 期货日线
        "dominant_contract"  # 主力合约映射
    ]
}


def get_client():
    return MongoClient(MONGO_HOST, MONGO_PORT)


def inspect_db():
    print("🚀 启动 [全资产数据库体检程序]...")
    client = get_client()

    report_data = []

    for db_name, collections in SCHEMA_CHECKLIST.items():
        db = client[db_name]

        for col_name in collections:
            try:
                count = db[col_name].count_documents({})

                # 状态判定
                if count > 100000:
                    status = "✅ 充裕"
                elif count > 0:
                    status = "⚠️ 部分"
                else:
                    status = "⬜ 空置"

                # 抽样时间 (如果有 date 字段)
                latest_date = "-"
                if count > 0:
                    sample = db[col_name].find_one(sort=[("date", -1)]) or \
                             db[col_name].find_one(sort=[("datetime", -1)]) or \
                             db[col_name].find_one(sort=[("report_date", -1)])

                    if sample:
                        for date_key in ["date", "datetime", "report_date", "list_date"]:
                            if date_key in sample:
                                val = sample[date_key]
                                latest_date = str(val).split()[0]
                                break

                report_data.append({
                    "Database": db_name,
                    "Collection": col_name,
                    "Count": count,
                    "Status": status,
                    "Latest Date": latest_date
                })

            except Exception as e:
                report_data.append({
                    "Database": db_name,
                    "Collection": col_name,
                    "Count": "Error",
                    "Status": f"❌ {str(e)}",
                    "Latest Date": "-"
                })

    print("\n" + "=" * 80)
    print("🏥 数据库体检报告 (Database Health Report)")
    print("=" * 80)

    df = pd.DataFrame(report_data)
    print(tabulate(df, headers='keys', tablefmt='simple_grid', showindex=False))

    print("\n💡 下一步建议:")
    empty_cols = df[df["Count"] == 0]["Collection"].tolist()
    print(f"   发现 {len(empty_cols)} 个空表，建议优先补充基础元数据表 (如 trading_calendar, industry_history)。")


if __name__ == "__main__":
    inspect_db()