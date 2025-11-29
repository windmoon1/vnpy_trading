"""
Script 18: Verify Financial Data Completeness (Audit)
-----------------------------------------------------
目标: 审计财务数据 (三大表) 的完整性。
逻辑:
1. 以 stock_info 为全集 (Universe)。
2. 统计 finance_balance / income / cashflow 的覆盖情况。
3. 输出“完美覆盖”的比例，并列出缺失样本。
"""

from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm

# --- 数据库配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"

# --- 集合映射 ---
COL_MAP = {
    "Balance": "finance_balance",   # 资产负债表
    "Income": "finance_income",     # 利润表
    "Cashflow": "finance_cashflow"  # 现金流量表
}

def get_all_symbols(db):
    """获取所有股票代码基准"""
    print("📋 正在读取 stock_info 基准...")
    cursor = db["stock_info"].find({}, {"symbol": 1, "name": 1, "list_date": 1})
    # 返回字典: symbol -> {name, list_date}
    return {doc["symbol"]: doc for doc in cursor}

def get_covered_symbols(db, col_name):
    """获取指定集合中包含的所有股票代码"""
    print(f"🔍 正在扫描 {col_name}...")
    # 使用聚合查询加速，只取 distinct symbol
    pipeline = [{"$group": {"_id": "$symbol"}}]
    cursor = db[col_name].aggregate(pipeline)
    return set([doc["_id"] for doc in cursor])

def run_audit():
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[DB_NAME]

    print("🏥 启动 [财务数据完整性审计] ...\n")

    # 1. 获取基准 Universe
    all_stocks_map = get_all_symbols(db)
    all_symbols = set(all_stocks_map.keys())
    total_count = len(all_symbols)

    if total_count == 0:
        print("❌ 错误: stock_info 表为空，无法进行审计。")
        return

    print(f"✅ 基准股票总数: {total_count}")
    print("-" * 60)

    # 2. 获取各表覆盖情况
    coverage_sets = {}
    for label, col_name in COL_MAP.items():
        s = get_covered_symbols(db, col_name)
        coverage_sets[label] = s
        rate = len(s) / total_count * 100
        print(f"   📊 {label:<10} 覆盖数: {len(s):<6} | 覆盖率: {rate:.2f}%")

    # 3. 计算“完美覆盖” (三表都有)
    perfect_symbols = set.intersection(*coverage_sets.values())
    perfect_count = len(perfect_symbols)
    perfect_rate = perfect_count / total_count * 100

    print("-" * 60)
    print(f"🏆 [完美覆盖] (三表齐全): {perfect_count} / {total_count} ({perfect_rate:.2f}%)")

    # 4. 找出完全缺失的“黑洞股票”
    #    (这里取并集，只要任意一个表有数据就算有，全没有才是黑洞)
    any_data_symbols = set.union(*coverage_sets.values())
    missing_symbols = all_symbols - any_data_symbols

    print(f"⚫ [完全缺失] (三表全无): {len(missing_symbols)}")

    # 5. 缺失样本分析
    if missing_symbols:
        print("\n🔎 缺失样本分析 (前 10 个):")
        missing_list = list(missing_symbols)[:10]
        for s in missing_list:
            info = all_stocks_map.get(s, {})
            name = info.get("name", "Unknown")
            list_date = info.get("list_date", "Unknown")
            print(f"   ❌ {s} | {name} | 上市日: {list_date}")

        print("\n💡 分析建议:")
        print("   1. 如果缺失的是近1个月上市的新股，正常 (财报还没发)。")
        print("   2. 如果是老股，可能是新浪源该代码变更或退市，建议手动检查 akshare 接口。")
        print("   3. 如果缺失数量很大 (>5%)，建议重新运行脚本 06 (它会自动跳过已下载的，只补漏)。")
    else:
        print("\n🎉 恭喜！所有股票均有数据。")

    print("=" * 60)

if __name__ == "__main__":
    run_audit()