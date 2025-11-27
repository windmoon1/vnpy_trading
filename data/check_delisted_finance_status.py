"""
Script: Check Delisted Stocks in Financial DB
---------------------------------------------
功能:
1. 从 stock_info 获取所有标记为 [DELISTED] 的股票。
2. 扫描 finance_balance (资产负债表)，统计有多少退市股已经有了财报数据。
3. 帮助决策: 是等待 06 跑完 (Plan D)，还是直接启动远程救援 (Plan C)。
"""

from pymongo import MongoClient
import pandas as pd

# ==========================================
# 配置
# ==========================================
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"


def get_db():
    return MongoClient(MONGO_HOST, MONGO_PORT)[DB_NAME]


def check_status():
    print("🕵️‍♂️ 启动 [退市股财报覆盖率] 侦察...")
    db = get_db()

    # 1. 获取退市名单
    delisted_cursor = db["stock_info"].find({"status": "DELISTED"}, {"symbol": 1, "name": 1})
    delisted_map = {doc["symbol"]: doc.get("name", "Unknown") for doc in delisted_cursor}
    delisted_symbols = list(delisted_map.keys())

    total_delisted = len(delisted_symbols)
    print(f"📋 数据库中标记为退市的股票总数: {total_delisted}")

    if total_delisted == 0:
        print("⚠️ 警告: stock_info 中没有找到 status='DELISTED' 的股票。")
        print("   -> 请先运行脚本 04 (download_delisted_final.py) 进行标记。")
        return

    # 2. 检查 finance_balance 表
    # 使用聚合查询快速统计，避免遍历
    print("🔍 正在扫描 finance_balance 表...")

    pipeline = [
        {"$match": {"symbol": {"$in": delisted_symbols}}},
        {"$group": {"_id": "$symbol"}}
    ]

    found_cursor = db["finance_balance"].aggregate(pipeline)
    found_symbols = set([doc["_id"] for doc in found_cursor])
    found_count = len(found_symbols)

    # 3. 计算覆盖率
    coverage = (found_count / total_delisted) * 100 if total_delisted > 0 else 0

    print("=" * 50)
    print(f"📊 侦察报告:")
    print(f"   - 退市股总数: {total_delisted}")
    print(f"   - 财报已就绪: {found_count}")
    print(f"   - 覆盖率:     {coverage:.2f}%")
    print("=" * 50)

    # 4. 抽样检查 (查看是否包含关键的 '000005')
    sample_target = "000005"
    if sample_target in found_symbols:
        print(f"✅ 关键样本 {sample_target} (世纪星源) -> 已有财报数据")
    else:
        print(f"❌ 关键样本 {sample_target} (世纪星源) -> 暂无财报数据")

    # 5. 决策建议
    print("\n💡 决策建议:")
    if coverage > 80:
        print("   👉 数据充裕: 可以直接运行 [Script 07-C Local Extraction] 从本地提取股本。")
    elif coverage > 0:
        print("   👉 数据部分存在: 可以先运行 [Script 07-C Local] 提取已有的，剩下的再想办法。")
    else:
        print("   👉 数据完全缺失: 本地提取法 (Plan D) 无效！")
        print("      必须使用 [Script 07-C Remote Rescue] (远程计算法) 主动去新浪抓取。")


if __name__ == "__main__":
    check_status()