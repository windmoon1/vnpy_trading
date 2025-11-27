"""
Script: Verify Delisted Share Capital (Quality Check)
-----------------------------------------------------
功能:
1. 专门审计 [退市股票] 在 share_capital 表中的数据质量。
2. 重点检查: 覆盖率、数值量级、数据来源 (Rescue tag)。
3. 抽样展示 '000005' 等典型退市股的详细记录。
"""

from pymongo import MongoClient
import pandas as pd
from tabulate import tabulate

# ==========================================
# 配置
# ==========================================
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
COLLECTION_NAME = "share_capital"


def get_db():
    return MongoClient(MONGO_HOST, MONGO_PORT)[DB_NAME]


def verify_delisted():
    print("🕵️‍♂️ 启动 [退市股本数据] 专项审计...")
    db = get_db()

    # 1. 获取退市名单
    delisted_cursor = db["stock_info"].find({"status": "DELISTED"}, {"symbol": 1, "name": 1})
    delisted_map = {doc["symbol"]: doc.get("name", "Unknown") for doc in delisted_cursor}
    delisted_symbols = sorted(list(delisted_map.keys()))

    if not delisted_symbols:
        print("⚠️ 数据库中没有标记为 DELISTED 的股票。")
        return

    print(f"📋 退市股票总数: {len(delisted_symbols)}")

    # 2. 统计覆盖率
    pipeline = [
        {"$match": {"symbol": {"$in": delisted_symbols}}},
        {"$group": {"_id": "$symbol", "count": {"$sum": 1}}}
    ]
    found_cursor = db[COLLECTION_NAME].aggregate(pipeline)
    found_stats = {doc["_id"]: doc["count"] for doc in found_cursor}

    found_count = len(found_stats)
    coverage = (found_count / len(delisted_symbols)) * 100

    print("=" * 60)
    print(f"📊 审计概览:")
    print(f"   - 已有股本数据: {found_count} / {len(delisted_symbols)}")
    print(f"   - 覆盖率:       {coverage:.2f}%")
    print("=" * 60)

    # 3. 重点抽样检查 (000005 世纪星源)
    # 这是我们之前反复 Debug 的对象，它的数据质量代表了救援行动的成败
    target = "000005"
    name = delisted_map.get(target, "世纪星源")

    print(f"\n🔍 [深度抽样] {target} {name}")

    if target in found_stats:
        cursor = db[COLLECTION_NAME].find({"symbol": target}).sort("date", -1).limit(5)
        df = pd.DataFrame(list(cursor))

        if not df.empty:
            # 整理显示列
            cols = ["date", "total_shares", "float_shares", "change_reason"]
            df_show = df[cols].copy()

            # 打印表格
            print(tabulate(df_show, headers='keys', tablefmt='grid'))

            # 核心指标验证
            latest_shares = df.iloc[0]["total_shares"]
            print(f"\n🧮 数值逻辑验证:")
            print(f"   最新总股本: {latest_shares:,.2f}")

            if latest_shares > 100_000_000:
                print("   ✅ 量级正确: [亿级] (符合预期)")
            elif latest_shares > 100_000:
                print("   ⚠️ 量级存疑: [万级] (可能偏小，需检查是否少乘了10000)")
            else:
                print("   ❌ 量级错误: [过小]")

            # 检查来源标记
            reason = df.iloc[0]["change_reason"]
            if "Rescue" in reason or "Calc" in reason:
                print(f"   ✅ 数据来源: 救援脚本 ({reason})")
            else:
                print(f"   ℹ️ 数据来源: 常规渠道 ({reason})")
    else:
        print("   ❌ 尚未获取到该股票数据 (请等待下载脚本完成)")

    # 4. 检查 000024 (招商地产 - 2015年退市)
    target2 = "000024"
    if target2 in found_stats:
        print(f"\n🔍 [对比抽样] {target2} {delisted_map.get(target2, '')}")
        doc = db[COLLECTION_NAME].find_one({"symbol": target2}, sort=[("date", -1)])
        print(f"   最新日期: {doc['date']} | 总股本: {doc['total_shares']:,.0f} | 来源: {doc.get('change_reason')}")


if __name__ == "__main__":
    verify_delisted()