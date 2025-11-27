"""
Script: Verify Share Capital Data Integrity
-------------------------------------------
功能:
1. 检查 MongoDB 中 share_capital 表的数据质量
2. 确认数值单位 (Unit Check) - 核心任务！
3. 检查流通股与总股本的逻辑关系
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


def verify_data():
    db = get_db()
    col = db[COLLECTION_NAME]

    # 1. 基础统计
    total_docs = col.count_documents({})
    unique_symbols = len(col.distinct("symbol"))

    print("=" * 60)
    print(f"🚀 [股本数据体检报告]")
    print("=" * 60)
    print(f"📚 总记录数: {total_docs}")
    print(f"🏢 覆盖股票数: {unique_symbols}")

    if total_docs == 0:
        print("❌ 错误: 数据库为空！请重新运行下载脚本。")
        return

    # 2. 抽样检查 (Unit Check)
    # 选取典型的蓝筹股检查，例如: 600519 (贵州茅台)
    sample_symbol = "600519"
    cursor = col.find({"symbol": sample_symbol}).sort("date", -1).limit(3)
    df_sample = pd.DataFrame(list(cursor))

    if not df_sample.empty:
        print(f"\n🔍 [抽样检查: {sample_symbol} 贵州茅台]")
        # 仅展示关键列
        cols = ["date", "total_shares", "float_shares", "change_reason"]
        print(tabulate(df_sample[cols], headers='keys', tablefmt='grid'))

        latest_shares = df_sample.iloc[0]['total_shares']
        print(f"\n🧮 单位推理:")
        print(f"   当前库存储值: {latest_shares:,.2f}")
        print(f"   茅台实际总股本(约): 12.56亿股 (1,256,197,800)")

        if latest_shares > 1_000_000_000:
            print("   ✅ 结论: 单位是 [股] (无需修正)")
        elif latest_shares > 100_000:
            print("   ⚠️ 结论: 单位是 [万股] (后续计算需 * 10,000)")
        else:
            print("   ⚠️ 结论: 单位是 [亿股] (后续计算需 * 100,000,000)")

    # 3. 逻辑检查 (Float > Total)
    # 理论上流通股不应大于总股本
    abnormal_count = col.count_documents({"$expr": {"$gt": ["$float_shares", "$total_shares"]}})
    print(f"\n🛡 [逻辑检查]")
    if abnormal_count > 0:
        print(f"   ⚠️ 发现 {abnormal_count} 条记录 '流通股 > 总股本' (可能是数据源错误或特殊AB股结构)")
        # 展示几条异常的看看
        abnormal_cursor = col.find({"$expr": {"$gt": ["$float_shares", "$total_shares"]}}).limit(3)
        print("   异常样例:")
        for doc in abnormal_cursor:
            print(f"   - {doc['symbol']} ({doc['date']}): Total={doc['total_shares']}, Float={doc['float_shares']}")
    else:
        print("   ✅ 所有记录逻辑正常 (流通股 <= 总股本)")

    # 4. 字段类型检查
    sample_doc = col.find_one()
    print(f"\n🧬 [字段类型检查]")
    for key, value in sample_doc.items():
        if key != "_id":
            print(f"   - {key}: {type(value).__name__} (Sample: {value})")


if __name__ == "__main__":
    verify_data()