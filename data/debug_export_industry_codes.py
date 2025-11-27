"""
Script: Export Downloaded Industry Codes for Manual Mapping
-----------------------------------------------------------
功能:
1. 统计 MongoDB 中 industry_history 表里所有出现的行业代码。
2. 计算每个代码的引用次数 (Ref Count)，帮助判断重要性。
3. 导出为 CSV 文件，方便用户去网上搜索对应的中文含义。
"""

import pandas as pd
from pymongo import MongoClient
import os

# ==========================================
# 配置
# ==========================================
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
COLLECTION_NAME = "industry_history"
EXPORT_FILE = "data/sw_industry_codes_to_map.csv"


def get_db():
    return MongoClient(MONGO_HOST, MONGO_PORT)[DB_NAME]


def run():
    print("🚀 启动 [行业代码清点工具]...")
    db = get_db()
    col = db[COLLECTION_NAME]

    # 1. 聚合查询: 按 industry_code 分组统计
    print("   📊 正在统计代码频次...")
    pipeline = [
        {"$group": {"_id": "$industry_code", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}  # 按代码排序
    ]

    cursor = col.aggregate(pipeline)

    data = []
    for doc in cursor:
        code = doc["_id"]
        count = doc["count"]
        # 尝试从现有数据中看是否偶尔有名字 (万一有漏网之鱼)
        # sample = col.find_one({"industry_code": code, "industry_name": {"$regex": "^[^SW_]"}})
        # name = sample["industry_name"] if sample else ""

        data.append({
            "Industry_Code": code,
            "Ref_Count": count,
            "Possible_Name": ""  # 留空给人工填
        })

    if not data:
        print("❌ 数据库为空，没有找到任何行业数据。")
        return

    # 2. 转为 DataFrame
    df = pd.DataFrame(data)
    print(f"   ✅ 共发现 {len(df)} 个独立行业代码")

    # 3. 简单分类预览
    # 申万代码通常规则:
    # 4开头 = 2014版?
    # 6开头 = 2021版?
    # 7开头 = ?
    df['Prefix'] = df['Industry_Code'].astype(str).str[:1]
    print("\n   🧮 代码前缀分布:")
    print(df['Prefix'].value_counts())

    # 4. 导出
    # 确保目录存在
    os.makedirs(os.path.dirname(EXPORT_FILE), exist_ok=True)
    df.to_csv(EXPORT_FILE, index=False, encoding="utf-8-sig")

    print("\n" + "=" * 50)
    print(f"📂 结果已导出至: {EXPORT_FILE}")
    print("   👉 请打开该 CSV 文件，你可以将这些代码复制到搜索引擎或 AI 聊天框中查询中文名。")
    print("=" * 50)

    # 打印前10个高频代码供预览
    print("\n   👀 Top 10 高频引用代码:")
    print(df.sort_values("Ref_Count", ascending=False).head(10).to_string(index=False))


if __name__ == "__main__":
    run()