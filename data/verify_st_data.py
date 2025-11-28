# data/verify_st_data.py

from pymongo import MongoClient
import pandas as pd

# ---------------- Configuration ----------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "vnpy_data"
COLLECTION_NAME = "stock_status_history"  # 注意：这里是目标集合


# -----------------------------------------------

def check_st_data():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # 1. 检查总数
    count = collection.count_documents({"st_history": {"$exists": True}})
    print(f"\n📊 Database Inspection Report for [{DB_NAME}.{COLLECTION_NAME}]")
    print(f"{'=' * 50}")
    print(f"Total documents with 'st_history': {count}")

    if count == 0:
        print("❌ 警告：集合中没有发现包含 st_history 字段的数据！")
        print("   -> 请检查 13 号脚本中的 DB_NAME 和 COLLECTION_NAME 配置是否与这里一致。")
        return

    # 2. 抽查一个样本 (比如 000004)
    # 注意：我们的脚本存入时去掉了 .SZ 后缀，所以这里查 '000004'
    target_symbol = "000004"
    doc = collection.find_one({"symbol": target_symbol})

    print(f"\n🔎 Sample Check: Symbol='{target_symbol}'")
    if doc:
        print("✅ Found!")
        if "st_history" in doc:
            history = doc["st_history"]
            print(f"   ST History Count: {len(history)} records")
            print("   Latest 3 records:")
            for rec in history[-3:]:
                print(f"     - {rec['date'].strftime('%Y-%m-%d')}: {rec['status']}")
        else:
            print("❌ Found document but 'st_history' field is missing!")
            print(doc)
    else:
        print(f"❌ Document for {target_symbol} NOT FOUND.")
        # 尝试模糊查询，看看是不是存成了带后缀的
        doc_suffix = collection.find_one({"symbol": "000004.SZ"})
        if doc_suffix:
            print(f"⚠️ 发现原因：数据被存为了 '000004.SZ' (带后缀)，请确认代码中的清洗逻辑。")

    # 3. 打印集合中的前 5 个 ID，确认存成了什么样
    print("\n📋 First 5 Symbols in DB:")
    cursor = collection.find({}, {"symbol": 1, "_id": 0}).limit(5)
    for d in cursor:
        print(f"   - {d.get('symbol', 'UNKNOWN')}")


if __name__ == "__main__":
    check_st_data()