"""
Script 23: Unify Stock Info Schema
-----------------------------------
目标: 修复 vnpy_stock.stock_info 表中的模式不一致问题。
      给所有旧的 A 股股票记录（缺少 category, product 等字段的）补齐元数据。

修复内容:
  1. 修复 NameError: name 're' is not defined。
  2. 缺失 category 字段的记录，根据代码规则推断为 "STOCK_A"。
  3. 补齐 product: "STOCK" 和 updated_at 字段。
"""

from pymongo import MongoClient, UpdateOne
from tqdm import tqdm
import datetime
# 🔥 修复：导入 re 模块
import re

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
COLLECTION = "stock_info"

client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client[DB_NAME]

def infer_category(symbol: str) -> str:
    """根据股票代码推断股票类型 (简易版)"""
    code = str(symbol).strip()

    # 理论上 B股/北交所 已经被 Script 22 录入时标记好了，这里主要处理旧 A 股
    if code.startswith(("60", "00", "30", "688")):
        return "STOCK_A" # A股（主板、创业板、科创板）
    elif code.startswith(("900", "200")):
        return "STOCK_B"
    elif code.startswith(("4", "8", "92")):
        return "STOCK_BJ"
    else:
        return "UNKNOWN_A"

def run_unification():
    print("🚀 启动 [stock_info 模式统一] 任务...")

    # 1. 查找需要修复的记录 (只要 category 不存在，就视为旧数据)
    query = {"category": {"$exists": False}}
    total_found = db[COLLECTION].count_documents(query)

    if total_found == 0:
        print("✅ 所有记录均已包含 category 字段，模式已统一。")
        return

    print(f"🔍 发现 {total_found} 条旧 A 股记录需要补齐元数据...")

    cursor = db[COLLECTION].find(query)
    ops = []

    # 2. 构造批量更新操作
    for doc in tqdm(cursor, total=total_found, desc="Patching Schema"):
        symbol = doc["symbol"]

        # 字段推断与补齐
        inferred_category = infer_category(symbol)

        ops.append(UpdateOne(
            {"_id": doc["_id"]},
            {"$set": {
                "category": inferred_category,
                "product": "STOCK",
                "updated_at": datetime.datetime.now()
            }}
        ))

    # 3. 执行更新
    if ops:
        result = db[COLLECTION].bulk_write(ops, ordered=False)
        print(f"💾 数据库写入完成!")
        print(f"   - 成功更新记录数: {result.modified_count}")

    # 4. 验证修复结果
    print("\n🔍 验证修复后的 A 股记录 (000001):")
    pingan = db[COLLECTION].find_one({"symbol": "000001"})
    if pingan:
        print(f"   - symbol: {pingan['symbol']}")
        print(f"   - name: {pingan['name']}")
        print(f"   - category: {pingan.get('category')}")
        print(f"   - product: {pingan.get('product')}")

if __name__ == "__main__":
    run_unification()