"""
Script 21: Fix & Verify Stock Codes (Unified Standard)
------------------------------------------------------
目标:
  1. [Verify] 检查 stock_info 和 bar_daily 的代码格式（是否包含后缀）。
  2. [Fix] 修复 index_components 中漏标后缀的 B股/北证/新代码。

标准格式 (VtSymbol):
  - 6xxxxx -> .SH
  - 0xxxxx, 3xxxxx -> .SZ
  - 8xxxxx, 4xxxxx, 92xxxx -> .BJ
  - 20xxxx -> .SZ (深B)
  - 90xxxx -> .SH (沪B)
"""

from pymongo import MongoClient, UpdateOne
from tqdm import tqdm
import re

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"

client = MongoClient(MONGO_HOST, MONGO_PORT)
db = client[DB_NAME]


def get_suffix(code):
    """
    更完善的代码后缀推断逻辑
    """
    code = str(code).strip()

    # 如果已经有后缀，直接返回
    if code.endswith((".SH", ".SZ", ".BJ")):
        return code

    # 纯数字判断
    if not code.isdigit():
        return code  # 可能是 weird string

    # 规则匹配
    if code.startswith(("6")):
        return f"{code}.SH"
    elif code.startswith(("0", "3", "20")):  # 20xxxx 是深B
        return f"{code}.SZ"
    elif code.startswith(("8", "4", "92")):  # 92xxxx 是北证新号段
        return f"{code}.BJ"
    elif code.startswith("90"):  # 90xxxx 是沪B
        return f"{code}.SH"

    return code  # 无法识别，保持原样


def inspect_collection_format(col_name, sample_size=5):
    """检查集合中的 symbol 格式"""
    print(f"\n🔍 正在检查集合 [{col_name}] ...")
    col = db[col_name]

    # 随机抽样
    pipeline = [{"$sample": {"size": sample_size}}]
    samples = list(col.aggregate(pipeline))

    if not samples:
        print("   (空集合)")
        return

    print(f"   抽样预览 ({sample_size}条):")
    for doc in samples:
        # 兼容不同表结构
        sym = doc.get("symbol")
        exchange = doc.get("exchange")
        print(f"   - Symbol: {sym:<10} | Exchange: {exchange}")


def fix_index_components():
    """修复成分股列表中的代码"""
    print(f"\n🛠️ 开始修复 [index_components] 中的成分股代码...")

    col = db["index_components"]
    cursor = col.find({})
    total = col.count_documents({})

    ops = []
    fixed_count = 0

    for doc in tqdm(cursor, total=total, desc="Scanning"):
        components = doc.get("components", [])
        weights = doc.get("weights", {})

        new_components = []
        new_weights = {}
        changed = False

        # 1. 修复列表
        for code in components:
            new_code = get_suffix(code)
            new_components.append(new_code)
            if new_code != code:
                changed = True

        # 2. 修复权重字典的 Key
        if weights:
            for k, v in weights.items():
                new_k = get_suffix(k)
                new_weights[new_k] = v
                if new_k != k:
                    changed = True

        if changed:
            ops.append(UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {
                    "components": new_components,
                    "weights": new_weights
                }}
            ))
            fixed_count += 1

    if ops:
        print(f"💾 正在写入修复 ({len(ops)} 条记录)...")
        result = col.bulk_write(ops, ordered=False)
        print(f"✅ 修复完成! 修正了 {result.modified_count} 个指数的成分股格式。")
    else:
        print("✅ 所有成分股格式均正确，无需修复。")


def run():
    print("🚀 启动代码格式标准化程序...\n")

    # 1. 先检查基础表，确认我们不需要修它们
    # 通常 stock_info 和 bar_daily 存储的是纯代码+Exchange字段，或者 VtSymbol
    # 我们需要确认现状，以免回测时拼接错误
    inspect_collection_format("stock_info")
    inspect_collection_format("bar_daily")

    # 2. 修复 index_components
    fix_index_components()

    # 3. 再次验证修复结果 (针对刚才报错的BK0470)
    print("\n🔍 复查 [造纸印刷 BK0470]:")
    doc = db["index_components"].find_one({"index_symbol": "BK0470"})
    if doc:
        # 打印几个特殊的看看修好没
        special_codes = [c for c in doc["components"] if c.startswith(("92", "20"))]
        print(f"   特殊代码示例: {special_codes}")


if __name__ == "__main__":
    run()