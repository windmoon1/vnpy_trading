"""
Script 19: Inspect Index Components Structure
---------------------------------------------
目标: 深度透视 [index_components] 表中各类数据的字段结构。
用途: 找出 BENCHMARK, INDUSTRY, CONCEPT 三者之间的数据结构差异，为统一修复做准备。
"""

from pymongo import MongoClient
import pprint

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
COL_NAME = "index_components"

def inspect_structure():
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[DB_NAME]
    col = db[COL_NAME]

    print(f"🔬 正在扫描集合 [{COL_NAME}] 的数据结构...\n")

    # 1. 获取所有存在的类别
    categories = col.distinct("category")
    if not categories:
        print("❌ 集合为空，无数据！")
        return

    print(f"📊 发现类别: {categories}\n")

    # 2. 逐个类别抽样检查
    for cat in categories:
        print("=" * 60)
        print(f"🧐 类别: [{cat}]")
        print("=" * 60)

        # 抽取最新的一条记录
        doc = col.find_one({"category": cat}, sort=[("date", -1)])

        if not doc:
            print("   (无数据)")
            continue

        # 打印所有字段及其类型/样例值
        keys = sorted(doc.keys())
        for k in keys:
            val = doc[k]
            val_type = type(val).__name__

            # 对长列表/字典做截断显示，避免刷屏
            display_val = str(val)
            if isinstance(val, list):
                count = len(val)
                if count > 5:
                    display_val = f"List(len={count}) -> {val[:3]} ... {val[-1]}"
                else:
                    display_val = f"List(len={count}) -> {val}"
            elif isinstance(val, dict):
                count = len(val)
                if count > 5:
                    # 取前3个key
                    sample_keys = list(val.keys())[:3]
                    sample_dict = {k: val[k] for k in sample_keys}
                    display_val = f"Dict(len={count}) -> {sample_dict} ..."
                else:
                    display_val = f"Dict(len={count}) -> {val}"

            print(f"   - {k:<15} ({val_type:<5}): {display_val}")

        print("\n")

if __name__ == "__main__":
    inspect_structure()