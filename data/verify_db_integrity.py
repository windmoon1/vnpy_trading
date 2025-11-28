"""
Script 17: Verify Database Integrity (Final Check)
--------------------------------------------------
目标: 全面体检 vnpy_stock 数据库的指数相关表。
检查项:
  1. [Coverage] index_daily vs index_components 的覆盖率。
  2. [Quality] 是否存在空成分股 (Empty Components)。
  3. [Metadata] index_info 的基础状态。
"""

from pymongo import MongoClient

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"


def run_check():
    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[DB_NAME]

    print("🏥 启动 [数据库完整性体检] ...\n")

    # =========================================================================
    # 1. 准备数据快照
    # =========================================================================
    print("⏳ 正在扫描集合索引...")

    # 获取所有有行情的指数 (作为基准)
    daily_cursor = db["index_daily"].aggregate([
        {"$group": {"_id": "$category", "symbols": {"$addToSet": "$symbol"}}}
    ])
    daily_map = {res["_id"]: set(res["symbols"]) for res in daily_cursor}

    # 获取所有有成分股的指数
    comp_cursor = db["index_components"].aggregate([
        {"$group": {"_id": "$category", "symbols": {"$addToSet": "$index_symbol"}}}
    ])
    comp_map = {res["_id"]: set(res["symbols"]) for res in comp_cursor}

    # =========================================================================
    # 2. 交叉验证 (Cross Validation)
    # =========================================================================
    categories = ["BENCHMARK", "INDUSTRY", "CONCEPT"]

    for cat in categories:
        daily_set = daily_map.get(cat, set())
        comp_set = comp_map.get(cat, set())

        # 计算缺失
        missing_comps = daily_set - comp_set  # 有行情但没成分股
        orphan_comps = comp_set - daily_set  # 有成分股但没行情 (罕见)

        print(f"📊 类别: [{cat}]")
        print(f"   ----------------------------------------")
        print(f"   - 行情标的数 (Daily):      {len(daily_set)}")
        print(f"   - 成分标的数 (Components): {len(comp_set)}")

        # 覆盖率
        if len(daily_set) > 0:
            coverage = len(comp_set) / len(daily_set) * 100
            print(f"   - 成分股覆盖率:            {coverage:.1f}%")
        else:
            print(f"   - 成分股覆盖率:            N/A (无行情数据)")

        # 缺失警告
        if missing_comps:
            print(f"   ❌ 严重缺失: {len(missing_comps)} 个指数缺少成分股!")
            # 打印前5个示例
            print(f"      示例: {list(missing_comps)[:5]}...")
        else:
            print("   ✅ 完美覆盖 (所有有行情的指数都有成分股)")

        # 孤儿警告
        if orphan_comps:
            print(f"   ⚠️  冗余数据: {len(orphan_comps)} 个指数有成分股但无行情 (可能是代码不匹配)")
            print(f"      示例: {list(orphan_comps)[:5]}...")

        # =========================================================================
        # 3. 质量检查 (Empty Check)
        # =========================================================================
        empty_docs = list(db["index_components"].find(
            {"category": cat, "components": {"$size": 0}},
            {"index_symbol": 1, "index_name": 1}
        ))

        if empty_docs:
            print(f"   ⚠️  空壳指数警告: {len(empty_docs)} 个指数成分股列表为空!")
            print(f"      (这通常是因为该板块已停止更新或接口无数据)")
            sample = [f"{d.get('index_name', 'Unknown')}({d['index_symbol']})" for d in empty_docs[:3]]
            print(f"      示例: {sample}...")
        else:
            print("   ✅ 数据质量良好 (无空壳指数)")

        print("\n")

    # =========================================================================
    # 4. 元数据 (index_info) 概览
    # =========================================================================
    print("📚 [index_info] 元数据概览:")
    info_count = db["index_info"].count_documents({})
    print(f"   - 总记录数: {info_count}")
    if info_count > 0:
        pipeline = [{"$group": {"_id": "$category", "count": {"$sum": 1}}}]
        for res in db["index_info"].aggregate(pipeline):
            cat = res["_id"] or "Unknown"
            print(f"   - {cat:<10}: {res['count']}")
    else:
        print("   ⚠️  警告: index_info 表为空！")
        print("      (这不影响回测，但建议运行脚本 11 补充概念元数据)")


if __name__ == "__main__":
    run_check()