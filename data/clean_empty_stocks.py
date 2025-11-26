"""
脚本 10: 幽灵股票清理器 (Cleaner)
------------------------------------------------
目标: 彻底删除那些标记为 DELISTED 但在 bar_daily 中没有任何 K 线数据的股票。
范围: stock_info (元数据), adjust_factor (因子), bar_daily (K线)
"""
from pymongo import MongoClient

# --- 配置 ---
DB_NAME = "vnpy_stock"
CLIENT = MongoClient("localhost", 27017)
db = CLIENT[DB_NAME]
col_info = db["stock_info"]
col_bar = db["bar_daily"]
col_adj = db["adjust_factor"]


def run_cleaner():
    print("🚀 启动 [幽灵股票清理器]...")

    # 1. 查找目标
    # 逻辑: 在 stock_info 里是 DELISTED，但在 bar_daily 里一条记录都没有
    cursor = col_info.find({"status": "DELISTED"})
    targets = []

    print("   🔍 扫描空壳股票...")
    for doc in cursor:
        symbol = doc['symbol']
        name = doc.get('name', 'Unknown')

        # 核心判断: K线数量为 0
        count = col_bar.count_documents({"symbol": symbol}, limit=1)
        if count == 0:
            targets.append(doc)
            print(f"      Found: {symbol} ({name})")

    print(f"   📋 锁定待删除目标: {len(targets)} 只")

    if not targets:
        print("   ✨ 数据库很干净，无需清理。")
        return

    # 2. 执行删除
    # 二次确认 (虽然脚本通常自动化，但这里稍微停顿一下显得安全)
    confirm = input(f"\n⚠️ 确定要从数据库中永久删除这 {len(targets)} 只股票吗? (y/n): ")
    if confirm.lower() != 'y':
        print("   🚫 操作已取消。")
        return

    deleted_count = 0
    for doc in targets:
        symbol = doc['symbol']

        # A. 删除元数据
        col_info.delete_one({"symbol": symbol})

        # B. 删除可能存在的残留因子
        col_adj.delete_many({"symbol": symbol})

        # C. 删除 K线 (虽然查出来是0，但为了保险还是执行一下)
        col_bar.delete_many({"symbol": symbol})

        deleted_count += 1
        print(f"   🗑️ 已删除: {symbol}")

    print(f"\n✨ 清理完成! 共移除 {deleted_count} 只无效股票。")


if __name__ == "__main__":
    run_cleaner()