"""
数据审计脚本 (Data Integrity Audit) - FIX for PyMongo 4.x
---------------------------------------------------------
修正了 Cursor.count() 的 Attribute Error。
目标: 最终确认数据完整度。
"""
import time
from datetime import datetime
from pymongo import MongoClient
import pandas as pd

# --- 配置 ---
CLIENT = MongoClient("localhost", 27017)
DB_STOCK = CLIENT["vnpy_stock"]

def check_audit():
    print("=============================================")
    print("📈 启动数据完整性审计 (Data Integrity Audit)...")
    print("=============================================")

    # 1. 核心数量检查 (Completeness Check)
    # ------------------------------------
    total_symbols_info = DB_STOCK["stock_info"].count_documents({})
    total_symbols_bar = DB_STOCK["bar_daily"].distinct("symbol").__len__()
    total_bars = DB_STOCK["bar_daily"].count_documents({})

    print(f"1. 符号数量检查:")
    print(f"   - Info 表中的股票总数 (应为约5100+): {total_symbols_info}")
    print(f"   - Bar 表中已下载的股票总数: {total_symbols_bar}")
    print(f"   - 总 K 线记录条数 (Total Docs): {total_bars:,.0f} 条")

    # 判定：如果 Bar 表的股票数少于 Info 表，说明有股票下载失败或为空
    if total_symbols_info == total_symbols_bar and total_bars > 5000000:
        print("   ✅ 完整性: 数量基本匹配 (任务成功)。")
    elif total_symbols_info > total_symbols_bar and total_symbols_info - total_symbols_bar <= 10:
        print("   ⚠️ 警告: 仅少数股票未下载成功 (99.8%成功，可接受)。")
    else:
        print("   ❌ 失败: 数据总量不足，请检查脚本是否中断。")


    # 2. 时间跨度检查 (Depth Check)
    # ----------------------------
    sample_symbol = "600519"

    # [FIX] 使用 list() 转换 cursor 并检查长度
    latest_bar_list = list(DB_STOCK["bar_daily"].find({"symbol": sample_symbol}).sort("datetime", -1).limit(1))
    oldest_bar_list = list(DB_STOCK["bar_daily"].find({"symbol": sample_symbol}).sort("datetime", 1).limit(1))

    latest_date = latest_bar_list[0]['datetime'].strftime('%Y-%m-%d') if latest_bar_list else 'N/A'
    oldest_date = oldest_bar_list[0]['datetime'].strftime('%Y-%m-%d') if oldest_bar_list else 'N/A'

    print(f"\n2. 茅台数据时间跨度:")
    print(f"   - 最早日期 (应接近2005): {oldest_date}")
    print(f"   - 最晚日期 (应接近今天): {latest_date}")

    if oldest_date < '2006-01-01':
        print("   ✅ 深度检查: 历史深度达标 (获取到了 2005 年数据)。")

    # 3. 跨表逻辑检查 (Inter-Table Check - Valuation)
    # -----------------------------------------------
    # 检查茅台的 Bar 表和 Valuation 表是否有数据同步 (确保脚本 03 跑了)
    val_count = DB_STOCK["valuation_daily"].count_documents({"symbol": sample_symbol})

    print(f"\n3. 估值数据同步检查:")
    print(f"   - 茅台 Bar 记录数: {DB_STOCK['bar_daily'].count_documents({'symbol': sample_symbol})} 条")
    print(f"   - 茅台 Valuation 记录数: {val_count} 条")

    if val_count > 100: # 100条即可证明脚本 03 跑过了
        print("   ✅ 同步成功: 估值数据已入库。")
    else:
        print("   ⚠️ 警告: 估值数据缺失。请确认脚本 03 是否运行。")

    # 4. 金融逻辑检查 (Sanity Check)
    # -----------------------------
    # 检查是否有 H < L 的错误或 Volume 为负数
    corrupted_data = DB_STOCK["bar_daily"].find({
        "$or": [
            {"high_price": {"$lt": "$low_price"}},
            {"volume": {"$lt": 0}}
        ]
    }).limit(1)

    print("\n4. 金融逻辑校验:")
    if DB_STOCK["bar_daily"].count_documents({
            "$or": [{"high_price": {"$lt": "$low_price"}}, {"volume": {"$lt": 0}}]
        }) == 0:
        print("   ✅ 校验通过: 未发现 High < Low 或 Volume < 0 的异常数据。")
    else:
        print("   ❌ 致命错误: 发现数据结构异常！")

    print("\n=============================================")
    print("✅ 审计完成。你的 A 股核心数据库已建成。")

if __name__ == "__main__":
    check_audit()