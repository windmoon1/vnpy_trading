"""
脚本 18 (V2 Debug版): 股本数据清洗 - 强力同步版
--------------------------------------------------------------
改进:
1. 解决 String vs ISODate 类型不匹配问题。
2. 增加详细日志，显示匹配过程。
3. 容错查找：如果当天是周末，自动向后找最近的交易日。
"""
from datetime import datetime, timedelta
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne, ASCENDING

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"

def normalize_date(date_obj):
    """核心修复：将字符串或时间戳统一转为 datetime (00:00:00)"""
    if isinstance(date_obj, str):
        try:
            # 处理 '2006-10-27' 或 '2006-10-27T00:00:00'
            if "T" in date_obj:
                return datetime.strptime(date_obj.split("T")[0], "%Y-%m-%d")
            return datetime.strptime(date_obj, "%Y-%m-%d")
        except:
            return None
    elif isinstance(date_obj, datetime):
        return date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
    elif hasattr(date_obj, "date"): # pandas Timestamp
        return date_obj.to_pydatetime().replace(hour=0, minute=0, second=0, microsecond=0)
    return None

def sync_float_shares():
    print("🚀 启动 [V2 强力同步] 任务 (解决类型不匹配)...")

    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[DB_NAME]
    col_capital = db["share_capital"]
    col_bars = db["bar_daily"]

    # 获取所有股票代码
    symbols = col_capital.distinct("symbol")
    # symbols = ["601398"] # 如果想先测试工行，可以取消注释这行

    print(f"📊 扫描股票数量: {len(symbols)}")

    total_matched = 0
    total_skipped = 0
    debug_print_count = 0

    pbar = tqdm(symbols, unit="stock")

    for symbol in pbar:
        # 获取该股票所有的股本变动记录
        cursor_cap = col_capital.find({"symbol": symbol}).sort("date", ASCENDING)

        bulk_updates = []

        for cap_doc in cursor_cap:
            raw_date = cap_doc.get("date")

            # [FIX] 强制转换类型
            target_date = normalize_date(raw_date)

            if not target_date:
                continue

            # 查找 bar_daily 中 datetime >= target_date 的第一条记录
            bar_doc = col_bars.find_one(
                {
                    "symbol": symbol,
                    "datetime": {"$gte": target_date}, # 向后查找最近一天
                    "outstanding_share": {"$exists": True} # 必须有清洗后的股本
                },
                sort=[("datetime", ASCENDING)]
            )

            # 校验日期偏差 (不超过10天)
            if bar_doc:
                bar_date = bar_doc["datetime"]
                bar_date_norm = normalize_date(bar_date)

                days_diff = (bar_date_norm - target_date).days

                if 0 <= days_diff <= 10:
                    real_float_a = bar_doc["outstanding_share"]

                    bulk_updates.append(
                        UpdateOne(
                            {"_id": cap_doc["_id"]},
                            {"$set": {"float_shares_a": real_float_a}} # 写入新字段
                        )
                    )

                    # 打印前 5 条成功的日志，让你看到它在工作
                    if debug_print_count < 5:
                        pbar.write(f"✅ [MATCH] {symbol} 原始:{raw_date} -> 匹配日:{bar_date_norm.date()} | A股流通:{real_float_a/1e8:.2f}亿")
                        debug_print_count += 1
                else:
                    # 找到的日子太久远了（比如停牌了一个月）
                    pass
            else:
                pass

        if bulk_updates:
            res = col_capital.bulk_write(bulk_updates)
            total_matched += res.modified_count
        else:
            total_skipped += 1

    print(f"\n✨ 同步结束 Report:")
    print(f"   - 成功更新记录数: {total_matched}")
    print(f"   - 无更新股票数: {total_skipped}")
    print(f"   - 现在可以去运行脚本 08 验证结果了！")

if __name__ == "__main__":
    sync_float_shares()