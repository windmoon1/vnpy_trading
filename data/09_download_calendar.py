"""
Script 09: Download Trading Calendar
------------------------------------
功能: 下载 A股 历史交易日历
来源: 新浪财经 (tool_trade_date_hist_sina)
存储: vnpy_master.trading_calendar
"""

import akshare as ak
import pandas as pd
from datetime import datetime
from pymongo import MongoClient, UpdateOne

# ==========================================
# 配置
# ==========================================
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_master"
COLLECTION_NAME = "trading_calendar"


def run():
    print("🚀 启动 [交易日历下载器]...")

    client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = client[DB_NAME]
    col = db[COLLECTION_NAME]

    # 1. 获取数据
    print("   📡 请求新浪财经接口...")
    try:
        df = ak.tool_trade_date_hist_sina()
        # 返回列: ['trade_date']
    except Exception as e:
        print(f"❌ 接口请求失败: {e}")
        return

    if df is None or df.empty:
        print("❌ 返回数据为空")
        return

    print(f"   ✅ 获取到 {len(df)} 个交易日")

    # 2. 转换与存储
    # A股交易所通常放假安排一致，我们统一标记为 SSE/SZSE/BSE 通用
    exchanges = ["SSE", "SZSE", "BSE"]

    requests = []
    for _, row in df.iterrows():
        date_obj = row['trade_date']  # 已经是 datetime.date 对象
        date_str = date_obj.strftime("%Y-%m-%d")

        for exc in exchanges:
            filter_doc = {
                "exchange": exc,
                "date": date_str
            }
            update_doc = {
                "$set": {
                    "is_trading": True,
                    "updated_at": datetime.now()
                }
            }
            requests.append(UpdateOne(filter_doc, update_doc, upsert=True))

    # 3. 批量写入
    if requests:
        print(f"   💾 正在写入数据库 ({len(requests)} 条记录)...")
        col.bulk_write(requests)
        print("   🎉 交易日历更新完毕！")


if __name__ == "__main__":
    run()