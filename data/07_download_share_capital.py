"""
脚本 07 (V2.0): 股本数据全能下载器 (Download & Fuse)
-------------------------------------------------------
功能:
1. [Download] 从 AKShare 下载最新的股本变动记录 (来源: 巨潮资讯).
2. [Fuse] 自动去 bar_daily (日线表) 查找对应的 A股流通股本 (outstanding_share).
3. [Clean] 将查到的准确流通股本回写到 share_capital 表的 float_shares_a 字段.

前置条件: 建议先运行 脚本 02 (下载日线)，以保证有最新的行情数据可供缝合。
"""
import time
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"

# 连接数据库
CLIENT = MongoClient(MONGO_HOST, MONGO_PORT)
DB = CLIENT[DB_NAME]
COL_CAPITAL = DB["share_capital"]
COL_BARS = DB["bar_daily"]
COL_INFO = DB["stock_info"]

def normalize_date(date_obj):
    """通用日期清洗工具"""
    if isinstance(date_obj, str):
        try:
            if "T" in date_obj:
                return datetime.strptime(date_obj.split("T")[0], "%Y-%m-%d")
            return datetime.strptime(date_obj, "%Y-%m-%d")
        except:
            return None
    elif isinstance(date_obj, datetime):
        return date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
    elif hasattr(date_obj, "date"):
        return date_obj.to_pydatetime().replace(hour=0, minute=0, second=0, microsecond=0)
    return None

def get_stock_list():
    """获取任务列表"""
    cursor = COL_INFO.find({"category": {"$in": ["STOCK_A", "STOCK_BJ"]}}, {"symbol": 1, "name": 1})
    return list(cursor)

def download_capital_cninfo(symbol):
    """Step 1: 下载 CNINFO 原始股本变动数据"""
    try:
        # 注意：AKShare 此接口返回该股票历史所有变动，我们需要做增量过滤
        df = ak.stock_share_changes_cninfo(symbol=symbol)
        if df.empty: return 0

        updates = []
        for _, row in df.iterrows():
            date_str = str(row['date'])
            date_obj = normalize_date(date_str)
            if not date_obj: continue

            # 原始数据 (注意：这里的 float_shares 包含了 H 股，是“全球流通股本”)
            total_shares = float(row['总股本'])
            float_shares_global = float(row['流通A股']) if '流通A股' in row else float(row.get('流通股本', 0))
            reason = row.get('变动原因', '')

            # 构造基础文档
            doc = {
                "symbol": symbol,
                "date": date_obj,
                "total_shares": total_shares,
                "float_shares": float_shares_global, # 存下来作为参考，但不用于核心计算
                "change_reason": reason,
                "update_at": datetime.now()
            }

            # Upsert: 按照 symbol + date 唯一索引更新
            filter_doc = {"symbol": symbol, "date": date_obj}
            updates.append(UpdateOne(filter_doc, {"$set": doc}, upsert=True))

        if updates:
            res = COL_CAPITAL.bulk_write(updates, ordered=False)
            return res.upserted_count + res.modified_count
        return 0

    except Exception as e:
        # 某些股票可能没有数据，忽略报错
        return 0

def fuse_float_shares(symbol):
    """Step 2: 缝合逻辑 - 从 bar_daily 补全 float_shares_a"""
    # 只查找该股票缺失 float_shares_a 的记录
    pending_cursor = COL_CAPITAL.find({
        "symbol": symbol,
        "float_shares_a": {"$exists": False}
    })

    updates = []

    for cap_doc in pending_cursor:
        raw_date = cap_doc.get("date")
        target_date = normalize_date(raw_date)
        if not target_date: continue

        # 查找 bar_daily (逻辑同 Script 18)
        # 找 >= 变动日 的最近一条有 outstanding_share 的 K 线
        bar_doc = COL_BARS.find_one(
            {
                "symbol": symbol,
                "datetime": {"$gte": target_date},
                "outstanding_share": {"$exists": True}
            },
            sort=[("datetime", ASCENDING)]
        )

        if bar_doc:
            bar_date = normalize_date(bar_doc["datetime"])
            days_diff = (bar_date - target_date).days

            # 允许 10 天内的偏差（应对停牌或非交易日）
            if 0 <= days_diff <= 10:
                real_float_a = bar_doc["outstanding_share"]
                updates.append(
                    UpdateOne(
                        {"_id": cap_doc["_id"]},
                        {"$set": {"float_shares_a": real_float_a}}
                    )
                )

    if updates:
        res = COL_CAPITAL.bulk_write(updates, ordered=False)
        return res.modified_count
    return 0

def run():
    print("🚀 启动 [股本数据全能下载器 V2.0] (Download + Fuse)...")

    tasks = get_stock_list()
    print(f"📊 待处理股票: {len(tasks)} 只")

    pbar = tqdm(tasks, unit="stock")

    total_downloaded = 0
    total_fused = 0

    for task in pbar:
        symbol = task['symbol']
        name = task['name']

        pbar.set_description(f"Processing {name}")

        # 1. 下载基础数据
        d_count = download_capital_cninfo(symbol)

        # 2. 执行缝合 (无论是否下载了新数据，都检查一遍有没有漏补的)
        f_count = fuse_float_shares(symbol)

        total_downloaded += d_count
        total_fused += f_count

        # 避免请求过于频繁
        time.sleep(0.05)

    print(f"\n✨ 任务完成 Report:")
    print(f"   - 新增/更新变动记录: {total_downloaded}")
    print(f"   - 成功缝合A股流通值: {total_fused}")
    print("✅ 数据库状态: share_capital 表已包含 float_shares_a 字段。")

if __name__ == "__main__":
    run()