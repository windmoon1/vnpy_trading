"""
Script: 11_download_concepts_em.py
Description: 下载东方财富概念数据 [V7.0 适配版]
Logic:
    1. 使用 NetworkGuard V7 (Cookie注入 + 身份轮替)。
    2. 使用 fix_akshare (慢速翻页补丁)。
    3. 主动控制请求节奏，避免触发风控。
"""

import akshare as ak
import pandas as pd
import datetime
import time
import random
import sys
import os
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne, ASCENDING

# 引入工具包
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from utils.network_guard import NetworkGuard
from utils.fix_akshare import apply_patches

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
SOURCE = "EM"

class Config:
    # 这里的休眠配置现在用于主循环控制
    SLEEP_RANGE = (2.0, 4.0)

client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = client[DB_NAME]

def ensure_indexes():
    db["index_info"].create_index([("symbol", ASCENDING)], unique=True)
    db["index_components"].create_index([("index_symbol", ASCENDING), ("date", ASCENDING)], unique=True)
    db["stock_concepts"].create_index([("symbol", ASCENDING), ("date", ASCENDING)], unique=True)

def format_stock_symbol(raw_code: str) -> str:
    raw_code = str(raw_code).strip()
    if raw_code.startswith(('60', '68')): return f"{raw_code}.SH"
    elif raw_code.startswith(('8', '4')): return f"{raw_code}.BJ"
    else: return f"{raw_code}.SZ"

def get_tasks_from_local_db():
    cursor = db["index_info"].find({"category": "CONCEPT", "source": SOURCE})
    tasks = []
    for doc in cursor:
        tasks.append({"name": doc["name"], "symbol": doc["symbol"]})
    return tasks

def get_completed_tasks_today(date_str):
    cursor = db["index_components"].find({"date": date_str}, {"index_symbol": 1, "_id": 0})
    return set(doc.get("index_symbol") for doc in cursor if doc.get("index_symbol"))

def main():
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    print(f"🚀 Starting Job [Source: {SOURCE}, Date: {today}]")

    # 1. 应用 AKShare 补丁 (慢速翻页 + Warning修复)
    apply_patches()

    # 2. 启动网络卫士 V7 (注入 Cookie)
    # 🔥 修复点: 不传参数，使用无参调用
    NetworkGuard.install()

    ensure_indexes()

    # 3. 获取任务
    all_tasks = get_tasks_from_local_db()
    if not all_tasks:
        print("❌ No tasks found. Please run with SYNC_META=True once (or check index_info).")
        return

    completed = get_completed_tasks_today(today)
    final_tasks = [t for t in all_tasks if t["symbol"] not in completed]
    print(f"📊 Pending: {len(final_tasks)} concepts.")

    pbar = tqdm(final_tasks, desc="Progress")

    for task in pbar:
        b_name = task["name"]
        vt_symbol = task["symbol"]
        # 提取纯代码 BKxxxx
        board_code = vt_symbol.split(".")[0]

        # 每次换概念，主动轮替一次身份 (保持 Cookie 但换 UA/连接)
        NetworkGuard.rotate_identity()

        pbar.set_description(f"Get {b_name}")

        try:
            start_time = time.time()

            # 传入代码，避免 AKShare 内部去查列表
            cons_df = ak.stock_board_concept_cons_em(symbol=board_code)

            elapsed = time.time() - start_time

            # --- 写入逻辑 ---
            component_list = []
            stock_ops = []
            concept_tag = {"code": vt_symbol, "name": b_name, "source": SOURCE}

            if not cons_df.empty:
                for _, row in cons_df.iterrows():
                    code_val = row.get('代码') or row.get('stock_code')
                    if not code_val: continue
                    stock_symbol = format_stock_symbol(str(code_val))
                    component_list.append(stock_symbol)
                    stock_ops.append(UpdateOne(
                        {"symbol": stock_symbol, "date": today},
                        {"$addToSet": {"concepts": concept_tag}},
                        upsert=True
                    ))

            if stock_ops:
                db["stock_concepts"].bulk_write(stock_ops, ordered=False)

            comp_doc = {
                "index_symbol": vt_symbol,
                "date": today,
                "components": component_list,
                "count": len(component_list)
            }
            db["index_components"].update_one(
                {"index_symbol": vt_symbol, "date": today},
                {"$set": comp_doc},
                upsert=True
            )

            # 🔥 智能冷却策略
            # 如果耗时 > 10秒，说明触发了 fix_akshare 里的翻页休眠，我们额外多歇会儿
            if elapsed > 10:
                sleep_t = random.uniform(5.0, 8.0)
            else:
                # 正常单页下载，休息 2-4 秒
                sleep_t = random.uniform(*Config.SLEEP_RANGE)

            time.sleep(sleep_t)

        except Exception as e:
            # 记录失败但不中断
            with open("failed_concepts.txt", "a") as f:
                f.write(f"{b_name}\n")
            # 遇到错误多睡一会
            time.sleep(10)
            continue

    print(f"\n✅ Job Finished.")

if __name__ == "__main__":
    main()