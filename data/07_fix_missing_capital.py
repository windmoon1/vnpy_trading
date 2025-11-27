"""
Script 07: Download Share Capital History (Final Repair Version)
----------------------------------------------------------------
功能: 下载/补全 A股股本变动数据
改进:
1. 利用 stock_info 中的 status 字段识别退市股。
2. 拒绝静默失败: 对非退市股的异常进行显式报错。
3. 智能补全: 自动识别缺失数据进行增量下载。
"""

import akshare as ak
import pandas as pd
import time
import random
import traceback
from datetime import datetime
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne

# ==========================================
# 配置
# ==========================================
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
COLLECTION_NAME = "share_capital"

def get_db():
    return MongoClient(MONGO_HOST, MONGO_PORT)[DB_NAME]

def get_stock_status_map(db):
    """
    从 stock_info 获取股票状态字典
    Returns: {symbol: status, ...} (e.g., {'000005': 'DELISTED'})
    """
    print("🔍 读取股票状态元数据 (stock_info)...")
    cursor = db["stock_info"].find({}, {"symbol": 1, "status": 1})
    status_map = {doc["symbol"]: doc.get("status", "ACTIVE") for doc in cursor}
    print(f"📖 已加载 {len(status_map)} 条股票状态信息")
    return status_map

def get_todo_list(db):
    """计算待处理列表 (全集 - 已有)"""
    print("🔍 扫描任务队列...")

    # 1. 全目标
    all_cursor = db["stock_info"].find({}, {"symbol": 1})
    all_symbols = set([doc["symbol"] for doc in all_cursor])
    if not all_symbols:
        # Fallback
        all_symbols = set(db["bar_daily"].distinct("symbol"))

    # 2. 已完成
    exist_symbols = set(db[COLLECTION_NAME].distinct("symbol"))

    # 3. 差集
    missing = sorted(list(all_symbols - exist_symbols))

    print("=" * 40)
    print(f"📊 目标总数: {len(all_symbols)}")
    print(f"✅ 已库已存: {len(exist_symbols)}")
    print(f"🚑 待修复数: {len(missing)}")
    print("=" * 40)

    return missing

def download_worker(symbol: str, status: str, db):
    """
    执行下载，根据 status 决定报错策略
    """
    try:
        current_date = datetime.now().strftime("%Y%m%d")

        # 1. 接口调用 (指定 1990 以获取全历史)
        df = ak.stock_share_change_cninfo(
            symbol=symbol,
            start_date="19900101",
            end_date=current_date
        )

        if df is None or df.empty:
            if status == 'DELISTED':
                print(f"⚠️ {symbol} [退市]: 源数据为空 (预期内)")
            else:
                print(f"❌ {symbol} [在市]: 源数据为空 (需检查)")
            return

        # 2. 字段校验
        rename_map = {
            '变动日期': 'date',
            '总股本': 'total_shares',
            '已流通股份': 'float_shares',
            '变动原因': 'change_reason'
        }

        if not set(rename_map.keys()).issubset(df.columns):
            # 如果列名不对，打印出来看一眼
            cols = df.columns.tolist()
            msg = f"❌ {symbol} [{status}]: 列名不匹配 {cols}"
            if status == 'DELISTED':
                print(f"⚠️ {symbol} [退市]: 数据结构已过时 (跳过)")
                return
            else:
                print(msg)
                return # 在市股票结构不对也要跳过防止脏数据，但已报警

        df = df.rename(columns=rename_map)
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        # 3. 数据清洗 (单位: 万股 -> 股)
        def clean_shares(val):
            if pd.isna(val) or val == '': return 0.0
            try:
                return float(val) * 10000
            except:
                return 0.0

        df['total_shares'] = df['total_shares'].apply(clean_shares)
        df['float_shares'] = df['float_shares'].apply(clean_shares)

        # 4. 入库
        requests = []
        for _, row in df.iterrows():
            filter_doc = {"symbol": symbol, "date": row["date"]}
            update_doc = {
                "$set": {
                    "total_shares": row["total_shares"],
                    "float_shares": row["float_shares"],
                    "change_reason": row["change_reason"],
                    "updated_at": datetime.now()
                }
            }
            requests.append(UpdateOne(filter_doc, update_doc, upsert=True))

        if requests:
            db[COLLECTION_NAME].bulk_write(requests)
            # 成功时不打印，保持清爽

    except KeyError as e:
        # 针对 "公告日期" 缺失的特定错误
        if '公告日期' in str(e):
            if status == 'DELISTED':
                # 退市股票缺字段是常态，打印简短警告
                print(f"⚠️ {symbol} [退市]: 缺少公告日期字段 (Skip)")
                pass
            else:
                # 在市股票缺字段，必须报红
                print(f"🛑 {symbol} [在市]: 异常 KeyError '公告日期' - 请手动检查源网站")
        else:
            print(f"❌ {symbol} KeyError: {e}")

    except Exception as e:
        # 其他未知错误 (网络超时等)
        print(f"💥 {symbol} [{status}] Unhandled Error: {e}")
        # 如果是在市股票，打印堆栈以便调试
        if status != 'DELISTED':
            traceback.print_exc()

def run():
    print("🚀 启动 [股本数据修复器 Final]...")
    db = get_db()

    # 1. 获取状态表
    status_map = get_stock_status_map(db)

    # 2. 获取任务
    todos = get_todo_list(db)

    if not todos:
        print("🎉 所有股票数据已存在！")
        return

    # 3. 执行
    pbar = tqdm(todos)
    for symbol in pbar:
        status = status_map.get(symbol, "UNKNOWN")
        pbar.set_description(f"[{status}] {symbol}")

        download_worker(symbol, status, db)

        # 动态延时: 退市股若失败通常很快，正常下载需要延时防封
        time.sleep(random.uniform(0.5, 1.0))

    print("\n✅ 修复流程结束。建议再次运行 verify_share_capital.py 查看最终覆盖率。")

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("\n🛑 用户终止")