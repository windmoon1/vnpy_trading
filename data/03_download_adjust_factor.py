"""
脚本 03 (V2.0): 复权因子下载器 (增量修复版) 需要每天运行
===========================================
目标: 每日增量更新所有股票的前复权因子（qfq-factor）。
策略:
  1. 优先从本地 stock_info 读取列表。
  2. 查询 adjust_factor 表中的最新日期。
  3. 从最新日期安全回溯两年（避免遗漏因子变动），并增量下载到今天。
-------------------------------------------
"""
import time
import random
from datetime import datetime, timedelta # ✅ 新增导入 timedelta
from tqdm import tqdm
from pymongo import UpdateOne, MongoClient
from vnpy.trader.constant import Exchange
import akshare as ak
import pandas as pd
import requests
import re


# --- 配置 ---
ADJUST = "qfq-factor" # 核心参数：请求前复权乘数因子
START_DATE = "19900101" # 首次下载的起始日期

# --- 数据库连接 ---
CLIENT = MongoClient("localhost", 27017)
db = CLIENT["vnpy_stock"]
col_adj = db["adjust_factor"] # 目标集合
col_info = db["stock_info"] # 基础信息集合

def get_symbols():
    """从本地数据库读取所有股票代码 (仅限 A股/北交所)"""
    # 查找 category 为 STOCK_A 或 STOCK_BJ 的股票
    cursor = col_info.find(
        {"category": {"$in": ["STOCK_A", "STOCK_BJ", "UNKNOWN_A"]}},
        {"symbol": 1, "exchange": 1}
    )
    # 返回 List[Tuple(symbol, exchange_value)]
    return [(doc['symbol'], doc.get('exchange')) for doc in cursor]

def get_sina_symbol(symbol, exchange_value):
    """根据交易所推断新浪查询前缀"""
    if exchange_value == Exchange.SSE.value: return f"sh{symbol}"
    if exchange_value == Exchange.SZSE.value: return f"sz{symbol}"
    if exchange_value == Exchange.BSE.value: return f"bj{symbol}" # 北交所修正为 bj 前缀
    return f"sz{symbol}"

def get_incremental_start_date_factor(symbol: str) -> datetime:
    """
    [NEW] 查询 adjust_factor 表中某个股票的最新因子日期，
    返回需要开始下载的日期对象 (最新日期 - 2 年的安全回溯期)。
    """
    doc = col_adj.find_one(
        {"symbol": symbol},
        sort=[("date", -1)],
        projection={"date": 1}
    )

    if doc and 'date' in doc:
        latest_dt = doc['date'].replace(tzinfo=None)
        # 安全回溯 2 年，避免因子变动导致缺失 (API 返回的是全量因子，但这里优化查询范围)
        return latest_dt - timedelta(days=365 * 2)

    # 如果没有找到任何记录，返回全局 START_DATE
    return datetime.strptime(START_DATE, "%Y%m%d")


def download_and_save_factor(symbol, exchange_value, pbar, start_date_factor):
    """核心下载与写入逻辑 (使用增量日期)"""
    sina_symbol = get_sina_symbol(symbol, exchange_value)

    try:
        # 核心调用: 获取因子数据 (使用传入的 start_date_factor)
        df = ak.stock_zh_a_daily(
            symbol=sina_symbol,
            start_date=start_date_factor, # <-- 使用增量起始日期
            end_date=datetime.now().strftime("%Y%m%d"),
            adjust=ADJUST
        )

        if df.empty or 'qfq_factor' not in df.columns:
            pbar.write(f"⚠️ {symbol}: 接口返回空或缺少 qfq_factor 字段。")
            return 0

        updates = []
        for _, row in df.iterrows():
            try:
                # 🚨 日期解析: 兼容 datetime.date 对象和 ISODate 字符串
                if isinstance(row['date'], datetime):
                    dt = row['date'].replace(tzinfo=None) # 去除时区信息
                elif isinstance(row['date'], pd.Timestamp):
                    dt = row['date'].to_pydatetime().replace(tzinfo=None)
                else:
                    # 假定为 YYYY-MM-DD 格式的字符串
                    dt_str_clean = str(row['date']).split()[0]
                    dt = datetime.strptime(dt_str_clean, "%Y-%m-%d")

                # 构造文档 (Upsert 保证不重复)
                updates.append(UpdateOne(
                    {"symbol": symbol, "date": dt},
                    {"$set": {"factor": float(row['qfq_factor']), "source": "SINA_FACTOR"}},
                    upsert=True
                ))
            except Exception:
                continue

        if updates:
            result = col_adj.bulk_write(updates)
            pbar.write(f"✅ {symbol}: 成功写入/更新 {result.upserted_count + result.modified_count} 条因子记录。")
            return len(updates)
        return 0

    except requests.exceptions.ConnectionError:
        pbar.write(f"❌ {symbol}: 网络连接错误，等待重试。")
        return 0
    except Exception as e:
        # 捕获其他致命错误，如 Key error 或 AkShare 内部错误
        pbar.write(f"❌ {symbol}: 致命错误 ({e.__class__.__name__})，跳过。")
        return 0


def run_factor_download():
    print("🚀 启动 [复权因子] 增量下载任务 (V2.0)...")
    tasks = get_symbols()

    # 检查增量任务是否已经完成到今天
    # 优化：不再使用 done_factor 列表，而是通过日期判断

    print(f"✅ 共有 {len(tasks)} 只股票，准备进行增量更新。")

    pbar = tqdm(tasks, unit="stock")

    for symbol, exchange_value in pbar:
        # 1. 确定增量起始日期 (安全回溯两年，或者从头开始)
        incremental_dt = get_incremental_start_date_factor(symbol)

        # 如果最新日期已经到今天/昨天，跳过
        yesterday_dt_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        if incremental_dt.strftime("%Y%m%d") > yesterday_dt_str:
            continue

        pbar.set_description(f"Processing {symbol} (Start: {incremental_dt.strftime('%Y%m%d')})")

        # 2. 调用核心下载逻辑 (含重试)
        for attempt in range(3):
            count = download_and_save_factor(symbol, exchange_value, pbar, incremental_dt.strftime("%Y%m%d"))
            if count > 0:
                break
            elif attempt < 2:
                time.sleep(1)

        time.sleep(random.uniform(0.1, 0.3))

    print("\n✨ 复权因子下载完成！")

if __name__ == "__main__":
    run_factor_download()