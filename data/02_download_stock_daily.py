"""
脚本 02 (V3.0): A股日线下载器 (增量修复版) 需要每天运行
-----------------------------------------------
更新日志:
- [FEAT] 切换为增量模式：查询 bar_daily 最新日期，只下载新数据。
- [FEAT] 股票列表源切换：优先从本地 stock_info 表中获取股票列表。
- [FIX] 修复代码前缀逻辑。
"""
import os
import time
import random
from datetime import datetime, timedelta # ✅ 新增导入 timedelta
from tqdm import tqdm
from pymongo import UpdateOne, MongoClient
from vnpy.trader.constant import Exchange, Interval
import akshare as ak
import pandas as pd
import requests

# --- 🛡️ 直连补丁 ---
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['all_proxy'] = ''
os.environ['NO_PROXY'] = '*'

# --- 配置 ---
START_DATE = "20050101" # 首次下载的起始日期
ADJUST = "" # Raw Data

CLIENT = MongoClient("localhost", 27017)
col_bar = CLIENT["vnpy_stock"]["bar_daily"]
col_info = CLIENT["vnpy_stock"]["stock_info"] # 本地股票元数据表

def get_local_stock_list():
    """
    [NEW] 从本地 stock_info 表中获取所有 A股/北交所 股票列表。
    返回: List of {"symbol": "...", "exchange": "...", "name": "..."}
    """
    # 查找 category 为 STOCK_A 或 STOCK_BJ 的股票
    # 排除 STOCK_B，因为它不是我们交易的品种
    cursor = col_info.find(
        {"category": {"$in": ["STOCK_A", "STOCK_BJ"]}},
        {"symbol": 1, "name": 1, "exchange": 1} # 只需要这几个字段
    )

    tasks = []
    for doc in cursor:
        tasks.append((doc['symbol'], doc['name'], doc['exchange']))

    if not tasks:
        print("⚠️ 本地 stock_info 表中无 A股/北交所 数据。请先运行 Script 02 初始版本。")

    return tasks

def get_incremental_start_date(symbol: str) -> str:
    """
    [NEW] 查询 bar_daily 表中某个股票的最新日期，返回 YYYYMMDD 格式的下一天。
    """
    doc = col_bar.find_one(
        {"symbol": symbol},
        sort=[("datetime", -1)],
        projection={"datetime": 1}
    )

    if doc and 'datetime' in doc:
        # 获取最新日期并加 1 天
        latest_dt = doc['datetime']
        if isinstance(latest_dt, str):
             # 确保能处理 MongoDB 存储的 ISODate 字符串
             latest_dt = datetime.fromisoformat(latest_dt.replace('Z', '+00:00'))

        # 排除时区信息，并加一天
        latest_dt = latest_dt.replace(tzinfo=None) + timedelta(days=1)
        return latest_dt.strftime("%Y%m%d")

    # 如果没有找到任何记录，返回全局 START_DATE
    return START_DATE

def save_bars_sina_full(symbol, exchange, df):
    # ... (此函数内容保持不变)
    if df.empty: return
    updates = []
    for _, row in df.iterrows():
        try:
            # 数据清洗与计算
            dt = datetime.combine(row['date'], datetime.min.time())
            vol_share = float(row['volume'])
            amount_rmb = float(row['amount'])
            outstanding = float(row['outstanding_share'])
            t_rate = (vol_share / outstanding) * 100 if outstanding > 0 else 0.0

            doc = {
                "symbol": symbol,
                "exchange": exchange, # 直接使用传入的 exchange value
                "interval": Interval.DAILY.value,
                "datetime": dt,
                "open_price": float(row['open']),
                "high_price": float(row['high']),
                "low_price": float(row['low']),
                "close_price": float(row['close']),
                "volume": vol_share,
                "turnover": amount_rmb,
                "turnover_rate": t_rate,
                "outstanding_share": outstanding,
                "gateway_name": "AKSHARE_SINA"
            }
            # 过滤器确保唯一性
            filter_doc = {"symbol": symbol, "exchange": exchange, "interval": Interval.DAILY.value, "datetime": dt}

            # Upsert=True: 存在则更新(补全字段)，不存在则插入
            updates.append(UpdateOne(filter_doc, {"$set": doc}, upsert=True))
        except: continue

    if updates:
        col_bar.bulk_write(updates)
        return len(updates)
    return 0


def get_sina_symbol(symbol, exchange_value):
    """根据交易所推断新浪查询前缀"""
    # 注意：这里接收的是 exchange.value (如 'SZSE')
    if exchange_value == Exchange.SSE.value: return f"sh{symbol}"
    if exchange_value == Exchange.SZSE.value: return f"sz{symbol}"
    if exchange_value == Exchange.BSE.value: return f"bj{symbol}" # 北交所修正为 bj 前缀
    return f"sz{symbol}"

def run():
    print("🚀 启动 [全市场日线] 增量下载任务 (V3.0)...")

    # 1. 获取本地股票列表
    tasks = get_local_stock_list()
    if not tasks: return

    print(f"📊 待处理任务: {len(tasks)} 只")

    pbar = tqdm(tasks, unit="stock")
    total_new_bars = 0
    today_ymd = datetime.now().strftime("%Y%m%d")

    for symbol, name, exchange_value in pbar:
        # 1. 确定下载的起始日期 (增量逻辑核心)
        adjusted_start_date = get_incremental_start_date(symbol)

        # 如果最新日期已经到今天，跳过
        if adjusted_start_date == today_ymd:
             continue

        pbar.set_description(f"Processing {name} (Start: {adjusted_start_date})")

        # 2. 构造查询参数
        sina_symbol = get_sina_symbol(symbol, exchange_value)

        try:
            # 3. 下载数据
            df = ak.stock_zh_a_daily(
                symbol=sina_symbol,
                start_date=adjusted_start_date, # 使用增量起始日期
                end_date=today_ymd,
                adjust=ADJUST
            )

            # 4. 入库
            new_bars = save_bars_sina_full(symbol, exchange_value, df)
            total_new_bars += new_bars

        except requests.exceptions.ConnectionError:
            pbar.write(f"\n🛑 网络中断 {name}，稍后重试。")
            time.sleep(5)
        except Exception as e:
            # 忽略极个别不支持的股票，但打印出来方便后续处理
            pbar.write(f"❌ 致命错误 {name} ({symbol}): {e}")

        # 适当休眠
        time.sleep(0.05)

    print(f"\n✨ 增量下载完成！共新增/更新 {total_new_bars} 条 K 线数据。")

if __name__ == "__main__":
    run()