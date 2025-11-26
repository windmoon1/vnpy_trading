"""
脚本 05: 核心指数日线下载器 (Benchmark)
---------------------------------------
目标: 下载核心宽基指数日线数据，作为策略回测的基准 (Benchmark) 和 择时信号源。
数据源: 东方财富 (ak.stock_zh_index_daily_em)
    - 相比新浪接口，东财数据包含 '成交额' 且历史更完整。
    - 涵盖: 上证, 深证, 沪深300, 中证500, 中证1000, 创业板, 科创50, 北证50。
"""

import os
import time
from datetime import datetime
from tqdm import tqdm
from pymongo import UpdateOne, MongoClient
from vnpy.trader.constant import Exchange, Interval
import akshare as ak
import pandas as pd

# --- 🛡️ 直连补丁 ---
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['all_proxy'] = ''
os.environ['NO_PROXY'] = '*'

# --- 配置 ---
# 既然是基准，我们尽量拉取全量历史
START_DATE = "19900101"

# --- 核心指数清单 ---
# 格式: "代码": (交易所枚举, "中文名称")
# 注意: vn.py 的 Exchange 枚举通常用于个股。对于指数，我们约定：
# 上交所指数 -> Exchange.SSE
# 深交所指数 -> Exchange.SZSE
# 北交所指数 -> Exchange.BSE
INDEX_CONFIG = {
    # --- 1. 市场总貌 (The Market) ---
    "000001": (Exchange.SSE, "上证指数"),  # 也就是大盘
    "399001": (Exchange.SZSE, "深证成指"),

    # --- 2. 规模宽基 (Size Benchmarks) ---
    "000300": (Exchange.SSE, "沪深300"),  # 大盘蓝筹 (核心基准)
    "000905": (Exchange.SSE, "中证500"),  # 中盘成长 (IC标的)
    "000852": (Exchange.SSE, "中证1000"),  # 小盘股 (IM标的)
    "399006": (Exchange.SZSE, "创业板指"),  # 成长/科技
    "000688": (Exchange.SSE, "科创50"),  # 硬科技
    "899050": (Exchange.BSE, "北证50"),  # 专精特新

    # --- 3. 策略风格 (Smart Beta) ---
    "000016": (Exchange.SSE, "上证50"),  # 超大盘/金融
    "000985": (Exchange.SSE, "中证全指"),  # 全市场代表
}

# --- 数据库连接 ---
CLIENT = MongoClient("localhost", 27017)
# 存入 vnpy_stock 库中的 index_daily 表
col_index = CLIENT["vnpy_stock"]["index_daily"]
col_info = CLIENT["vnpy_stock"]["index_info"]


def save_index_data(symbol, exchange, name, df):
    if df.empty: return

    updates = []
    for _, row in df.iterrows():
        try:
            # akshare 东财接口返回列名: date, open, close, high, low, volume, amount...
            # 日期处理: 可能是字符串 "2023-01-01"
            dt_str = str(row['date']).split()[0]
            dt = datetime.strptime(dt_str, "%Y-%m-%d")

            doc = {
                "symbol": symbol,
                "exchange": exchange.value,
                "interval": Interval.DAILY.value,
                "datetime": dt,
                "open_price": float(row['open']),
                "high_price": float(row['high']),
                "low_price": float(row['low']),
                "close_price": float(row['close']),
                "volume": float(row['volume']),
                "turnover": float(row['amount']),  # 指数成交额通常很大
                "gateway_name": "AKSHARE_EM_INDEX"
            }

            filter_doc = {
                "symbol": symbol,
                "exchange": exchange.value,
                "interval": Interval.DAILY.value,
                "datetime": dt
            }
            updates.append(UpdateOne(filter_doc, {"$set": doc}, upsert=True))
        except Exception:
            continue

    if updates:
        col_index.bulk_write(updates)

        # 同时更新 Index 基础信息
        col_info.update_one(
            {"symbol": symbol},
            {"$set": {
                "symbol": symbol,
                "exchange": exchange.value,
                "name": name,
                "category": "BENCHMARK"
            }},
            upsert=True
        )


def run():
    print("🚀 启动 [脚本 05] 核心指数下载任务...")

    # 将字典转换为进度条列表
    pbar = tqdm(INDEX_CONFIG.items(), unit="index")

    for symbol, (exchange, name) in pbar:
        pbar.set_description(f"下载 {name}")

        try:
            # 核心接口: 东方财富指数历史数据
            # 该接口返回数据质量较高，且包含历史全量
            df = ak.stock_zh_index_daily_em(symbol=symbol)

            save_index_data(symbol, exchange, name, df)

        except Exception as e:
            pbar.write(f"❌ {name} ({symbol}) 下载失败: {e}")
            time.sleep(1)

        time.sleep(0.5)  # 避免由于请求过快被封IP

    print("\n✨ 核心指数数据注入完成！(Database: vnpy_stock.index_daily)")


if __name__ == "__main__":
    run()