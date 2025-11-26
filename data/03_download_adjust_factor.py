"""
脚本 05: 复权因子下载器 (最终修复版)
=====================================
目标: 下载并存储 K 线数据复权所需的 qfq-factor（前复权因子）。
状态: 修复了 ValueError (日期解析错误)。
-------------------------------------
"""
import os
import time
import random
from datetime import datetime
from tqdm import tqdm
from pymongo import UpdateOne, MongoClient
from vnpy.trader.constant import Exchange
import akshare as ak
import pandas as pd
import requests # 用于捕获网络错误

# --- 🛡️ 直连补丁 ---
# 确保在 import requests 之后，显式清理代理环境变量，保证直连国内数据源。
os.environ['http_proxy'] = ''; os.environ['https_proxy'] = ''; os.environ['all_proxy'] = ''; os.environ['NO_PROXY'] = '*'

# --- 配置 ---
ADJUST = "qfq-factor" # 核心参数：请求前复权乘数因子
START_DATE = "19900101" # 因子数据需要从最早开始拉取

# --- 数据库连接 ---
CLIENT = MongoClient("localhost", 27017)
db = CLIENT["vnpy_stock"]
col_adj = db["adjust_factor"] # 目标集合
col_info = db["stock_info"] # 基础信息集合 (用于获取股票列表)

def identify_exchange(symbol):
    """根据股票代码识别交易所"""
    if symbol.startswith("6"): return Exchange.SSE
    if symbol.startswith("0") or symbol.startswith("3"): return Exchange.SZSE
    return Exchange.SSE

def get_symbols():
    """从本地数据库读取所有股票代码 (基于脚本02的结果)"""
    return list(col_info.distinct("symbol"))

def get_sina_symbol(symbol, exchange):
    """Sina 接口需要 sh/sz 前缀"""
    if exchange == Exchange.SSE: return f"sh{symbol}"
    if exchange == Exchange.SZSE: return f"sz{symbol}"
    return symbol

def download_and_save_factor(symbol, pbar):
    """核心下载与写入逻辑 (含日期修复)"""
    info = col_info.find_one({"symbol": symbol})
    if not info: return 0
    exchange = Exchange(info.get('exchange'))
    sina_symbol = get_sina_symbol(symbol, exchange)

    try:
        # 核心调用: 获取因子数据
        df = ak.stock_zh_a_daily(
            symbol=sina_symbol,
            start_date=START_DATE,
            end_date=datetime.now().strftime("%Y%m%d"),
            adjust=ADJUST
        )

        if df.empty or 'qfq_factor' not in df.columns:
            pbar.write(f"⚠️ {symbol}: 接口返回空或缺少 qfq_factor 字段。")
            return 0

        updates = []
        for _, row in df.iterrows():
            try:
                # 🚨 BUG 修复点: 分离日期和时间，解决 ValueError 🚨
                # str(row['date']) 结果是 "YYYY-MM-DD 00:00:00"，我们只需要日期部分
                dt_str_clean = str(row['date']).split()[0]
                dt = datetime.strptime(dt_str_clean, "%Y-%m-%d")

                # 构造文档 (Upsert 保证不重复)
                updates.append(UpdateOne(
                    {"symbol": symbol, "date": dt},
                    {"$set": {"factor": float(row['qfq_factor']), "source": "SINA_FACTOR"}},
                    upsert=True
                ))
            except Exception:
                # 日期解析失败，跳过该行，不影响整体写入
                continue

        if updates:
            result = col_adj.bulk_write(updates)
            pbar.write(f"✅ {symbol}: 成功写入/更新 {result.upserted_count + result.modified_count} 条因子记录。")
        return len(updates)

    except requests.exceptions.ConnectionError:
        pbar.write(f"❌ {symbol}: 网络连接错误，等待重试。")
        return 0
    except Exception as e:
        # 捕获其他致命错误，如 Key error 或其他 AkShare 内部错误
        pbar.write(f"❌ {symbol}: 致命错误 ({e.__class__.__name__})，跳过。")
        return 0


def run_factor_download():
    print("🚀 启动 [脚本 05] 复权因子下载任务 (最终修复版)...")
    symbols = get_symbols()

    # 检查哪些已完成，跳过
    done_factor = set(col_adj.distinct("symbol"))
    tasks = [s for s in symbols if s not in done_factor]

    print(f"✅ 共有 {len(symbols)} 只股票，本次需更新 {len(tasks)} 只。")

    pbar = tqdm(tasks, unit="stock")

    for symbol in pbar:
        # 增加重试机制 (3次)
        for attempt in range(3):
            count = download_and_save_factor(symbol, pbar)
            if count > 0:
                break # 成功写入数据，跳出重试
            elif attempt < 2:
                time.sleep(1) # 失败则等待1秒再试

        time.sleep(random.uniform(0.1, 0.3)) # 基础延迟

    print("\n✨ 复权因子下载完成！")

if __name__ == "__main__":
    run_factor_download()