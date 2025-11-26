"""
脚本 08: 退市数据定向修复 (Target Repair)
------------------------------------------------
目标: 读取审计报告 (csv)，针对 MISSING_BARS 和 MISSING_FACTOR 进行高强度重试修复。
逻辑:
1. 读取 data/delisted_data_audit.csv
2. 过滤出问题股票
3. 针对性调用接口补全
"""
import os
import time
import pandas as pd
import akshare as ak
import requests
import functools
import socket
from datetime import datetime
from pymongo import UpdateOne, MongoClient
from vnpy.trader.constant import Exchange, Interval

# --- ⚡ 核心配置 ---
socket.setdefaulttimeout(20)  # 强制防卡死
CSV_PATH = "delisted_data_audit.csv"
DB_NAME = "vnpy_stock"

# 数据库连接
client = MongoClient("localhost", 27017)
db = client[DB_NAME]
col_bar = db["bar_daily"]
col_adj = db["adjust_factor"]
col_info = db["stock_info"]


# --- 装饰器: 强力重试 (复用 v7.4 的逻辑) ---
def retry_request(max_retries=5, base_sleep=3):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (requests.exceptions.RequestException, socket.timeout) as e:
                    if attempt == max_retries:
                        print(f"      ❌ {func.__name__} 最终失败: {str(e)[:50]}")
                        return None
                    sleep_time = base_sleep * (2 ** (attempt - 1))
                    print(f"      ⚠️ 网络波动, 重试 {attempt}/{max_retries} (等待 {sleep_time}s)...")
                    time.sleep(sleep_time)
                except Exception as e:
                    print(f"      ❌ 逻辑错误: {e}")
                    return None
            return None

        return wrapper

    return decorator


# --- 接口封装 ---
@retry_request()
def fetch_stock_history(symbol):
    """下载日线 (东财)"""
    return ak.stock_zh_a_hist(
        symbol=symbol, period="daily", start_date="20050101", adjust=""
    )


@retry_request()
def fetch_stock_factor(symbol, exchange):
    """下载因子 (新浪)"""
    sina_symbol = ("sh" if exchange == Exchange.SSE else "sz") + symbol
    return ak.stock_zh_a_daily(symbol=sina_symbol, adjust="qfq-factor")


# --- 存储逻辑 ---
def save_bars(symbol, exchange, df):
    if df is None or df.empty: return False
    updates = []
    for row in df.to_dict('records'):
        try:
            dt = datetime.strptime(str(row['日期']).split()[0], "%Y-%m-%d")
            # 简单换算
            vol_share = float(row['成交量']) * 100
            updates.append(UpdateOne(
                {"symbol": symbol, "exchange": exchange.value, "interval": "d", "datetime": dt},
                {"$set": {
                    "symbol": symbol, "exchange": exchange.value, "interval": "d",
                    "datetime": dt, "open_price": float(row['开盘']), "high_price": float(row['最高']),
                    "low_price": float(row['最低']), "close_price": float(row['收盘']),
                    "volume": vol_share, "turnover": float(row['成交额']), "gateway_name": "REPAIR"
                }}, upsert=True
            ))
        except:
            continue

    if updates:
        col_bar.bulk_write(updates, ordered=False)
        return True
    return False


def save_factors(symbol, df):
    if df is None or df.empty or 'qfq_factor' not in df.columns: return False
    updates = []
    for row in df.to_dict('records'):
        dt = row['date']
        if isinstance(dt, str): dt = datetime.strptime(dt.split()[0], "%Y-%m-%d")
        updates.append(UpdateOne(
            {"symbol": symbol, "date": dt},
            {"$set": {"factor": float(row['qfq_factor']), "source": "REPAIR"}},
            upsert=True
        ))
    if updates:
        col_adj.bulk_write(updates, ordered=False)
        return True
    return False


def run_repair():
    if not os.path.exists(CSV_PATH):
        print(f"❌ 找不到审计报告: {CSV_PATH}，请先运行 audit 脚本。")
        return

    print("🚀 启动 [退市数据定向修复]...")
    df = pd.read_csv(CSV_PATH, dtype={"symbol": str})

    # 1. 筛选任务
    tasks_bars = df[df['status'] == 'MISSING_BARS']
    tasks_factor = df[df['status'] == 'MISSING_FACTOR']

    total_tasks = len(tasks_bars) + len(tasks_factor)
    print(f"📋 发现待修复项: MISSING_BARS={len(tasks_bars)}, MISSING_FACTOR={len(tasks_factor)}")

    if total_tasks == 0:
        print("🎉 没有需要修复的数据！(LARGE_GAP 通常无需修复)")
        return

    # 2. 修复 K线缺失 (MISSING_BARS)
    if not tasks_bars.empty:
        print("\n🔧 [Step 1] 修复 K线缺失...")
        for _, row in tasks_bars.iterrows():
            symbol = row['symbol']
            # 从数据库查 exchange，或者根据代码猜
            ex_str = "SSE" if symbol.startswith('6') else "SZSE"
            exchange = Exchange.SSE if ex_str == "SSE" else Exchange.SZSE

            print(f"   Fixing Bars: {symbol} ... ", end="")
            df_hist = fetch_stock_history(symbol)
            if save_bars(symbol, exchange, df_hist):
                print("✅ 成功入库")
                # 顺便把因子也尝试补一下
                fetch_stock_factor(symbol, exchange)
            else:
                print("❌ 数据源仍为空 (可能已完全无法获取)")

    # 3. 修复 因子缺失 (MISSING_FACTOR)
    if not tasks_factor.empty:
        print("\n🔧 [Step 2] 修复 因子缺失...")
        for _, row in tasks_factor.iterrows():
            symbol = row['symbol']
            ex_str = "SSE" if symbol.startswith('6') else "SZSE"
            exchange = Exchange.SSE if ex_str == "SSE" else Exchange.SZSE

            print(f"   Fixing Factor: {symbol} ... ", end="")
            df_fac = fetch_stock_factor(symbol, exchange)
            if save_factors(symbol, df_fac):
                print("✅ 成功入库")
            else:
                print("❌ 新浪源缺失 (尝试备用方案)...")
                # 备用方案：如果新浪拿不到因子，尝试直接从东财拿前复权数据，
                # 但 vn.py 架构需要独立因子表。
                # 暂时先标记失败，手动处理个别顽固分子。

    print("\n✨ 修复流程结束。请重新运行 audit 脚本验证结果。")


if __name__ == "__main__":
    run_repair()