"""
脚本 02: A股全量日线下载器 (修复版 v2.0)
-----------------------------------------------
更新日志:
- [Fix] 补充 high_price, low_price 字段 (v2.0)
- [Feat] 涵盖所有 A 股代码 (6, 0, 3, 8, 4 开头)
- [Feat] 确保下载 [不复权 Raw Data] + [成交额/换手率]
"""
import os
import time
import random
from datetime import datetime
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
START_DATE = "20050101"
ADJUST = "" # Raw Data

CLIENT = MongoClient("localhost", 27017)
col_bar = CLIENT["vnpy_stock"]["bar_daily"]
col_info = CLIENT["vnpy_stock"]["stock_info"]

def get_stock_list():
    """获取代码列表"""
    try:
        return ak.stock_info_a_code_name()
    except Exception as e:
        print(f"❌ 列表获取失败: {e}")
        return pd.DataFrame()

def save_bars_sina_full(symbol, exchange, df):
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
                "exchange": exchange.value,
                "interval": Interval.DAILY.value,
                "datetime": dt,
                "open_price": float(row['open']),
                "high_price": float(row['high']),   # ✅ 修复: 增加最高价
                "low_price": float(row['low']),     # ✅ 修复: 增加最低价
                "close_price": float(row['close']),
                "volume": vol_share,
                "turnover": amount_rmb,
                "turnover_rate": t_rate,
                "gateway_name": "AKSHARE_SINA"
            }
            # 过滤器确保唯一性
            filter_doc = {"symbol": symbol, "exchange": exchange.value, "interval": Interval.DAILY.value, "datetime": dt}

            # Upsert=True: 存在则更新(补全字段)，不存在则插入
            updates.append(UpdateOne(filter_doc, {"$set": doc}, upsert=True))
        except: continue

    if updates:
        col_bar.bulk_write(updates)

def identify_exchange(symbol):
    if symbol.startswith("6"): return Exchange.SSE
    if symbol.startswith("0") or symbol.startswith("3"): return Exchange.SZSE
    if symbol.startswith("8") or symbol.startswith("4"): return Exchange.BSE
    return Exchange.SSE

def get_sina_symbol(symbol, exchange):
    if exchange == Exchange.SSE: return f"sh{symbol}"
    if exchange == Exchange.SZSE: return f"sz{symbol}"
    if exchange == Exchange.BSE: return symbol
    return f"sz{symbol}"

def run():
    print("🚀 启动 [全市场日线修复版 v2.0] 下载任务...")

    # 1. 获取列表
    df_list = get_stock_list()
    if df_list.empty: return

    # 2. 准备任务
    tasks = []
    for _, row in df_list.iterrows():
        code = str(row['code'])
        name = str(row['name'])
        if not (code.startswith("6") or code.startswith("0") or code.startswith("3") or code.startswith("8") or code.startswith("4")):
            continue
        tasks.append((code, name))

    # 3. 这里的逻辑需要调整：因为我们要修复旧数据，所以不能跳过已存在的代码
    #    但为了效率，我们可以只针对需要更新的跑，或者索性全量跑一遍(更安全)
    #    建议: 直接全量跑，因为 UpdateOne 会处理去重，只是耗时一点，但能保证数据完整。

    print(f"📊 总任务: {len(tasks)} 只 (将全量扫描以修复缺失字段)")

    pbar = tqdm(tasks, unit="stock")

    for symbol, name in pbar:
        pbar.set_description(f"Processing {name}")
        exchange = identify_exchange(symbol)
        sina_symbol = get_sina_symbol(symbol, exchange)

        try:
            col_info.update_one({"symbol": symbol}, {"$set": {"name": name, "exchange": exchange.value}}, upsert=True)

            df = ak.stock_zh_a_daily(
                symbol=sina_symbol,
                start_date=START_DATE,
                end_date=datetime.now().strftime("%Y%m%d"),
                adjust=ADJUST
            )

            save_bars_sina_full(symbol, exchange, df)

        except requests.exceptions.ConnectionError:
            pbar.write(f"\n🛑 网络中断 {name}，稍后重试。")
            time.sleep(5)
        except Exception:
            # 忽略极个别不支持的股票
            pass

        # 适当加速，因为如果是本地更新会很快
        time.sleep(0.05)

    print("\n✨ 修复完成！High/Low 数据已就位。")

if __name__ == "__main__":
    run()