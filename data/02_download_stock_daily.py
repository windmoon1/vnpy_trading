"""
脚本 02: A股全量日线下载器 (最终整合版 - 含北交所)
-----------------------------------------------
策略:
1. 涵盖所有 A 股代码 (6, 0, 3, 8, 4 开头)。
2. 确保下载 [不复权 Raw Data] + [成交额/换手率]。
3. 断点续传和异常透明化。
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
    """获取代码列表 (极简接口)"""
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
                "close_price": float(row['close']),
                "volume": vol_share,
                "turnover": amount_rmb,
                "turnover_rate": t_rate,
                "gateway_name": "AKSHARE_SINA"
            }
            filter_doc = {"symbol": symbol, "exchange": exchange.value, "interval": Interval.DAILY.value, "datetime": dt}
            updates.append(UpdateOne(filter_doc, {"$set": doc}, upsert=True))
        except: continue

    if updates:
        col_bar.bulk_write(updates)

def identify_exchange(symbol):
    if symbol.startswith("6"): return Exchange.SSE
    if symbol.startswith("0") or symbol.startswith("3"): return Exchange.SZSE
    if symbol.startswith("8") or symbol.startswith("4"): return Exchange.BSE # <--- 北交所
    return Exchange.SSE

def get_sina_symbol(symbol, exchange):
    if exchange == Exchange.SSE: return f"sh{symbol}"
    if exchange == Exchange.SZSE: return f"sz{symbol}"
    # 新浪接口对北交所支持可能较弱，但我们仍需尝试
    if exchange == Exchange.BSE:
        # 北交所可能需要特定的前缀或直接代码
        # 尝试直接返回代码，让 AkShare 内部处理
        return symbol
    return f"sz{symbol}"

def run():
    print("🚀 启动 [全市场最终整合版] 下载任务...")

    # 1. 获取列表
    df_list = get_stock_list()
    if df_list.empty: return

    # 2. 准备任务列表
    tasks = []
    for _, row in df_list.iterrows():
        code = str(row['code'])
        name = str(row['name'])

        # --- 核心修改：纳入 8 和 4 开头的北交所代码 ---
        if not (code.startswith("6") or code.startswith("0") or code.startswith("3") or code.startswith("8") or code.startswith("4")):
            continue

        tasks.append((code, name))

    # 3. 检查断点
    done_set = set()
    try:
        done_set = set(col_bar.distinct("symbol"))
    except: pass

    print(f"📊 总任务: {len(tasks)} 只 | 已完成: {len(done_set)} 只 | 待下载: {len(tasks) - len(done_set)} 只")

    # 4. 循环下载
    pbar = tqdm(tasks, unit="stock")

    for symbol, name in pbar:
        if symbol in done_set:
            continue

        pbar.set_description(f"下载 {name}")
        exchange = identify_exchange(symbol)
        sina_symbol = get_sina_symbol(symbol, exchange)

        try:
            # 存信息
            col_info.update_one({"symbol": symbol}, {"$set": {"name": name, "exchange": exchange.value}}, upsert=True)

            # 核心下载
            df = ak.stock_zh_a_daily(
                symbol=sina_symbol,
                start_date=START_DATE,
                end_date=datetime.now().strftime("%Y%m%d"),
                adjust=ADJUST
            )

            save_bars_sina_full(symbol, exchange, df)
            done_set.add(symbol)

        except requests.exceptions.ConnectionError as e:
            # 严重网络错误，休眠后跳过
            pbar.write(f"\n🛑 网络中断，跳过 {name}。下次续传。")
            time.sleep(5)
        except Exception as e:
            # 数据解析错误或接口不支持 (如部分北交所股)
            pbar.write(f"\n⚠️ 数据错误或不支持 {name}: {e.__class__.__name__}。")
            time.sleep(1)

        time.sleep(0.1)

    print("\n✨ 全市场数据注入完成！")

if __name__ == "__main__":
    run()