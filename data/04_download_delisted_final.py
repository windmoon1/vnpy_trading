"""
脚本 04: 退市股票恢复器 (消除幸存者偏差)
------------------------------------
目标: 暴力扫描常见号段，使用新浪接口获取所有已退市股票的历史数据。
数据源: ak.stock_zh_a_daily (Sina)
"""
import os
import time
from tqdm import tqdm
from pymongo import UpdateOne, MongoClient
from vnpy.trader.constant import Exchange, Interval
import akshare as ak
import pandas as pd
from datetime import datetime

# --- 🛡️ 直连补丁 ---
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['all_proxy'] = ''
os.environ['NO_PROXY'] = '*'

# --- 配置 ---
START_DATE = "19900101"  # 退市股可能年代久远，起始日期设为最早
ADJUST = ""  # Raw Data

# 数据库
CLIENT = MongoClient("localhost", 27017)
col_bar = CLIENT["vnpy_stock"]["bar_daily"]
col_info = CLIENT["vnpy_stock"]["stock_info"]


def identify_exchange(symbol):
    if symbol.startswith("6"): return Exchange.SSE
    if symbol.startswith("0") or symbol.startswith("3"): return Exchange.SZSE
    if symbol.startswith("8") or symbol.startswith("4"): return Exchange.BSE
    return Exchange.SSE


def get_sina_symbol(symbol, exchange):
    if exchange == Exchange.SSE: return f"sh{symbol}"
    if exchange == Exchange.SZSE: return f"sz{symbol}"
    return symbol  # 北交所和科创板尝试直接传


def generate_target_codes(existing_symbols):
    """生成待扫描的代码池，并排除掉所有活着的股票"""
    targets = []
    # 常用号段 (包含主板、中小板、创业板、科创板)
    ranges = [(600000, 605999), (1, 3999), (300000, 302999), (688000, 688999)]

    for start, end in ranges:
        for i in range(start, end + 1):
            symbol = f"{i:06d}"
            if symbol not in existing_symbols:  # 只扫描我们本地数据库里没有的
                targets.append(symbol)

    print(f"🎯 待扫描的空缺代码池: {len(targets)} 个")
    return targets


def save_delisted_data(symbol, exchange, df):
    """保存退市数据，并标记状态"""
    if df.empty: return

    # 1. 保存行情 (逻辑同脚本 02)
    updates = []
    for _, row in df.iterrows():
        try:
            # 新浪返回的列：date, open, close, volume...
            dt = datetime.combine(row['date'], datetime.min.time())
            doc = {
                "symbol": symbol, "exchange": exchange.value, "interval": Interval.DAILY.value,
                "datetime": dt, "close_price": float(row['close']), "volume": float(row['volume']),
                "turnover": float(row['volume']) * float(row['close']),  # 估算成交额
                "gateway_name": "DELISTED_SINA"
            }
            filter_doc = {"symbol": symbol, "exchange": exchange.value, "interval": Interval.DAILY.value,
                          "datetime": dt}
            updates.append(UpdateOne(filter_doc, {"$set": doc}, upsert=True))
        except:
            continue

    if updates:
        col_bar.bulk_write(updates)

        # 2. 标记基础信息 (标记为已退市)
        col_info.update_one(
            {"symbol": symbol},
            {"$set": {
                "symbol": symbol,
                "exchange": exchange.value,
                "name": f"DELISTED_{symbol}",
                "status": "DELISTED"
            }},
            upsert=True
        )
        return True
    return False


def run_delisted_recovery():
    print("🚀 启动 [消除幸存者偏差] 任务...")

    # 1. 获取已存在的股票列表 (前提是脚本 02 已经跑完了)
    try:
        existing_symbols = set(col_info.distinct("symbol"))
    except Exception:
        print("❌ 错误：请先运行脚本 02 完成主力下载！")
        return

    # 2. 生成待扫描池
    target_codes = generate_target_codes(existing_symbols)

    pbar = tqdm(target_codes, unit="code")
    recovered_count = 0

    for symbol in pbar:
        pbar.set_description(f"Scanning {symbol}")
        exchange = identify_exchange(symbol)
        sina_symbol = get_sina_symbol(symbol, exchange)

        try:
            # 核心下载: 如果代码是有效的历史代码，Sina 会返回数据
            df = ak.stock_zh_a_daily(
                symbol=sina_symbol,
                start_date=START_DATE,
                end_date=datetime.now().strftime("%Y%m%d"),
                adjust=ADJUST
            )

            if not df.empty:
                # 有数据！是退市股！
                if save_delisted_data(symbol, exchange, df):
                    recovered_count += 1
                    pbar.write(f"🎉 成功打捞: {symbol} (已标记为 DELISTED)")

        except requests.exceptions.ConnectionError:
            # 遇到网络错误休眠
            time.sleep(5)
        except Exception:
            # 绝大多数代码是无效代码，AkShare 抛出异常，直接跳过
            pass

        # 基础延迟
        time.sleep(0.05)

    print("\n" + "=" * 60)
    print(f"✨ 退市股票打捞完成！共找回 {recovered_count} 只历史记录。")


if __name__ == "__main__":
    run_delisted_recovery()