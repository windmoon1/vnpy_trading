"""
脚本 04: 退市股票恢复器 (修复版 v2.0 - 含 High/Low)
------------------------------------------------
目标: 暴力扫描常见号段，使用新浪接口获取所有已退市股票的历史数据。
更新: 修复了缺失 high/low 字段的问题。
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
START_DATE = "19900101"
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
    return symbol

def generate_target_codes(existing_symbols):
    """生成待扫描的代码池，排除已存在的活股票"""
    targets = []
    # 常用号段
    ranges = [(600000, 605999), (1, 3999), (300000, 302999), (688000, 688999)]

    for start, end in ranges:
        for i in range(start, end + 1):
            symbol = f"{i:06d}"
            if symbol not in existing_symbols:
                targets.append(symbol)

    print(f"🎯 待扫描的空缺代码池: {len(targets)} 个")
    return targets

def save_delisted_data(symbol, exchange, df):
    """保存退市数据"""
    if df.empty: return

    updates = []
    for _, row in df.iterrows():
        try:
            dt = datetime.combine(row['date'], datetime.min.time())
            # 核心修复点: 补全 High / Low
            doc = {
                "symbol": symbol,
                "exchange": exchange.value,
                "interval": Interval.DAILY.value,
                "datetime": dt,
                "open_price": float(row['open']),
                "high_price": float(row['high']),    # ✅ 修复
                "low_price": float(row['low']),      # ✅ 修复
                "close_price": float(row['close']),
                "volume": float(row['volume']),
                "turnover": float(row['volume']) * float(row['close']),
                "gateway_name": "DELISTED_SINA"
            }
            filter_doc = {"symbol": symbol, "exchange": exchange.value, "interval": Interval.DAILY.value, "datetime": dt}
            updates.append(UpdateOne(filter_doc, {"$set": doc}, upsert=True))
        except:
            continue

    if updates:
        col_bar.bulk_write(updates)
        # 标记为已退市
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
    print("🚀 启动 [消除幸存者偏差 v2.0] 任务...")

    try:
        existing_symbols = set(col_info.distinct("symbol"))
    except Exception:
        print("❌ 错误：请先运行脚本 02 完成主力下载！")
        return

    target_codes = generate_target_codes(existing_symbols)
    pbar = tqdm(target_codes, unit="code")
    recovered_count = 0

    for symbol in pbar:
        # pbar.set_description(f"Scan {symbol}") # 减少刷屏
        exchange = identify_exchange(symbol)
        sina_symbol = get_sina_symbol(symbol, exchange)

        try:
            df = ak.stock_zh_a_daily(
                symbol=sina_symbol,
                start_date=START_DATE,
                end_date=datetime.now().strftime("%Y%m%d"),
                adjust=ADJUST
            )

            if not df.empty:
                if save_delisted_data(symbol, exchange, df):
                    recovered_count += 1
                    pbar.write(f"🎉 成功打捞: {symbol}")

        except Exception:
            pass

        time.sleep(0.01) # 极速扫描

    print("\n" + "=" * 60)
    print(f"✨ 退市股票打捞完成！共找回 {recovered_count} 只历史记录。")

if __name__ == "__main__":
    run_delisted_recovery()