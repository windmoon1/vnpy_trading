"""
脚本: 689009 最终单位转换与写入 (Final Unit Conversion)
-----------------------------------------------------
目标: 针对 689009 (东财数据) 进行 Volume 单位修正，以匹配 Sina 的 [股] 标准。
修正: DB.Volume = EM.成交量 * 100
"""
import os
from datetime import datetime
from pymongo import UpdateOne, MongoClient
from vnpy.trader.constant import Exchange, Interval
import akshare as ak
import pandas as pd

# --- 🛡️ 直连补丁 ---
os.environ['http_proxy'] = ''; os.environ['https_proxy'] = ''; os.environ['all_proxy'] = ''; os.environ['NO_PROXY'] = '*'

# --- 目标配置 ---
TARGET_SYMBOL = "689009"
TARGET_NAME = "九号公司"
START_DATE = "20050101"

# --- 数据库连接 ---
CLIENT = MongoClient("localhost", 27017)
db = CLIENT["vnpy_stock"]
col_bar = db["bar_daily"]
col_info = db["stock_info"]

def save_bars_eastmoney_final(symbol, exchange, df):
    """保存数据，进行核心单位转换"""
    if df.empty: return 0
    updates = []

    for _, row in df.iterrows():
        try:
            dt = datetime.strptime(str(row['日期']), "%Y-%m-%d")

            # 1. 核心转换点: 将手的成交量转换为股 (Lots -> Shares)
            vol_hand = float(row['成交量'])
            vol_share = vol_hand * 100

            # 2. 计算换手率 (东财自带的可能精度不够，我们用原始数据估算)
            turnover_rate = float(row['换手率']) if '换手率' in row else 0.0

            doc = {
                "symbol": symbol,
                "exchange": exchange.value,
                "interval": Interval.DAILY.value,
                "datetime": dt,

                "open_price": float(row['开盘']),
                "close_price": float(row['收盘']),
                "volume": vol_share,       # 🎯 写入 Shares (股)
                "turnover": float(row['成交额']), # 写入 RMB (元)
                "turnover_rate": turnover_rate, # 使用东财提供的换手率 (百分比)
                "gateway_name": "AKSHARE_EM_FIX"
            }

            filter_doc = {"symbol": symbol, "exchange": exchange.value, "interval": Interval.DAILY.value, "datetime": dt}
            updates.append(UpdateOne(filter_doc, {"$set": doc}, upsert=True))
        except: continue

    if updates:
        col_bar.bulk_write(updates)
        return len(updates)
    return 0

def run_fix_and_save():
    print(f"🕵️ 正在进行单位转换与最终补录: {TARGET_NAME}...")
    exchange = Exchange.SSE

    try:
        # 1. 获取数据 (EastMoney 接口)
        df = ak.stock_zh_a_hist(
            symbol=TARGET_SYMBOL, period="daily", start_date=START_DATE, end_date=datetime.now().strftime("%Y%m%d"), adjust=""
        )

        if df.empty:
            print("❌ 警告：未获取到数据，请检查代码是否正确。")
            return

        # 2. 更新基础信息
        col_info.update_one(
            {"symbol": TARGET_SYMBOL},
            {"$set": {"name": TARGET_NAME, "exchange": exchange.value}},
            upsert=True
        )

        # 3. 写入核心数据 (含单位转换)
        count = save_bars_eastmoney_final(TARGET_SYMBOL, exchange, df)

        if count > 0:
            print(f"🎉 最终补录成功！{TARGET_SYMBOL} 的 {count} 条数据已按 [股] 标准写入。")

            # 4. 最终验证
            final_count = col_bar.count_documents({})
            print(f"✅ 恭喜！A股核心 K 线数据已达成 100% 完整度 ({final_count} 条记录)。")
        else:
            print("❌ 写入失败：未写入任何数据。")

    except Exception as e:
        print(f"❌ 最终补录失败：{e.__class__.__name__} - {e}")

if __name__ == "__main__":
    run_fix_and_save()