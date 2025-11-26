"""
脚本 04: 退市股票恢复器 (v7.2 时间阀门版)
------------------------------------------------
策略升级:
1. [时间阀门]: 仅收录 2005-01-01 之后退市的股票。
   (在此之前退市的股票对当前回测无意义，直接过滤)
2. [数据对齐]: 行情下载起点统一为 2005-01-01。
3. [单位统一]: 严格执行 东财成交量(手) -> 数据库(股) 的转换。
"""
import os
import time
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from pymongo import UpdateOne, MongoClient
from vnpy.trader.constant import Exchange, Interval
import akshare as ak
import random

# --- 🛡️ 直连补丁 ---
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['all_proxy'] = ''
os.environ['NO_PROXY'] = '*'

# --- 核心配置 ---
# 1. 历史行情起点 (回测只从这里开始)
START_DATE = "20050101"
# 2. 退市过滤线 (在此之前退市的直接忽略)
FILTER_DATE = datetime(2005, 1, 1)

# 数据库
CLIENT = MongoClient("localhost", 27017)
db = CLIENT["vnpy_stock"]
col_bar = db["bar_daily"]
col_info = db["stock_info"]
col_adj = db["adjust_factor"]

def parse_date(date_val):
    """通用日期解析器，处理各种怪异格式"""
    if pd.isna(date_val) or str(date_val).strip() == "":
        return None
    try:
        # 常见格式处理
        return pd.to_datetime(date_val).to_pydatetime()
    except:
        return None

def update_delisted_metadata():
    """
    阶段一：同步名单 + 时间过滤
    """
    print(f"\n[Phase 1] 同步交易所退市名单 (过滤阈值: {FILTER_DATE.strftime('%Y-%m-%d')})...")

    updates = []
    valid_count = 0
    skipped_count = 0

    # --- 1. 深交所 ---
    try:
        df_sz = ak.stock_info_sz_delist(symbol="终止上市公司")
        if not df_sz.empty:
            for _, row in df_sz.iterrows():
                symbol = str(row['证券代码'])
                if symbol.startswith("200"): continue # 忽略B股

                # 解析退市日期
                d_date = parse_date(row['终止上市日期'])

                # 🚨 核心过滤逻辑 🚨
                if d_date and d_date < FILTER_DATE:
                    skipped_count += 1
                    continue

                updates.append(UpdateOne(
                    {"symbol": symbol},
                    {"$set": {
                        "symbol": symbol,
                        "name": str(row['证券简称']),
                        "exchange": Exchange.SZSE.value,
                        "status": "DELISTED",
                        "delisted_date": d_date.strftime("%Y-%m-%d") if d_date else ""
                    }},
                    upsert=True
                ))
                valid_count += 1
    except Exception as e:
        print(f"   ❌ 深交所名单获取失败: {e}")

    # --- 2. 上交所 ---
    try:
        df_sh = ak.stock_info_sh_delist(symbol="全部")
        if not df_sh.empty:
            for _, row in df_sh.iterrows():
                symbol = str(row['公司代码'])
                if symbol.startswith("900"): continue # 忽略B股

                # 解析退市日期 (上交所字段叫 '暂停上市日期'，通常即为退市相关节点)
                d_date = parse_date(row['暂停上市日期'])

                # 🚨 核心过滤逻辑 🚨
                if d_date and d_date < FILTER_DATE:
                    skipped_count += 1
                    continue

                updates.append(UpdateOne(
                    {"symbol": symbol},
                    {"$set": {
                        "symbol": symbol,
                        "name": str(row['公司简称']),
                        "exchange": Exchange.SSE.value,
                        "status": "DELISTED",
                        "delisted_date": d_date.strftime("%Y-%m-%d") if d_date else ""
                    }},
                    upsert=True
                ))
                valid_count += 1
    except Exception as e:
        print(f"   ❌ 上交所名单获取失败: {e}")

    # 3. 写入数据库
    if updates:
        col_info.bulk_write(updates)
        print(f"   📊 名单处理完毕:")
        print(f"      ✅ 入库/更新: {valid_count} 只 (2005年后退市)")
        print(f"      🗑️ 过滤丢弃: {skipped_count} 只 (2005年前退市)")
    else:
        print("   ⚠️ 未获取到有效数据。")

def save_bars_eastmoney(symbol, exchange, df):
    """
    保存行情 (东财源 - 单位换算)
    """
    if df.empty: return False
    updates = []
    for _, row in df.iterrows():
        try:
            # 1. 日期解析
            date_val = row['日期']
            dt_str = str(date_val).split()[0]
            dt = datetime.strptime(dt_str, "%Y-%m-%d")

            # 🚨 核心过滤: 再次确保只存 2005-01-01 之后的数据
            if dt < FILTER_DATE:
                continue

            # 2. 单位换算 (手 -> 股)
            vol_hand = float(row['成交量'])
            vol_share = vol_hand * 100
            amount = float(row['成交额'])

            doc = {
                "symbol": symbol,
                "exchange": exchange.value,
                "interval": Interval.DAILY.value,
                "datetime": dt,
                "open_price": float(row['开盘']),
                "high_price": float(row['最高']),
                "low_price": float(row['最低']),
                "close_price": float(row['收盘']),
                "volume": vol_share,        # ✅ 股数
                "turnover": amount,
                "gateway_name": "DELISTED_EM"
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
        col_bar.bulk_write(updates)
        return True
    return False

def try_save_factors(symbol, exchange):
    """获取复权因子"""
    sina_symbol = ("sh" if exchange == Exchange.SSE else "sz") + symbol
    try:
        # 注意：因子数据最好还是从上市首日开始拿，以保证计算准确，
        # 但 vn.py 回测引擎通常只看回测区间内的因子。
        # 这里我们还是从 START_DATE 开始请求。
        df = ak.stock_zh_a_daily(
            symbol=sina_symbol,
            start_date=START_DATE, # 20050101
            adjust="qfq-factor"
        )

        if not df.empty and 'qfq_factor' in df.columns:
            updates = []
            for _, row in df.iterrows():
                dt = row['date']
                if isinstance(dt, str):
                    dt = datetime.strptime(dt.split()[0], "%Y-%m-%d")

                updates.append(UpdateOne(
                    {"symbol": symbol, "date": dt},
                    {"$set": {"factor": float(row['qfq_factor']), "source": "SINA_FACTOR"}},
                    upsert=True
                ))
            if updates:
                col_adj.bulk_write(updates)
                return True
    except: pass
    return False

def download_missing_data():
    """
    阶段二：补全行情
    """
    print("\n[Phase 2] 扫描任务队列，补全历史行情...")

    # 1. 找出所有 2005 年后退市的股票
    # 注意：因为Phase 1已经过滤了，所以stock_info里标记为DELISTED的应该都是符合要求的
    cursor = col_info.find({"status": "DELISTED"})
    targets = list(cursor)

    # 2. 筛选真正缺数据的
    tasks = []
    print("   🔍 正在核对本地数据存量...")
    for doc in targets:
        symbol = doc['symbol']
        # 只要有一条数据，就认为下载过了 (断点续传)
        if col_bar.count_documents({"symbol": symbol}, limit=1) == 0:
            tasks.append(doc)

    print(f"   📊 目标退市股: {len(targets)} | 需补全: {len(tasks)}")

    if not tasks:
        print("   ✨ 所有退市股票数据已就绪，无需下载。")
        return

    # 3. 执行下载
    pbar = tqdm(tasks, unit="stock")
    success_count = 0

    for doc in pbar:
        symbol = doc['symbol']
        name = doc.get('name', symbol)
        exchange = Exchange(doc.get('exchange', 'SSE'))

        pbar.set_description(f"补全 {name}")

        try:
            # 请求历史行情 (从 20050101 开始)
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=START_DATE,
                end_date=datetime.now().strftime("%Y%m%d"),
                adjust=""
            )

            if not df.empty:
                # 存储 (函数内部会再次校验日期 >= 2005-01-01)
                if save_bars_eastmoney(symbol, exchange, df):
                    # 尝试因子
                    try_save_factors(symbol, exchange)
                    success_count += 1

        except Exception as e:
            pbar.write(f"   ❌ {name} 失败: {e}")

        time.sleep(random.uniform(60, 120))

    print(f"\n✨ 补全结束! 成功恢复 {success_count} 只股票数据。")

def run():
    print(f"🚀 启动 [退市股票恢复器 v7.2] (Filter: >={FILTER_DATE.strftime('%Y-%m-%d')})...")
    update_delisted_metadata()
    download_missing_data()
    print("\n🎉 任务完成。")

if __name__ == "__main__":
    run()