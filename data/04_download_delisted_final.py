"""
脚本 04: 退市股票恢复器 (v7.4 拒绝卡死版) 初始化运行一次即可
------------------------------------------------
策略升级:
1. [强制超时]: 引入 socket.setdefaulttimeout(20)，防止 requests 无限挂起。
2. [重试可见]: 打印重试日志，不再静默等待。
3. [异常透明]: 明确区分网络问题与代码逻辑错误。
"""
import os
import time
import random
import requests
import functools
import socket  # 👈 新增
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from pymongo import UpdateOne, MongoClient
from vnpy.trader.constant import Exchange, Interval
import akshare as ak

# --- 🛡️ 直连补丁 ---
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['all_proxy'] = ''
os.environ['NO_PROXY'] = '*'

# --- ⚡ 核心配置 ---
# 1. 强制全局超时 (秒): 解决 requests 默认无 timeout 导致的无限卡死
socket.setdefaulttimeout(5)

START_DATE = "20050101"
FILTER_DATE = datetime(2005, 1, 1)
MAX_RETRIES = 3       # 减少重试次数，快速失败
BASE_SLEEP = 2        # 基础休眠秒数

# 数据库
CLIENT = MongoClient("localhost", 27017)
db = CLIENT["vnpy_stock"]
col_bar = db["bar_daily"]
col_info = db["stock_info"]
col_adj = db["adjust_factor"]


def retry_request(max_retries=MAX_RETRIES, base_sleep=BASE_SLEEP):
    """
    [工程优化] 网络请求重试装饰器 (带日志 + 指数退避)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout,
                        requests.exceptions.ReadTimeout, # 👈 捕获超时
                        socket.timeout,                  # 👈 捕获 socket 超时
                        requests.exceptions.ChunkedEncodingError,
                        requests.exceptions.ProxyError) as e:

                    if attempt == max_retries:
                        print(f"\n❌ [Network] {func.__name__} 最终失败: {str(e)[:100]}...")
                        raise e

                    sleep_time = base_sleep * (2 ** (attempt - 1))
                    # 👇 关键修复: 打印出来，不要静默重试
                    print(f"   ⚠️ 网络卡顿，正在重试 {func.__name__} ({attempt}/{max_retries})，等待 {sleep_time}s...")
                    time.sleep(sleep_time)
                except Exception as e:
                    # 逻辑错误/解析错误直接抛出，不吞没
                    print(f"\n❌ [Logic] {func.__name__} 发生非网络错误: {e}")
                    raise e
            return None
        return wrapper
    return decorator


def parse_date(date_val):
    if pd.isna(date_val) or str(date_val).strip() == "":
        return None
    try:
        return pd.to_datetime(date_val).to_pydatetime()
    except:
        return None


# --- 封装带重试的 AKShare 接口 ---

@retry_request()
def fetch_sz_delist_list():
    print("   📡 连接深交所接口...", end="\r")
    return ak.stock_info_sz_delist(symbol="终止上市公司")

@retry_request()
def fetch_sh_delist_list():
    print("   📡 连接上交所接口...", end="\r")
    return ak.stock_info_sh_delist(symbol="全部")

@retry_request()
def fetch_stock_history(symbol):
    return ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=START_DATE,
        end_date=datetime.now().strftime("%Y%m%d"),
        adjust=""
    )

@retry_request()
def fetch_stock_factor(sina_symbol):
    return ak.stock_zh_a_daily(
        symbol=sina_symbol,
        start_date=START_DATE,
        adjust="qfq-factor"
    )


def update_delisted_metadata():
    """阶段一：同步名单"""
    print(f"\n[Phase 1] 同步交易所退市名单 (Timeout set to 20s)...")

    updates = []
    valid_count = 0

    # --- 1. 深交所 ---
    try:
        df_sz = fetch_sz_delist_list()
        if not df_sz.empty:
            for _, row in df_sz.iterrows():
                symbol = str(row['证券代码'])
                if symbol.startswith("200"): continue
                d_date = parse_date(row['终止上市日期'])
                if d_date and d_date < FILTER_DATE: continue

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
            print("   ✅ 深交所名单获取成功")
    except Exception as e:
        print(f"   ❌ 深交所名单获取跳过: {e}")

    # --- 2. 上交所 ---
    try:
        df_sh = fetch_sh_delist_list()
        if not df_sh.empty:
            for _, row in df_sh.iterrows():
                symbol = str(row['公司代码'])
                if symbol.startswith("900"): continue
                d_date = parse_date(row['暂停上市日期'])
                if d_date and d_date < FILTER_DATE: continue

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
            print("   ✅ 上交所名单获取成功")
    except Exception as e:
        print(f"   ❌ 上交所名单获取跳过: {e}")

    # 3. 写入数据库
    if updates:
        col_info.bulk_write(updates)
        print(f"   📊 名单同步完毕: {valid_count} 只目标股票入库。")
    else:
        print("   ⚠️ 未能获取新的名单数据。")


def save_bars_eastmoney(symbol, exchange, df):
    """保存行情"""
    if df.empty: return False
    updates = []
    records = df.to_dict('records')

    for row in records:
        try:
            date_val = row['日期']
            dt_str = str(date_val).split()[0]
            dt = datetime.strptime(dt_str, "%Y-%m-%d")
            if dt < FILTER_DATE: continue

            vol_share = float(row['成交量']) * 100
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
                "volume": vol_share,
                "turnover": amount,
                "gateway_name": "DELISTED_EM"
            }
            filter_doc = {"symbol": symbol, "exchange": exchange.value, "interval": Interval.DAILY.value, "datetime": dt}
            updates.append(UpdateOne(filter_doc, {"$set": doc}, upsert=True))
        except Exception:
            continue

    if updates:
        col_bar.bulk_write(updates, ordered=False)
        return True
    return False


def try_save_factors(symbol, exchange):
    """获取复权因子"""
    sina_symbol = ("sh" if exchange == Exchange.SSE else "sz") + symbol
    try:
        df = fetch_stock_factor(sina_symbol)
        if df is not None and not df.empty and 'qfq_factor' in df.columns:
            updates = []
            records = df.to_dict('records')
            for row in records:
                dt = row['date']
                if isinstance(dt, str): dt = datetime.strptime(dt.split()[0], "%Y-%m-%d")
                updates.append(UpdateOne(
                    {"symbol": symbol, "date": dt},
                    {"$set": {"factor": float(row['qfq_factor']), "source": "SINA_FACTOR"}},
                    upsert=True
                ))
            if updates:
                col_adj.bulk_write(updates, ordered=False)
    except: pass


def download_missing_data():
    """阶段二：补全行情"""
    print("\n[Phase 2] 扫描任务队列，补全历史行情...")
    cursor = col_info.find({"status": "DELISTED"})
    targets = list(cursor)

    tasks = []
    print("   🔍 核对本地数据...")
    for doc in targets:
        symbol = doc['symbol']
        if col_bar.count_documents({"symbol": symbol}, limit=1) == 0:
            tasks.append(doc)

    print(f"   📊 需补全: {len(tasks)} / {len(targets)}")

    if not tasks: return

    # Tqdm 配置: 实时显示当前处理的股票
    pbar = tqdm(tasks, unit="stock")
    success_count = 0

    for doc in pbar:
        symbol = doc['symbol']
        name = doc.get('name', symbol)
        exchange = Exchange(doc.get('exchange', 'SSE'))

        pbar.set_description(f"Processing {symbol}")

        try:
            df = fetch_stock_history(symbol) # 如果这里超时，会抛出异常被下面捕获

            if df is not None and not df.empty:
                if save_bars_eastmoney(symbol, exchange, df):
                    try_save_factors(symbol, exchange)
                    success_count += 1

        except Exception as e:
            # 这里的 print 确保报错不会被“吞掉”
            pbar.write(f"   ❌ {name}({symbol}) 失败: {str(e)[:50]}")

        #time.sleep(random.uniform(3, 5))

    print(f"\n✨ 任务完成! 成功恢复 {success_count} 只股票。")


if __name__ == "__main__":
    print(f"🚀 启动 [退市股票恢复器 v7.4 Anti-Freeze]...")
    # update_delisted_metadata()
    download_missing_data()
    print("\n🎉 All Done.")