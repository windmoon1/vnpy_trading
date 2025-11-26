"""
脚本 06: 全市场财务数据下载器 (v3.1 智能避险版)
------------------------------------------------
目标: 下载 A 股三大财务报表 (Sina Source)。
升级:
1. [智能避险]: 遇到 JSONDecodeError (被封) 自动触发指数级退避 (Sleep 10s -> 30s -> 60s...)。
2. [顽强重试]: 单个接口失败会自动重试最多 5 次，确保数据完整。
3. [PIT/分表]: 保持 v3.0 的 PIT 架构和分表存储逻辑。
"""
import os
import time
import random
import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime
from tqdm import tqdm
from pymongo import UpdateOne, MongoClient
from vnpy.trader.constant import Exchange
import akshare as ak

# --- 🛡️ 直连补丁 ---
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['all_proxy'] = ''
os.environ['NO_PROXY'] = '*'

# --- 配置 ---
NORMAL_SLEEP = (20, 30)   # 正常请求间隔 (秒)
MAX_RETRIES = 5         # 最大重试次数
BASE_WAIT = 60          # 基础等待时间 (秒)

# 数据库连接
CLIENT = MongoClient("localhost", 27017)
DB = CLIENT["vnpy_stock"]
COL_INFO = DB["stock_info"]

# 集合映射
COL_MAP = {
    "资产负债表": DB["finance_balance"],
    "利润表": DB["finance_income"],
    "现金流量表": DB["finance_cashflow"]
}

# 关键字段检查清单
CHECK_FIELDS = {
    "资产负债表": "资产总计",
    "利润表": "净利润",
    "现金流量表": "经营活动产生的现金流量净额"
}

def clean_date(date_val):
    """清洗日期"""
    if not date_val: return None
    s = str(date_val).strip()
    if not s or s.lower() == 'nan': return None
    try:
        s = s.replace("-", "")
        return datetime.strptime(s, "%Y%m%d")
    except:
        return None

def is_stock_completed(symbol):
    """检查完整性"""
    for sheet_name, col_obj in COL_MAP.items():
        latest = col_obj.find_one({"symbol": symbol}, sort=[("report_date", -1)])
        if not latest: return False
        key_field = CHECK_FIELDS[sheet_name]
        if latest.get(key_field) is None: return False
    return True

def get_todo_list():
    """生成待下载清单"""
    print("🔍 正在扫描全市场股票列表...")
    all_stocks = list(COL_INFO.find({}, {"symbol": 1, "exchange": 1, "name": 1}))

    todo_list = []
    print("🕵️‍♂️ 正在执行断点完整性检查...")

    # 仅检查最近入库的股票，避免每次全量扫描太慢 (优化点)
    # 这里为了稳妥，还是建议全量检查一次，或者你可以相信 MongoDB 的查询速度
    for stock in tqdm(all_stocks, desc="Checking Status"):
        symbol = stock['symbol']
        if not is_stock_completed(symbol):
            todo_list.append(stock)

    print(f"📊 扫描完毕: 总数 {len(all_stocks)} | 待下载 {len(todo_list)}")
    return todo_list

def fetch_sina_data_with_retry(sina_symbol, sheet_name, stock_name):
    """
    带指数级退避的请求函数 (核心升级)
    """
    for attempt in range(MAX_RETRIES):
        try:
            # 尝试请求
            df = ak.stock_financial_report_sina(stock=sina_symbol, symbol=sheet_name)
            return df

        except (requests.exceptions.JSONDecodeError, json.decoder.JSONDecodeError) as e:
            # 捕捉到 JSON 错误，说明 IP 可能被限制了
            wait_time = BASE_WAIT * (2 ** attempt) + random.randint(1, 10)
            print(f"\n   ⚠️  [{stock_name}] {sheet_name} 遭遇风控 (Attempt {attempt+1}/{MAX_RETRIES})")
            print(f"       🛑 错误信息: {e}")
            print(f"       ⏳ 避险休眠: {wait_time} 秒...")
            time.sleep(wait_time)

        except Exception as e:
            # 其他网络错误
            print(f"\n   ❌ [{stock_name}] {sheet_name} 未知错误: {e}")
            time.sleep(5)
            # 如果不是风控错误，可能重试也没用，但也试一下

    # 超过重试次数
    print(f"   ☠️ [{stock_name}] {sheet_name} 彻底失败，跳过。")
    return pd.DataFrame()

def download_one_stock(symbol, exchange_str, stock_name):
    """下载单只股票"""
    prefix = "sh" if exchange_str == "SSE" else "sz"
    sina_symbol = f"{prefix}{symbol}"

    success_count = 0

    for sheet_name, col_obj in COL_MAP.items():
        # 使用带重试的请求函数
        df = fetch_sina_data_with_retry(sina_symbol, sheet_name, stock_name)

        if df.empty: continue

        try:
            # 预处理
            df = df.where(pd.notnull(df), None)
            updates = []
            for _, row in df.iterrows():
                r_date = clean_date(row.get('报告日'))
                if not r_date: continue

                doc = row.to_dict()
                doc.update({
                    "symbol": symbol,
                    "exchange": exchange_str,
                    "report_date": r_date,
                    "publish_date": clean_date(row.get('公告日期')),
                    "gateway_name": "SINA_FINANCE"
                })
                doc.pop('报告日', None); doc.pop('公告日期', None)

                filter_doc = {"symbol": symbol, "report_date": r_date}
                updates.append(UpdateOne(filter_doc, {"$set": doc}, upsert=True))

            if updates:
                col_obj.bulk_write(updates)
                success_count += 1

        except Exception as e:
            print(f"   ❌ 数据入库解析错误: {e}")

        # 表间微小延时
        time.sleep(random.uniform(1, 2))

    return success_count

def run():
    print("🚀 启动 [A股财务数据下载器 v3.1] (智能避险版)...")

    tasks = get_todo_list()
    if not tasks:
        print("✨ 任务列表为空，所有数据已就绪。")
        return

    pbar = tqdm(tasks, unit="stock")

    for stock in pbar:
        symbol = stock['symbol']
        name = stock.get('name', symbol)
        exch_val = stock.get('exchange', '')
        exchange = "SZSE" if "SZSE" in str(exch_val) else "SSE"

        pbar.set_description(f"下载 {name}")

        download_one_stock(symbol, exchange, name)

        # 任务间随机休眠
        time.sleep(random.uniform(*NORMAL_SLEEP))

    print("\n🎉 财务数据下载任务结束。")

if __name__ == "__main__":
    run()