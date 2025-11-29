"""
脚本 17 (V4): 全市场分红数据下载器 (同花顺 THS 适配版)
--------------------------------------------------------------
目标: 下载全市场股票的历史分红送转记录。
源头: 同花顺 (stock_fhps_detail_ths)
字段适配:
  - A股除权除息日 -> ex_date
  - A股股权登记日 -> record_date
  - 分红方案说明 -> plan_desc
  - 实施公告日 -> notice_date
"""
import akshare as ak
import pandas as pd
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
from tqdm import tqdm
from datetime import datetime, date
import time
import re
import traceback

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
CLIENT = MongoClient(MONGO_HOST, MONGO_PORT)
DB = CLIENT[DB_NAME]

COL_INFO = DB["stock_info"]
COL_DIVIDEND = DB["finance_dividend"]

# ================= 配置区域 =================
# 调试模式: True=只跑测试股; False=跑全量
DEBUG_MODE = False
DEBUG_SYMBOLS = ["600519", "601398"]

# 强制更新: False=断点续传; True=覆盖更新
FORCE_UPDATE = False
# ===========================================

def to_datetime_obj(dt_obj):
    """安全转换为 datetime 对象 (用于 MongoDB 存储)"""
    if dt_obj is None or str(dt_obj) in ['nan', 'NaT', 'None', '', '--']:
        return None
    try:
        if isinstance(dt_obj, datetime): return dt_obj
        elif isinstance(dt_obj, date): return datetime.combine(dt_obj, datetime.min.time())
        elif isinstance(dt_obj, pd.Timestamp): return dt_obj.to_pydatetime()
        elif isinstance(dt_obj, str):
            # 尝试解析 '2025-08-29'
            return datetime.strptime(dt_obj[:10], "%Y-%m-%d")
    except:
        return None
    return None

def parse_ths_bonus(plan_str):
    """
    解析同花顺分红方案说明
    示例: "10派3.6元(含税)", "10转4股派5元"
    """
    if not isinstance(plan_str, str): return 0.0, 0.0

    cash_div = 0.0
    share_div = 0.0

    try:
        # 1. 现金 (10派X)
        if "派" in plan_str:
            cash_match = re.search(r'派([\d\.]+)', plan_str)
            if cash_match:
                cash_div = float(cash_match.group(1)) / 10.0 # 转为每股

        # 2. 送转 (10送X 或 10转X)
        if "送" in plan_str:
            song_match = re.search(r'送([\d\.]+)', plan_str)
            if song_match:
                share_div += float(song_match.group(1)) / 10.0
        if "转" in plan_str:
            zhuan_match = re.search(r'转([\d\.]+)', plan_str)
            if zhuan_match:
                share_div += float(zhuan_match.group(1)) / 10.0

    except:
        pass
    return cash_div, share_div

def download_one_stock(symbol: str):
    try:
        # 接口: 同花顺-分红融资
        df = ak.stock_fhps_detail_ths(symbol=symbol)

        if df.empty:
            return []

        # --- 关键列名映射 (根据用户提供的结构) ---
        # 原始列: 报告期, 董事会日期, 股东大会预案公告日期, 实施公告日, 分红方案说明, A股股权登记日, A股除权除息日, 分红总额, 方案进度, ...

        # 检查关键列是否存在
        if "A股除权除息日" not in df.columns:
            # print(f"   ⚠️ {symbol} 缺少 'A股除权除息日' 列")
            return []

        # 1. 过滤无效的除权日 (NaT 或 --)
        # 很多预案阶段的数据没有除权日，必须剔除
        df = df.dropna(subset=['A股除权除息日'])

        updates = []
        for _, row in df.iterrows():
            ex_date_raw = row['A股除权除息日']

            # 2. 日期清洗
            ex_date_dt = to_datetime_obj(ex_date_raw)
            if not ex_date_dt:
                continue

            # 3. 方案解析
            plan_str = row.get('分红方案说明', '')
            # 同花顺有时只写 "不分配"，需要跳过
            if "不分配" in str(plan_str):
                continue

            cash_per_share, share_per_share = parse_ths_bonus(str(plan_str))

            # 如果解析结果全是0，且不是送股，则跳过
            if cash_per_share == 0 and share_per_share == 0:
                continue

            doc = {
                "symbol": symbol,
                "ex_date": ex_date_dt, # 必须是 datetime
                "record_date": to_datetime_obj(row.get('A股股权登记日')),
                "cash_dividend_per_share": float(cash_per_share),
                "stock_dividend_per_share": float(share_per_share),
                "plan_desc": str(plan_str),
                "notice_date": to_datetime_obj(row.get('实施公告日')),
                "progress": str(row.get('方案进度', '')) # 额外保存进度状态
            }

            # 唯一键: symbol + ex_date
            updates.append(UpdateOne(
                {"symbol": symbol, "ex_date": doc["ex_date"]},
                {"$set": doc},
                upsert=True
            ))

        return updates

    except Exception as e:
        # print(f"   ❌ 异常 {symbol}: {e}")
        # traceback.print_exc()
        return []

def get_existing_symbols():
    """断点续传：获取已下载的股票"""
    if FORCE_UPDATE: return set()
    return set(COL_DIVIDEND.distinct("symbol"))

def run():
    print(f"🚀 启动 [分红数据下载器 V4 - 同花顺适配] (模式: {'DEBUG' if DEBUG_MODE else 'PRODUCTION'})...")

    COL_DIVIDEND.create_index([("symbol", ASCENDING), ("ex_date", DESCENDING)], unique=True)

    if DEBUG_MODE:
        tasks = [{"symbol": s} for s in DEBUG_SYMBOLS]
        print(f"⚠️ 调试模式: 仅处理 {len(tasks)} 只")
    else:
        all_stocks = list(COL_INFO.find({}, {"symbol": 1, "name": 1}))
        all_stocks = [s for s in all_stocks if not s['symbol'].startswith("8100")]

        existing = get_existing_symbols()
        if existing:
            print(f"   - 已存在: {len(existing)} 只")
            tasks = [s for s in all_stocks if s['symbol'] not in existing]
            print(f"   - 剩余任务: {len(tasks)} 只")
        else:
            tasks = all_stocks

    pbar = tqdm(tasks)
    success_cnt = 0

    for s in pbar:
        symbol = s['symbol']
        pbar.set_description(f"下载 {symbol}")

        ops = download_one_stock(symbol)
        if ops:
            COL_DIVIDEND.bulk_write(ops, ordered=False)
            success_cnt += 1

        time.sleep(0.2) # 同花顺建议稍慢一点

    print(f"\n🎉 下载完成！成功处理 {success_cnt} 只股票。")

if __name__ == "__main__":
    run()