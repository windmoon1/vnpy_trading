"""
Script 07: Download Share Capital History (MongoDB Version) - Fixed
-------------------------------------------------------------------
修复记录:
1. 适配 AKShare stock_share_change_cninfo 返回的新列名 (总股本/已流通股份)
2. 修正单位问题: 源数据为[万股], 入库转换为 [股]
3. 增加 start_date 参数，确保拉取完整历史数据
"""

import akshare as ak
import pandas as pd
import time
from datetime import datetime
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne

# ==========================================
# 配置项 (Configuration)
# ==========================================
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
COLLECTION_NAME = "share_capital"

def get_db():
    """获取数据库连接"""
    client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    return client[DB_NAME]

def get_stock_list() -> list:
    """获取待下载的股票列表"""
    db = get_db()

    # 尝试 1: 从基础信息表获取
    cursor = db["stock_info"].find({}, {"symbol": 1})
    symbols = [doc["symbol"] for doc in cursor]

    # 尝试 2: 如果为空，从行情表获取
    if not symbols:
        symbols = db["bar_daily"].distinct("symbol")

    # 尝试 3: 在线兜底
    if not symbols:
        print("⚠️ 本地数据库无股票列表，从 AKShare 在线获取全A股列表...")
        try:
            df = ak.stock_zh_a_spot_em()
            symbols = df['code'].tolist()
        except Exception as e:
            print(f"❌ 在线获取失败: {e}")
            return []

    return sorted(list(set(symbols)))

def download_and_save(symbol: str, db):
    """
    下载单个股票的股本变动并存入 MongoDB
    """
    try:
        # 1. 接口调用
        # 显式指定 start_date 为很早的日期，确保拿到上市以来的所有变动
        current_date = datetime.now().strftime("%Y%m%d")
        df = ak.stock_share_change_cninfo(
            symbol=symbol,
            start_date="19900101",
            end_date=current_date
        )

        if df is None or df.empty:
            return

        # 2. 字段映射 (根据 Debug 结果修正)
        # 原始列: ['变动日期', '总股本', '已流通股份', '变动原因', ...]
        rename_map = {
            '变动日期': 'date',
            '总股本': 'total_shares',
            '已流通股份': 'float_shares',
            '变动原因': 'change_reason'
        }

        # 检查关键列是否存在
        if not set(rename_map.keys()).issubset(df.columns):
            # print(f"⚠️ {symbol} 列名不匹配，跳过")
            return

        df = df.rename(columns=rename_map)

        # 3. 数据清洗
        # 日期格式化: datetime.date -> str (YYYY-MM-DD)
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

        # 数值清洗:
        # a. 填充 NaN 为 0
        # b. 单位转换: 万股 -> 股 (* 10000)
        def clean_shares(val):
            if pd.isna(val) or val == '':
                return 0.0
            try:
                # 假设源数据单位是 万股
                return float(val) * 10000
            except:
                return 0.0

        df['total_shares'] = df['total_shares'].apply(clean_shares)
        df['float_shares'] = df['float_shares'].apply(clean_shares)

        # 4. 构造写入操作 (Upsert)
        requests = []
        for _, row in df.iterrows():
            filter_doc = {
                "symbol": symbol,
                "date": row["date"]
            }
            update_doc = {
                "$set": {
                    "total_shares": row["total_shares"],
                    "float_shares": row["float_shares"],
                    "change_reason": row["change_reason"],
                    "updated_at": datetime.now()
                }
            }
            requests.append(UpdateOne(filter_doc, update_doc, upsert=True))

        if requests:
            db[COLLECTION_NAME].bulk_write(requests)

    except Exception as e:
        # print(f"Error {symbol}: {e}")
        pass

def run():
    print("🚀 启动 [A股股本变动下载器] (Fixed Version)...")
    print("📋 配置: 单位[万股->股] | 历史回溯[1990+]")

    db = get_db()
    symbols = get_stock_list()
    print(f"📊 目标股票数量: {len(symbols)}")

    if not symbols:
        return

    # 简单进度条
    pbar = tqdm(symbols)
    for symbol in pbar:
        pbar.set_description(f"下载 {symbol}")
        download_and_save(symbol, db)
        # 稍微快一点，cninfo 接口通常比较耐抗，但还是保留微小延时
        time.sleep(0.1)

    print("\n✅ 下载完成。请运行 verify_share_capital.py 进行最终核验。")

if __name__ == "__main__":
    run()