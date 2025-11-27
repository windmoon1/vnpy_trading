"""
Script 07-C: Fill Delisted Capital (Remote Rescue / Calculation Mode)
---------------------------------------------------------------------
场景:
本地财报库 (Script 06) 尚未覆盖退市股票，且巨潮官方接口已移除相关数据。

方案:
主动请求新浪财经 [财务摘要] 接口，利用会计恒等式反推股本:
[总股本] = [股东权益合计(净资产)] / [每股净资产]

目标:
快速修复 285+ 只退市股票的 share_capital 数据，确保回测系统闭环。
"""

import akshare as ak
import pandas as pd
import time
import random
from datetime import datetime
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne

# ==========================================
# 配置
# ==========================================
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
COLLECTION_NAME = "share_capital"


def get_db():
    return MongoClient(MONGO_HOST, MONGO_PORT)[DB_NAME]


def get_rescue_targets(db):
    """
    找出 [已退市] 且 [share_capital 依然为空] 的急救名单
    """
    print("🔍 扫描急救名单...")

    # 1. 所有退市股
    delisted_cursor = db["stock_info"].find({"status": "DELISTED"}, {"symbol": 1, "name": 1})
    delisted_map = {doc["symbol"]: doc.get("name", "") for doc in delisted_cursor}
    delisted_symbols = set(delisted_map.keys())

    if not delisted_symbols:
        print("⚠️ 警告: 未找到退市股票标记，请先运行 Script 04。")
        return []

    # 2. 已有数据的
    existing_symbols = set(db[COLLECTION_NAME].distinct("symbol"))

    # 3. 差集
    targets = sorted(list(delisted_symbols - existing_symbols))

    print("=" * 40)
    print(f"👻 退市总数: {len(delisted_symbols)}")
    print(f"✅ 已有数据: {len(delisted_symbols & existing_symbols)}")
    print(f"🚑 需救援数: {len(targets)}")
    print("=" * 40)
    return targets


def fetch_and_calc_shares(symbol: str):
    """
    核心逻辑:
    1. 下载新浪财务摘要 (Abstract)
    2. 提取 [股东权益] 和 [每股净资产]
    3. 计算 [股本] 并构建时间序列
    """
    try:
        # 接口: 新浪财经-财务摘要
        df = ak.stock_financial_abstract(symbol=symbol)

        if df is None or df.empty:
            return None

        # 1. 定位关键行
        # 新浪返回列: ['选项', '指标', '20241231', ...]
        # 我们需要按 '指标' 列的内容来筛选
        indicator_col = '指标'
        if indicator_col not in df.columns: return None

        # 模糊匹配
        mask_equity = df[indicator_col].astype(str).str.contains("股东权益", na=False)
        mask_nav = df[indicator_col].astype(str).str.contains("每股净资产", na=False)

        if not mask_equity.any() or not mask_nav.any():
            # 缺少核心字段，无法计算
            return None

        row_equity = df[mask_equity].iloc[0]
        row_nav = df[mask_nav].iloc[0]

        # 2. 遍历日期列进行计算
        # 排除非日期列 ('选项', '指标' 等)
        date_cols = [c for c in df.columns if c.isdigit() and len(c) == 8]

        data_list = []
        for date_str in date_cols:
            try:
                # 提取数值
                equity_val = row_equity[date_str]
                nav_val = row_nav[date_str]

                # 基础清洗
                if pd.isna(equity_val) or pd.isna(nav_val): continue

                equity = float(equity_val)
                nav = float(nav_val)

                # 避免除零
                if abs(nav) < 0.001: continue

                # ---------------------------
                # 核心公式: Shares = Equity / NAV
                # ---------------------------
                calc_shares = equity / nav

                # 3. 单位自适应修正 (Heuristic Adjustment)
                # 场景 A: Equity 单位是 [元], NAV 是 [元] -> 结果是 [股] (正确)
                # 场景 B: Equity 单位是 [万元], NAV 是 [元] -> 结果偏小 10000倍
                # 判据: A股极少有总股本小于 1000万股 的
                if calc_shares < 10000000:
                    calc_shares *= 10000

                # 格式化日期
                fmt_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")

                data_list.append({
                    "symbol": symbol,
                    "date": fmt_date,
                    "total_shares": calc_shares,
                    "float_shares": calc_shares,  # 退市股简化处理: 假设全流通
                    "change_reason": "Rescue_Calc_Equity_NAV",
                    "updated_at": datetime.now()
                })
            except:
                continue

        return data_list

    except Exception as e:
        print(f"⚠️ {symbol} Err: {e}")
        return None


def save_to_db(data_list, db):
    if not data_list: return

    requests = []
    for item in data_list:
        # 唯一索引: symbol + date
        filter_doc = {"symbol": item["symbol"], "date": item["date"]}
        update_doc = {"$set": item}
        requests.append(UpdateOne(filter_doc, update_doc, upsert=True))

    if requests:
        db[COLLECTION_NAME].bulk_write(requests)


def run_rescue():
    print("🚀 启动 [退市股本数据远程救援]...")
    print("📋 策略: 新浪摘要 -> 反推股本")

    db = get_db()
    targets = get_rescue_targets(db)

    if not targets:
        print("🎉 恭喜！所有退市股票数据已完整。")
        return

    pbar = tqdm(targets)
    success_count = 0

    for symbol in pbar:
        pbar.set_description(f"救援 {symbol}")

        data = fetch_and_calc_shares(symbol)

        if data:
            save_to_db(data, db)
            success_count += 1

        # 关键: 新浪接口风控较严，必须加延时
        time.sleep(random.uniform(10, 20))

    print(f"\n✅ 救援行动结束。")
    print(f"   成功恢复: {success_count} / {len(targets)}")
    print("   (注: 剩余未恢复的股票可能在新浪也无财务记录，建议放弃)")


if __name__ == "__main__":
    try:
        run_rescue()
    except KeyboardInterrupt:
        print("\n🛑 用户手动停止")