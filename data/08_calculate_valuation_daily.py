"""
脚本 08: 全市场每日估值指标计算器 (V22 - 增量更新生产版)
--------------------------------------------------------------
目标: 每日增量更新全市场估值指标 (PE/PB/PS/股息率等)。
特性:
1. [智能增量] 自动识别上次计算日期，只计算新产生的交易日。
2. [严谨逻辑] 财务/分红数据全量回溯，确保 TTM 窗口准确。
3. [工程优化] 支持断点续传，支持强制全量刷新。
"""
import pandas as pd
from datetime import datetime, date, timedelta
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
import numpy as np

# ================= 配置区域 =================
# 🟢 调试模式: True=只跑测试股; False=跑全量
DEBUG_MODE = True
DEBUG_SYMBOLS = ["600519", "601398"]

# 🟢 强制全量更新开关:
# False (默认): 增量模式。从数据库中最后一条日期往后计算。
# True : 覆盖模式。无视已有数据，重算所有历史数据。
FORCE_UPDATE = False
# ===========================================

# --- 数据库配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
CLIENT = MongoClient(MONGO_HOST, MONGO_PORT)
DB = CLIENT[DB_NAME]

COL_INFO = DB["stock_info"]
COL_BARS = DB["bar_daily"]
COL_CAPITAL = DB["share_capital"]
COL_INCOME = DB["finance_income"]
COL_BALANCE = DB["finance_balance"]
COL_DIVIDEND = DB["finance_dividend"]
COL_VALUATION = DB["valuation_daily"]
COL_INDUSTRY = DB["industry_history"]

# --- 字段映射 ---
NET_PROFIT_FIELDS = ["归属于母公司所有者的净利润", "归属于母公司股东的净利润", "归属于母公司的净利润", "净利润"]
REVENUE_FIELDS = ["营业总收入", "营业收入"]
EQUITY_FIELDS = ["归属于母公司股东权益合计", "归属于母公司股东的权益", "归属于上市公司股东的权益", "所有者权益合计", "股东权益合计"]
OTHER_EQUITY_FIELD = "其他权益工具"

def get_last_update_date(symbol: str):
    """获取数据库中该股票最后一次计算估值的日期"""
    if FORCE_UPDATE:
        return None

    last_record = COL_VALUATION.find_one(
        {"symbol": symbol},
        sort=[("date", DESCENDING)],
        projection={"date": 1}
    )
    if last_record:
        return last_record["date"]
    return None

def get_clean_financial_data(symbol: str) -> pd.DataFrame:
    """提取并清洗财务数据 (全量)"""
    proj_bal = {"report_date": 1, "publish_date": 1, OTHER_EQUITY_FIELD: 1}
    for f in EQUITY_FIELDS: proj_bal[f] = 1
    proj_inc = {"report_date": 1, "publish_date": 1}
    for f in NET_PROFIT_FIELDS + REVENUE_FIELDS: proj_inc[f] = 1

    cursor_bal = COL_BALANCE.find({"symbol": symbol}, proj_bal).sort("report_date", ASCENDING)
    df_bal = pd.DataFrame(list(cursor_bal))
    cursor_inc = COL_INCOME.find({"symbol": symbol}, proj_inc).sort("report_date", ASCENDING)
    df_inc = pd.DataFrame(list(cursor_inc))

    if df_bal.empty and df_inc.empty: return pd.DataFrame()

    # 1. 资产负债表
    if not df_bal.empty:
        df_bal[OTHER_EQUITY_FIELD] = pd.to_numeric(df_bal.get(OTHER_EQUITY_FIELD), errors='coerce').fillna(0)
        df_bal['total_equity'] = np.nan
        for col in EQUITY_FIELDS:
            if col in df_bal.columns:
                df_bal['total_equity'] = df_bal['total_equity'].fillna(pd.to_numeric(df_bal[col], errors='coerce'))
        df_bal['equity_adjusted'] = df_bal['total_equity'] - df_bal[OTHER_EQUITY_FIELD]
        df_bal = df_bal.rename(columns={'publish_date': 'publish_date_bal'})
        df_bal = df_bal[['report_date', 'publish_date_bal', 'equity_adjusted']].copy()

    # 2. 利润表
    if not df_inc.empty:
        df_inc['net_profit'] = np.nan
        for col in NET_PROFIT_FIELDS:
            if col in df_inc.columns:
                df_inc['net_profit'] = df_inc['net_profit'].fillna(pd.to_numeric(df_inc[col], errors='coerce'))
        df_inc['revenue'] = np.nan
        for col in REVENUE_FIELDS:
            if col in df_inc.columns:
                df_inc['revenue'] = df_inc['revenue'].fillna(pd.to_numeric(df_inc[col], errors='coerce'))
        df_inc = df_inc.rename(columns={'publish_date': 'publish_date_inc'})
        df_inc = df_inc[['report_date', 'publish_date_inc', 'net_profit', 'revenue']].copy()

    # 3. 合并
    if df_bal.empty: df = df_inc
    elif df_inc.empty: df = df_bal
    else:
        df = pd.merge(df_inc, df_bal, on='report_date', how='outer')

    df['report_date'] = pd.to_datetime(df['report_date'])
    df['publish_date'] = df['publish_date_inc'].fillna(df['publish_date_bal'])
    df['publish_date'] = pd.to_datetime(df['publish_date'])

    df = df.dropna(subset=['report_date', 'publish_date'])
    df = df.sort_values('publish_date').drop_duplicates('report_date', keep='last').sort_values('report_date')

    return df

def get_dividend_data(symbol: str) -> pd.DataFrame:
    """提取分红数据 (全量)"""
    cursor = COL_DIVIDEND.find({"symbol": symbol}, {"ex_date": 1, "cash_dividend_per_share": 1, "_id": 0}).sort("ex_date", ASCENDING)
    df = pd.DataFrame(list(cursor))
    if df.empty: return pd.DataFrame()
    df['ex_date'] = pd.to_datetime(df['ex_date'])
    df['cash_dividend_per_share'] = pd.to_numeric(df['cash_dividend_per_share'], errors='coerce').fillna(0.0)
    return df.set_index('ex_date')

def calculate_financial_time_series(df_fin: pd.DataFrame) -> pd.DataFrame:
    """计算财报指标流 (TTM/LF)"""
    if df_fin.empty: return pd.DataFrame()

    df_ttm = df_fin.copy().set_index('report_date').sort_index()

    # TTM 滚动
    for metric in ['net_profit', 'revenue']:
        ttm_col = f"{metric}_ttm"
        df_ttm[ttm_col] = np.nan
        for date_idx in df_ttm.index:
            if date_idx.month == 12:
                df_ttm.loc[date_idx, ttm_col] = df_ttm.loc[date_idx, metric]
            elif date_idx.month in [3, 6, 9]:
                last_year = date_idx.year - 1
                try:
                    prev_same = df_ttm.at[date_idx.replace(year=last_year), metric]
                    prev_ann = df_ttm.at[datetime(last_year, 12, 31), metric]
                    if pd.notna(prev_same) and pd.notna(prev_ann):
                        df_ttm.loc[date_idx, ttm_col] = df_ttm.loc[date_idx, metric] + (prev_ann - prev_same)
                except KeyError: pass

    df_q4 = df_ttm[df_ttm.index.month == 12].copy()
    df_q4 = df_q4[['net_profit']].rename(columns={'net_profit': 'net_profit_lf'})

    df_ttm = df_ttm.reset_index()
    df_ttm = pd.merge(df_ttm, df_q4, left_on='report_date', right_index=True, how='left')

    df_pub = df_ttm.dropna(subset=['publish_date']).sort_values('publish_date')
    df_pub['net_profit_lf'] = df_pub['net_profit_lf'].ffill()
    df_pub['report_date_audit'] = df_pub['report_date']

    return df_pub.set_index('publish_date')

def calculate_dividend_full_series(df_div: pd.DataFrame) -> pd.DataFrame:
    """
    计算全历史每日滚动的 TTM 分红
    注意: 即使只计算最近1天的估值，我们也需要完整的历史分红来计算 rolling sum。
    计算全量比复杂的切片逻辑更安全且不慢。
    """
    if df_div.empty: return pd.DataFrame()

    start = df_div.index.min()
    end = datetime.now() # 延伸到今天
    idx = pd.date_range(start, end)
    df_daily = df_div.reindex(idx).fillna(0.0)

    # 核心: 过去 365 天的分红总和
    df_daily['dividend_ttm'] = df_daily['cash_dividend_per_share'].rolling(window=365, min_periods=0).sum()

    return df_daily[['dividend_ttm']]

def calculate_one_stock(symbol: str, name: str, industry: str):
    """单股计算逻辑 (支持增量)"""

    # --- 1. 确定计算时间范围 ---
    last_date = get_last_update_date(symbol)
    start_date = None

    # 查询条件
    bars_query = {"symbol": symbol}
    cap_query = {"symbol": symbol}

    if last_date:
        start_date = last_date + timedelta(days=1)
        # 增量查询：只查上次更新之后的行情
        bars_query["datetime"] = {"$gte": start_date}
        # 股本查询：我们查全量或从start_date查均可。
        # 为了 ffill 安全，查全量或从 start_date 前一条查比较稳妥。
        # 简单起见，增量模式下股本依然查全量 (数据量极小)，确保 ffill 正确。

    # --- 2. 获取数据 ---
    # 2.1 市场数据
    # 增量模式下，df_bars 只包含新产生的K线
    cursor_bars = COL_BARS.find(bars_query, {"datetime": 1, "close_price": 1, "outstanding_share": 1}).sort("datetime", ASCENDING)
    df_bars = pd.DataFrame(list(cursor_bars))

    if df_bars.empty:
        # 没有新数据，直接返回
        return []

    df_bars['date'] = pd.to_datetime(df_bars['datetime'])
    df_bars = df_bars.set_index('date')[['close_price']]

    # 股本 (查全量以保证 ffill 连续性)
    cursor_cap = COL_CAPITAL.find(cap_query, {"date": 1, "total_shares": 1, "float_shares": 1, "float_shares_a": 1}).sort("date", ASCENDING)
    df_cap = pd.DataFrame(list(cursor_cap))
    if df_cap.empty: return []
    df_cap['date'] = pd.to_datetime(df_cap['date'])
    df_cap = df_cap.set_index('date')[['total_shares', 'float_shares']]

    # 2.2 财务 & 分红 (查全量，因为需要历史窗口)
    df_fin = get_clean_financial_data(symbol)
    df_fin_pub = calculate_financial_time_series(df_fin) # 稀疏时间序列

    df_div = get_dividend_data(symbol)
    df_div_daily = calculate_dividend_full_series(df_div) # 密集日线序列 (全历史)

    # --- 3. 合并数据 ---
    # 将股本并入行情 (Left Join: 也就是只保留我们要计算的那几天)
    df_market = df_bars.join(df_cap, how='left')

    # FFILL 股本: 这里的 trick 是，如果 df_market 是增量的，第一行可能纳不到股本。
    # 所以我们应该先对 df_cap 做一个截至到今天的 ffill，或者 merge_asof。
    # 更稳妥的方法：
    # 使用 merge_asof 将 股本 匹配到 行情 (direction='backward')
    df_market = df_market.sort_index()
    df_cap = df_cap.sort_index()

    # 临时移除列以便重新匹配
    if 'total_shares' in df_market.columns: del df_market['total_shares']
    if 'float_shares' in df_market.columns: del df_market['float_shares']

    df_market = pd.merge_asof(
        df_market,
        df_cap,
        left_index=True,
        right_index=True,
        direction='backward'
    )

    # 过滤掉还未上市(无股本)的早期数据
    df_market = df_market.dropna(subset=['total_shares'])
    if df_market.empty: return []

    # 3.1 Merge 财务 (ASOF)
    # 将全量财务历史匹配到增量的行情日期上
    if not df_fin_pub.empty:
        df_fin_pub = df_fin_pub.sort_index()
        df_calc = pd.merge_asof(
            df_market,
            df_fin_pub[['equity_adjusted', 'net_profit_ttm', 'revenue_ttm', 'net_profit_lf', 'report_date_audit']],
            left_index=True, right_index=True, direction='backward'
        )
    else:
        df_calc = df_market.copy()
        for col in ['equity_adjusted', 'net_profit_ttm', 'revenue_ttm', 'net_profit_lf', 'report_date_audit']:
            df_calc[col] = np.nan

    # 3.2 Merge 分红 (Left Join)
    # df_div_daily 是全历史日线，df_calc 是增量日线
    if not df_div_daily.empty:
        df_calc = df_calc.join(df_div_daily, how='left')
        df_calc['dividend_ttm'] = df_calc['dividend_ttm'].fillna(0.0)
    else:
        df_calc['dividend_ttm'] = 0.0

    # --- 4. 计算指标 ---
    df_calc = df_calc.dropna(subset=['close_price', 'total_shares', 'equity_adjusted']).copy()
    if df_calc.empty: return []

    df_calc['total_mv'] = df_calc['close_price'] * df_calc['total_shares']
    df_calc['circ_mv'] = df_calc['close_price'] * df_calc['float_shares']
    df_calc['dv_ratio'] = np.where(df_calc['close_price'] > 0, df_calc['dividend_ttm'] / df_calc['close_price'], 0.0)

    with np.errstate(divide='ignore', invalid='ignore'):
        df_calc['bps'] = np.where(df_calc['equity_adjusted'].notna(), df_calc['equity_adjusted'] / df_calc['total_shares'], None)
        df_calc['pb_lf'] = np.where(df_calc['equity_adjusted'] > 0, df_calc['total_mv'] / df_calc['equity_adjusted'], None)
        df_calc['pe_ttm'] = np.where(df_calc['net_profit_ttm'] > 0, df_calc['total_mv'] / df_calc['net_profit_ttm'], None)
        df_calc['pe_lf'] = np.where(df_calc['net_profit_lf'] > 0, df_calc['total_mv'] / df_calc['net_profit_lf'], None)
        df_calc['ps_ttm'] = np.where(df_calc['revenue_ttm'] > 0, df_calc['total_mv'] / df_calc['revenue_ttm'], None)
        df_calc['roe_ttm'] = np.where(df_calc['equity_adjusted'] > 0, df_calc['net_profit_ttm'] / df_calc['equity_adjusted'], None)
        df_calc['eps_ttm'] = np.where(df_calc['net_profit_ttm'].notna(), df_calc['net_profit_ttm'] / df_calc['total_shares'], None)

    # --- 5. 生成 Updates ---
    updates = []
    for date_idx, row in df_calc.iterrows():
        report_dt = row.get('report_date_audit')
        report_dt_ts = datetime.combine(report_dt, datetime.min.time()) if isinstance(report_dt, date) else report_dt

        doc = {
            "symbol": symbol, "date": date_idx, "industry": industry,
            "close_price": row['close_price'],
            "total_mv": row['total_mv'], "circ_mv": row['circ_mv'],
            "total_shares": row['total_shares'], "float_shares": row['float_shares'],
            "dv_ratio": row['dv_ratio'], "pb_lf": row['pb_lf'],
            "pe_ttm": row['pe_ttm'], "pe_lf": row['pe_lf'],
            "ps_ttm": row['ps_ttm'], "bps": row['bps'],
            "eps_ttm": row['eps_ttm'], "roe_ttm": row['roe_ttm'],
            "net_profit_ttm": row.get('net_profit_ttm') if pd.notna(row.get('net_profit_ttm')) else None,
            "net_profit_lf": row.get('net_profit_lf') if pd.notna(row.get('net_profit_lf')) else None,
            "total_equity_latest": row.get('equity_adjusted'),
            "revenue_ttm": row['revenue_ttm'] if pd.notna(row['revenue_ttm']) else None,
            "report_date_pb": report_dt_ts, "publish_date_pb": date_idx
        }

        clean_doc = {k: v for k, v in doc.items() if v is not None and not (isinstance(v, float) and np.isnan(v))}
        updates.append(UpdateOne({"symbol": symbol, "date": date_idx}, {"$set": clean_doc}, upsert=True))

    return updates

def run():
    mode_str = "全量覆盖" if FORCE_UPDATE else "增量更新"
    print(f"🚀 启动 [全市场估值计算器 V22] ({mode_str})...")

    COL_VALUATION.create_index([("symbol", ASCENDING), ("date", ASCENDING)], unique=True)

    if DEBUG_MODE:
        tasks = [{"symbol": s} for s in DEBUG_SYMBOLS]
    else:
        stocks = list(COL_INFO.find({}, {"symbol": 1, "name": 1}))
        tasks = [s for s in stocks if not s['symbol'].startswith("8100")]

    print(f"📋 扫描任务: {len(tasks)} 只股票")

    batch = []
    processed_count = 0

    for s in tqdm(tasks):
        symbol = s['symbol']
        name = DB["stock_info"].find_one({"symbol": symbol}).get('name', symbol)
        ind = DB["industry_history"].find_one({"symbol": symbol}, sort=[("date", DESCENDING)])
        industry = ind.get('industry_name', 'Unknown') if ind else 'Unknown'

        try:
            ops = calculate_one_stock(symbol, name, industry)
            if ops:
                batch.extend(ops)
                processed_count += 1

                if DEBUG_MODE:
                    latest = ops[-1]._doc['$set']
                    print(f"   ✅ {symbol} 新增 {len(ops)} 条. 最新: {latest['date']} PE:{latest.get('pe_ttm')}")

            if len(batch) >= 5000:
                COL_VALUATION.bulk_write(batch, ordered=False)
                batch = []
        except Exception as e:
            if DEBUG_MODE: print(f"❌ Error {symbol}: {e}")
            continue

    if batch:
        COL_VALUATION.bulk_write(batch, ordered=False)

    print(f"\n🎉 计算完成！实际更新了 {processed_count} 只股票的数据。")

if __name__ == "__main__":
    run()