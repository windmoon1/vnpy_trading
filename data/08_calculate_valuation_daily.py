"""
脚本 08 (V23 - 修复版): 全市场每日估值指标计算器
--------------------------------------------------------------
更新重点:
1. [双轨制股本]
   - 流通市值 (Circ MV) = 收盘价 * 日线.outstanding_share (精确A股流通)
   - 总市值 (Total MV) = 收盘价 * 股本表.total_shares (用于PE/PB)
2. [健壮性] 增加对缺失股本的处理，避免程序崩溃。
"""
import pandas as pd
from datetime import datetime, date, timedelta
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
import numpy as np

# ================= 配置区域 =================
DEBUG_MODE = False  # 生产环境请设为 False
DEBUG_SYMBOLS = ["601336"]
FORCE_UPDATE = False # 设为 True 可重算所有历史数据
# ===========================================

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

NET_PROFIT_FIELDS = ["归属于母公司所有者的净利润", "归属于母公司股东的净利润", "归属于母公司的净利润", "净利润"]
REVENUE_FIELDS = ["营业总收入", "营业收入"]
EQUITY_FIELDS = ["归属于母公司股东权益合计", "归属于母公司股东的权益", "归属于上市公司股东的权益", "所有者权益合计", "股东权益合计"]
OTHER_EQUITY_FIELD = "其他权益工具"

def get_last_update_date(symbol: str):
    if FORCE_UPDATE: return None
    last_record = COL_VALUATION.find_one({"symbol": symbol}, sort=[("date", DESCENDING)], projection={"date": 1})
    return last_record["date"] if last_record else None

def get_clean_financial_data(symbol: str) -> pd.DataFrame:
    """提取并清洗财务数据 (保持不变)"""
    proj_bal = {"report_date": 1, "publish_date": 1, OTHER_EQUITY_FIELD: 1}
    for f in EQUITY_FIELDS: proj_bal[f] = 1
    proj_inc = {"report_date": 1, "publish_date": 1}
    for f in NET_PROFIT_FIELDS + REVENUE_FIELDS: proj_inc[f] = 1

    cursor_bal = COL_BALANCE.find({"symbol": symbol}, proj_bal).sort("report_date", ASCENDING)
    df_bal = pd.DataFrame(list(cursor_bal))
    cursor_inc = COL_INCOME.find({"symbol": symbol}, proj_inc).sort("report_date", ASCENDING)
    df_inc = pd.DataFrame(list(cursor_inc))

    if df_bal.empty and df_inc.empty: return pd.DataFrame()

    if not df_bal.empty:
        df_bal[OTHER_EQUITY_FIELD] = pd.to_numeric(df_bal.get(OTHER_EQUITY_FIELD), errors='coerce').fillna(0)
        df_bal['total_equity'] = np.nan
        for col in EQUITY_FIELDS:
            if col in df_bal.columns:
                df_bal['total_equity'] = df_bal['total_equity'].fillna(pd.to_numeric(df_bal[col], errors='coerce'))
        df_bal['equity_adjusted'] = df_bal['total_equity'] - df_bal[OTHER_EQUITY_FIELD]
        df_bal = df_bal.rename(columns={'publish_date': 'publish_date_bal'})
        df_bal = df_bal[['report_date', 'publish_date_bal', 'equity_adjusted']].copy()

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
    """提取分红数据 (保持不变)"""
    cursor = COL_DIVIDEND.find({"symbol": symbol}, {"ex_date": 1, "cash_dividend_per_share": 1, "_id": 0}).sort("ex_date", ASCENDING)
    df = pd.DataFrame(list(cursor))
    if df.empty: return pd.DataFrame()
    df['ex_date'] = pd.to_datetime(df['ex_date'])
    df['cash_dividend_per_share'] = pd.to_numeric(df['cash_dividend_per_share'], errors='coerce').fillna(0.0)
    return df.set_index('ex_date')

def calculate_financial_time_series(df_fin: pd.DataFrame) -> pd.DataFrame:
    """计算财报指标流 (保持不变)"""
    if df_fin.empty: return pd.DataFrame()
    df_ttm = df_fin.copy().set_index('report_date').sort_index()

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
    """计算分红 (保持不变)"""
    if df_div.empty: return pd.DataFrame()
    start = df_div.index.min()
    end = datetime.now()
    idx = pd.date_range(start, end)
    df_daily = df_div.reindex(idx).fillna(0.0)
    # [MODIFIED]
    # 方案 1: 简单粗暴延长窗口到 13个月 (395天)，容忍派息日推迟一个月
    # 这能解决 90% 的“比东财少”的问题
    df_daily['dividend_ttm'] = df_daily['cash_dividend_per_share'].rolling(window=395, min_periods=0).sum()
    return df_daily[['dividend_ttm']]

def calculate_one_stock(symbol: str, name: str, industry: str):
    """单股计算逻辑 (已修正：使用日线流通股本)"""
    last_date = get_last_update_date(symbol)
    bars_query = {"symbol": symbol}
    cap_query = {"symbol": symbol} # 查全部股本变动

    if last_date:
        start_date = last_date + timedelta(days=1)
        bars_query["datetime"] = {"$gte": start_date}

    # 1. 获取行情 (含 outstanding_share)
    cursor_bars = COL_BARS.find(bars_query, {"datetime": 1, "close_price": 1, "outstanding_share": 1}).sort("datetime", ASCENDING)
    df_bars = pd.DataFrame(list(cursor_bars))
    if df_bars.empty: return []

    df_bars['date'] = pd.to_datetime(df_bars['datetime'])
    # 核心修正: 将日线里的股本重命名为 float_shares_daily
    df_bars = df_bars.set_index('date')[['close_price', 'outstanding_share']].rename(columns={'outstanding_share': 'float_shares_daily'})

    # 2. 获取总股本 (来自公告)
    # 我们只取 total_shares，忽略那个不准确的 float_shares
    cursor_cap = COL_CAPITAL.find(cap_query, {"date": 1, "total_shares": 1}).sort("date", ASCENDING)
    df_cap = pd.DataFrame(list(cursor_cap))

    if not df_cap.empty:
        df_cap['date'] = pd.to_datetime(df_cap['date'])
        df_cap = df_cap.set_index('date')[['total_shares']]
    else:
        df_cap = pd.DataFrame(columns=['total_shares'])

    # 3. 匹配 Total Shares
    df_market = df_bars.sort_index()
    df_cap = df_cap.sort_index()

    if not df_cap.empty:
        df_market = pd.merge_asof(df_market, df_cap, left_index=True, right_index=True, direction='backward')
    else:
        df_market['total_shares'] = np.nan

    df_market = df_market.dropna(subset=['close_price'])
    if df_market.empty: return []

    # 4. 匹配财务与分红
    df_fin = get_clean_financial_data(symbol)
    df_fin_pub = calculate_financial_time_series(df_fin)
    df_div = get_dividend_data(symbol)
    df_div_daily = calculate_dividend_full_series(df_div)

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

    if not df_div_daily.empty:
        df_calc = df_calc.join(df_div_daily, how='left')
        df_calc['dividend_ttm'] = df_calc['dividend_ttm'].fillna(0.0)
    else:
        df_calc['dividend_ttm'] = 0.0

    # 5. 计算指标
    # 注意: 计算流通市值时，优先用 float_shares_daily (日线准确值)
    # 如果日线里没有(比如刚上市前几天数据缺失)，用 total_shares 兜底
    df_calc['final_float_shares'] = df_calc['float_shares_daily'].fillna(df_calc['total_shares'])

    # 总市值 (Total MV) -> 用于 PE, PB
    df_calc['total_mv'] = df_calc['close_price'] * df_calc['total_shares']
    # 流通市值 (Circ MV) -> 用于 换手率, 小市值策略
    df_calc['circ_mv'] = df_calc['close_price'] * df_calc['final_float_shares']

    df_calc['dv_ratio'] = np.where(df_calc['close_price'] > 0, df_calc['dividend_ttm'] / df_calc['close_price'], 0.0)

    # 矢量化计算避免除零警告
    with np.errstate(divide='ignore', invalid='ignore'):
        # BPS / PB / ROE 分母是净资产/总股本 -> 使用 total_shares
        df_calc['bps'] = np.where(df_calc['total_shares'] > 0, df_calc['equity_adjusted'] / df_calc['total_shares'], None)
        df_calc['pb_lf'] = np.where(df_calc['equity_adjusted'] > 0, df_calc['total_mv'] / df_calc['equity_adjusted'], None)
        df_calc['pe_ttm'] = np.where(df_calc['net_profit_ttm'] > 0, df_calc['total_mv'] / df_calc['net_profit_ttm'], None)
        df_calc['pe_lf'] = np.where(df_calc['net_profit_lf'] > 0, df_calc['total_mv'] / df_calc['net_profit_lf'], None)
        df_calc['ps_ttm'] = np.where(df_calc['revenue_ttm'] > 0, df_calc['total_mv'] / df_calc['revenue_ttm'], None)
        df_calc['roe_ttm'] = np.where(df_calc['equity_adjusted'] > 0, df_calc['net_profit_ttm'] / df_calc['equity_adjusted'], None)
        df_calc['eps_ttm'] = np.where(df_calc['total_shares'] > 0, df_calc['net_profit_ttm'] / df_calc['total_shares'], None)

    # 6. 生成写入请求
    updates = []
    for date_idx, row in df_calc.iterrows():
        # 如果连流通市值都算不出来(没价格或没股本)，跳过
        if pd.isna(row['circ_mv']): continue

        report_dt = row.get('report_date_audit')
        report_dt_ts = datetime.combine(report_dt, datetime.min.time()) if isinstance(report_dt, date) else report_dt

        doc = {
            "symbol": symbol, "date": date_idx, "industry": industry,
            "close_price": row['close_price'],
            "total_mv": row['total_mv'],
            "circ_mv": row['circ_mv'],
            "total_shares": row['total_shares'],
            "float_shares": row['final_float_shares'], # 保存最终使用的流通股本
            "dv_ratio": row['dv_ratio'], "pb_lf": row['pb_lf'],
            "pe_ttm": row['pe_ttm'], "pe_lf": row['pe_lf'],
            "ps_ttm": row['ps_ttm'], "bps": row['bps'],
            "eps_ttm": row['eps_ttm'], "roe_ttm": row['roe_ttm'],
            "net_profit_ttm": row.get('net_profit_ttm'),
            "net_profit_lf": row.get('net_profit_lf'),
            "total_equity_latest": row.get('equity_adjusted'),
            "revenue_ttm": row.get('revenue_ttm'),
            "report_date_pb": report_dt_ts, "publish_date_pb": date_idx
        }

        # 清理 None 和 NaN
        clean_doc = {k: v for k, v in doc.items() if v is not None and not (isinstance(v, float) and np.isnan(v))}
        updates.append(UpdateOne({"symbol": symbol, "date": date_idx}, {"$set": clean_doc}, upsert=True))

    return updates

def run():
    print(f"🚀 启动 [全市场估值计算器 V23] (双轨制股本版)...")
    COL_VALUATION.create_index([("symbol", ASCENDING), ("date", ASCENDING)], unique=True)

    if DEBUG_MODE:
        tasks = [{"symbol": s} for s in DEBUG_SYMBOLS]
    else:
        stocks = list(COL_INFO.find({}, {"symbol": 1}))
        tasks = [s for s in stocks if not s['symbol'].startswith("8")] # 排除北交所8开头

    print(f"📋 任务数: {len(tasks)}")

    batch = []
    for s in tqdm(tasks):
        try:
            # 简单查一下行业
            ind_doc = COL_INDUSTRY.find_one({"symbol": s['symbol']}, sort=[("date", DESCENDING)])
            industry = ind_doc.get('industry_name', 'Unknown') if ind_doc else 'Unknown'

            ops = calculate_one_stock(s['symbol'], "", industry)
            if ops:
                batch.extend(ops)
                if len(batch) >= 2000:
                    COL_VALUATION.bulk_write(batch, ordered=False)
                    batch = []
        except Exception as e:
            if DEBUG_MODE: print(f"Err {s['symbol']}: {e}")

    if batch:
        COL_VALUATION.bulk_write(batch, ordered=False)
    print("\n✨ 全部完成.")

if __name__ == "__main__":
    run()