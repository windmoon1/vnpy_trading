"""
脚本 08: 全市场每日估值指标计算器 (V18 - 股本合并修复版)
--------------------------------------------------------------
修复: 修改 Market Data 合并逻辑，由 Inner Join 改为 Left Join + FFill。
      解决因股本数据日期对齐问题导致的数据丢失（只剩几十条）的问题。
"""
import pandas as pd
from datetime import datetime, date
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
import numpy as np

# ================= 配置区域 =================
DEBUG_MODE = True  # 调试完成后改为 False
DEBUG_SYMBOLS = ["600519", "601398"]
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
COL_VALUATION = DB["valuation_daily"]
COL_INDUSTRY = DB["industry_history"]

# --- 关键字段映射 ---
NET_PROFIT_FIELDS = ["归属于母公司所有者的净利润", "归属于母公司股东的净利润", "归属于母公司的净利润", "净利润"]
REVENUE_FIELDS = ["营业总收入", "营业收入"]
EQUITY_FIELDS = ["归属于母公司股东权益合计", "归属于母公司股东的权益", "归属于上市公司股东的权益", "所有者权益合计", "股东权益合计"]
OTHER_EQUITY_FIELD = "其他权益工具"

def get_clean_financial_data(symbol: str) -> pd.DataFrame:
    """提取并清洗财务数据"""
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

def calculate_financial_time_series(df_fin: pd.DataFrame) -> pd.DataFrame:
    """计算 TTM 和 LF 数据"""
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

def calculate_one_stock(symbol: str):
    """单只股票计算逻辑"""

    # 1. 准备财务数据
    df_fin = get_clean_financial_data(symbol)
    if df_fin.empty: return []

    df_fin_pub = calculate_financial_time_series(df_fin)
    if df_fin_pub.empty: return []

    # 2. 准备市场数据
    cursor_bars = COL_BARS.find({"symbol": symbol}, {"datetime": 1, "close_price": 1}).sort("datetime", ASCENDING)
    df_bars = pd.DataFrame(list(cursor_bars))
    if df_bars.empty: return []
    df_bars['date'] = pd.to_datetime(df_bars['datetime'])
    df_bars = df_bars.set_index('date')[['close_price']]

    cursor_cap = COL_CAPITAL.find({"symbol": symbol}, {"date": 1, "total_shares": 1, "float_shares": 1}).sort("date", ASCENDING)
    df_cap = pd.DataFrame(list(cursor_cap))
    if df_cap.empty: return []
    df_cap['date'] = pd.to_datetime(df_cap['date'])
    df_cap = df_cap.set_index('date')[['total_shares', 'float_shares']]

    # 3. 合并市场数据 (【核心修复】：Left Join + FFill)
    # 以行情为准，股本对不上的地方向前填充
    df_market = df_bars.join(df_cap, how='left')
    df_market['total_shares'] = df_market['total_shares'].ffill()
    df_market['float_shares'] = df_market['float_shares'].ffill()

    # 删除上市前没有股本数据的行
    df_market = df_market.dropna(subset=['total_shares'])

    df_market = df_market.sort_index()
    df_fin_pub = df_fin_pub.sort_index()

    # 4. Merge AsOf (防未来)
    df_daily = pd.merge_asof(
        df_market,
        df_fin_pub[['equity_adjusted', 'net_profit_ttm', 'revenue_ttm', 'net_profit_lf', 'report_date_audit']],
        left_index=True,
        right_index=True,
        direction='backward'
    )

    # 5. 计算指标
    df_calc = df_daily.dropna(subset=['close_price', 'total_shares', 'equity_adjusted']).copy()
    if df_calc.empty: return []

    df_calc['total_mv'] = df_calc['close_price'] * df_calc['total_shares']
    df_calc['circ_mv'] = df_calc['close_price'] * df_calc['float_shares']
    df_calc['bps'] = df_calc['equity_adjusted'] / df_calc['total_shares']
    df_calc['eps_ttm'] = np.where(df_calc['net_profit_ttm'].notna(), df_calc['net_profit_ttm'] / df_calc['total_shares'], None)

    df_calc['pb_lf'] = df_calc['total_mv'] / df_calc['equity_adjusted']
    df_calc['pe_ttm'] = np.where(df_calc['net_profit_ttm'] > 0, df_calc['total_mv'] / df_calc['net_profit_ttm'], None)
    df_calc['pe_lf'] = np.where(df_calc['net_profit_lf'] > 0, df_calc['total_mv'] / df_calc['net_profit_lf'], None)
    df_calc['ps_ttm'] = np.where(df_calc['revenue_ttm'] > 0, df_calc['total_mv'] / df_calc['revenue_ttm'], None)
    df_calc['roe_ttm'] = np.where(df_calc['equity_adjusted'] > 0, df_calc['net_profit_ttm'] / df_calc['equity_adjusted'], None)
    df_calc['dv_ratio'] = None

    # 获取行业
    industry_doc = COL_INDUSTRY.find_one({"symbol": symbol}, sort=[("date", DESCENDING)])
    industry_name = industry_doc.get('industry_name', 'Unknown') if industry_doc else 'Unknown'

    updates = []
    for date_idx, row in df_calc.iterrows():
        report_dt = row['report_date_audit']
        report_dt_ts = datetime.combine(report_dt, datetime.min.time()) if isinstance(report_dt, date) else report_dt

        doc = {
            "symbol": symbol,
            "date": date_idx,
            "close_price": row['close_price'],
            "industry": industry_name,
            "total_shares": row['total_shares'],
            "float_shares": row['float_shares'],
            "total_mv": row['total_mv'],
            "circ_mv": row['circ_mv'],
            "bps": row['bps'],
            "eps_ttm": row['eps_ttm'],
            "pe_ttm": row['pe_ttm'],
            "pe_lf": row['pe_lf'],
            "pb_lf": row['pb_lf'],
            "ps_ttm": row['ps_ttm'],
            "dv_ratio": row['dv_ratio'],
            "roe_ttm": row['roe_ttm'],
            "net_profit_ttm": row['net_profit_ttm'] if pd.notna(row['net_profit_ttm']) else None,
            "net_profit_lf": row['net_profit_lf'] if pd.notna(row['net_profit_lf']) else None,
            "total_equity_latest": row['equity_adjusted'],
            "revenue_ttm": row['revenue_ttm'] if pd.notna(row['revenue_ttm']) else None,
            "report_date_pb": report_dt_ts,
            "publish_date_pb": date_idx
        }

        clean_doc = {}
        for k, v in doc.items():
            if v is None: continue
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)): continue
            if isinstance(v, pd.Timestamp) and pd.isna(v): continue
            clean_doc[k] = v

        updates.append(UpdateOne(
            {"symbol": symbol, "date": date_idx},
            {"$set": clean_doc},
            upsert=True
        ))

    return updates

def run_debug():
    """调试模式"""
    print(f"🛠️ 启动 [调试模式] - 目标: {DEBUG_SYMBOLS}")
    COL_VALUATION.create_index([("symbol", ASCENDING), ("date", ASCENDING)], unique=True)

    for symbol in DEBUG_SYMBOLS:
        print(f"\n⚡ 处理: {symbol}")
        try:
            updates = calculate_one_stock(symbol)
            if not updates:
                print(f"   ⚠️ 无数据")
                continue

            print(f"   ✅ 生成 {len(updates)} 条记录")
            first, last = updates[0]._doc['$set'], updates[-1]._doc['$set']
            print(f"   📅 [首] {first['date']} | PE: {first.get('pe_ttm')} | PB: {first.get('pb_lf')}")
            print(f"   📅 [末] {last['date']} | PE: {last.get('pe_ttm')} | PB: {last.get('pb_lf')}")

            print(f"   💾 写入 DB...")
            COL_VALUATION.bulk_write(updates, ordered=False)
            print("   OK.")

        except Exception as e:
            print(f"   ❌ 错误: {e}")
            import traceback; traceback.print_exc()

def run_production():
    """生产模式"""
    print("🚀 启动 [生产模式]...")
    COL_VALUATION.create_index([("symbol", ASCENDING), ("date", ASCENDING)], unique=True)

    stocks = list(COL_INFO.find({}, {"symbol": 1, "name": 1}))
    tasks = [s for s in stocks if not s['symbol'].startswith("8100")]
    print(f"📋 任务数: {len(tasks)}")

    batch = []
    for s in tqdm(tasks):
        try:
            ops = calculate_one_stock(s['symbol'])
            if ops:
                batch.extend(ops)
            if len(batch) >= 5000:
                COL_VALUATION.bulk_write(batch, ordered=False)
                batch = []
        except: continue

    if batch:
        COL_VALUATION.bulk_write(batch, ordered=False)
    print("\n🎉 完成！")

if __name__ == "__main__":
    if DEBUG_MODE:
        run_debug()
    else:
        run_production()