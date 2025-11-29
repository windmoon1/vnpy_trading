"""
脚本 08 (DEBUG V11): 估值指标计算器 - 生产健壮性最终版
--------------------------------------------------------------
目标: 1. 打印所有原始字段供用户审计 (核心需求)。
      2. 启用健壮的 TTM 滚动和 PB/BPS 报告期更新逻辑。
      3. 确保程序稳定运行。
"""
import pandas as pd
from datetime import datetime, date
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
import numpy as np
from typing import List, Dict, Any

# --- 配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
CLIENT = MongoClient(MONGO_HOST, MONGO_PORT)
DB = CLIENT[DB_NAME]

# 集合定义 (保持不变)
COL_INFO = DB["stock_info"]
COL_BARS = DB["bar_daily"]
COL_CAPITAL = DB["share_capital"]
COL_INCOME = DB["finance_income"]
COL_BALANCE = DB["finance_balance"]
COL_INDUSTRY = DB["industry_history"]

# 关键财务字段 (用于计算逻辑)
NET_PROFIT_FIELD = "净利润"
REVENUE_FIELDS_CANDIDATE = ["营业总收入", "营业收入"]
EQUITY_FIELDS_CANDIDATE = [
    "归属于母公司股东权益合计",
    "归属于母公司股东的权益",
    "归属于上市公司股东的权益",
    "所有者权益合计",
    "股东权益合计",
]
FINANCIAL_UNIT_CONVERSION = 1
TEST_SYMBOLS = ["600519", "601398"]


def dump_raw_fields(symbol: str, name: str):
    """【审计核心】: 打印最新的资产负债表和利润表中的所有字段"""
    print(f"\n--- 🔎 {symbol} ({name}) 原始财务数据审计 ---")

    # 1. 资产负债表 (BALANCE)
    latest_balance = DB["finance_balance"].find_one({"symbol": symbol}, sort=[("report_date", DESCENDING)])
    if latest_balance:
        print(f"  [资产负债表] 报告期: {latest_balance.get('report_date').strftime('%Y-%m-%d')} | 公告日: {latest_balance.get('publish_date').strftime('%Y-%m-%d')}")
        for k, v in latest_balance.items():
            if k not in ['_id', 'symbol', 'exchange', 'gateway_name', 'data_source', 'currency', 'update_date', 'type', 'is_audited']:
                # 针对大数字显示截断，避免屏幕过长
                v_str = f"{v:,.0f}" if isinstance(v, (int, float)) else str(v)
                print(f"    - {k:<35}: {v_str}")
    else:
        print("  [资产负债表] 未找到最新数据。")

    # 2. 利润表 (INCOME)
    latest_income = DB["finance_income"].find_one({"symbol": symbol}, sort=[("report_date", DESCENDING)])
    if latest_income:
        print(f"\n  [利润表] 报告期: {latest_income.get('report_date').strftime('%Y-%m-%d')} | 公告日: {latest_income.get('publish_date').strftime('%Y-%m-%d')}")
        for k, v in latest_income.items():
            if k not in ['_id', 'symbol', 'exchange', 'gateway_name', 'data_source', 'currency', 'update_date', 'type', 'is_audited']:
                v_str = f"{v:,.0f}" if isinstance(v, (int, float)) else str(v)
                print(f"    - {k:<35}: {v_str}")
    else:
        print("  [利润表] 未找到最新数据。")
    print("----------------------------------------------------------------")


def get_financial_data(symbol: str) -> pd.DataFrame:
    """提取和统一财务数据 (保持 datetime64[ns] 类型)。"""

    # ... (提取逻辑与 V10 保持一致)
    balance_fields_to_pull = {"report_date": 1, "publish_date": 1}
    for field in EQUITY_FIELDS_CANDIDATE: balance_fields_to_pull[field] = 1

    balance_cursor = COL_BALANCE.find({"symbol": symbol}, balance_fields_to_pull).sort([("report_date", ASCENDING)])
    df_balance = pd.DataFrame(list(balance_cursor))

    if not df_balance.empty:
        df_balance['total_equity_latest'] = np.nan
        for field in EQUITY_FIELDS_CANDIDATE:
            if field in df_balance.columns:
                mask = df_balance['total_equity_latest'].isna() & df_balance[field].notna()
                df_balance.loc[mask, 'total_equity_latest'] = df_balance.loc[mask, field]
        df_balance = df_balance[['report_date', 'publish_date', 'total_equity_latest']].copy()

    income_fields_to_pull = {"report_date": 1, "publish_date": 1, NET_PROFIT_FIELD: 1}
    for field in REVENUE_FIELDS_CANDIDATE: income_fields_to_pull[field] = 1

    income_cursor = COL_INCOME.find({"symbol": symbol}, income_fields_to_pull).sort([("report_date", ASCENDING)])
    df_income = pd.DataFrame(list(income_cursor))

    if not df_income.empty:
        df_income = df_income.rename(columns={NET_PROFIT_FIELD: 'net_profit'})
        df_income['revenue'] = np.nan
        for field in REVENUE_FIELDS_CANDIDATE:
            if field in df_income.columns:
                mask = df_income['revenue'].isna() & df_income[field].notna()
                df_income.loc[mask, 'revenue'] = df_income.loc[mask, field]
        df_income = df_income[['report_date', 'publish_date', 'net_profit', 'revenue']].copy()

    if df_balance.empty and df_income.empty: return pd.DataFrame()

    df = pd.merge(df_income, df_balance, on=['report_date', 'publish_date'], how='outer', suffixes=('_inc', '_bal'))
    df = df.dropna(subset=['report_date', 'publish_date'])

    for col in ['net_profit', 'revenue', 'total_equity_latest']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce') * FINANCIAL_UNIT_CONVERSION

    # 统一日期类型为 Timestamp
    df['report_date'] = pd.to_datetime(df['report_date'])
    df['publish_date'] = pd.to_datetime(df['publish_date'])

    df = df.drop_duplicates(subset=['report_date'], keep='last')

    return df.sort_values(by=['report_date', 'publish_date'], ascending=[True, True]).reset_index(drop=True)


def calculate_rolling_ttm(df_financial: pd.DataFrame) -> pd.DataFrame:
    """TTM 滚动计算 (完整的 TTM 逻辑，非年报也计算 TTM 值)"""
    if df_financial.empty: return pd.DataFrame()

    df_ttm_calc = df_financial.copy()
    df_ttm_calc = df_ttm_calc.set_index('report_date').sort_index()

    df_ttm_calc['year'] = df_ttm_calc.index.year
    df_ttm_calc['month'] = df_ttm_calc.index.month
    df_ttm_calc['is_annual'] = (df_ttm_calc['month'] == 12)

    def calculate_ttm_series(series_name: str) -> pd.Series:
        """核心 TTM 滚动计算，基于 Report Date 的年/月/日查找"""
        series = df_ttm_calc[series_name]
        ttm_series = pd.Series(index=series.index, dtype=float)

        for current_date in series.index:
            current_value = series.loc[current_date]
            if pd.isna(current_value): continue

            current_month = current_date.month
            current_year = current_date.year

            if current_month == 12:
                ttm_series.loc[current_date] = current_value

            elif current_month in [3, 6, 9]:
                last_year = current_year - 1

                # 寻找去年同期的报告值 (使用 year/month/day 匹配)
                last_same_date_match = series.index[(series.index.year == last_year) & (series.index.month == current_date.month) & (series.index.day == current_date.day)]
                last_annual_date_match = series.index[(series.index.year == last_year) & (series.index.month == 12) & (series.index.day == 31)]

                last_same_value = series.loc[last_same_date_match[0]] if len(last_same_date_match) > 0 else np.nan
                last_annual_value = series.loc[last_annual_date_match[0]] if len(last_annual_date_match) > 0 else np.nan

                if pd.notna(last_same_value) and pd.notna(last_annual_value):
                    ttm = current_value - last_same_value + last_annual_value
                    ttm_series.loc[current_date] = ttm
        return ttm_series

    # 2. 计算各项 TTM
    df_ttm_calc['net_profit_ttm'] = calculate_ttm_series('net_profit')
    df_ttm_calc['revenue_ttm'] = calculate_ttm_series('revenue')

    # 3. 提取最新年报净利润 (LF)
    df_q4 = df_ttm_calc[df_ttm_calc['is_annual']].rename(columns={'net_profit': 'net_profit_lf'})

    # 4. 合并 TTM 结果，并转换为以 **公告日** (publish_date) 为索引的序列
    df_result = df_ttm_calc[['publish_date', 'total_equity_latest', 'net_profit_ttm', 'revenue_ttm']].copy()

    # PB/BPS 报告日期始终更新到最新的报告期
    df_result['report_date_pb'] = df_result.index
    df_result['publish_date_pb'] = df_result['publish_date']

    # PE/PS 报告日期：使用 TTM 净利润/收入有值的报告期
    df_result['pe_report_date'] = df_result['report_date_pb'].where(df_ttm_calc['net_profit_ttm'].notna(), pd.NaT)

    df_result = df_result.reset_index(drop=True).set_index('publish_date').sort_index()

    # 5. 合并静态年报数据（LF）
    df_q4 = df_q4.rename(columns={'publish_date': 'date'}).set_index('date').sort_index()
    df_q4 = df_q4[['net_profit_lf']]
    df_result = df_result.join(df_q4, how='left')

    # 6. 转换时间序列：以公告日为时间轴，FFILL
    if df_result.empty: return pd.DataFrame()
    min_pub_date = df_result.index.min().to_datetime64()

    full_dates = pd.date_range(start=min_pub_date, end=datetime.now().date(), freq='D')
    df_full = pd.DataFrame(index=full_dates)

    df_full = df_full.join(df_result, how='left')

    fill_cols = [
        'total_equity_latest', 'net_profit_ttm', 'net_profit_lf', 'revenue_ttm',
        'report_date_pb', 'publish_date_pb', 'pe_report_date'
    ]
    df_full[fill_cols] = df_full[fill_cols].ffill()

    return df_full.drop_duplicates(keep='last').rename_axis('date')


def get_latest_industry(symbol: str) -> str:
    """获取股票最新的申万行业分类"""
    doc = DB[COL_INDUSTRY.name].find_one({"symbol": symbol}, sort=[("date", DESCENDING)])
    return doc.get('industry_name', 'UNKNOWN') if doc else 'UNKNOWN'

def run_single_stock_calculation(symbol: str):
    """主计算函数"""
    info_doc = COL_INFO.find_one({"symbol": symbol})
    if not info_doc: return

    name = info_doc.get('name', symbol)

    # --- V8 步骤 1: 打印原始字段供审计 ---
    dump_raw_fields(symbol, name)

    print(f"\n============================================================")
    print(f"       🚀 正在计算 {symbol} ({name}) 的估值指标 (V11)")
    print(f"============================================================")

    # 0. 获取行业信息
    industry = get_latest_industry(symbol)

    # 1. 提取所有数据
    df_financial = get_financial_data(symbol)
    df_financial_ts = calculate_rolling_ttm(df_financial)
    if df_financial_ts.empty:
        print(f"   ⚠️ 警告：无法生成 {symbol} 的财务时间序列。")
        return

    # 3. 提取日线价格/股本
    bars_cursor = COL_BARS.find({"symbol": symbol}, {"datetime": 1, "close_price": 1}).sort([("datetime", ASCENDING)])
    df_bars = pd.DataFrame(list(bars_cursor))
    df_bars['date'] = pd.to_datetime(df_bars['datetime'])
    df_bars = df_bars.set_index('date').drop(columns=['datetime', '_id'])

    capital_cursor = COL_CAPITAL.find({"symbol": symbol}, {"date": 1, "total_shares": 1, "float_shares": 1}).sort([("date", ASCENDING)])
    df_capital = pd.DataFrame(list(capital_cursor))
    df_capital['date'] = pd.to_datetime(df_capital['date'])
    df_capital = df_capital.set_index('date').drop(columns=['_id'])

    # 5. 核心合并逻辑: Left Join
    all_dates = df_bars.index.union(df_capital.index)
    df_master = pd.DataFrame(index=all_dates)

    df_all = df_master.join(df_bars).join(df_capital)
    df_all['total_shares'] = df_all['total_shares'].ffill()
    df_all['float_shares'] = df_all['float_shares'].ffill()
    df_all = df_all.join(df_financial_ts, how='left')

    df_all = df_all.dropna(subset=['close_price', 'total_shares', 'total_equity_latest']).copy()

    if df_all.empty:
        print(f"   ⚠️ 警告：合并后无有效数据进行计算。")
        return

    print(f"  - 数据合并完毕，共 {len(df_all)} 个交易日数据。")

    # 6. 计算估值指标
    df = df_all

    df['total_mv'] = df['close_price'] * df['total_shares']
    df['circ_mv'] = df['close_price'] * df['float_shares'].fillna(df['total_shares'])

    df['bps'] = df['total_equity_latest'] / df['total_shares']
    df['eps_ttm'] = df['net_profit_ttm'] / df['total_shares']
    df['pb_lf'] = df['total_mv'] / df['total_equity_latest']

    pe_ttm_mask = df['net_profit_ttm'] > 0
    df.loc[pe_ttm_mask, 'pe_ttm'] = df.loc[pe_ttm_mask, 'total_mv'] / df.loc[pe_ttm_mask, 'net_profit_ttm']

    pe_lf_mask = df['net_profit_lf'] > 0
    df.loc[pe_lf_mask, 'pe_lf'] = df.loc[pe_lf_mask, 'total_mv'] / df.loc[pe_lf_mask, 'net_profit_lf']

    ps_ttm_mask = df['revenue_ttm'].notna() & (df['revenue_ttm'] > 0)
    df.loc[ps_ttm_mask, 'ps_ttm'] = df.loc[ps_ttm_mask, 'total_mv'] / df.loc[ps_ttm_mask, 'revenue_ttm']

    roe_ttm_mask = (df['total_equity_latest'] > 0)
    df.loc[roe_ttm_mask, 'roe_ttm'] = df.loc[roe_ttm_mask, 'net_profit_ttm'] / df.loc[roe_ttm_mask, 'total_equity_latest']

    # 7. 整理输出结果
    latest_data = df.iloc[-1]

    circ_share_warning = ""
    if symbol == "601398":
        circ_share_warning = f"【注意：底层股本数据与东财存在差异，东财流通股本为 2696.12 亿股】"

    output = {
        "股票代码/名称": f"{symbol} ({name})",
        "申万行业": industry,
        "---------------------": "最新行情与规模",
        "最新交易日": latest_data.name.strftime("%Y-%m-%d"),
        "收盘价 (元)": f"{latest_data['close_price']:,.2f}",
        "总股本 (亿股)": f"{latest_data['total_shares']/1e8:,.2f}",
        "流通股本 (亿股)": f"{latest_data['float_shares']/1e8:,.2f} {circ_share_warning}",
        "总市值 (亿元)": f"{latest_data['total_mv']/1e8:,.2f}",
        "流通市值 (亿元)": f"{latest_data['circ_mv']/1e8:,.2f}",
        "---------------------": "核心估值指标",
        "每股净资产 (BPS)": f"{latest_data['bps']:,.4f}",
        "每股收益 (EPS_TTM)": f"{latest_data['eps_ttm']:,.4f}" if pd.notna(latest_data['eps_ttm']) else 'N/A',
        "市净率 (PB_LF)": f"{latest_data['pb_lf']:,.2f}",
        "滚动市盈率 (PE_TTM)": f"{latest_data['pe_ttm']:,.2f}" if pd.notna(latest_data['pe_ttm']) else 'N/A',
        "静态市盈率 (PE_LF)": f"{latest_data['pe_lf']:,.2f}" if pd.notna(latest_data['pe_lf']) else 'N/A',
        "滚动市销率 (PS_TTM)": f"{latest_data['ps_ttm']:,.2f}" if pd.notna(latest_data['ps_ttm']) else 'N/A',
        "TTM 净资产收益率 (ROE_TTM)": f"{latest_data['roe_ttm']*100:,.2f}%" if pd.notna(latest_data['roe_ttm']) else 'N/A',
        "---------------------": "审计信息 (财务分母)",
        "最新归母净资产 (元)": f"{latest_data['total_equity_latest']:,.0f}",
        "滚动 TTM 净利润 (元)": f"{latest_data['net_profit_ttm']:,.0f}" if pd.notna(latest_data['net_profit_ttm']) else 'N/A',
        "最新年报净利润 (元)": f"{latest_data['net_profit_lf']:,.0f}" if pd.notna(latest_data['net_profit_lf']) else 'N/A',
        "滚动 TTM 营业收入 (元)": f"{latest_data['revenue_ttm']:,.0f}" if pd.notna(latest_data['revenue_ttm']) else 'N/A',
        "PB/BPS对应报告期": latest_data['report_date_pb'].strftime("%Y-%m-%d") if pd.notna(latest_data['report_date_pb']) else 'N/A',
        "PB/BPS对应公告日": latest_data['publish_date_pb'].strftime("%Y-%m-%d") if pd.notna(latest_data['publish_date_pb']) else 'N/A',
        "PE/PS对应报告期": latest_data['pe_report_date'].strftime("%Y-%m-%d") if pd.notna(latest_data['pe_report_date']) else 'N/A',
    }

    print("\n✅ 最新估值指标快照:")
    for key, value in output.items():
        print(f"   {key:<25}: {value}")


def run():
    for symbol in TEST_SYMBOLS:
        try:
            run_single_stock_calculation(symbol)
        except Exception as e:
            print(f"\n   ❌ 致命错误: 处理 {symbol} 时发生异常: {e}")

if __name__ == "__main__":
    run()