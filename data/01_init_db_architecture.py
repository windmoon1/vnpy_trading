"""
VNPY Database Architecture Initialization (Final Edition)
-------------------------------------------------------
Version: 5.0 (Unified Index + Limit Down + All Assets)
Author: QuantDev Copilot

[使用说明]
直接运行此脚本。它会连接 MongoDB 并建立所有必要的 Collections 和 Unique Indexes。
字段说明仅作为数据字典参考，MongoDB 自身不需要预定义字段。
"""

from pymongo import ASCENDING, DESCENDING
from pymongo import MongoClient

# --- 1. 连接配置 ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017
CLIENT = MongoClient(host=MONGO_HOST, port=MONGO_PORT)

SCHEMA_MAP = {

    # =========================================================================
    # 🇨🇳 数据库 1: A股股票 (vnpy_stock) - 核心资产
    # =========================================================================
    "vnpy_stock": {

        # --- 1.1 个股行情 ---
        # [字段]: symbol, exchange, interval, datetime
        #         open, high, low, close, volume, turnover
        #         average_price (均价), turnover_rate (换手率)
        #         limit_up (涨停价), limit_down (跌停价)
        # [备注]: 基础原料。
        "bar_daily": [
            ("symbol", ASCENDING), ("exchange", ASCENDING), ("interval", ASCENDING), ("datetime", ASCENDING)
        ],

        # --- 1.2 [广义指数] 行情 (Unified Index) ---
        # [包含]:
        #   - 宽基指数 (如 000300.XSHG)
        #   - 行业指数 (如 申万半导体)
        #   - 概念指数 (如 东财算力概念)
        #   - 地域指数 (如 福建板块)
        # [字段]: symbol, datetime, open, high, low, close, volume
        #         category (枚举: BENCHMARK, INDUSTRY, CONCEPT, REGION)
        "index_daily": [
            ("symbol", ASCENDING), ("exchange", ASCENDING), ("datetime", ASCENDING)
        ],

        # --- 1.3 [广义指数] 成分股 ---
        # [字段]: index_symbol (指数代码), date (快照日), components (Dict/List: 股票代码+权重)
        # [作用]: 用于"自上而下"选股（如：找出半导体板块里的所有票）。
        "index_components": [
            ("index_symbol", ASCENDING), ("date", ASCENDING)
        ],

        # --- 1.4 [广义指数] 基础信息 ---
        # [字段]: symbol, name, category (类别), source (来源: SINA/SW/EM)
        "index_info": [("symbol", ASCENDING)],

        # --- 1.5 每日估值 (Valuation) ---
        # [字段]: pe_ttm (核心), pb, ps, dv_ratio (股息率)
        #         total_mv (总市值 - 微盘股核心), circ_mv (流通市值),
        #         total_share (总股本), float_share (流通股本)
        "valuation_daily": [
            ("symbol", ASCENDING), ("date", ASCENDING)
        ],

        # --- 1.6 财务报表 (Financial - PIT Mode) [NEW] ---
        # 我们将三大表拆分存储，支持 Point-in-Time (公告日) 查询
        # [索引]: symbol + report_date (唯一确定一期财报)
        # [查询]: 通常按 symbol 查，按 publish_date 过滤

        "finance_balance": [  # 资产负债表
            ("symbol", ASCENDING), ("report_date", DESCENDING), ("publish_date", DESCENDING)
        ],
        "finance_income": [  # 利润表
            ("symbol", ASCENDING), ("report_date", DESCENDING), ("publish_date", DESCENDING)
        ],
        "finance_cashflow": [  # 现金流量表
            ("symbol", ASCENDING), ("report_date", DESCENDING), ("publish_date", DESCENDING)
        ],

        # --- 1.7 股本变动 (Capital Structure) [NEW] ---
        # [字段]: total_shares (总股本), float_shares (流通股本), change_reason (变动原因)
        # [用途]: 计算每日 PE, PB, 市值
        "share_capital": [
            ("symbol", ASCENDING), ("date", ASCENDING)
        ],

        # --- 1.8 游资/情绪分析 (Analysis - 阴阳双极) ---
        # > 涨停分析 (Greed)
        # [字段]: is_limit_up, limit_seq (连板数), limit_amount (封单额),
        #         limit_time (首封时间), limit_success (炸板否)
        "analysis_limit_up": [
            ("symbol", ASCENDING), ("date", ASCENDING)
        ],
        # > 跌停分析 (Fear)
        # [字段]: is_limit_down, limit_down_seq (连续跌停数), limit_down_amount (跌停封单),
        #         open_times (撬板次数)
        "analysis_limit_down": [
            ("symbol", ASCENDING), ("date", ASCENDING)
        ],

        # --- 1.9 历史档案 (Meta History) ---
        # > 行业历史 (用于回测板块轮动) - 记录某只票在2015年属于什么行业
        "industry_history": [("symbol", ASCENDING), ("date", ASCENDING)],
        # > 状态历史 (用于防雷) - 记录 ST, *ST, 停牌, 退市整理期
        "stock_status_history": [("symbol", ASCENDING), ("date", ASCENDING)],
        # > 复权因子 - 用于计算后复权价格
        "adjust_factor": [("symbol", ASCENDING), ("date", ASCENDING)],
        # > 基础信息 - 上市日, 退市日, 中文名
        "stock_info": [("symbol", ASCENDING)]
    },

    # =========================================================================
    # 📈 数据库 2: ETF基金 (vnpy_etf)
    # =========================================================================
    "vnpy_etf": {
        "bar_daily": [("symbol", ASCENDING), ("exchange", ASCENDING), ("datetime", ASCENDING)],
        # [核心字段]:
        #   nav (单位净值), discount_rate (折溢价率 - 套利),
        #   shares (份额 - 资金流向)
        "etf_daily_metrics": [("symbol", ASCENDING), ("date", ASCENDING)],
        # [字段]: components (成分股清单 - 用于IOPV计算)
        "etf_components": [("symbol", ASCENDING), ("date", ASCENDING)],
        "etf_info": [("symbol", ASCENDING)]
    },

    # =========================================================================
    # 🧪 数据库 3: 因子库 (vnpy_factor)
    # =========================================================================
    # 设计理念: 宽表存储。每日收盘后计算，供策略直接读取。
    "vnpy_factor": {
        # 技术类: rsi, kdj, macd, boll, cci
        "factor_technical":  [("symbol", ASCENDING), ("date", ASCENDING)],
        # 动量类: mom_1m, mom_12m, roc, bias
        "factor_momentum":   [("symbol", ASCENDING), ("date", ASCENDING)],
        # 价值类: ep_ttm, bp, peg (通常存倒数或分位值)
        "factor_value":      [("symbol", ASCENDING), ("date", ASCENDING)],
        # 质量类: roe_ttm, profit_growth, gross_margin
        "factor_quality":    [("symbol", ASCENDING), ("date", ASCENDING)],
        # 情绪类: limit_up_count (近期涨停数), turnover_std (换手率异动)
        "factor_sentiment":  [("symbol", ASCENDING), ("date", ASCENDING)],
        # 波动类: atr, std_20, beta
        "factor_volatility": [("symbol", ASCENDING), ("date", ASCENDING)],
        # 因子元数据: 记录因子公式和含义
        "factor_master":     [("factor_name", ASCENDING)]
    },

    # =========================================================================
    # 🌽 数据库 4: 期货 (vnpy_future)
    # =========================================================================
    "vnpy_future": {
        # [核心字段]: open_interest (持仓量)
        "bar_daily": [("symbol", ASCENDING), ("exchange", ASCENDING), ("datetime", ASCENDING)],
        "bar_1m":    [("symbol", ASCENDING), ("exchange", ASCENDING), ("datetime", ASCENDING)],
        # [核心字段]: dominant_symbol (如 'rb2305') - 解决主力合约换月回测
        "dominant_contract_history": [("symbol", ASCENDING), ("date", ASCENDING)]
    },

    # =========================================================================
    # 📜 数据库 5: 期权 (vnpy_option)
    # =========================================================================
    "vnpy_option": {
        # [索引优化]: 增加 underlying_symbol (标的)
        "bar_daily": [
            ("symbol", ASCENDING), ("exchange", ASCENDING), ("datetime", ASCENDING), ("underlying_symbol", ASCENDING)
        ],
        # [核心字段]: iv (隐含波动率), delta, gamma, theta, vega
        "market_greeks": [("symbol", ASCENDING), ("date", ASCENDING)],
        # [字段]: strike_price, expiry_date, option_type (C/P)
        "contract_info": [("symbol", ASCENDING), ("list_date", ASCENDING)]
    },

    # =========================================================================
    # 🌍 数据库 6 & 7: 海外与数字资产 (预留)
    # =========================================================================
    "vnpy_crypto": {
        "bar_daily": [("symbol", ASCENDING), ("exchange", ASCENDING), ("datetime", ASCENDING)],
        "funding_rate": [("symbol", ASCENDING), ("exchange", ASCENDING), ("datetime", ASCENDING)]
    },
    "vnpy_us": {
        "bar_daily": [("symbol", ASCENDING), ("exchange", ASCENDING), ("datetime", ASCENDING)],
        "stock_info": [("symbol", ASCENDING)]
    },

    # =========================================================================
    # ⚙️ 数据库 8: 全局主数据 (vnpy_master)
    # =========================================================================
    "vnpy_master": {
        # [字段]: date, is_open
        # [注意]: 必须按 exchange 区分 (SSE vs NYSE vs Crypto)
        "trading_calendar": [("exchange", ASCENDING), ("date", ASCENDING)],
        "exchange_rate": [("currency_pair", ASCENDING), ("date", ASCENDING)]
    }
}

def init_final_system():
    print("🚀 正在初始化 [全资产量化数据库架构 v5.0] ...")
    print("=" * 80)

    for db_name, collections in SCHEMA_MAP.items():
        db = CLIENT[db_name]
        print(f"\n🏛  数据疆域: [{db_name}]")

        for col_name, keys in collections.items():
            print(f"   └── 集合/表: {col_name:<30}", end="")
            try:
                # create_index(unique=True) 是本脚本的灵魂
                # 它保证了数据的一致性和幂等性
                db[col_name].create_index(keys, unique=True, background=True)
                print(f"✅ 索引就绪")
            except Exception as e:
                print(f"❌ 错误: {e}")

    print("=" * 80)
    print("\n✨ 基础设施部署完毕 (MISSION COMPLETE).")
    print("   下一步: 运行 Master Downloader，向 [vnpy_stock] 注入第一批数据。")

if __name__ == "__main__":
    init_final_system()