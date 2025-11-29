# data_quality_auditor_fixed.py

import pandas as pd
from pymongo import MongoClient, ASCENDING, DESCENDING
from typing import List, Dict, Any, Union

# --- 1. 配置 (Config) ---
MONGO_HOST = "localhost"
MONGO_PORT = 27017

# 从 01_init_db_architecture.py 导入架构定义 (仅保留键值用于遍历)
# 保持和您现有配置一致
SCHEMA_MAP = {
    "vnpy_stock": {
        "bar_daily": [], "index_daily": [], "index_components": [], "index_info": [],
        "valuation_daily": [], "finance_balance": [], "finance_income": [],
        "finance_cashflow": [], "share_capital": [], "analysis_limit_up": [],
        "analysis_limit_down": [], "industry_history": [], "stock_status_history": [],
        "adjust_factor": [], "stock_info": []
    },
    "vnpy_etf": {
        "bar_daily": [], "etf_daily_metrics": [], "etf_components": [], "etf_info": []
    },
    "vnpy_factor": {
        "factor_technical": [], "factor_momentum": [], "factor_value": [],
        "factor_quality": [], "factor_sentiment": [], "factor_volatility": [],
        "factor_master": []
    },
    "vnpy_future": {
        "bar_daily": [], "bar_1m": [], "dominant_contract_history": []
    },
    "vnpy_option": {
        "bar_daily": [], "market_greeks": [], "contract_info": []
    },
    "vnpy_crypto": {
        "bar_daily": [], "funding_rate": []
    },
    "vnpy_us": {
        "bar_daily": [], "stock_info": []
    },
    "vnpy_master": {
        "trading_calendar": [], "exchange_rate": []
    }
}


# -------------------------


def get_all_fields_by_aggregation(collection) -> List[str]:
    """使用 MongoDB 聚合管道动态发现集合中所有文档中出现过的字段名"""
    # 保持与原脚本一致
    print("      - 正在进行全字段发现...")
    pipeline = [
        {"$project": {"data": {"$objectToArray": "$$ROOT"}}},
        {"$unwind": "$data"},
        {"$group": {"_id": None, "keys": {"$addToSet": "$data.k"}}}
    ]

    result = list(collection.aggregate(pipeline, allowDiskUse=True))

    if result and 'keys' in result[0]:
        fields = [k for k in result[0]['keys'] if k != '_id']
        return sorted(fields)

    return []


def analyze_field_quality(collection, field_name: str, total_count: int) -> Dict[str, Union[str, int]]:
    """对单个字段运行精确的 MongoDB 统计查询，计算 Null 值和 Missing 字段的合计数量和比例"""
    report = {
        'Null Value Count': 0,
        'Missing Field Count': 0,
        'Total Empty Count': 0,
        'Empty Ratio (%)': '0.00%',
    }

    if total_count == 0:
        return report

    try:
        # ======================= [核心修复区域] =======================
        # 1. Meaningful Count: 字段存在且值不为 BSON Null (即 Python None)
        meaningful_count = collection.count_documents({
            field_name: {'$exists': True, '$ne': None}
        })

        # 2. Total Empty Count: 总缺失/空置数量 (总行数 - 有意义计数)
        # 包含了明确为 None 和完全缺失的文档，是修复后的核心指标。
        total_empty_count = total_count - meaningful_count

        # 3. Null Value Count: 字段存在且明确为 None/null 的文档数量
        # 使用 $eq: None 且 $exists: true 来精确查找显式的 null 值
        null_value_count = collection.count_documents({
            field_name: {'$eq': None, '$exists': True}
        })

        # 4. Missing Field Count: 字段完全不存在于文档中的数量
        missing_field_count = collection.count_documents({
            field_name: {'$exists': False}
        })

        # =============================================================

        # 5. 计算比率
        if total_count > 0:
            empty_ratio = (total_empty_count / total_count) * 100
        else:
            empty_ratio = 0.0

        report['Null Value Count'] = null_value_count
        report['Missing Field Count'] = missing_field_count
        report['Total Empty Count'] = total_empty_count
        report['Empty Ratio (%)'] = f"{empty_ratio:.2f}%"

    except Exception as e:
        report['Total Empty Count'] = 'Error'
        report['Empty Ratio (%)'] = f"Error: {type(e).__name__}"

    # ... (Rest of the function remains the same) ...
    return report


# --- 3. 主函数 (Main Execution) ---

def main():
    """执行完整的数据库字段质量审计"""
    client: MongoClient = None
    report_data: List[Dict[str, Any]] = []

    print("==================================================")
    print("          📈 MongoDB 数据库字段质量审计报告 (FIXED)      ")
    print("==================================================")
    print(f"连接: {MONGO_HOST}:{MONGO_PORT}")
    print("审计模式: **全字段发现** & **缺失率计算**")
    print("--------------------------------------------------")

    try:
        client = MongoClient(host=MONGO_HOST, port=MONGO_PORT, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')

        for db_name, collections_map in SCHEMA_MAP.items():
            db = client[db_name]
            print(f"\n🏛  正在检查数据疆域: [{db_name}]...")

            for col_name in collections_map.keys():
                collection = db[col_name]

                try:
                    total_count = collection.count_documents({})
                    print(f"   └── 集合/表: {col_name:<30} | 总行数: {total_count:,} ", end="")

                    if total_count == 0:
                        report_data.append({
                            "Database": db_name, "Collection": col_name, "Column Name": "N/A",
                            "Total Rows": 0, "Null Value Count": 0, "Missing Field Count": 0,
                            "Total Empty Count": 0, "Empty Ratio (%)": "0.00%"
                        })
                        print("⬜ (空，跳过详细审计)")
                        continue

                    print("✅")

                    # 1. 动态发现所有字段名
                    field_names = get_all_fields_by_aggregation(collection)

                    if not field_names:
                        print(f"      - 警告: 无法通过聚合管道获取 {col_name} 的任何字段信息。")
                        continue

                    # 2. 遍历所有字段并进行深度质量分析
                    for field_name in field_names:
                        quality_metrics = analyze_field_quality(collection, field_name, total_count)

                        report_data.append({
                            "Database": db_name,
                            "Collection": col_name,
                            "Column Name": field_name,
                            "Total Rows": total_count,
                            **quality_metrics
                        })

                        # 3. 实时警告输出
                        if quality_metrics['Total Empty Count'] > 0 and isinstance(quality_metrics['Total Empty Count'],
                                                                                   int):
                            print(
                                f"      - ⚠️ 字段 '{field_name}' 缺失/空值: {quality_metrics['Total Empty Count']:,} ({quality_metrics['Empty Ratio (%)']})")

                except Exception as e:
                    print(f"    ❌ 致命错误：处理集合 {col_name} 失败: {type(e).__name__}: {str(e)}")
                    report_data.append({
                        "Database": db_name,
                        "Collection": col_name,
                        "Column Name": "COLLECTION ERROR",
                        "Total Rows": "Error",
                        "Null Value Count": "Error",
                        "Missing Field Count": "Error",
                        "Total Empty Count": "Error",
                        "Empty Ratio (%)": f"Error: {type(e).__name__}"
                    })

        # --- 4. 格式化最终报告 ---
        final_report = pd.DataFrame(report_data)
        final_report.sort_values(by=['Database', 'Collection', 'Column Name'], inplace=True)

        print("\n" + "=" * 80)
        print("                🚀 最终 MongoDB 字段质量审计报告 🚀                ")
        print("=" * 80)

        print(final_report.to_markdown(index=False))

    except Exception as e:
        print(f"\n[致命错误] 无法连接到 MongoDB: {type(e).__name__}: {str(e)}")
        print("请确保 MongoDB 服务正在运行在 localhost:27017")
    finally:
        if client:
            client.close()


if __name__ == "__main__":
    main()