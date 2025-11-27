"""
Script 10: Download Shenwan Industry History (The Real Fix)
-----------------------------------------------------------
核心修复:
1. [关键] 修正参数名: "申万行业分类标准" -> "申银万国行业分类标准"。
   这是 akshare 源码中定义的唯一正确 Key。
2. [关键] 数据库字段映射: 将 API 返回的 start_date 映射为 date。
3. 完整流程: 自动构建字典 -> 下载历史 -> 汉化名称 -> 入库。
"""

import akshare as ak
import pandas as pd
from datetime import datetime
from tqdm import tqdm
from pymongo import MongoClient, UpdateOne

# ==========================================
# 配置
# ==========================================
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DB_NAME = "vnpy_stock"
COLLECTION_NAME = "industry_history"

def get_db():
    return MongoClient(MONGO_HOST, MONGO_PORT)[DB_NAME]

def build_correct_mapping():
    """
    从巨潮资讯构建申万行业代码字典
    Target: { '480301': '银行II', '440101': '银行I', ... }
    """
    print("📚 正在从巨潮资讯构建申万代码字典...")
    mapping = {}

    # 🌟 核心修正: 必须使用 "申银万国行业分类标准"
    target_symbol = "申银万国行业分类标准"

    try:
        # 接口: 巨潮资讯-行业分类数据
        df = ak.stock_industry_category_cninfo(symbol=target_symbol)

        if df is not None and not df.empty:
            # df columns: ['类目编码', '类目名称', ...]
            for _, row in df.iterrows():
                code = str(row['类目编码']).strip()
                name = str(row['类目名称']).strip()
                mapping[code] = name

            print(f"   ✅ 字典构建成功! 收录 {len(mapping)} 条行业映射")

            # 抽样验证我们关心的代码
            test_codes = ['440101', '480101', '480301']
            print("   🧪 关键代码抽检 (Code -> Name):")
            for c in test_codes:
                print(f"      - {c} -> {mapping.get(c, '❌ 未找到')}")

        else:
            print("   ⚠️ 巨潮接口返回空，请检查网络或 AKShare 版本。")

    except Exception as e:
        print(f"   ❌ 字典下载失败: {e}")

    return mapping

def run():
    print("🚀 启动 [申万行业数据修复流程]...")
    db = get_db()
    col = db[COLLECTION_NAME]

    # 1. 构建正确的字典
    industry_map = build_correct_mapping()

    if not industry_map:
        print("❌ 无法构建映射字典，无法继续。")
        return

    # 2. 获取历史变动数据
    print("\n📡 请求申万个股历史数据 (stock_industry_clf_hist_sw)...")
    try:
        df_hist = ak.stock_industry_clf_hist_sw()
        print(f"   ✅ 获取历史数据: {len(df_hist)} 条")
    except Exception as e:
        print(f"   ❌ 历史数据下载失败: {e}")
        return

    # 3. 清洗与入库
    print("⚙️ 正在执行映射与入库...")
    requests = []
    mapped_count = 0

    pbar = tqdm(df_hist.iterrows(), total=len(df_hist))

    for _, row in pbar:
        symbol = str(row['symbol'])

        # 修复 1: 字段名 start_date -> date (解决 MongoDB 索引冲突)
        date_raw = row.get('start_date')
        if pd.isna(date_raw) or str(date_raw) == 'NaT':
            continue
        date_str = str(date_raw).split(" ")[0]

        # 获取代码
        code = str(row['industry_code'])

        # 修复 2: 使用字典翻译中文名
        industry_name = industry_map.get(code)

        if industry_name:
            mapped_count += 1
        else:
            # 找不到就保留 SW_Code，防止空值
            industry_name = f"SW_{code}"

        # 构造文档
        filter_doc = {
            "symbol": symbol,
            "date": date_str
        }

        update_doc = {
            "$set": {
                "industry_code": code,
                "industry_name": industry_name, # 终于有中文名了！
                "source": "SHENWAN",
                "type": "INDUSTRY",
                "updated_at": datetime.now()
            }
        }

        requests.append(UpdateOne(filter_doc, update_doc, upsert=True))

        if len(requests) >= 2000:
            try:
                col.bulk_write(requests, ordered=False)
                requests = []
            except Exception:
                pass

    if requests:
        try:
            col.bulk_write(requests, ordered=False)
        except Exception:
            pass

    print(f"\n✅ 修复完成。")
    print(f"   - 成功汉化率: {mapped_count / len(df_hist):.1%}")

    # 最终验证
    print("\n🔍 [最终验证] 000001 (平安银行) 行业变迁:")
    cursor = col.find({"symbol": "000001"}).sort("date", 1)
    for doc in cursor:
        print(f"   📅 {doc['date']}: {doc['industry_name']} (Code: {doc['industry_code']})")

if __name__ == "__main__":
    run()