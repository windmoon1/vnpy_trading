"""
Script 10: Rebuild Industry History with Full Hierarchy
-------------------------------------------------------
目标: 重构行业历史表 (industry_history)
流程:
1. [清理] 清空现有 industry_history 表 (从零开始)。
2. [映射] 读取本地 sw_2021.csv (Sheet1), 构建全层级映射字典 (Code -> L1/L2/L3)。
3. [下载] 在线拉取申万个股历史数据 (ak.stock_industry_clf_hist_sw)。
4. [入库] 将历史数据的 Code 翻译为全层级结构并存储。

注意:
如果线上数据包含旧版代码(如4xxxx)而Sheet1只有新版代码(11xxxx),
未匹配的记录将只存储原始代码，标记 is_mapped=False。
"""

import akshare as ak
import pandas as pd
import os
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
MAPPING_FILE = "data/行业分类.csv"  # 请确保你已将 Sheet1.csv 重命名为此文件名

def get_db():
    return MongoClient(MONGO_HOST, MONGO_PORT)[DB_NAME]

def load_full_hierarchy_map(file_path):
    """
    从 CSV 构建全维度映射字典
    Dict Structure:
    {
        '110101': {'l1_c': '110000', 'l1_n': '农林牧渔', 'l2_c': ..., 'l3_n': '种子'},
        '801010': {'l1_c': '...', ...} (兼容指数代码)
    }
    """
    print(f"📚 正在加载映射文件: {file_path}")
    if not os.path.exists(file_path):
        print(f"❌ 文件未找到: {file_path}")
        return {}

    try:
        # 强制读取为字符串，避免代码前导0丢失
        df = pd.read_csv(file_path, dtype=str)

        # 清理列名空格
        df.columns = [c.strip() for c in df.columns]

        mapping = {}

        for _, row in df.iterrows():
            # 提取各级信息 (处理可能的空值)
            l1_c = str(row.get('industry_level1_code', '')).strip()
            l1_n = str(row.get('industry_level1_name', '')).strip()
            l2_c = str(row.get('industry_level2_code', '')).strip()
            l2_n = str(row.get('industry_level2_name', '')).strip()
            l3_c = str(row.get('industry_level3_code', '')).strip()
            l3_n = str(row.get('industry_level3_name', '')).strip()

            # 构造完整数据包
            full_info = {
                "level1_code": l1_c, "level1_name": l1_n,
                "level2_code": l2_c, "level2_name": l2_n,
                "level3_code": l3_c, "level3_name": l3_n
            }

            # 策略: 将所有层级的代码都作为 Key 指向这个 Info
            # 这样无论 API 返回的是一级还是三级代码，都能查到家族信息

            if l3_c and l3_c.lower() != 'nan': mapping[l3_c] = full_info
            if l2_c and l2_c.lower() != 'nan':
                # 如果 L2 已经作为 Key 存在 (可能来自另一行)，不要覆盖，因为 L2 对应多个 L3
                # 但对于"查询 L2 属于哪个 L1"，任意一行都是一样的。
                # 为了简单，我们只存第一次出现的映射 (L2 -> L1 关系是固定的)
                if l2_c not in mapping:
                    mapping[l2_c] = full_info
            if l1_c and l1_c.lower() != 'nan':
                if l1_c not in mapping:
                    mapping[l1_c] = full_info

        print(f"✅ 映射字典构建完成，索引数: {len(mapping)}")
        return mapping

    except Exception as e:
        print(f"❌ 读取 CSV 失败: {e}")
        return {}

def run():
    print("🚀 启动 [行业全量重构脚本]...")
    db = get_db()
    col = db[COLLECTION_NAME]

    # 1. 清空旧数据 (慎重操作)
    print(f"🗑️  正在清空表 [{COLLECTION_NAME}]...")
    col.delete_many({})
    print("   已清空。")

    # 2. 加载映射
    hierarchy_map = load_full_hierarchy_map(MAPPING_FILE)
    if not hierarchy_map:
        print("❌ 缺少映射文件，无法继续。")
        return

    # 3. 下载线上数据
    print("📡 正在拉取申万个股历史数据 (ak.stock_industry_clf_hist_sw)...")
    try:
        df_hist = ak.stock_industry_clf_hist_sw()
        print(f"✅ 获取成功! 原始记录: {len(df_hist)} 条")
    except Exception as e:
        print(f"❌ 下载失败: {e}")
        return

    if df_hist is None or df_hist.empty:
        return

    # 4. 处理与入库
    print("⚙️  正在进行层级映射与入库...")
    requests = []
    matched_count = 0

    # 这里的 tqdm 显示进度
    for _, row in tqdm(df_hist.iterrows(), total=len(df_hist)):
        symbol = str(row['symbol'])

        # 日期处理
        date_raw = row.get('start_date')
        if pd.isna(date_raw) or str(date_raw) == 'NaT':
            continue
        date_str = str(date_raw).split(" ")[0]

        # 行业代码 (这是 API 返回的)
        raw_code = str(row.get('industry_code', '')).strip()

        # 查字典
        info = hierarchy_map.get(raw_code)

        # 构造基础文档
        doc = {
            "symbol": symbol,
            "date": date_str,
            "source": "SHENWAN",
            "industry_code": raw_code, # 保留原始代码
            "updated_at": datetime.now()
        }

        if info:
            # 匹配成功: 注入全层级信息
            doc.update({
                "is_mapped": True,
                # 核心层级
                "level1_code": info['level1_code'],
                "level1_name": info['level1_name'],
                "level2_code": info['level2_code'],
                "level2_name": info['level2_name'],
                "level3_code": info['level3_code'],
                "level3_name": info['level3_name'],
                # 兼容旧字段 (优先显示最细粒度名称)
                "industry_name": info['level3_name'] or info['level2_name'] or info['level1_name']
            })
            matched_count += 1
        else:
            # 匹配失败: 可能是旧版代码 (如 440101) 不在 2021 版 CSV 里
            doc.update({
                "is_mapped": False,
                "industry_name": f"Unknown_{raw_code}"
            })

        # 构造 Upsert 请求 (虽然表已空，但用 upsert 更安全)
        requests.append(UpdateOne(
            {"symbol": symbol, "date": date_str},
            {"$set": doc},
            upsert=True
        ))

        # 批量写入
        if len(requests) >= 2000:
            col.bulk_write(requests, ordered=False)
            requests = []

    # 剩余写入
    if requests:
        col.bulk_write(requests, ordered=False)

    # 5. 总结
    print("\n" + "="*40)
    print(f"🎉 重构完成!")
    print(f"   - 数据库记录数: {col.count_documents({})}")
    print(f"   - 成功映射层级: {matched_count} ({(matched_count/len(df_hist)):.1%})")

    if matched_count < len(df_hist) * 0.5:
        print("⚠️ 警告: 匹配率较低。这通常是因为线上历史数据包含大量 2014 版旧代码 (4xxxx)，")
        print("   而你的 CSV 仅包含 2021 版新代码 (11xxxx/801xxx)。")
        print("   建议: 对于未映射的记录，回测时可能无法获取其板块归属。")

    # 抽样
    print("\n🔍 [抽样检查] 000001:")
    cursor = col.find({"symbol": "000001"}).sort("date", -1).limit(3)
    for d in cursor:
        print(f"   {d['date']}: {d.get('industry_name')} (Mapped: {d.get('is_mapped')})")

if __name__ == "__main__":
    run()