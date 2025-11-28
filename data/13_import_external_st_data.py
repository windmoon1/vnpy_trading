# data/13_import_external_st_data.py

import pandas as pd
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import re
import os

# ---------------- Configuration ----------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "vnpy_stock"
COLLECTION_NAME = "stock_status_history"
FILE_PATH = "data/st_source.txt"  # 请确保文件路径正确


# -----------------------------------------------

def get_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]


def parse_st_text_file(file_path):
    """
    解析自定义格式的 ST 数据文本文件
    格式示例:
    Index: 0
      instrument: 000004.SZ
      special_treatment: *ST:20060421;摘*:20070525;...
    """
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return []

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 使用分隔符切分每个股票的块
    # 假设分隔符是 "--------------------------------------------------"
    blocks = content.split('-' * 50)

    parsed_data = []

    for block in blocks:
        if not block.strip():
            continue

        try:
            # 提取 instrument
            # 兼容 .SZ/.SH 以及可能的 .BJ
            inst_match = re.search(r'instrument:\s*(\d+\.(?:SZ|SH|BJ))', block)
            if not inst_match:
                continue
            symbol = inst_match.group(1)

            # 提取 special_treatment 字符串
            # 注意：有些股票可能没有特别处理，或者字段为空
            st_match = re.search(r'special_treatment:\s*(.*)', block)
            st_str = st_match.group(1).strip() if st_match else ""

            if not st_str:
                continue

            # 解析时间线: "*ST:20060421;摘*:20070525"
            events = []
            items = st_str.split(';')
            for item in items:
                if ':' not in item:
                    continue
                state, date_str = item.split(':')

                try:
                    dt = datetime.strptime(date_str.strip(), "%Y%m%d")
                    events.append({
                        "date": dt,
                        "status": state.strip()  # ST, *ST, 摘帽, 摘*, 等
                    })
                except ValueError:
                    # 容错：防止出现非法日期格式
                    continue

            if events:
                # 按日期排序，确保时间轴正确
                events.sort(key=lambda x: x['date'])

                parsed_data.append({
                    "symbol": symbol,  # e.g., 000004.SZ (注意这里带了后缀，库里可能存的是纯数字)
                    "st_history": events
                })

        except Exception as e:
            print(f"⚠️ Error parsing block: {e}")
            continue

    return parsed_data


def save_to_mongo(data_list):
    if not data_list:
        print("No data to save.")
        return

    db = get_db()
    collection = db[COLLECTION_NAME]

    ops = []
    for item in data_list:
        # 数据清洗：vn.py 标准通常用 000001 (不带 .SZ) 或者 000001.SZ
        # 我们的数据库 stock_info 里存的是什么格式？假设是纯数字 symbol
        # 我们需要把 000004.SZ -> 000004
        raw_symbol = item['symbol']
        clean_symbol = raw_symbol.split('.')[0]

        ops.append(
            UpdateOne(
                {"symbol": clean_symbol},
                {
                    "$set": {
                        "st_history": item['st_history'],
                        "st_source_file": "uploaded_st_data",
                        "updated_at": datetime.now()
                    }
                },
                upsert=True
            )
        )

    if ops:
        print(f"🚀 Writing {len(ops)} ST history records into MongoDB...")
        result = collection.bulk_write(ops)
        print(
            f"✅ Completed. Matched: {result.matched_count}, Modified: {result.modified_count}, Upserted: {result.upserted_count}")


if __name__ == "__main__":
    # 1. 解析文件
    print(f"Reading {FILE_PATH}...")
    st_records = parse_st_text_file(FILE_PATH)
    print(f"Parsed {len(st_records)} stocks with ST history.")

    # 2. 存入数据库
    save_to_mongo(st_records)