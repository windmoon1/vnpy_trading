"""
Script: Check Industry Data Status
----------------------------------
目标: 检查行业数据的存储状态，验证 Symbol 格式和最新日期。
"""
from pymongo import MongoClient

def check():
    client = MongoClient("localhost", 27017)
    db = client["vnpy_stock"]
    col = db["index_daily"]

    print("🏥 行业数据体检报告:")

    # 1. 统计行业总数
    count = col.count_documents({"category": "INDUSTRY"})
    print(f"   - 行业 K 线总数: {count}")

    if count == 0:
        print("   ❌ 数据库中没有行业数据！(难怪会重下)")
        return

    # 2. 抽样检查 Symbol 格式
    print("\n   - 抽样检查 (Symbol 格式):")
    samples = col.find({"category": "INDUSTRY"}).limit(5)
    unique_symbols = col.distinct("symbol", {"category": "INDUSTRY"})
    print(f"   - 行业板块数量: {len(unique_symbols)} 个")

    for doc in samples:
        print(f"     Symbol: {doc['symbol']} | Date: {doc['datetime']} | Name: {doc.get('name')}")

    # 3. 检查是否有 'BK' 前缀
    bk_count = 0
    for s in unique_symbols:
        if s.startswith("BK"):
            bk_count += 1

    print(f"\n   - 带 'BK' 前缀的比例: {bk_count} / {len(unique_symbols)}")
    if bk_count < len(unique_symbols):
        print("   ⚠️ 警告: 部分/全部行业代码缺失 'BK' 前缀，这可能导致断点检查失效！")

if __name__ == "__main__":
    check()