import os
import sys

# =========================================================
# 🛑 核心验证：直连补丁 (必须在 import requests 前执行)
# =========================================================
print("🛡️  正在应用直连补丁 (强制清除代理设置)...")
# 这几行代码会告诉 Python："忘掉系统代理，忘掉 VPN，直接用网卡发包"
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['all_proxy'] = ''
os.environ['NO_PROXY'] = '*'

import requests
import akshare as ak
import pandas as pd


def test_single_stock():
    print("-" * 50)
    print("🧪 开始测试：东方财富接口 (EastMoney) - 直连模式")

    symbol = "600519"  # 贵州茅台
    start_date = "20240101"
    end_date = "20240110"

    try:
        print(f"👉 正在尝试下载 {symbol} (茅台) 的日线数据...")

        # 调用 AkShare 的东财接口
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"
        )

        if not df.empty:
            print("✅ 连接成功！(Connection Established)")
            print(f"   数据行数: {len(df)}")
            print("   数据预览:")
            print(df[['日期', '收盘', '成交量']].head())
            return True
        else:
            print("⚠️ 连接没报错，但返回数据为空 (可能是参数问题)。")
            return False

    except Exception as e:
        print(f"❌ 连接失败: {e}")
        # 如果这里报错 RemoteDisconnected，说明直连补丁没生效，或者网络本身有问题
        return False


def test_raw_requests():
    print("-" * 50)
    print("🧪 双重验证：Requests 底层直连测试")
    url = "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5&fs=m:0+t:6"

    try:
        # 不加任何 headers，完全裸连，测试纯网络通断
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            print("✅ HTTP 底层握手成功！")
        else:
            print(f"❌ HTTP 状态码异常: {resp.status_code}")
    except Exception as e:
        print(f"❌ HTTP 请求失败: {e}")


if __name__ == "__main__":
    test_raw_requests()
    success = test_single_stock()

    print("-" * 50)
    if success:
        print("🎉 验证通过！你可以放心运行 [download_stock_data.py] 了。")
    else:
        print("🚫 验证失败，请不要运行全量下载，继续排查网络。")