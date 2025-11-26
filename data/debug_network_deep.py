import requests
import akshare as ak
import os


# --- 1. 代理设置 ---
# 如果你决定尝试开 VPN，请注释掉下面这三行！
# 如果你决定裸连，请保留这三行。
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['all_proxy'] = ''

def test_connection():
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    print("🩺 Starting Deep Network Diagnosis...")

    # 1. 测试百度 (基础互联网连接)
    try:
        print("\n1️⃣ Pinging Baidu (Basic Connectivity)...")
        resp = requests.get("https://www.baidu.com", headers=headers, timeout=5)
        print(f"   ✅ Baidu Status: {resp.status_code}")
    except Exception as e:
        print(f"   ❌ Baidu Failed: {e}")

    # 2. 测试东方财富 (HTTP 接口 - AkShare 常用)
    try:
        print("\n2️⃣ Testing EastMoney API (HTTP)...")
        url = "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5"
        resp = requests.get(url, headers=headers, timeout=5)
        print(f"   ✅ EastMoney Status: {resp.status_code}")
    except Exception as e:
        print(f"   ❌ EastMoney Failed: {e}")

    # 3. 测试 AkShare - 新浪源 (替代方案)
    try:
        print("\n3️⃣ Testing AkShare (Sina Source)...")
        # 这是一个获取历史行情数据的接口，走的是新浪财经，通常对海外IP更友好
        df = ak.stock_zh_index_daily(symbol="sh000001")
        print(f"   ✅ Sina Data Retrieved: {len(df)} rows.")
    except Exception as e:
        print(f"   ❌ Sina Failed: {e}")


if __name__ == "__main__":
    test_connection()