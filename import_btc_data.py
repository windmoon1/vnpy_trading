import requests
import time
import traceback  # 引入这个库以便查看真实报错
from datetime import datetime, timedelta
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData
from vnpy.trader.database import get_database

# --- 配置区域 ---
# 如果不开全局VPN，请取消下面这行的注释并填入端口
PROXY_URL = None  # "http://127.0.0.1:7890"
SYMBOL = "BTCUSDT"
START_DATE = "2020-01-01"
END_DATE = "2020-01-02"


def download_5y_data():
    print(f"--- 🚀 开始下载 {SYMBOL} 1分钟数据 ({START_DATE} 至 {END_DATE}) ---")

    # 1. 准备数据库
    database = get_database()

    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")

    current_start = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    total_bars = 0
    session = requests.Session()
    if PROXY_URL:
        session.proxies = {"http": PROXY_URL, "https": PROXY_URL}

    while current_start < end_ts:
        try:
            url = "https://api.binance.com/api/v3/klines"
            params = {
                "symbol": SYMBOL,
                "interval": "1m",
                "startTime": current_start,
                "limit": 1000
            }

            resp = session.get(url, params=params, timeout=10)

            # 如果状态码不是200，抛出异常
            if resp.status_code != 200:
                print(f"❌ API 请求失败: 状态码 {resp.status_code}, 内容: {resp.text}")
                break

            data = resp.json()

            if not isinstance(data, list):
                print(f"❌ 数据格式错误: {data}")
                break

            if len(data) == 0:
                print("⚠️ 无更多数据，结束。")
                break

            bars = []
            for row in data:
                dt = datetime.fromtimestamp(row[0] / 1000)

                bar = BarData(
                    symbol=SYMBOL,
                    # 【关键修改】: 这里改成了 SMART，确保兼容性
                    exchange=Exchange.SMART,
                    datetime=dt,
                    interval=Interval.MINUTE,
                    volume=float(row[5]),
                    open_price=float(row[1]),
                    high_price=float(row[2]),
                    low_price=float(row[3]),
                    close_price=float(row[4]),
                    gateway_name="DB",
                    open_interest=0
                )
                bars.append(bar)

            if bars:
                database.save_bar_data(bars)
                total_bars += len(bars)

                last_ts = data[-1][0]
                current_start = last_ts + 60000

                last_dt_str = datetime.fromtimestamp(last_ts / 1000).strftime("%Y-%m-%d %H:%M")
                print(f"✅ 已存入: {total_bars} 条 | 进度: {last_dt_str}")

            time.sleep(0.1)

        except Exception as e:
            print(f"❌ 发生错误: {e}")
            # 打印详细的 traceback，这样你就知道是网络还是代码问题了
            traceback.print_exc()
            print("3秒后重试...")
            time.sleep(3)
            # 如果是 Attribute Error，通常重试也没用，这里 break 比较好，但为了保险还是 continue
            if "AttributeError" in str(e):
                print("🛑 检测到代码错误，停止运行。")
                break
            continue

    print("=" * 30)
    print(f"🎉 下载完成！总计入库: {total_bars} 条")


if __name__ == "__main__":
    download_5y_data()