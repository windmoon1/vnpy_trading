from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy.trader.constant import Interval
from vnpy.trader.ui import create_qapp  # 关键：引入 GUI 核心
from datetime import datetime
import sys
import os

# 路径补丁
sys.path.append(os.getcwd())
from strategies.demo_strategy import DoubleMaStrategy


def show_best_performance():
    # 1. 【核心修复】在一切开始前，先创建 GUI 应用对象
    # 这就像先启动画板，再开始画画
    app = create_qapp()

    engine = BacktestingEngine()

    # 2. 设置回测参数
    engine.set_parameters(
        vt_symbol="BTCUSDT.SMART",
        interval=Interval.MINUTE,
        start=datetime(2019, 1, 1),
        end=datetime(2025, 11, 22),
        rate=0.5 / 1000,
        slippage=5,
        size=1,
        pricetick=0.01,
        capital=10_000_000,
    )

    # 3. 填入【冠军参数】
    engine.add_strategy(DoubleMaStrategy, {
        "fast_window": 50,
        "slow_window": 90,
        "fixed_size": 1
    })

    # 4. 加载数据 & 运行
    print("⏳ 正在加载数据...")
    engine.load_data()

    print("🚀 正在重跑回测...")
    engine.run_backtesting()

    engine.calculate_result()
    stats = engine.calculate_statistics()
    print(f"最终收益率: {stats['total_return']:.2f}%")

    # 5. 打印交易记录
    trades = engine.trades
    if trades:
        print("📝 最近 5 笔交易记录:")
        last_keys = list(trades.keys())[-5:]
        for key in last_keys:
            trade = trades[key]
            print(f"时间: {trade.datetime} | 方向: {trade.direction.value} | "
                  f"开平: {trade.offset.value} | 价格: {trade.price}")

    # 6. 【核心修复】启动图表并阻塞程序
    print("\n📈 正在启动图表...")
    engine.show_chart()

    print("✅ 窗口已启动！(请不要关闭控制台，关闭图表窗口后程序会自动结束)")

    # app.exec() 会让程序进入"发呆"状态，直到你手动关闭图表窗口
    app.exec()


if __name__ == "__main__":
    show_best_performance()