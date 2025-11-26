# backtest_btc_5y.py

# 1. 这里的导入路径必须修正！
# 错误: from vnpy.app.cta_strategy.backtesting ...
# 正确: from vnpy_ctastrategy.backtesting ...
from vnpy_ctastrategy.backtesting import BacktestingEngine, OptimizationSetting

from datetime import datetime
import sys
import os

# 路径补丁
sys.path.append(os.getcwd())

# 导入策略
from strategies.demo_strategy import DoubleMaStrategy
from vnpy.trader.constant import Interval


def run_backtest():
    engine = BacktestingEngine()

    engine.set_parameters(
        vt_symbol="BTCUSDT.SMART",
        interval=Interval.MINUTE,  # 👈哪怕是IDE不报错，也要改成这样，这是最标准的写法
        start=datetime(2017, 8, 17),  # 币安最早数据
        end=datetime(2025, 11, 20),
        rate=0.5 / 1000,
        slippage=5,
        size=1,
        pricetick=0.01,
        capital=10_000_000,
    )

    engine.add_strategy(DoubleMaStrategy, {
        "fast_window": 10,
        "slow_window": 20,
    })

    print("⏳ 正在加载数据...")
    engine.load_data()
    print(f"✅ 数据加载完毕: {len(engine.history_data)} 条")

    print("🚀 开始回测...")
    engine.run_backtesting()

    print("\n--- 📊 回测结果 ---")
    df = engine.calculate_result()
    stats = engine.calculate_statistics()

    print(f"总收益率: {stats['total_return']:.2f}%")
    print(f"最大回撤: {stats['max_drawdown']:.2f}%")
    print(f"夏普比率: {stats['sharpe_ratio']:.2f}")

    engine.show_chart()


if __name__ == "__main__":
    run_backtest()