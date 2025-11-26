from vnpy_ctastrategy.backtesting import BacktestingEngine, OptimizationSetting
from vnpy.trader.constant import Interval
from datetime import datetime
import sys
import os
import multiprocessing

# 路径补丁
sys.path.append(os.getcwd())
from strategies.demo_strategy import DoubleMaStrategy


def run_optimization():
    engine = BacktestingEngine()

    # 1. 基础设置 (和回测一致)
    engine.set_parameters(
        vt_symbol="BTCUSDT.SMART",
        interval=Interval.MINUTE,
        start=datetime(2019, 1, 1),  # 优化为了快一点，我们先跑最近 5-6 年
        end=datetime(2025, 11, 22),
        rate=0.5 / 1000,
        slippage=5,
        size=1,
        pricetick=0.01,
        capital=10_000_000,
    )

    engine.add_strategy(DoubleMaStrategy, {})

    # 2. 加载数据
    print("⏳ 正在加载数据用于优化 (这需要一点时间)...")
    engine.load_data()
    print(f"✅ 数据加载完成，数据量: {len(engine.history_data)}")

    # 3. 设置优化目标
    setting = OptimizationSetting()
    setting.set_target("total_return")  # 目标：寻找总回报最高的组合

    # 4. 设置参数搜索空间 (暴力穷举)
    # 寻找 15分钟级别 的均线组合
    # 我们让它找更长周期的线，减少交易频率
    # fast: 20, 30, ... 100
    # slow: 50, 60, ... 200
    setting.add_parameter("fast_window", 20, 100, 10)
    setting.add_parameter("slow_window", 50, 200, 10)

    # 5. 运行优化
    print("🚀 开始多进程参数优化 (CPU火力全开)...")
    # 这里的 result 会返回表现最好的前 10 组参数
    results = engine.run_optimization(setting)

    # 6. 输出结果
    print("\n🏆 优化结果 Top 5:")
    for i, result in enumerate(results[:5]):
        print(f"No.{i + 1}: {result}")


if __name__ == "__main__":
    # Mac 系统必须加这行，否则多进程会报错
    multiprocessing.set_start_method("fork")
    run_optimization()