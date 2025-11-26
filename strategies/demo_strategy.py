# strategies/demo_strategy.py

# 1. 从 vnpy 核心导入工具和数据结构 (最稳定的路径)
from vnpy.trader.utility import BarGenerator, ArrayManager
from vnpy.trader.object import TickData, BarData, TradeData, OrderData

# 2. 从 CTA 策略模块导入模板和特殊对象
from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
)


class DoubleMaStrategy(CtaTemplate):
    """
    双均线策略 (适配 15分钟合成)
    """
    author = "QuantDev Copilot"

    fast_window = 10
    slow_window = 20
    fixed_size = 1

    fast_ma0 = 0.0
    slow_ma0 = 0.0

    parameters = ["fast_window", "slow_window", "fixed_size"]
    variables = ["fast_ma0", "slow_ma0"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        # K线生成器: 1m -> 15m
        self.bg = BarGenerator(self.on_bar, 15, self.on_15min_bar)
        self.am = ArrayManager()

    def on_init(self):
        self.write_log("策略初始化")
        self.load_bar(10)  # <--- 去掉 's'，改为单数

    def on_start(self):
        self.write_log("策略启动")

    def on_stop(self):
        self.write_log("策略停止")

    def on_tick(self, tick: TickData):
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        """收到 1分钟 K线 -> 喂给生成器"""
        self.bg.update_bar(bar)

    def on_15min_bar(self, bar: BarData):
        """收到 15分钟 K线 -> 交易逻辑"""
        self.cancel_all()

        am = self.am
        am.update_bar(bar)
        if not am.inited:
            return

        fast_ma_array = am.sma(self.fast_window, array=True)
        self.fast_ma0 = fast_ma_array[-1]
        fast_ma1 = fast_ma_array[-2]

        slow_ma_array = am.sma(self.slow_window, array=True)
        self.slow_ma0 = slow_ma_array[-1]
        slow_ma1 = slow_ma_array[-2]

        cross_over = (fast_ma1 <= slow_ma1) and (self.fast_ma0 > self.slow_ma0)
        cross_below = (fast_ma1 >= slow_ma1) and (self.fast_ma0 < self.slow_ma0)

        if cross_over:
            if self.pos < 0:
                self.cover(bar.close_price + 50, abs(self.pos))
            if self.pos == 0:
                self.buy(bar.close_price + 50, self.fixed_size)

        elif cross_below:
            if self.pos > 0:
                self.sell(bar.close_price - 50, abs(self.pos))
            if self.pos == 0:
                self.short(bar.close_price - 50, self.fixed_size)

        self.put_event()



