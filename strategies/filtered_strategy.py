from vnpy_ctastrategy import (
    CtaTemplate,
    BarData,
    TickData,
    BarGenerator,
    ArrayManager,
)


class AtrFilterStrategy(CtaTemplate):
    """
    升级版：使用 ATR 百分比过滤的双均线策略
    (适应 BTC 从 1k 到 90k 的巨大跨度)
    """
    author = "QuantDev Copilot"

    # --- 参数 ---
    fast_window = 20
    slow_window = 60
    atr_window = 24

    # 【核心修改】这里改成百分比阈值
    # 0.002 代表 0.2% (即15分钟波动超过0.2%才交易)
    atr_ratio_threshold = 0.002

    fixed_size = 1

    # --- 变量 ---
    fast_ma = 0.0
    slow_ma = 0.0
    atr_value = 0.0
    atr_ratio = 0.0  # 当前的波动率百分比

    parameters = ["fast_window", "slow_window", "atr_window", "atr_ratio_threshold", "fixed_size"]
    variables = ["fast_ma", "slow_ma", "atr_value", "atr_ratio"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)

        self.bg = BarGenerator(self.on_bar, 15, self.on_15min_bar)
        self.am = ArrayManager()

    def on_init(self):
        self.write_log("策略初始化")
        self.load_bar(10)

    def on_start(self):
        self.write_log("策略启动")

    def on_stop(self):
        self.write_log("策略停止")

    def on_tick(self, tick: TickData):
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        self.bg.update_bar(bar)

    def on_15min_bar(self, bar: BarData):
        """15分钟逻辑"""
        self.cancel_all()

        am = self.am
        am.update_bar(bar)
        if not am.inited:
            return

        # 1. 计算双均线
        fast_ma_array = am.sma(self.fast_window, array=True)
        self.fast_ma = fast_ma_array[-1]
        fast_ma1 = fast_ma_array[-2]

        slow_ma_array = am.sma(self.slow_window, array=True)
        self.slow_ma = slow_ma_array[-1]
        slow_ma1 = slow_ma_array[-2]

        # 2. 计算 ATR (波动率)
        self.atr_value = am.atr(self.atr_window)

        # 【核心修改】计算 ATR 占比 (ATR / 价格)
        self.atr_ratio = self.atr_value / bar.close_price

        # 3. 判断均线交叉
        cross_over = (fast_ma1 <= slow_ma1) and (self.fast_ma > self.slow_ma)
        cross_below = (fast_ma1 >= slow_ma1) and (self.fast_ma < self.slow_ma)

        # 4. 【核心过滤】判断波动率百分比
        volatility_ok = (self.atr_ratio > self.atr_ratio_threshold)

        if cross_over:
            if self.pos < 0:
                self.cover(bar.close_price + 50, abs(self.pos))
            if self.pos == 0 and volatility_ok:
                self.buy(bar.close_price + 50, self.fixed_size)

        elif cross_below:
            if self.pos > 0:
                self.sell(bar.close_price - 50, abs(self.pos))
            if self.pos == 0 and volatility_ok:
                self.short(bar.close_price - 50, self.fixed_size)

        self.put_event()