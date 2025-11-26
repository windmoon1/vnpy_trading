from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)


class AtrTrailingStrategy(CtaTemplate):
    """
    V3.0: ATR过滤 + 移动止损 (Trailing Stop)
    核心改进: 既然均线出场太慢，我们就用移动止损来锁定利润。
    """
    author = "QuantDev Copilot"

    # --- 参数 ---
    fast_window = 50
    slow_window = 90
    atr_window = 24
    atr_ratio_threshold = 0.002  # 0.2% 波动率门槛

    trailing_percent = 0.04  # 【新增】回撤 4% 就止盈离场
    fixed_size = 1

    # --- 变量 ---
    fast_ma = 0.0
    slow_ma = 0.0
    atr_value = 0.0
    atr_ratio = 0.0

    intra_trade_high = 0.0  # 【新增】持仓期间的最高价
    intra_trade_low = 0.0  # 【新增】持仓期间的最低价

    parameters = ["fast_window", "slow_window", "atr_window", "atr_ratio_threshold", "trailing_percent", "fixed_size"]
    variables = ["fast_ma", "slow_ma", "atr_ratio", "intra_trade_high", "intra_trade_low"]

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
        self.cancel_all()

        am = self.am
        am.update_bar(bar)
        if not am.inited:
            return

        # 计算指标
        fast_ma_array = am.sma(self.fast_window, array=True)
        self.fast_ma = fast_ma_array[-1]
        fast_ma1 = fast_ma_array[-2]

        slow_ma_array = am.sma(self.slow_window, array=True)
        self.slow_ma = slow_ma_array[-1]
        slow_ma1 = slow_ma_array[-2]

        self.atr_value = am.atr(self.atr_window)
        self.atr_ratio = self.atr_value / bar.close_price

        cross_over = (fast_ma1 <= slow_ma1) and (self.fast_ma > self.slow_ma)
        cross_below = (fast_ma1 >= slow_ma1) and (self.fast_ma < self.slow_ma)
        volatility_ok = (self.atr_ratio > self.atr_ratio_threshold)

        # ===========================
        # 核心逻辑：移动止损 (Trailing Stop)
        # ===========================
        if self.pos > 0:
            # 多单持仓中：更新最高价
            self.intra_trade_high = max(self.intra_trade_high, bar.high_price)
            # 计算止损线：最高价 * (1 - 回撤比例)
            long_stop = self.intra_trade_high * (1 - self.trailing_percent)

            # 如果当前价格跌破止损线，市价止盈/止损
            if bar.close_price < long_stop:
                self.sell(bar.close_price - 10, abs(self.pos))  # 立即离场
                return  # 离场后本次循环结束

        elif self.pos < 0:
            # 空单持仓中：更新最低价
            self.intra_trade_low = min(self.intra_trade_low, bar.low_price)
            # 计算止损线：最低价 * (1 + 回撤比例)
            short_stop = self.intra_trade_low * (1 + self.trailing_percent)

            if bar.close_price > short_stop:
                self.cover(bar.close_price + 10, abs(self.pos))
                return

        # ===========================
        # 入场逻辑 (Entry)
        # ===========================
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

    def on_trade(self, trade: TradeData):
        """成交回调：重置最高/最低价"""
        if trade.offset.value == "开":
            if trade.direction.value == "多":
                self.intra_trade_high = trade.price
            else:
                self.intra_trade_low = trade.price
        self.put_event()