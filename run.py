# run.py - The "Ultimate" Version
# 强制修复路径问题，确保策略能被加载

import sys
import os

# --- 1. 核心路径修复 (关键) ---
# 获取当前 run.py 所在的绝对路径
current_path = os.path.abspath(os.path.dirname(__file__))
# 将这个路径加入 Python 搜索路径
sys.path.append(current_path)
# 强制切换工作目录到这里 (防止 PyCharm 用错误的目录启动)
os.chdir(current_path)

print(f"🚀 启动目录已锁定: {os.getcwd()}")
print(f"📂 正在扫描策略文件夹: {os.path.join(os.getcwd(), 'strategies')}")

from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp

# --- 2. 应用模块 ---
from vnpy_ctastrategy import CtaStrategyApp
from vnpy_ctabacktester import CtaBacktesterApp
from vnpy_datamanager import DataManagerApp
from vnpy_chartwizard import ChartWizardApp
from vnpy_riskmanager import RiskManagerApp

# --- 3. 数据服务 ---
try:
    from vnpy_tushare import TushareDatafeed

    DATA_TUSHARE = True
except ImportError:
    DATA_TUSHARE = False

# --- 4. 显式导入策略 (虽然显示灰名，但有助于 Debug) ---
try:
    from strategies.demo_strategy import DoubleMaStrategy

    print("✅ DoubleMaStrategy 导入成功")
except ImportError as e:
    print(f"❌ DoubleMaStrategy 导入失败: {e}")

try:
    from strategies.filtered_strategy import AtrFilterStrategy

    print("✅ AtrFilterStrategy 导入成功")
except ImportError as e:
    print(f"❌ AtrFilterStrategy 导入失败: {e}")

# --- 5. 交易接口 (只保留现货，防报错) ---
try:
    from vnpy_binance import BinanceSpotGateway

    BINANCE_INSTALLED = True
except ImportError:
    BINANCE_INSTALLED = False


def main():
    qapp = create_qapp()
    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)

    # 加载数据服务
    if DATA_TUSHARE:
        main_engine.add_datafeed(TushareDatafeed)

    # 加载应用
    main_engine.add_app(CtaStrategyApp)
    main_engine.add_app(CtaBacktesterApp)
    main_engine.add_app(DataManagerApp)
    main_engine.add_app(ChartWizardApp)
    main_engine.add_app(RiskManagerApp)

    # 加载接口
    if BINANCE_INSTALLED:
        main_engine.add_gateway(BinanceSpotGateway)

    main_window = MainWindow(main_engine, event_engine)
    main_window.showMaximized()

    print("\n⭐️ 系统启动成功！请在 CTA回测 中查找策略。")
    qapp.exec()


if __name__ == "__main__":
    main()