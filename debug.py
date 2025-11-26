# debug_binance.py
import traceback

print("--- Starting Debug for Binance ---")

try:
    print("1. Attempting to import vnpy_binance...")
    import vnpy_binance
    print("   ✅ vnpy_binance package found.")
except ImportError:
    print("   ❌ vnpy_binance package NOT installed.")

try:
    print("2. Attempting to import Spot Gateway...")
    from vnpy_binance import BinanceSpotGateway
    print("   ✅ BinanceSpotGateway loaded success.")
except Exception:
    print("   ❌ Failed to import Spot Gateway. Reason:")
    # 这行代码会打印出红色的详细错误信息
    traceback.print_exc()

try:
    print("3. Attempting to import USDT Gateway...")
    from vnpy_binance import BinanceUsdtGateway
    print("   ✅ BinanceUsdtGateway loaded success.")
except Exception:
    print("   ❌ Failed to import USDT Gateway. Reason:")
    traceback.print_exc()

print("--- End Debug ---")