import sys
import os

# 路径补丁
sys.path.append(os.getcwd())

print("正在尝试加载 AtrFilterStrategy...")

try:
    # 尝试导入该模块，如果有错，Python 会立刻报错
    from strategies.filtered_strategy import AtrFilterStrategy
    print("✅ 成功！策略文件没有语法错误。")
    print(f"策略类名为: {AtrFilterStrategy.__name__}")
except Exception as e:
    print("\n❌ 失败！策略文件包含错误，导致 vn.py 无法识别它。")
    print("-" * 30)
    print(f"错误类型: {type(e).__name__}")
    print(f"错误详情: {e}")
    print("-" * 30)
    print("请根据上面的报错信息修改 filtered_strategy.py 文件。")