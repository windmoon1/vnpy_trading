import vnpy_ctastrategy
import os
import subprocess


def open_system_strategy_folder():
    # 1. 找到 vnpy_ctastrategy 包的安装路径
    # 路径通常长这样: /Users/.../miniforge3/envs/vnpy_env/lib/python3.10/site-packages/vnpy_ctastrategy
    package_path = os.path.dirname(vnpy_ctastrategy.__file__)

    # 2. 定位到里面的 strategies 子文件夹
    strategies_path = os.path.join(package_path, "strategies")

    print(f"🎯 锁定目标路径: {strategies_path}")

    # 3. 检查文件夹是否存在
    if os.path.exists(strategies_path):
        print("✅ 文件夹存在，正在打开...")
        # 4. 调用 Mac 系统命令打开 Finder
        subprocess.run(["open", strategies_path])
    else:
        print("❌ 奇怪，strategies 文件夹不存在。")
        print(f"请尝试手动打开上一级: {package_path}")
        subprocess.run(["open", package_path])


if __name__ == "__main__":
    open_system_strategy_folder()