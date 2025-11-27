"""
Module: fix_akshare.py
Description: AKShare 深度运行时补丁 (Self-Contained Edition)
Fix: 彻底解决翻页过快问题。不再依赖库函数引用，而是直接嵌入智能分页逻辑。
"""

import math
import time
import random
import pandas as pd
import requests
from akshare.utils.tqdm import get_tqdm
import akshare.stock.stock_board_concept_em as em_module
from functools import lru_cache

# =========================================================================
# 🐢 核心：自包含的智能分页器 (自带强制休眠)
# =========================================================================
def smart_fetch_paginated_data(url: str, base_params: dict, timeout: int = 15):
    """
    完全重写的智能分页函数，不依赖 akshare 原版代码。
    """
    params = base_params.copy()

    # 1. 强制回归标准页容量 (浏览器行为)
    if "pz" in params and int(params["pz"]) > 100:
        params["pz"] = "100"

    # 2. 获取第一页
    try:
        r = requests.get(url, params=params, timeout=timeout)
        data_json = r.json()
    except Exception as e:
        print(f"⚠️ First page request failed: {e}")
        return pd.DataFrame()

    if not data_json or "data" not in data_json or not data_json["data"]:
        return pd.DataFrame()

    diff_data = data_json["data"]["diff"]
    # 容错处理：有时 diff 是 None
    if not diff_data:
        return pd.DataFrame()

    per_page_num = len(diff_data)
    total_count = data_json["data"]["total"]

    # 防止除零错误
    if per_page_num == 0:
        return pd.DataFrame()

    total_page = math.ceil(total_count / per_page_num)

    temp_list = [pd.DataFrame(diff_data)]

    # 3. 智能循环 (如果有多页)
    if total_page > 1:
        tqdm = get_tqdm()
        desc = f"🐢 Slow-Motion Fetching ({total_page} pages)"

        for page in tqdm(range(2, total_page + 1), leave=False, desc=desc):
            # 🔥🔥 强制休眠区 🔥🔥
            # 这是一个无法被绕过的物理休眠
            sleep_t = random.uniform(1.0, 2.0)
            time.sleep(sleep_t)

            params.update({"pn": page})
            try:
                r = requests.get(url, params=params, timeout=timeout)
                data_json = r.json()
                if data_json["data"] and "diff" in data_json["data"]:
                    temp_list.append(pd.DataFrame(data_json["data"]["diff"]))
            except Exception as e:
                print(f"   ⚠️ Error on page {page}: {e}. Skipping.")
                # 遇到错稍微多睡会
                time.sleep(5)
                continue

    temp_df = pd.concat(temp_list, ignore_index=True)

    # 排序逻辑 (保留原版特性)
    if "f3" in temp_df.columns:
        temp_df["f3"] = pd.to_numeric(temp_df["f3"], errors="coerce")
        temp_df.sort_values(by=["f3"], ascending=False, inplace=True, ignore_index=True)

    temp_df.reset_index(inplace=True)
    return temp_df

# =========================================================================
# 🔧 补丁应用逻辑
# =========================================================================

def apply_patches():
    print("🔧 Applying AKShare hard-patches...")
    patch_stock_board_concept_cons_em()
    patch_stock_board_concept_name_em()
    print("✅ Patches applied: 'Fast-Scroll' killed successfully.")

def patch_stock_board_concept_cons_em():
    """
    替换 akshare.stock.stock_board_concept_em.stock_board_concept_cons_em
    """
    def fixed_cons_func(symbol: str = "融资融券") -> pd.DataFrame:
        stock_board_code = symbol
        url = "https://29.push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": "1",
            "pz": "100",
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            "fs": f"b:{stock_board_code} f:!50",
            "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,"
            "f24,f25,f22,f11,f62,f128,f136,f115,f152,f45",
        }

        # 🔥 直接调用本地定义的 smart 函数，而不是 akshare 里的
        temp_df = smart_fetch_paginated_data(url, params)

        if temp_df.empty:
            return pd.DataFrame()

        temp_df.columns = [
            "序号", "_", "最新价", "涨跌幅", "涨跌额", "成交量", "成交额", "振幅", "换手率",
            "市盈率-动态", "_", "_", "代码", "_", "名称", "最高", "最低", "今开", "昨收",
            "_", "_", "_", "市净率", "_", "_", "_", "_", "_", "_", "_", "_", "_", "_",
        ]
        temp_df = temp_df[[
            "序号", "代码", "名称", "最新价", "涨跌幅", "涨跌额", "成交量", "成交额",
            "振幅", "最高", "最低", "今开", "昨收", "换手率", "市盈率-动态", "市净率",
        ]].copy()

        numeric_cols = [
            "最新价", "涨跌幅", "涨跌额", "成交量", "成交额", "振幅",
            "最高", "最低", "今开", "昨收", "换手率", "市盈率-动态", "市净率"
        ]
        for col in numeric_cols:
            temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")
        return temp_df

    em_module.stock_board_concept_cons_em = fixed_cons_func

def patch_stock_board_concept_name_em():
    """
    替换 akshare.stock.stock_board_concept_em.stock_board_concept_name_em
    """
    @lru_cache()
    def fixed_func() -> pd.DataFrame:
        url = "https://79.push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": "1",
            "pz": "100",
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            "fs": "m:90 t:3 f:!50",
            "fields": "f2,f3,f4,f8,f12,f14,f15,f16,f17,f18,f20,f21,f24,f25,f22,f33,f11,f62,f128,f124,f107,f104,f105,f136",
        }

        # 🔥 同样直接调用本地 smart 函数
        temp_df = smart_fetch_paginated_data(url, params)

        temp_df.columns = [
            "排名", "最新价", "涨跌幅", "涨跌额", "换手率", "_", "板块代码", "板块名称",
            "_", "_", "_", "_", "总市值", "_", "_", "_", "_", "_", "_",
            "上涨家数", "下跌家数", "_", "_", "领涨股票", "_", "_", "领涨股票-涨跌幅",
        ]

        temp_df = temp_df[[
            "排名", "板块名称", "板块代码", "最新价", "涨跌额", "涨跌幅",
            "总市值", "换手率", "上涨家数", "下跌家数", "领涨股票", "领涨股票-涨跌幅",
        ]].copy()

        cols = ["最新价", "涨跌额", "涨跌幅", "总市值", "换手率", "上涨家数", "下跌家数", "领涨股票-涨跌幅"]
        for col in cols:
            temp_df[col] = pd.to_numeric(temp_df[col], errors="coerce")
        return temp_df

    em_module.__stock_board_concept_name_em = fixed_func