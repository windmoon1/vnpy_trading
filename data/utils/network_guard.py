"""
Module: network_guard.py
Description: 全局网络卫士 V7.1 (Consistency Edition)
Upgrades:
    1. UA Locking: 当提供 Cookie 时，强制锁定 User-Agent，避免因 UA 突变导致的会话失效。
    2. Consistency: 确保 Cookie 和 User-Agent 一一对应，模拟真实的稳定浏览器环境。
Author: QuantDev Copilot
"""

import time
import random
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import RequestException, ConnectionError, SSLError, ProxyError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================================================================
# 🍪 核心配置：身份信息 (请从浏览器 F12 网络面板复制)
# =========================================================================
# 1. Cookie (必填，用于身份认证)
USER_COOKIE = "qgqp_b_id=2771b5010e8b63546b37537f5901f8a3; st_nvi=pOke3RP07EkOlvZJemHed43f2; nid=0ce3a2e7865907ec479232c109e9c66d; nid_create_time=1764073863637; gvi=GEDeHwMschwCq7FNYpfS59f05; gvi_create_time=1764073863637; st_si=38079765608464; websitepoptg_api_time=1764243410000; fullscreengg=1; fullscreengg2=1; mtp=1; st_asi=delete; ct=rkSrezSmAoAyOwnE6FVnJBXYLHaPg-D73zTvFjsGj8C8kt6MUOxOMEtfp-XWYhG4lPnG8OfqFNKZ9SzjQ9taZOnjZ_lyHOCEHZ5az-nFeACDDboGNDyuMZVLSvLNeTrtTPIhZ-7qBWWo4Hnyu6q3F9N-Au5js1uYglb19LzODVk; ut=FobyicMgeV78LQRcptXpVQpGBWvHhNYqa-29dJl5_ledSGQxSih6xWqJytaKHgg3AahmBQxjwcwvRrcXbQNZ2BScckDF6wO7f4H2M65hQMtEeKqR_4ruANsgLUKgrrGV6fmvvvpMVmTaoBiWs26cxtry2yDHsUMUjsEfmIxcLl0fwEgv8_AAivGWAWr1y0P0IMw2GTce-JIIjXJc3Y5g5QQ3WXVqIDccVTLKwnTmKQsjp5ca1XE0Onixp86u_b0RBWTxs7REjQvOlWHRsyL9OBm0Eswyl1cyENNACkGqYyHiKsqN6WVlfouL5H0dgsDKywXVJUh2q6AaxjdzkjuemPsBgcckh10CLlmmCQIXkmjtU0PSRMDGH_a5M2mYB6wRNQAEhmnNU1J6ZUx_mEUCtqNrSfsB5yQbcSaUy6SufuRt5aG3pUaHKs_omNxt1YM6Z5RUc6l3h4b_Ow7JICrW8MqqAN8Wgl5I7TVV6lk0OH6gWTrZe-ZQeA; pi=7068007406398526%3Bo7068007406398526%3B%E8%82%A1%E5%8F%8B2566w61O52%3BoG3wDz1W%2Bqg75mh2uiJ7qXfZjLiIArqJDGHvkFf45WVKKBXlHpgxWPbIhdHPoTgzwgCHE53bPRWRiaLtr92D%2FEPrlk4sOtB3iRzjCf6LUZLBCOpV0jGGxueWdyj32fpnCI7iEDA1P%2Fw4stUBhRV3x1rpAMM%2B0oSSE%2BqBjvDAR11yNwpn8HW3PzuVA626KgC1%2FEMUVkFh%3BIFLG689wE753Q5nrqrrlw2fsA5ZpFz%2B0sWpquOI4Jtl9p8oXULF0tUTtJp%2BFFyAsOCiB9vXcICpcvSDzRKutnNmPOSKVNPhk0FQ0Q%2BxrVS2P6E%2B19gqHAGGO6wOxCUC24Bq4MUNoAd%2B9BK95wGKzfL1CHRiwow%3D%3D; uidal=7068007406398526%e8%82%a1%e5%8f%8b2566w61O52; sid=; vtpst=|; st_pvi=61406708383604; st_sp=2025-11-25%2023%3A38%3A40; st_inirUrl=https%3A%2F%2Flink.csdn.net%2F; st_sn=38; st_psi=202511272157344-113200313002-4278777050"  # <--- 在此处粘贴你的 Cookie

# 2. User-Agent (必填，必须与 Cookie 来源浏览器一致！)
# 如何获取: 在 F12 -> Network -> Request Headers -> User-Agent 字段复制
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36"

# =========================================================================
# 🎭 备用身份池 (仅在未提供 Cookie 时用于匿名伪装)
# =========================================================================
RANDOM_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

DOMAIN_REFERERS = {
    "eastmoney.com": "https://quote.eastmoney.com/",
    "10jqka.com.cn": "http://q.10jqka.com.cn/",
}

class NetworkGuard:
    _session = None
    _original_get = None
    _original_post = None
    _is_patched = False

    # 策略
    MAX_RESURRECTIONS = 3

    @classmethod
    def _get_ua(cls):
        """
        获取 User-Agent 策略:
        1. 如果有 Cookie，必须使用配套的固定 UA。
        2. 如果无 Cookie，则随机轮换 UA 进行伪装。
        """
        if USER_COOKIE and USER_AGENT:
            return USER_AGENT.strip()
        else:
            return random.choice(RANDOM_USER_AGENTS)

    @classmethod
    def rotate_identity(cls):
        """
        [外部调用] 重置会话
        注意：在有 Cookie 模式下，Rotate 只是重建 TCP 连接，不会改变身份特征。
        """
        if cls._session:
            try: cls._session.close()
            except: pass

        sess = requests.Session()
        ua = cls._get_ua()

        # 基础头
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "User-Agent": ua,
            "DNT": "1",
        }

        # 🔥 注入 VIP 通行证
        if USER_COOKIE:
            headers["Cookie"] = USER_COOKIE.strip()

        sess.headers.update(headers)

        # 基础重试
        retry = Retry(
            total=3, backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry)
        sess.mount("https://", adapter)
        sess.mount("http://", adapter)

        cls._session = sess

    @classmethod
    def install(cls):
        if cls._is_patched: return
        print(f"🛡️  NetworkGuard V7.1 (Consistency) Installed.")

        if USER_COOKIE:
            print("✅  Authenticated Mode: Cookie loaded.")
            print(f"    UA Locked: {USER_AGENT[:30]}...")
        else:
            print("⚠️  Anonymous Mode: Using random identity rotation.")

        cls.rotate_identity()
        cls._original_get = requests.get
        cls._original_post = requests.post

        def patched_request(method, url, **kwargs):
            # 1. 注入 Referer
            req_headers = kwargs.get("headers") or {}
            for domain, referer in DOMAIN_REFERERS.items():
                if domain in url and "Referer" not in req_headers:
                    req_headers["Referer"] = referer
                    break

            # 双重保险: 确保 Cookie 在 header 里
            if USER_COOKIE and "Cookie" not in req_headers:
                req_headers["Cookie"] = USER_COOKIE.strip()

            kwargs["headers"] = req_headers
            if "timeout" not in kwargs: kwargs["timeout"] = 20

            # 2. 执行
            for attempt in range(cls.MAX_RESURRECTIONS + 1):
                try:
                    if method == 'GET':
                        return cls._session.get(url, **kwargs)
                    else:
                        return cls._session.post(url, **kwargs)

                except (ConnectionError, RequestException, SSLError, ProxyError) as e:
                    if attempt == cls.MAX_RESURRECTIONS:
                        print(f"\n💀  NetworkGuard gave up.")
                        raise e

                    wait_time = 5 * (2 ** attempt) + random.uniform(1, 3)
                    print(f"\n🧟  Connection dropped. Reconnecting in {wait_time:.1f}s... ({attempt+1}/{cls.MAX_RESURRECTIONS})")

                    # 重建连接 (但在 Cookie 模式下，身份特征不变)
                    cls.rotate_identity()
                    time.sleep(wait_time)
                    continue

        requests.get = lambda url, **kwargs: patched_request('GET', url, **kwargs)
        requests.post = lambda url, **kwargs: patched_request('POST', url, **kwargs)
        cls._is_patched = True

    @classmethod
    def uninstall(cls):
        if cls._is_patched:
            requests.get = cls._original_get
            requests.post = cls._original_post
            if cls._session: cls._session.close()
            cls._is_patched = False