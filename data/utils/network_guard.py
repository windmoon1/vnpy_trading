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
USER_COOKIE = ""  # <--- 在此处粘贴你的 Cookie

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