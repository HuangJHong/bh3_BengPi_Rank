"""
Minimal B站 data fetch helpers using公开接口。
注意：为简化实现，只做轻量请求；在高并发或生产场景请加入重试、限速、错误处理、user-agent 伪装等。
"""
import requests
from typing import List, Dict, Any
import time
import random
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SEARCH_URL = "https://api.bilibili.com/x/web-interface/search/type"
VIEW_URL = "https://api.bilibili.com/x/web-interface/view"

SEARCH_ORDER_MODE = "pubdate"

# use a session with common browser headers to reduce 412/403 risk
SESSION = requests.Session()

# Prepare a pool of user agents to rotate
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
]

# mount retry adapter for robustness
def _install_retry(session: requests.Session, total: int = 5, backoff_factor: float = 0.3):
    retry = Retry(
        total=total,
        read=total,
        connect=total,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)


_install_retry(SESSION)

DEFAULT_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Referer": "https://www.bilibili.com/",
    "X-Requested-With": "XMLHttpRequest",
}

def set_proxy(proxies: Dict[str, str]):
    """Set proxies for the session. Example: {'http': 'http://127.0.0.1:8080', 'https': 'http://127.0.0.1:8080'}"""
    SESSION.proxies.update(proxies)


# Proxy pool support
PROXY_POOL: List[str] = []
PROXY_STATS: Dict[str, Dict[str, int]] = {}

# Crawl workers configuration
CRAWL_WORKERS: int = 5  # default concurrent workers for video detail fetching


def set_proxy_pool(proxies: List[str]):
    """Set a list of proxy URLs for rotation (e.g. ['http://ip:port', ...])."""
    global PROXY_POOL, PROXY_STATS
    PROXY_POOL = [p.strip() for p in proxies if p and p.strip()]
    PROXY_STATS = {p: {"fails": 0, "success": 0} for p in PROXY_POOL}


def set_search_order(mode: str):
    """Set search ordering strategy."""
    global SEARCH_ORDER_MODE
    if isinstance(mode, str) and mode.lower() == "pubdate":
        SEARCH_ORDER_MODE = "pubdate"
    else:
        SEARCH_ORDER_MODE = "default"


def get_proxy_pool():
    return list(PROXY_POOL)


def set_crawl_workers(workers: int):
    """Set the number of concurrent workers for video detail fetching."""
    global CRAWL_WORKERS
    CRAWL_WORKERS = max(1, min(10, workers))


def _choose_proxy() -> Dict[str, str]:
    """Choose a proxy from pool (random) and return proxies dict for requests, or {} if no proxy."""
    if not PROXY_POOL:
        return {}
    # choose least-failed proxies preferentially
    sorted_pool = sorted(PROXY_POOL, key=lambda p: PROXY_STATS.get(p, {}).get("fails", 0))
    proxy = random.choice(sorted_pool[:max(1, min(len(sorted_pool), 3))])
    return {"http": proxy, "https": proxy}


def report_proxy_result(proxy_url: str, ok: bool):
    if proxy_url not in PROXY_STATS:
        return
    if ok:
        PROXY_STATS[proxy_url]["success"] += 1
        # occasionally reduce fail count
        if PROXY_STATS[proxy_url]["fails"] > 0:
            PROXY_STATS[proxy_url]["fails"] -= 1
    else:
        PROXY_STATS[proxy_url]["fails"] += 1
        # if too many fails, temporarily remove from pool
        if PROXY_STATS[proxy_url]["fails"] >= 3:
            try:
                PROXY_POOL.remove(proxy_url)
            except ValueError:
                pass


def test_proxy(proxy_url: str, timeout: int = 8) -> bool:
    """Quick test whether a proxy can fetch B站 search endpoint."""
    try:
        headers = DEFAULT_HEADERS.copy()
        headers["User-Agent"] = random.choice(USER_AGENTS)
        proxies = {"http": proxy_url, "https": proxy_url}
        r = SESSION.get(SEARCH_URL, params={"search_type": "video", "keyword": "崩坏3", "page": 1}, timeout=timeout, headers=headers, proxies=proxies)
        if r.status_code == 200:
            return True
        return False
    except Exception:
        return False


LAST_RESP = None


def _safe_get(url: str, params: dict = None, timeout: int = 10, attempts: int = 3) -> Dict[str, Any]:
    """Perform GET with retries and anti-scraping mitigations.
    - rotate user-agent and slightly randomize headers
    - add jitter between retries
    - on 412 (banned) try header rotation and longer backoff before failing
    """
    global LAST_RESP
    last_exc = None
    for i in range(attempts):
        # rotate UA and build headers
        headers = DEFAULT_HEADERS.copy()
        ua = random.choice(USER_AGENTS)
        headers["User-Agent"] = ua
        # small chance to change Accept header
        if random.random() < 0.2:
            headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        try:
            # choose proxy for this request if pool configured
            proxies = _choose_proxy()
            # record which proxy used
            used_proxy = None
            if proxies:
                used_proxy = proxies.get('http')
            r = SESSION.get(url, params=params, timeout=timeout, headers=headers, proxies=proxies or None)
            # optimized random delay: reduced from 0.2-0.8s to 0.1-0.4s for better speed
            # while still maintaining anti-scraping protection
            time.sleep(0.1 + random.random() * 0.3)
            if r.status_code != 200:
                LAST_RESP = {"status_code": r.status_code, "text": r.text}
                # if banned (412) try a few more times with longer backoff and UA rotation
                if r.status_code == 412:
                    last_exc = Exception(f"HTTP 412 banned for {r.url}")
                    # additional attempts with exponential backoff and jitter
                    extra_sleep = (2 ** i) + random.random() * 2
                    time.sleep(extra_sleep)
                    # report proxy failure if used
                    if used_proxy:
                        report_proxy_result(used_proxy, False)
                    continue
                last_exc = Exception(f"HTTP {r.status_code} for {r.url}")
                time.sleep(0.5 * (i + 1))
                if used_proxy:
                    report_proxy_result(used_proxy, False)
                continue
            try:
                j = r.json()
                LAST_RESP = j
                # success
                if used_proxy:
                    report_proxy_result(used_proxy, True)
                return j
            except ValueError:
                LAST_RESP = {"status_code": r.status_code, "text": r.text}
                last_exc = Exception("Invalid JSON response")
                time.sleep(0.5 * (i + 1))
                continue
        except requests.exceptions.RequestException as e:
            last_exc = e
            LAST_RESP = {"error": str(e)}
            time.sleep(0.5 * (i + 1) + random.random())
            continue
    # after attempts
    raise last_exc


def search_videos(keyword: str, page: int = 1, order: str = None, up_mid: int = None) -> List[Dict[str, Any]]:
    # Try a few common parameter variants as B 站 search endpoints differ
    # If up_mid is provided, combine keyword with up主 filter
    search_keyword = keyword
    if up_mid:
        # B站搜索支持 "关键词 up主:mid" 格式
        search_keyword = f"{keyword} up主:{up_mid}"
    
    param_variants = [
        {"search_type": "video", "keyword": search_keyword, "page": page},
        {"search_type": "video", "keyword": search_keyword, "pn": page, "ps": 20},
        {"search_type": "video", "keyword": search_keyword, "page": page, "ps": 20},
    ]
    # Also try with mid parameter if provided
    if up_mid:
        param_variants.extend([
            {"search_type": "video", "keyword": keyword, "mid": up_mid, "page": page},
            {"search_type": "video", "keyword": keyword, "mid": up_mid, "pn": page, "ps": 20},
        ])
    
    if order and order != "default":
        for params in param_variants:
            params["order"] = order
    for params in param_variants:
        try:
            j = _safe_get(SEARCH_URL, params=params, timeout=8, attempts=3)
        except Exception:
            j = None
        if not isinstance(j, dict):
            continue
        # prefer successful code and non-empty results
        if j.get("code") == 0:
            data = j.get("data", {}) or {}
            res = data.get("result") or data.get("result", [])
            if isinstance(res, list) and len(res) > 0:
                return res
            # sometimes result is nested under 'items' or similar
            if isinstance(data, dict):
                for k in ("result", "items", "list"):
                    if isinstance(data.get(k), list) and data.get(k):
                        return data.get(k)
        # otherwise try next variant
    return []


def get_video_detail(bvid: str) -> Dict[str, Any]:
    params = {"bvid": bvid}
    try:
        j = _safe_get(VIEW_URL, params=params, timeout=8, attempts=3)
    except Exception:
        return {}
    if not isinstance(j, dict):
        return {}
    if j.get("code") != 0:
        return {}
    return j.get("data", {})


def collect_all_videos_by_up(up_mid: int, max_pages: int = 100) -> List[Dict[str, Any]]:
    """获取指定UP主的所有视频（不限制关键词，遍历所有页面直到没有结果）
    
    Args:
        up_mid: UP主的mid
        max_pages: 最大页数限制，防止无限循环
    
    Returns:
        该UP主的所有视频列表
    """
    out = []
    max_workers = CRAWL_WORKERS
    seen_bvids = set()  # 用于去重
    
    # 通过搜索"up主:mid"格式来获取该UP主的视频
    # 使用一个通用关键词来触发搜索，然后通过up_mid参数过滤
    page = 1
    while page <= max_pages:
        items = []
        try:
            # 尝试不同的搜索方式
            mode = SEARCH_ORDER_MODE if SEARCH_ORDER_MODE != "default" else None
            # 方式1: 使用空关键词 + up_mid参数
            items = search_videos("", page=page, order=mode, up_mid=up_mid)
            # 方式2: 如果方式1不行，尝试使用通用关键词
            if not items:
                items = search_videos("视频", page=page, order=mode, up_mid=up_mid)
            # 方式3: 如果还不行，尝试搜索"up主:mid"格式
            if not items:
                items = search_videos(f"up主:{up_mid}", page=page, order=mode)
        except Exception:
            items = []
        
        if not items:
            # 没有更多结果，停止
            break
        
        # map bvid -> original item so we can merge detail responses
        bvid_map = {}
        bvids = []
        for it in items:
            bvid = it.get("bvid") or it.get("bvid")
            if not bvid or bvid in seen_bvids:
                continue
            bvids.append(bvid)
            bvid_map[bvid] = it
            seen_bvids.add(bvid)
        
        if not bvids:
            # 这一页都是重复的，可能已经获取完所有视频
            break
        
        # fetch details concurrently but limit parallelism
        details_map: Dict[str, Dict[str, Any]] = {}
        if bvids:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, len(bvids))) as ex:
                future_to_bvid = {ex.submit(get_video_detail, b): b for b in bvids}
                for fut in concurrent.futures.as_completed(future_to_bvid):
                    b = future_to_bvid[fut]
                    try:
                        details_map[b] = fut.result()
                    except Exception:
                        details_map[b] = {}
        
        for bvid in bvids:
            it = bvid_map.get(bvid, {})
            detail = details_map.get(bvid, {}) or {}
            # 验证视频确实属于该UP主
            owner = detail.get("owner") or it.get("owner") or {}
            owner_mid = owner.get("mid")
            if owner_mid and str(owner_mid) != str(up_mid):
                # 视频不属于该UP主，跳过
                continue
            
            entry = {
                "keyword": "",  # 空关键词，表示获取所有视频
                "bvid": bvid,
                "title": it.get("title"),
                "desc": it.get("description") or detail.get("desc"),
                "pubdate": detail.get("pubdate") or it.get("pubdate"),
                "owner": detail.get("owner") or it.get("owner"),
                "stat": detail.get("stat") or it.get("stat"),
                "arc": detail,
            }
            out.append(entry)
        
        page += 1
    
    return out


def collect_by_keyword(keyword: str, pages: int = 2, up_mid: int = None) -> List[Dict[str, Any]]:
    """Collect search results for a keyword and fetch video details in parallel.

    To avoid creating too many concurrent requests (which may trigger anti-scraping),
    this function uses a ThreadPoolExecutor with a limited number of workers and
    relies on the underlying `_safe_get` jitter/backoff as well.

    Args:
        keyword: Search keyword
        pages: Number of pages to fetch
        up_mid: Optional UP主 mid to filter results by specific UP主

    max_workers: cap concurrent detail fetches (configurable via set_crawl_workers).
    """
    out = []
    # use configurable worker cap
    max_workers = CRAWL_WORKERS
    for p in range(1, pages + 1):
        items = []
        try:
            mode = SEARCH_ORDER_MODE if SEARCH_ORDER_MODE != "default" else None
            items = search_videos(keyword, page=p, order=mode, up_mid=up_mid)
        except Exception:
            items = []

        if not items:
            continue

        # map bvid -> original item so we can merge detail responses
        bvid_map = {}
        bvids = []
        for it in items:
            bvid = it.get("bvid") or it.get("bvid")
            if not bvid:
                continue
            bvids.append(bvid)
            bvid_map[bvid] = it

        # fetch details concurrently but limit parallelism
        details_map: Dict[str, Dict[str, Any]] = {}
        if bvids:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, len(bvids))) as ex:
                future_to_bvid = {ex.submit(get_video_detail, b): b for b in bvids}
                for fut in concurrent.futures.as_completed(future_to_bvid):
                    b = future_to_bvid[fut]
                    try:
                        details_map[b] = fut.result()
                    except Exception:
                        details_map[b] = {}

        for bvid in bvids:
            it = bvid_map.get(bvid, {})
            detail = details_map.get(bvid, {}) or {}
            entry = {
                "keyword": keyword,
                "bvid": bvid,
                "title": it.get("title"),
                "desc": it.get("description") or detail.get("desc"),
                "pubdate": detail.get("pubdate") or it.get("pubdate"),
                "owner": detail.get("owner") or it.get("owner"),
                "stat": detail.get("stat") or it.get("stat"),
                "arc": detail,
            }
            out.append(entry)
    return out


def get_last_response():
    """Return the last raw response (for debugging)."""
    return LAST_RESP
