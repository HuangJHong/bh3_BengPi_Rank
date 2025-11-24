"""
Main GUI application (tkinter) for crawling B站并生成崩坏3 UP 主排行榜。
"""
import threading
import concurrent.futures
import requests
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog
from datetime import datetime
import csv
import traceback
import os
import json
import statistics
import copy

from bilibili import collect_by_keyword, get_last_response, set_crawl_workers
from llm_client import LLMClient
from utils import ts_to_dt
import bilibili


DEFAULT_KEYWORDS = [
    "崩坏3 深渊",
    "崩坏3 记忆战场",
    "崩坏3 凹分",
    "崩坏3 榜一",
    "崩坏3 作业",
    "崩坏3 无限",
    "崩坏3 寂灭",
    "崩坏3 红莲",
    "崩坏3 乐土",
]

WEIGHT_METRICS = [
    ("counts", "视频数量"),
    ("views", "播放量"),
    ("desc", "简介字数"),
    ("favorites", "收藏数"),
    ("likes", "点赞数"),
]

WEIGHT_PRESET_LABELS = {
    "normal": "常规（默认）",
    "jm": "含寂灭视频",
    "top1": "含榜一视频",
}

DEFAULT_WEIGHT_PRESETS = {
    "normal": {"counts": 0.3, "views": 0.3, "desc": 0.1, "favorites": 0.15, "likes": 0.15},
    "jm": {"counts": 0.4, "views": 0.3, "desc": 0.1, "favorites": 0.1, "likes": 0.1},
    "top1": {"counts": 0.5, "views": 0.2, "desc": 0.1, "favorites": 0.1, "likes": 0.1},
}


class App:
    def __init__(self, root):
        self.root = root
        root.title("B站崩批统计排行榜")
        root.geometry("1000x720")

        style = ttk.Style()
        try:
            style.theme_use('clam')
        except Exception:
            pass

        # main container
        self.main = ttk.Frame(root, padding=8)
        self.main.pack(fill=tk.BOTH, expand=True)

        # --- Filter frame (keywords, date, pages, leaderboard) ---
        filter_frame = ttk.LabelFrame(self.main, text="筛选条件", padding=8)
        filter_frame.pack(fill=tk.X, padx=4, pady=4)

        ttk.Label(filter_frame, text="关键词（逗号分隔）:").grid(row=0, column=0, sticky=tk.W)
        self.kv = tk.StringVar(value=",".join(DEFAULT_KEYWORDS))
        ttk.Entry(filter_frame, textvariable=self.kv, width=70).grid(row=0, column=1, columnspan=3, sticky=tk.W, padx=6, pady=2)

        ttk.Label(filter_frame, text="开始日期:").grid(row=1, column=0, sticky=tk.W)
        self.start = tk.StringVar(value=(datetime.now().strftime("%Y-01-01")))
        ttk.Entry(filter_frame, textvariable=self.start, width=14).grid(row=1, column=1, sticky=tk.W)

        ttk.Label(filter_frame, text="结束日期:").grid(row=1, column=2, sticky=tk.W)
        self.end = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d"))
        ttk.Entry(filter_frame, textvariable=self.end, width=14).grid(row=1, column=3, sticky=tk.W)

        ttk.Label(filter_frame, text="Pages/关键词:").grid(row=1, column=4, sticky=tk.W, padx=(10,0))
        self.pages = tk.IntVar(value=2)
        ttk.Spinbox(filter_frame, from_=1, to=10, textvariable=self.pages, width=6).grid(row=1, column=5, sticky=tk.W)

        ttk.Label(filter_frame, text="显示榜单:").grid(row=0, column=4, sticky=tk.W, padx=(10,0))
        self.leaderboard_var = tk.StringVar(value="总榜")
        self.leaderboard_cb = ttk.Combobox(filter_frame, values=["总榜", "深渊榜", "战场榜"], textvariable=self.leaderboard_var, width=12, state='readonly')
        self.leaderboard_cb.grid(row=0, column=5, sticky=tk.W)
        self.leaderboard_cb.bind("<<ComboboxSelected>>", lambda e: self.on_leaderboard_change())

        # --- Settings frame (LLM, cookie, proxy) ---
        settings_frame = ttk.LabelFrame(self.main, text="设置", padding=8)
        settings_frame.pack(fill=tk.X, padx=4, pady=4)

        ttk.Label(settings_frame, text="LLM Provider:").grid(row=0, column=0, sticky=tk.W)
        self.provider = tk.StringVar(value="openai")
        ttk.Combobox(settings_frame, values=["openai", "ollama", "none"], textvariable=self.provider, width=12, state='readonly').grid(row=0, column=1, sticky=tk.W)

        # enable/disable LLM usage switch
        self.use_llm = tk.BooleanVar(value=True)
        tk.Checkbutton(settings_frame, text="启用 LLM", variable=self.use_llm).grid(row=0, column=6, sticky=tk.W, padx=(8,0))

        ttk.Label(settings_frame, text="LLM API Key:").grid(row=0, column=2, sticky=tk.W, padx=(10,0))
        self.api_key = tk.StringVar(value="")
        ttk.Entry(settings_frame, textvariable=self.api_key, width=34, show='*').grid(row=0, column=3, sticky=tk.W)

        ttk.Label(settings_frame, text="LLM API URL:").grid(row=1, column=0, sticky=tk.W)
        self.api_url = tk.StringVar(value="")
        ttk.Entry(settings_frame, textvariable=self.api_url, width=50).grid(row=1, column=1, columnspan=3, sticky=tk.W, pady=2)

        ttk.Label(settings_frame, text="LLM Model:").grid(row=2, column=4, sticky=tk.W, padx=(10,0))
        self.llm_model = tk.StringVar(value="gpt-3.5-turbo")
        ttk.Entry(settings_frame, textvariable=self.llm_model, width=20).grid(row=2, column=5, sticky=tk.W)

        ttk.Label(settings_frame, text="B站 Cookie:").grid(row=2, column=0, sticky=tk.W)
        self.bil_cookie = tk.StringVar(value="")
        ttk.Entry(settings_frame, textvariable=self.bil_cookie, width=70, show='*').grid(row=2, column=1, columnspan=3, sticky=tk.W)

        ttk.Label(settings_frame, text="代理池 (逗号分隔):").grid(row=3, column=0, sticky=tk.W)
        self.proxy_list = tk.StringVar(value="")
        ttk.Entry(settings_frame, textvariable=self.proxy_list, width=70).grid(row=3, column=1, columnspan=3, sticky=tk.W)
        self.use_proxy = tk.BooleanVar(value=False)
        tk.Checkbutton(settings_frame, text="启用代理池", variable=self.use_proxy).grid(row=3, column=4, sticky=tk.W, padx=6)
        self.use_proxypool = tk.BooleanVar(value=False)
        tk.Checkbutton(settings_frame, text="使用 proxypool 框架", variable=self.use_proxypool).grid(row=3, column=5, sticky=tk.W, padx=6)

        ttk.Label(settings_frame, text="LLM 权重 (0-1):").grid(row=0, column=4, sticky=tk.W, padx=(10,0))
        self.llm_weight = tk.DoubleVar(value=0.4)
        ttk.Spinbox(settings_frame, from_=0.0, to=1.0, increment=0.1, textvariable=self.llm_weight, width=6).grid(row=0, column=5, sticky=tk.W)
        ttk.Label(settings_frame, text="LLM 并发数:").grid(row=1, column=4, sticky=tk.W, padx=(10,0))
        self.llm_threads = tk.IntVar(value=4)
        ttk.Spinbox(settings_frame, from_=1, to=10, textvariable=self.llm_threads, width=6).grid(row=1, column=5, sticky=tk.W)
        ttk.Label(settings_frame, text="检索并发数:").grid(row=4, column=0, sticky=tk.W)
        self.crawl_threads = tk.IntVar(value=3)
        ttk.Spinbox(settings_frame, from_=1, to=8, textvariable=self.crawl_threads, width=6).grid(row=4, column=1, sticky=tk.W)

        ttk.Button(settings_frame, text="保存设置", command=self.save_config).grid(row=4, column=2, sticky=tk.W, pady=6, padx=4)
        ttk.Button(settings_frame, text="测试 LLM", command=self.test_llm_connection).grid(row=4, column=3, sticky=tk.W, pady=6, padx=4)
        ttk.Button(settings_frame, text="测试代理", command=self.test_proxies).grid(row=4, column=4, sticky=tk.W, pady=6)
        ttk.Button(settings_frame, text="配置评分权重", command=self.open_weight_config).grid(row=4, column=5, sticky=tk.W, pady=6, padx=4)
        ttk.Button(settings_frame, text="排除名单", command=self.edit_blacklist).grid(row=4, column=6, sticky=tk.W, pady=6, padx=4)

        # --- Actions frame ---
        actions = ttk.Frame(self.main)
        actions.pack(fill=tk.X, padx=4, pady=4)
        self.start_btn = ttk.Button(actions, text="开始采集并排行", command=self.start_scan)
        self.start_btn.pack(side=tk.LEFT, padx=(0,6))
        self.stop_btn = ttk.Button(actions, text="停止采集", command=self.stop_scan, state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT, padx=(0,6))
        self.export_btn = ttk.Button(actions, text="导出 CSV", command=self.export_csv, state=tk.DISABLED)
        self.export_btn.pack(side=tk.LEFT, padx=(0,6))
        self.exclude_outliers = tk.BooleanVar(value=False)
        ttk.Checkbutton(actions, text="排除异常数据", variable=self.exclude_outliers, command=self.on_outlier_toggle).pack(side=tk.LEFT, padx=(10,6))
        ttk.Label(actions, text="阈值系数:").pack(side=tk.LEFT, padx=(4,2))
        self.outlier_sigma = tk.DoubleVar(value=2.5)
        sigma_spin = ttk.Spinbox(actions, from_=1.0, to=5.0, increment=0.1, textvariable=self.outlier_sigma, width=4, command=self.on_outlier_sigma_change)
        sigma_spin.pack(side=tk.LEFT, padx=(0,6))
        self.outlier_sigma.trace_add("write", lambda *args: self.on_outlier_sigma_change())
        self.progress = ttk.Progressbar(actions, length=360)
        self.progress.pack(side=tk.RIGHT)

        # --- Table frame ---
        table_frame = ttk.Frame(self.main)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
        cols = ("rank", "up_name", "rating", "videos", "views", "likes", "score", "llm_summary")
        self.tree = ttk.Treeview(table_frame, columns=cols, show="headings")
        # headings: show friendly Chinese label for rating
        headings = {"rank": "排名", "up_name": "up", "rating": "评级", "videos": "视频数", "views": "播放量", "likes": "收藏数", "score": "分数", "llm_summary": "评价"}
        for c in cols:
            self.tree.heading(c, text=headings.get(c, c))
            # narrow rating column
            if c == 'rating':
                self.tree.column(c, width=80, anchor=tk.CENTER)
            else:
                self.tree.column(c, width=120, anchor=tk.W)
        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.grid(row=0, column=0, sticky=tk.NSEW)
        vsb.grid(row=0, column=1, sticky=tk.NS)
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        # --- Log frame ---
        log_frame = ttk.LabelFrame(self.main, text="运行日志", padding=6)
        log_frame.pack(fill=tk.BOTH, padx=4, pady=4)
        self.log_text = tk.Text(log_frame, height=8, wrap=tk.WORD)
        lvsb = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=lvsb.set)
        self.log_text.grid(row=0, column=0, sticky=tk.NSEW)
        lvsb.grid(row=0, column=1, sticky=tk.NS)
        log_frame.rowconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)

        self.weight_configs = copy.deepcopy(DEFAULT_WEIGHT_PRESETS)
        self._weight_win = None
        self._suppress_sigma_callback = False
        self.banned_upnames = set()
        self._results_unfiltered = {}
        self.results = []
        self.results_by_category_raw = {}
        self.results_by_category = {}
        self._llm_used_last = False
        # load saved config if exists
        try:
            self.load_config()
        except Exception:
            pass
        # stop event for canceling scans
        self._stop_event = threading.Event()

    def log(self, msg: str):
        # thread-safe append
        def _append():
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.log_text.insert(tk.END, f"[{timestamp}] {msg}\n")
            self.log_text.see(tk.END)

        try:
            self.root.after(0, _append)
        except Exception:
            pass

    def config_path(self):
        return os.path.join(os.path.dirname(__file__), "config.json")

    def save_config(self):
        cfg = {
            "provider": self.provider.get(),
            "api_key": self.api_key.get(),
            "api_url": self.api_url.get(),
            "llm_model": self.llm_model.get(),
            "use_llm": bool(self.use_llm.get()),
            "bili_cookie": self.bil_cookie.get(),
            "proxies": self.proxy_list.get(),
            "use_proxy": bool(self.use_proxy.get()),
            "use_proxypool": bool(self.use_proxypool.get()),
            "llm_weight": float(self.llm_weight.get()),
            "llm_threads": int(self.llm_threads.get()),
            "crawl_threads": int(self.crawl_threads.get()),
            "weight_configs": self.weight_configs,
            "outlier_sigma": float(self.outlier_sigma.get()),
            "blacklist": sorted(self.banned_upnames),
        }
        try:
            with open(self.config_path(), "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)
            self.log("设置已保存")
            messagebox.showinfo("设置", "设置已保存到 config.json")
        except Exception as e:
            self.log(f"保存设置失败: {e}")
            messagebox.showerror("错误", f"保存失败: {e}")

    def load_config(self):
        p = self.config_path()
        if not os.path.exists(p):
            return
        try:
            with open(p, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            self.provider.set(cfg.get("provider", "openai"))
            self.api_key.set(cfg.get("api_key", ""))
            self.api_url.set(cfg.get("api_url", ""))
            # load model name if present
            try:
                self.llm_model.set(cfg.get("llm_model", "gpt-3.5-turbo"))
            except Exception:
                self.llm_model.set("gpt-3.5-turbo")
            # load use_llm flag
            try:
                self.use_llm.set(bool(cfg.get("use_llm", True)))
            except Exception:
                self.use_llm.set(True)
            self.bil_cookie.set(cfg.get("bili_cookie", ""))
            self.proxy_list.set(cfg.get("proxies", ""))
            self.use_proxy.set(cfg.get("use_proxy", False))
            self.use_proxypool.set(cfg.get("use_proxypool", False))
            try:
                self.llm_weight.set(float(cfg.get("llm_weight", 0.4)))
            except Exception:
                self.llm_weight.set(0.4)
            try:
                self.llm_threads.set(int(cfg.get("llm_threads", 4)))
            except Exception:
                self.llm_threads.set(4)
            try:
                self.crawl_threads.set(int(cfg.get("crawl_threads", 3)))
            except Exception:
                self.crawl_threads.set(3)
            try:
                self._apply_weight_config(cfg.get("weight_configs"))
            except Exception:
                self.weight_configs = copy.deepcopy(DEFAULT_WEIGHT_PRESETS)
            try:
                val = float(cfg.get("outlier_sigma", 2.5))
                self.outlier_sigma.set(max(0.5, min(10.0, val)))
            except Exception:
                self.outlier_sigma.set(2.5)
            try:
                bl = cfg.get("blacklist") or []
                if isinstance(bl, list):
                    self.banned_upnames = {str(x).strip() for x in bl if str(x).strip()}
            except Exception:
                self.banned_upnames = set()
            self.log("已加载配置")
        except Exception as e:
            self.log(f"加载配置失败: {e}")

    def test_llm_connection(self):
        provider = self.provider.get()
        api_key = self.api_key.get().strip()
        api_url = self.api_url.get().strip() or None
        if not self.use_llm.get() or provider == "none":
            messagebox.showinfo("测试连接", "未启用 LLM（开关或 provider 设置为 none）")
            return
        client = LLMClient(provider=provider, endpoint=api_url, api_key=api_key, model=self.llm_model.get())
        self.log("正在测试 LLM 连接...")
        res = client.test_connection()
        if res.get("ok"):
            self.log(f"LLM 连接成功: {res.get('msg')}")
            messagebox.showinfo("测试连接", "连接成功")
        else:
            self.log(f"LLM 连接失败: {res.get('msg')}")
            messagebox.showerror("测试连接失败", res.get("msg"))

    def test_proxies(self):
        raw = self.proxy_list.get() or ""
        proxies = [p.strip() for p in raw.split(',') if p.strip()]
        if not proxies:
            messagebox.showinfo("测试代理", "没有配置任何代理")
            return
        # If proxypool framework enabled, treat entries as proxypool API endpoints
        if self.use_proxypool.get():
            self.log(f"使用 proxypool 模式，尝试从 {len(proxies)} 个 proxypool endpoint 拉取代理...")
            fetched = self._fetch_from_proxypool(proxies)
            if not fetched:
                messagebox.showwarning("测试代理", "从 proxypool API 未获取到任何代理")
                return
            proxies = fetched

        self.log(f"开始测试 {len(proxies)} 个代理...")
        good = []
        for p in proxies:
            ok = bilibili.test_proxy(p)
            self.log(f"代理 {p} 测试结果: {'可用' if ok else '不可用'}")
            if ok:
                good.append(p)
        if good:
            # set pool to good ones by default
            bilibili.set_proxy_pool(good)
            self.log(f"已将 {len(good)} 个可用代理加入代理池")
            messagebox.showinfo("测试代理", f"{len(good)} 个代理可用，已启用")
        else:
            messagebox.showwarning("测试代理", "没有可用的代理")
 

    def start_scan(self):
        self.start_btn.config(state=tk.DISABLED)
        self.export_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        # clear stop flag
        self._stop_event.clear()
        # apply bilibili cookie if provided
        cookie = self.bil_cookie.get().strip()
        if cookie:
            try:
                bilibili.SESSION.headers.update({"Cookie": cookie})
                self.log("已设置 B站 Cookie（仅用于当前会话）")
            except Exception as e:
                self.log(f"设置 Cookie 出错: {e}")

        # apply proxy pool if enabled
        try:
            if self.use_proxy.get():
                proxies = [p.strip() for p in (self.proxy_list.get() or "").split(",") if p.strip()]
                if proxies:
                    bilibili.set_proxy_pool(proxies)
                    self.log(f"已设置代理池，共 {len(proxies)} 个代理 (启用)")
                else:
                    self.log("启用代理池但未提供任何代理字符串")
            elif self.use_proxypool.get():
                # treat proxy_list entries as proxypool endpoints
                endpoints = [p.strip() for p in (self.proxy_list.get() or "").split(',') if p.strip()]
                if endpoints:
                    fetched = self._fetch_from_proxypool(endpoints)
                    if fetched:
                        bilibili.set_proxy_pool(fetched)
                        self.log(f"已从 proxypool 拉取并设置代理池，共 {len(fetched)} 个代理")
                    else:
                        self.log("未能从 proxypool API 拉取到代理")
        except Exception as e:
            self.log(f"设置代理池失败: {e}")
        
        # apply crawl workers setting
        try:
            crawl_workers = max(1, min(8, int(self.crawl_threads.get())))
            set_crawl_workers(crawl_workers)
            self.log(f"已设置检索并发数为 {crawl_workers}")
        except Exception as e:
            self.log(f"设置检索并发数失败: {e}")

        t = threading.Thread(target=self._scan_worker, daemon=True)
        t.start()

    def stop_scan(self):
        """Called by Stop button to signal the worker to stop."""
        try:
            self._stop_event.set()
            self.stop_btn.config(state=tk.DISABLED)
            self.log("用户已请求停止采集（stop 按钮已按下）。")
        except Exception as e:
            self.log(f"停止采集失败: {e}")

    def _fetch_from_proxypool(self, endpoints):
        """Try to fetch proxy strings from common proxypool endpoints.
        endpoints: list of base URLs or full endpoints. Return list of proxy strings like 'http://ip:port'."""
        out = []
        tried = set()
        common_paths = ["", "/get", "/api/get", "/proxies", "/api/proxies", "/get_proxy"]
        headers = {"User-Agent": "proxy-fetcher/1.0"}
        for base in endpoints:
            if not base:
                continue
            for p in common_paths:
                url = base.rstrip('/') + p
                if url in tried:
                    continue
                tried.add(url)
                try:
                    r = requests.get(url, timeout=4, headers=headers)
                    if r.status_code != 200:
                        continue
                    # try parse JSON
                    try:
                        j = r.json()
                        # common shapes: list of proxies, or {'proxy': 'ip:port'} or {'data': [...]}
                        if isinstance(j, list):
                            for it in j:
                                if isinstance(it, str) and ':' in it:
                                    out.append(it.strip())
                        elif isinstance(j, dict):
                            # check common keys
                            if 'proxy' in j and isinstance(j['proxy'], str):
                                out.append(j['proxy'].strip())
                            if 'data' in j and isinstance(j['data'], list):
                                for it in j['data']:
                                    if isinstance(it, str) and ':' in it:
                                        out.append(it.strip())
                            # some frameworks return {'proxies': [...]}
                            if 'proxies' in j and isinstance(j['proxies'], list):
                                for it in j['proxies']:
                                    if isinstance(it, str) and ':' in it:
                                        out.append(it.strip())
                    except ValueError:
                        # plain text containing proxy or multiple lines
                        text = r.text.strip()
                        for line in text.splitlines():
                            line = line.strip()
                            if ':' in line and len(line) > 6:
                                out.append(line)
                except Exception:
                    continue
        # deduplicate and prefix http if missing
        cleaned = []
        for p in out:
            p = p.strip()
            if p.startswith('http'):
                cleaned.append(p)
            else:
                cleaned.append('http://' + p)
        # unique preserve order
        seen = set()
        res = []
        for p in cleaned:
            if p not in seen:
                seen.add(p)
                res.append(p)
        return res

    def _map_label(self, score):
        try:
            v = float(score)
        except Exception:
            v = 0.0
        if v >= 8.5:
            return '夯', 10.0
        if v >= 7.0:
            return '顶级', 8.0
        if v >= 5.5:
            return '人上人', 6.0
        if v >= 3.5:
            return 'NPC', 4.0
        return '拉完了', 2.0

    def _get_weight_preset(self, preset_key: str):
        base = DEFAULT_WEIGHT_PRESETS.get(preset_key, DEFAULT_WEIGHT_PRESETS['normal'])
        custom = self.weight_configs.get(preset_key) if isinstance(self.weight_configs, dict) else None
        out = base.copy()
        if isinstance(custom, dict):
            for metric, _ in WEIGHT_METRICS:
                try:
                    val = float(custom.get(metric, out.get(metric, 0.0)))
                except Exception:
                    val = out.get(metric, 0.0)
                out[metric] = max(0.0, val)
        total = sum(out.values()) or 1.0
        for k in out:
            out[k] = out[k] / total
        return out

    def _apply_weight_config(self, cfg):
        sanitized = copy.deepcopy(DEFAULT_WEIGHT_PRESETS)
        if isinstance(cfg, dict):
            for preset, defaults in DEFAULT_WEIGHT_PRESETS.items():
                incoming = cfg.get(preset)
                if not isinstance(incoming, dict):
                    continue
                for metric, _ in WEIGHT_METRICS:
                    try:
                        val = float(incoming.get(metric, defaults[metric]))
                    except Exception:
                        val = defaults[metric]
                    sanitized[preset][metric] = max(0.0, val)
        # ensure normalization
        for preset in sanitized:
            total = sum(sanitized[preset].values()) or 1.0
            for metric in sanitized[preset]:
                sanitized[preset][metric] = sanitized[preset][metric] / total
        self.weight_configs = sanitized

    def _norm_value(self, val, mn, mx):
        try:
            v = float(val)
        except Exception:
            v = 0.0
        if mx == mn:
            return 5.0
        return ((v - mn) / (mx - mn)) * 10.0

    def _prepare_weighted_metrics(self, lst):
        if not lst:
            return
        counts_list = [(x.get('total_videos') or len(x.get('videos_list') or [])) for x in lst]
        views_list = [(x.get('views') or 0) for x in lst]
        likes_list = [(x.get('likes') or 0) for x in lst]
        favorites_list = [(x.get('favorites') or 0) for x in lst]
        desc_len_list = [(x.get('desc_len') or 0) for x in lst]

        cmin, cmax = (min(counts_list), max(counts_list)) if counts_list else (0, 0)
        vmin, vmax = (min(views_list), max(views_list)) if views_list else (0, 0)
        lmin, lmax = (min(likes_list), max(likes_list)) if likes_list else (0, 0)
        fmin, fmax = (min(favorites_list), max(favorites_list)) if favorites_list else (0, 0)
        dmin, dmax = (min(desc_len_list), max(desc_len_list)) if desc_len_list else (0, 0)

        for r in lst:
            counts_val = (r.get('total_videos') or len(r.get('videos_list') or []))
            views_val = r.get('views') or 0
            likes_val = r.get('likes') or 0
            favorites_val = r.get('favorites') or 0
            desc_len_val = r.get('desc_len') or 0

            counts_n = self._norm_value(counts_val, cmin, cmax)
            views_n = self._norm_value(views_val, vmin, vmax)
            likes_n = self._norm_value(likes_val, lmin, lmax)
            favorites_n = self._norm_value(favorites_val, fmin, fmax)
            desc_len_n = self._norm_value(desc_len_val, dmin, dmax)

            has_jm = False
            has_top1 = False
            for vv in (r.get('videos_list') or []):
                try:
                    t = (vv.get('title') or '')
                    if '寂灭' in t:
                        has_jm = True
                    if '榜一' in t:
                        has_top1 = True
                except Exception:
                    continue

            if has_top1:
                weights = self._get_weight_preset('top1')
                rule_label = '含榜一'
            elif has_jm:
                weights = self._get_weight_preset('jm')
                rule_label = '含寂灭'
            else:
                weights = self._get_weight_preset('normal')
                rule_label = '常规'
            w_counts = weights.get('counts', 0.3)
            w_views = weights.get('views', 0.3)
            w_desc = weights.get('desc', 0.1)
            w_fav = weights.get('favorites', 0.15)
            w_likes = weights.get('likes', 0.15)

            composite = (
                counts_n * w_counts
                + views_n * w_views
                + desc_len_n * w_desc
                + favorites_n * w_fav
                + likes_n * w_likes
            )

            r['weighted_score'] = composite
            r['_local_metrics'] = {
                "counts_val": counts_val,
                "views_val": views_val,
                "likes_val": likes_val,
                "favorites_val": favorites_val,
                "desc_len_val": desc_len_val,
                "counts_n": counts_n,
                "views_n": views_n,
                "likes_n": likes_n,
                "favorites_n": favorites_n,
                "desc_len_n": desc_len_n,
                "rule_label": rule_label,
            }

    def _normalize_scores(self, lst):
        vals = [x.get('weighted_score', x.get('score', 0)) for x in lst]
        if not vals:
            return {}
        mn = min(vals)
        mx = max(vals)
        if mx == mn:
            return {x['mid']: 5.0 for x in lst}
        out = {}
        for x in lst:
            base_val = x.get('weighted_score', x.get('score', 0))
            out[x['mid']] = self._norm_value(base_val, mn, mx)
        return out

    def _apply_local_summaries(self, lst, log_output=True):
        for r in lst:
            composite = r.get('weighted_score', 5.0)
            metrics = r.get('_local_metrics') or {}
            label, val = self._map_label(composite)
            counts_val = metrics.get('counts_val', r.get('total_videos') or 0)
            views_val = metrics.get('views_val', r.get('views') or 0)
            likes_val = metrics.get('likes_val', r.get('likes') or 0)
            favorites_val = metrics.get('favorites_val', r.get('favorites') or 0)
            desc_len_val = metrics.get('desc_len_val', r.get('desc_len') or 0)
            counts_n = metrics.get('counts_n', 5.0)
            views_n = metrics.get('views_n', 5.0)
            likes_n = metrics.get('likes_n', 5.0)
            favorites_n = metrics.get('favorites_n', 5.0)
            desc_len_n = metrics.get('desc_len_n', 5.0)
            rule_label = metrics.get('rule_label', '常规')

            r['llm_score'] = val
            r['llm_summary'] = (
                f"本地评级({rule_label}): {label} (评分={composite:.2f}); "
                f"counts={counts_val}({counts_n:.2f}), views={views_val}({views_n:.2f}), "
                f"desc_len={desc_len_val}({desc_len_n:.2f}), favorites={favorites_val}({favorites_n:.2f}), "
                f"likes={likes_val}({likes_n:.2f})"
            )
            if log_output:
                try:
                    self.log(
                        f"本地加权评级 - {r.get('name')} ({r.get('mid')}): {label}, score={composite:.2f}, "
                        f"counts={counts_val}, views={views_val}, desc_len={desc_len_val}, "
                        f"favorites={favorites_val}, likes={likes_val}, rule={rule_label}"
                    )
                except Exception:
                    pass
            r['final_score'] = composite
            r['score'] = round(composite, 3)

    def on_outlier_toggle(self):
        """Callback when user toggles the outlier exclusion option."""
        self._rebuild_filtered_results()
        sel = self.leaderboard_var.get()
        fallback = self.results_by_category_raw.get(sel, [])
        self.results = self.results_by_category.get(sel, fallback)
        self._update_table()
        try:
            self.log(f"{'已启用' if self.exclude_outliers.get() else '已关闭'}异常数据排除开关")
        except Exception:
            pass

    def on_outlier_sigma_change(self):
        if getattr(self, "_suppress_sigma_callback", False):
            return
        try:
            val = float(self.outlier_sigma.get())
        except Exception:
            return
        val = max(0.5, min(10.0, val))
        if abs(val - float(self.outlier_sigma.get())) > 1e-4:
            self._suppress_sigma_callback = True
            self.outlier_sigma.set(round(val, 2))
            self._suppress_sigma_callback = False
        if not self.results_by_category_raw:
            return
        self._rebuild_filtered_results()
        sel = self.leaderboard_var.get()
        fallback = self.results_by_category_raw.get(sel, [])
        self.results = self.results_by_category.get(sel, fallback)
        self._update_table()
        try:
            self.log(f"异常数据阈值系数已更新为 {val:.2f}")
        except Exception:
            pass

    def _rebuild_filtered_results(self):
        """Rebuild filtered results based on current outlier exclusion setting."""
        base = getattr(self, "results_by_category_raw", {}) or {}
        filtered = {}
        llm_enabled = bool(getattr(self, "_llm_used_last", False))
        llm_weight = max(0.0, min(1.0, float(self.llm_weight.get())))
        for name, lst in base.items():
            if not lst:
                filtered[name] = []
                continue
            working_src = self._filter_outliers(lst) if self.exclude_outliers.get() else lst
            working = copy.deepcopy(working_src)
            self._prepare_weighted_metrics(working)
            if not llm_enabled:
                self._apply_local_summaries(working, log_output=False)
                filtered[name] = working
                continue

            lst_norm = self._normalize_scores(working)
            for r in working:
                mid = r.get('mid')
                base_norm = lst_norm.get(mid, r.get('weighted_score', 5.0))
                llm_score = r.get('llm_score')
                if llm_score is None:
                    final = base_norm
                else:
                    final = (1.0 - llm_weight) * base_norm + llm_weight * llm_score
                r['final_score'] = final
                r['score'] = round(final, 3)
            filtered[name] = working
        self.results_by_category = filtered

    def _filter_outliers(self, records):
        """Remove records whose metrics deviate abnormally from the group."""
        if not records or len(records) < 3:
            return records
        metrics = ["total_videos", "views", "favorites", "likes", "desc_len"]
        thresholds = {}
        try:
            sigma = float(self.outlier_sigma.get())
        except Exception:
            sigma = 2.5
        sigma = max(0.5, min(10.0, sigma))
        for metric in metrics:
            vals = []
            for r in records:
                try:
                    val = float(r.get(metric) or 0)
                except Exception:
                    val = 0.0
                vals.append(val)
            if len(vals) < 5:
                continue
            mean_val = sum(vals) / len(vals)
            std_val = statistics.pstdev(vals)
            if std_val == 0:
                continue
            thresholds[metric] = mean_val + std_val * sigma
        if not thresholds:
            return records
        filtered = []
        removed = []
        for r in records:
            flagged = False
            for metric, limit in thresholds.items():
                try:
                    value = float(r.get(metric) or 0)
                except Exception:
                    value = 0.0
                if value > limit:
                    flagged = True
                    break
            if flagged:
                removed.append(r)
            else:
                filtered.append(r)
        if removed:
            try:
                sample_names = ", ".join((x.get("name") or "未知") for x in removed[:3])
                extra = "" if len(removed) <= 3 else f"...(+{len(removed)-3})"
                self.log(f"排除 {len(removed)} 个疑似异常UP: {sample_names}{extra}")
            except Exception:
                pass
        return filtered or records

    def _refresh_results_with_new_weights(self, silent=False, update_ui=True):
        if not self.results_by_category_raw:
            return
        try:
            for lst in self.results_by_category_raw.values():
                self._prepare_weighted_metrics(lst)
            if not self._llm_used_last:
                for lst in self.results_by_category_raw.values():
                    self._apply_local_summaries(lst, log_output=not silent)
            else:
                llm_weight = max(0.0, min(1.0, float(self.llm_weight.get())))
                for lst in self.results_by_category_raw.values():
                    norms = self._normalize_scores(lst)
                    for r in lst:
                        base_norm = norms.get(r.get('mid'), r.get('weighted_score', 5.0))
                        llm_score = r.get('llm_score')
                        if llm_score is None:
                            final = base_norm
                        else:
                            final = (1.0 - llm_weight) * base_norm + llm_weight * llm_score
                        r['final_score'] = final
                        r['score'] = round(final, 3)
            self._rebuild_filtered_results()
            if update_ui:
                try:
                    current_category = self.leaderboard_var.get()
                except Exception:
                    current_category = "总榜"
                fallback = self.results_by_category_raw.get(current_category, [])
                self.results = self.results_by_category.get(current_category, fallback) or fallback
                try:
                    self._update_table()
                except Exception:
                    pass
        except Exception as e:
            try:
                self.log(f"重算权重时出错: {e}")
            except Exception:
                pass

    def _apply_results_to_ui(self):
        try:
            current_category = self.leaderboard_var.get()
        except Exception:
            current_category = "总榜"
        fallback = self.results_by_category_raw.get(current_category, [])
        self.results = self.results_by_category.get(current_category, fallback) or fallback
        self._update_table()

    def _close_weight_window(self):
        if self._weight_win and self._weight_win.winfo_exists():
            self._weight_win.destroy()
        self._weight_win = None

    def _save_weight_config(self, entry_vars):
        new_cfg = copy.deepcopy(DEFAULT_WEIGHT_PRESETS)
        for preset in DEFAULT_WEIGHT_PRESETS:
            total_percent = 0.0
            collected = {}
            for metric_key, _ in WEIGHT_METRICS:
                var = entry_vars.get((preset, metric_key))
                try:
                    val = max(0.0, float(var.get()))
                except Exception:
                    val = 0.0
                collected[metric_key] = val
                total_percent += val
            if total_percent <= 0:
                total_percent = 1.0
            for metric_key, val in collected.items():
                new_cfg[preset][metric_key] = (val / total_percent)
        self.weight_configs = new_cfg
        self._close_weight_window()
        try:
            self.log("评分权重已更新，已基于新系数即时重算榜单（如需持久化请点击“保存设置”）")
        except Exception:
            pass
        self._refresh_results_with_new_weights(silent=True)

    def open_weight_config(self):
        if self._weight_win and self._weight_win.winfo_exists():
            self._weight_win.lift()
            return
        win = tk.Toplevel(self.root)
        win.title("配置评分权重")
        win.resizable(False, False)
        self._weight_win = win

        entries = {}
        for row, preset in enumerate(["normal", "jm", "top1"]):
            frame = ttk.LabelFrame(win, text=WEIGHT_PRESET_LABELS.get(preset, preset), padding=8)
            frame.grid(row=row, column=0, sticky="ew", padx=12, pady=6)
            frame.columnconfigure(1, weight=1)
            preset_cfg = self.weight_configs.get(preset, DEFAULT_WEIGHT_PRESETS.get(preset, {}))
            for idx, (metric_key, label) in enumerate(WEIGHT_METRICS):
                ttk.Label(frame, text=f"{label} (%):").grid(row=idx, column=0, sticky=tk.W, pady=2)
                var = tk.DoubleVar(value=round(preset_cfg.get(metric_key, 0.0) * 100, 2))
                entries[(preset, metric_key)] = var
                ttk.Entry(frame, textvariable=var, width=10).grid(row=idx, column=1, sticky=tk.W, pady=2)

        btn_frame = ttk.Frame(win, padding=8)
        btn_frame.grid(row=len(WEIGHT_PRESET_LABELS), column=0, sticky="ew")
        ttk.Button(btn_frame, text="保存", command=lambda: self._save_weight_config(entries)).pack(side=tk.LEFT, padx=4)
        ttk.Button(btn_frame, text="取消", command=self._close_weight_window).pack(side=tk.LEFT, padx=4)
        win.protocol("WM_DELETE_WINDOW", self._close_weight_window)

    def edit_blacklist(self):
        items = sorted(self.banned_upnames)
        current = ",".join(items)
        resp = simpledialog.askstring("UP 黑名单", "请输入要排除的UP名称（逗号分隔）:", initialvalue=current, parent=self.root)
        if resp is None:
            return
        new_items = [x.strip() for x in resp.split(",") if x.strip()]
        self.banned_upnames = set(new_items)
        self._refresh_results_with_blacklist()
        try:
            self.log(f"黑名单已更新，共 {len(self.banned_upnames)} 个UP")
        except Exception:
            pass

    def _refresh_results_with_blacklist(self, update_ui=True):
        base = getattr(self, "_results_unfiltered", {})
        if not base:
            return
        ban = { (x or "").strip().lower() for x in self.banned_upnames if (x or "").strip() }
        filtered_raw = {}
        for name, lst in base.items():
            source = lst or []
            cleaned = []
            removed = []
            for r in source:
                uname = (r.get('name') or '').strip()
                if ban and uname.lower() in ban:
                    removed.append(uname)
                    continue
                cleaned.append(copy.deepcopy(r))
            filtered_raw[name] = cleaned
            if removed:
                try:
                    sample = ", ".join(removed[:3])
                    extra = "" if len(removed) <= 3 else f"...(+{len(removed)-3})"
                    self.log(f"{name}: 黑名单排除 {len(removed)} 个UP: {sample}{extra}")
                except Exception:
                    pass
        self.results_by_category_raw = filtered_raw
        self._refresh_results_with_new_weights(silent=not update_ui, update_ui=update_ui)

    def _scan_worker(self):
            keywords = [k.strip() for k in self.kv.get().split(',') if k.strip()]
            start_ts = None
            end_ts = None
            try:
                start_ts = int(datetime.fromisoformat(self.start.get()).timestamp())
                end_ts = int(datetime.fromisoformat(self.end.get()).timestamp())
            except Exception:
                pass

            pages = int(self.pages.get())
            collected = []
            total = max(1, len(keywords) * pages)
            cnt = 0
            
            # 使用线程池并发处理关键词检索
            crawl_workers = max(1, min(8, int(self.crawl_threads.get())))
            
            def fetch_keyword_page(kw, p):
                """获取单个关键词的单个页面"""
                if self._stop_event.is_set():
                    return []
                try:
                    items = collect_by_keyword(kw, pages=1)
                    self.log(f"已检索关键词 '{kw}' 第 {p} 页，返回 {len(items)} 条结果")
                    if not items:
                        try:
                            last = get_last_response()
                            if isinstance(last, dict) and last.get('status_code') == 412:
                                self.log("检测到 B站 安全拦截 (412)，当前 IP/请求被封。建议：使用有效的 B站 Cookie、代理或通过浏览器登录并抓取。")
                                self.log("已停止采集以避免进一步封禁。若要继续，请配置 Cookie 或代理后重新开始。")
                                self._stop_event.set()
                                return []
                        except Exception:
                            pass
                    return items
                except Exception as e:
                    self.log(f"关键词 '{kw}' 第 {p} 页检索出错: {e}")
                    return []
            
            # 创建所有任务
            tasks = []
            for kw in keywords:
                for p in range(1, pages + 1):
                    tasks.append((kw, p))
            
            # 使用线程池并发执行
            collected_lock = threading.Lock()
            with concurrent.futures.ThreadPoolExecutor(max_workers=crawl_workers) as executor:
                future_to_task = {executor.submit(fetch_keyword_page, kw, p): (kw, p) for kw, p in tasks}
                for future in concurrent.futures.as_completed(future_to_task):
                    if self._stop_event.is_set():
                        break
                    kw, p = future_to_task[future]
                    try:
                        items = future.result()
                        with collected_lock:
                            for it in items:
                                pub = it.get('pubdate')
                                if pub and start_ts and end_ts:
                                    try:
                                        if not (start_ts <= int(pub) <= end_ts):
                                            continue
                                    except Exception:
                                        pass
                                collected.append(it)
                            cnt += 1
                            try:
                                self.root.after(0, lambda v=(cnt / total) * 100: self.progress.configure(value=v))
                            except Exception:
                                pass
                    except Exception as e:
                        self.log(f"处理关键词 '{kw}' 第 {p} 页结果时出错: {e}")
                        cnt += 1
                        try:
                            self.root.after(0, lambda v=(cnt / total) * 100: self.progress.configure(value=v))
                        except Exception:
                            pass

            # aggregate by owner with per-category stats
            by_owner = {}
            for it in collected:
                owner = it.get('owner') or {}
                mid = owner.get('mid') or owner.get('mid')
                if not mid:
                    continue
                entry = by_owner.setdefault(
                    mid,
                    {
                        "name": owner.get('name') or owner.get('uname') or str(mid),
                        "mid": mid,
                        "videos": [],
                        "views_total": 0,
                        "likes_total": 0,
                        "favorites_total": 0,
                        "desc_len_total": 0,
                        "by": {
                            "abyss": {"count": 0, "views": 0, "likes": 0, "favorites": 0, "desc_len": 0, "videos": []},
                            "battle": {"count": 0, "views": 0, "likes": 0, "favorites": 0, "desc_len": 0, "videos": []},
                            "other": {"count": 0, "views": 0, "likes": 0, "favorites": 0, "desc_len": 0, "videos": []},
                        },
                    },
                )
                stat = it.get('stat') or {}
                views = int(stat.get('view', 0) or 0)
                likes = int(stat.get('like', 0) or stat.get('like') or 0)
                favorites = int(stat.get('favorite') or stat.get('favorites') or stat.get('favorite_count') or stat.get('collect') or 0)
                title = (it.get('title') or '')
                kw = (it.get('keyword') or '')
                desc_text = (it.get('desc') or it.get('description') or "")
                desc_len = len(desc_text.strip())
                cat = 'other'
                if '深渊' in kw or '深渊' in title:
                    cat = 'abyss'
                elif '记忆战场' in kw or '记忆战场' in title or '战场' in kw or '战场' in title:
                    cat = 'battle'
                entry['videos'].append({
                    "bvid": it.get('bvid'),
                    'title': title,
                    'views': views,
                    'likes': likes,
                    'favorites': favorites,
                    'desc_len': desc_len,
                    'pubdate': it.get('pubdate'),
                    'cat': cat,
                })
                entry['views_total'] += views
                entry['likes_total'] += likes
                entry['favorites_total'] += favorites
                entry['desc_len_total'] += desc_len
                entry['by'][cat]['count'] += 1
                entry['by'][cat]['views'] += views
                entry['by'][cat]['likes'] += likes
                entry['by'][cat]['favorites'] += favorites
                entry['by'][cat]['desc_len'] += desc_len
                entry['by'][cat]['videos'].append({"bvid": it.get('bvid'), 'title': title, 'views': views, 'likes': likes, 'favorites': favorites, 'desc_len': desc_len, 'pubdate': it.get('pubdate')})

            # scoring: create three leaderboards
            overall = []
            abyss = []
            battle = []
            def build_entry(mid, name, stats, videos_subset):
                return {
                    'mid': mid,
                    'name': name,
                    'total_videos': stats.get('count', 0),
                    'views': stats.get('views', 0),
                    'likes': stats.get('likes', 0),
                    'favorites': stats.get('favorites', 0),
                    'desc_len': stats.get('desc_len', 0),
                    'videos_list': videos_subset or [],
                    'score': 0.0,
                }

            for mid, v in by_owner.items():
                overall_stats = {
                    'count': len(v['videos']),
                    'views': v['views_total'],
                    'likes': v['likes_total'],
                    'favorites': v.get('favorites_total', 0),
                    'desc_len': v.get('desc_len_total', 0),
                }
                abyss_stats = v['by']['abyss']
                battle_stats = v['by']['battle']

                overall.append(build_entry(mid, v['name'], overall_stats, v['videos']))
                abyss.append(build_entry(mid, v['name'], abyss_stats, abyss_stats.get('videos') or []))
                battle.append(build_entry(mid, v['name'], battle_stats, battle_stats.get('videos') or []))

            overall.sort(key=lambda x: x['score'], reverse=True)
            abyss.sort(key=lambda x: x['score'], reverse=True)
            battle.sort(key=lambda x: x['score'], reverse=True)

            self._prepare_weighted_metrics(overall)
            self._prepare_weighted_metrics(abyss)
            self._prepare_weighted_metrics(battle)

            # optional LLM analysis for top N (use configured LLM settings)
            provider = self.provider.get()
            api_key = self.api_key.get().strip()
            api_url = self.api_url.get().strip() or None
            llm = None
            if self.use_llm.get() and provider != 'none':
                llm = LLMClient(provider=provider, endpoint=api_url, api_key=api_key, model=self.llm_model.get())

            overall_norm = self._normalize_scores(overall)
            abyss_norm = self._normalize_scores(abyss)
            battle_norm = self._normalize_scores(battle)

            # combine with LLM if available
            llm_weight = max(0.0, min(1.0, float(self.llm_weight.get())))
            def enrich_with_llm_and_combine(lst, lst_norm):
                for r in lst[:50]:
                    r['llm_score'] = None
                    r['llm_summary'] = ''
                if not llm:
                    self._apply_local_summaries(lst)
                    return

                # perform LLM analysis in parallel for top N (configurable via self.llm_threads)
                top_n = min(50, len(lst))
                tops = lst[:top_n]

                def _call_llm_safe(uinfo, rref):
                    try:
                        return llm.analyze_uploader(uinfo, top_videos=rref.get('videos_list')[:3])
                    except Exception as e:
                        try:
                            self.log(f"LLM 分析 {rref.get('name')} 出错: {e}")
                            self.log(traceback.format_exc())
                        except Exception:
                            pass
                        return {"score": None, "summary": f"LLM error: {e}"}

                max_workers = 4
                try:
                    max_workers = max(1, min(10, int(self.llm_threads.get())))
                except Exception:
                    max_workers = 4

                future_to_mid = {}
                results_map = {}
                with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
                    for r in tops:
                        info = {"name": r['name'], 'mid': r['mid'], 'videos': r.get('total_videos'), 'views': r.get('views'), 'likes': r.get('likes'), 'top_videos': r.get('videos_list')[:3]}
                        fut = ex.submit(_call_llm_safe, info, r)
                        future_to_mid[fut] = r['mid']
                    for fut in concurrent.futures.as_completed(future_to_mid):
                        mid = future_to_mid[fut]
                        try:
                            out = fut.result()
                        except Exception as e:
                            out = {"score": None, "summary": f"LLM error: {e}"}
                        results_map[mid] = out

                # apply results to corresponding entries
                for r in tops:
                    out = results_map.get(r['mid']) or {"score": None, "summary": ""}
                    llm_score = out.get('score') if isinstance(out, dict) else None
                    try:
                        llm_score = float(llm_score)
                    except Exception:
                        llm_score = None
                    if llm_score is None:
                        llm_score = 5.0
                    llm_score = max(1.0, min(10.0, llm_score))
                    r['llm_score'] = llm_score
                    r['llm_summary'] = out.get('summary', '') if isinstance(out, dict) else str(out)
                    # write LLM output to app log (truncate to avoid flooding)
                    try:
                        summary_text = r['llm_summary'] or ''
                        short = (summary_text[:400] + '...') if len(summary_text) > 400 else summary_text
                        self.log(f"LLM 分析 - {r.get('name')} ({r.get('mid')}): score={llm_score}, summary={short}")
                    except Exception:
                        pass
                    # also log raw model output if available (truncated)
                    try:
                        raw_text = ''
                        if isinstance(out, dict):
                            raw_text = out.get('raw') or out.get('_raw') or ''
                        else:
                            raw_text = str(out)
                        if raw_text:
                            short_raw = (raw_text[:1000] + '...') if len(raw_text) > 1000 else raw_text
                            self.log(f"LLM 原始输出 - {r.get('name')} ({r.get('mid')}): {short_raw}")
                    except Exception:
                        pass

                for r in lst:
                    base = lst_norm.get(r['mid'], 0.0)
                    llm_s = r.get('llm_score')
                    if llm_s is None:
                        final = base
                    else:
                        final = (1.0 - llm_weight) * base + llm_weight * (llm_s)
                    r['final_score'] = final

            enrich_with_llm_and_combine(overall, overall_norm)
            enrich_with_llm_and_combine(abyss, abyss_norm)
            enrich_with_llm_and_combine(battle, battle_norm)

            def sort_by_final(lst):
                lst.sort(key=lambda x: x.get('final_score', x.get('score', 0)), reverse=True)

            sort_by_final(overall)
            sort_by_final(abyss)
            sort_by_final(battle)

            for lst in (overall, abyss, battle):
                for r in lst:
                    r['score'] = round(r.get('final_score', r.get('score', 0)), 3)

            self._llm_used_last = bool(llm)
            self._results_unfiltered = {
                "总榜": copy.deepcopy(overall),
                "深渊榜": copy.deepcopy(abyss),
                "战场榜": copy.deepcopy(battle),
            }
            self._refresh_results_with_blacklist(update_ui=False)
            self.root.after(0, self._apply_results_to_ui)
            self.root.after(0, lambda: self.start_btn.config(state=tk.NORMAL))
            self.root.after(0, lambda: self.export_btn.config(state=tk.NORMAL))
            self.root.after(0, lambda: self.stop_btn.config(state=tk.DISABLED))
            self.log(f"采集完成，共 {len(collected)} 条视频，聚合后 {len(by_owner)} 个 UP 主")

    

    def _update_table(self):
        for i in self.tree.get_children():
            self.tree.delete(i)
        def _label_from_score(s):
            try:
                v = float(s)
            except Exception:
                return ''
            if v >= 8.5:
                return '夯'
            if v >= 7.0:
                return '顶级'
            if v >= 5.5:
                return '人上人'
            if v >= 3.5:
                return 'NPC'
            return '拉完了'

        for idx, r in enumerate(self.results, start=1):
            # prefer explicit tag if available
            label = ''
            try:
                tag = r.get('tag') if isinstance(r, dict) else None
                if tag:
                    label = tag
                else:
                    score_val = r.get('llm_score')
                    if score_val is not None:
                        label = _label_from_score(score_val)
            except Exception:
                label = ''

            self.tree.insert("", tk.END, values=(idx, r.get("name"), label, r.get("total_videos") or r.get("videos") or 0, r.get("views") or 0, r.get("likes") or 0, round(r.get("score", 0), 2), r.get("llm_summary", "")))

    def export_csv(self):
        if not self.results:
            messagebox.showinfo("提示", "没有可导出的结果")
            return
        path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV", "*.csv")])
        if not path:
            return
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(["rank", "up_name", "rating", "videos", "views", "likes", "score", "llm_summary"])
            for idx, r in enumerate(self.results, start=1):
                videos = r.get("total_videos") or r.get("videos") or (len(r.get("videos_list") or []))
                # export textual label rating instead of mid
                try:
                    tag = r.get('tag') if r.get('tag') else None
                    if not tag:
                        sv = r.get('llm_score')
                        if sv is None:
                            rating = ''
                        else:
                            # same mapping as display
                            try:
                                svf = float(sv)
                            except Exception:
                                svf = None
                            if svf is None:
                                rating = ''
                            elif svf >= 8.5:
                                rating = '夯'
                            elif svf >= 7.0:
                                rating = '顶级'
                            elif svf >= 5.5:
                                rating = '人上人'
                            elif svf >= 3.5:
                                rating = 'NPC'
                            else:
                                rating = '拉完了'
                    else:
                        rating = tag
                except Exception:
                    rating = ''
                w.writerow([idx, r.get("name"), rating, videos, r.get("views") or 0, r.get("likes") or 0, r.get("score"), r.get("llm_summary")])
        messagebox.showinfo("完成", f"已导出 {path}")

    def on_leaderboard_change(self):
        sel = self.leaderboard_var.get()
        base = self.results_by_category.get(sel)
        if base is None:
            base = self.results_by_category_raw.get(sel, self.results)
        self.results = base or []
        self._update_table()


if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
