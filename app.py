"""
Main GUI application (tkinter) for crawling B站并生成崩坏3 UP 主排行榜。
"""
import threading
import concurrent.futures
import requests
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime
import csv
import traceback
import os
import json

from bilibili import collect_by_keyword, get_last_response
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


class App:
    def __init__(self, root):
        self.root = root
        root.title("B站崩批统计排行榜")
        root.geometry("1200x800")
        root.minsize(1000, 700)
        
        # 配置颜色主题
        root.configure(bg='#f0f0f0')
        
        style = ttk.Style()
        try:
            style.theme_use('clam')
        except Exception:
            pass
        
        # 自定义样式
        style.configure('Title.TLabel', font=('Microsoft YaHei UI', 12, 'bold'), background='#f0f0f0')
        style.configure('Header.TLabel', font=('Microsoft YaHei UI', 9, 'bold'), background='#f0f0f0')
        # 配置 LabelFrame 样式（使用默认的 TLabelFrame 并配置字体）
        style.configure('TLabelFrame', font=('Microsoft YaHei UI', 9, 'bold'))
        # 配置 Checkbutton 字体
        style.configure('TCheckbutton', font=('Microsoft YaHei UI', 9))
        # 配置 Entry、Combobox、Spinbox 字体
        style.configure('TEntry', font=('Microsoft YaHei UI', 9))
        style.configure('TCombobox', font=('Microsoft YaHei UI', 9))
        style.configure('TSpinbox', font=('Microsoft YaHei UI', 9))
        style.configure('Action.TButton', font=('Microsoft YaHei UI', 9), padding=6)
        style.configure('Primary.TButton', font=('Microsoft YaHei UI', 9, 'bold'), padding=8)
        style.map('Primary.TButton', 
                  background=[('active', '#4CAF50'), ('!active', '#45a049')],
                  foreground=[('active', 'white'), ('!active', 'white')])
        # 表格样式
        style.configure("Treeview", font=('Microsoft YaHei UI', 9), rowheight=25, fieldbackground='white')
        style.configure("Treeview.Heading", font=('Microsoft YaHei UI', 9, 'bold'), background='#e0e0e0', relief='flat')
        style.map("Treeview", 
                  background=[('selected', '#4CAF50')],
                  foreground=[('selected', 'white')])
        
        # 主容器
        self.main = ttk.Frame(root, padding=12)
        self.main.pack(fill=tk.BOTH, expand=True)

        # --- Filter frame (keywords, date, pages, leaderboard) ---
        filter_frame = ttk.LabelFrame(self.main, text="📊 筛选条件", padding=12)
        filter_frame.pack(fill=tk.X, padx=6, pady=6)

        ttk.Label(filter_frame, text="关键词（逗号分隔）:", style='Header.TLabel').grid(row=0, column=0, sticky=tk.W, pady=4)
        self.kv = tk.StringVar(value=",".join(DEFAULT_KEYWORDS))
        entry_keywords = ttk.Entry(filter_frame, textvariable=self.kv, width=70)
        entry_keywords.grid(row=0, column=1, columnspan=3, sticky=tk.W, padx=8, pady=4)

        ttk.Label(filter_frame, text="开始日期:", style='Header.TLabel').grid(row=1, column=0, sticky=tk.W, pady=4)
        self.start = tk.StringVar(value=(datetime.now().strftime("%Y-01-01")))
        ttk.Entry(filter_frame, textvariable=self.start, width=14).grid(row=1, column=1, sticky=tk.W, padx=8, pady=4)

        ttk.Label(filter_frame, text="结束日期:", style='Header.TLabel').grid(row=1, column=2, sticky=tk.W, pady=4)
        self.end = tk.StringVar(value=datetime.now().strftime("%Y-%m-%d"))
        ttk.Entry(filter_frame, textvariable=self.end, width=14).grid(row=1, column=3, sticky=tk.W, padx=8, pady=4)

        ttk.Label(filter_frame, text="Pages/关键词:", style='Header.TLabel').grid(row=1, column=4, sticky=tk.W, padx=(16,0), pady=4)
        self.pages = tk.IntVar(value=2)
        ttk.Spinbox(filter_frame, from_=1, to=10, textvariable=self.pages, width=6).grid(row=1, column=5, sticky=tk.W, padx=4, pady=4)

        ttk.Label(filter_frame, text="显示榜单:", style='Header.TLabel').grid(row=0, column=4, sticky=tk.W, padx=(16,0), pady=4)
        self.leaderboard_var = tk.StringVar(value="总榜")
        self.leaderboard_cb = ttk.Combobox(filter_frame, values=["总榜", "深渊榜", "战场榜"], textvariable=self.leaderboard_var, width=12, state='readonly')
        self.leaderboard_cb.grid(row=0, column=5, sticky=tk.W, padx=4, pady=4)
        self.leaderboard_cb.bind("<<ComboboxSelected>>", lambda e: self.on_leaderboard_change())

        # --- Settings frame (LLM, cookie, proxy) ---
        settings_frame = ttk.LabelFrame(self.main, text="⚙️ 设置", padding=12)
        settings_frame.pack(fill=tk.X, padx=6, pady=6)

        ttk.Label(settings_frame, text="LLM Provider:", style='Header.TLabel').grid(row=0, column=0, sticky=tk.W, pady=4)
        self.provider = tk.StringVar(value="openai")
        ttk.Combobox(settings_frame, values=["openai", "ollama", "none"], textvariable=self.provider, width=12, state='readonly').grid(row=0, column=1, sticky=tk.W, padx=4, pady=4)

        # enable/disable LLM usage switch
        self.use_llm = tk.BooleanVar(value=True)
        ttk.Checkbutton(settings_frame, text="启用 LLM", variable=self.use_llm).grid(row=0, column=6, sticky=tk.W, padx=(12,0), pady=4)

        ttk.Label(settings_frame, text="LLM API Key:", style='Header.TLabel').grid(row=0, column=2, sticky=tk.W, padx=(12,0), pady=4)
        self.api_key = tk.StringVar(value="")
        ttk.Entry(settings_frame, textvariable=self.api_key, width=34, show='*').grid(row=0, column=3, sticky=tk.W, padx=4, pady=4)

        ttk.Label(settings_frame, text="LLM API URL:", style='Header.TLabel').grid(row=1, column=0, sticky=tk.W, pady=4)
        self.api_url = tk.StringVar(value="")
        ttk.Entry(settings_frame, textvariable=self.api_url, width=50).grid(row=1, column=1, columnspan=3, sticky=tk.W, padx=4, pady=4)

        ttk.Label(settings_frame, text="LLM Model:", style='Header.TLabel').grid(row=2, column=4, sticky=tk.W, padx=(12,0), pady=4)
        self.llm_model = tk.StringVar(value="gpt-3.5-turbo")
        ttk.Entry(settings_frame, textvariable=self.llm_model, width=20).grid(row=2, column=5, sticky=tk.W, padx=4, pady=4)

        ttk.Label(settings_frame, text="B站 Cookie:", style='Header.TLabel').grid(row=2, column=0, sticky=tk.W, pady=4)
        self.bil_cookie = tk.StringVar(value="")
        # mask cookie display for privacy
        ttk.Entry(settings_frame, textvariable=self.bil_cookie, width=70, show='*').grid(row=2, column=1, columnspan=3, sticky=tk.W, padx=4, pady=4)

        ttk.Label(settings_frame, text="代理池 (逗号分隔):", style='Header.TLabel').grid(row=3, column=0, sticky=tk.W, pady=4)
        self.proxy_list = tk.StringVar(value="")
        ttk.Entry(settings_frame, textvariable=self.proxy_list, width=70).grid(row=3, column=1, columnspan=3, sticky=tk.W, padx=4, pady=4)
        self.use_proxy = tk.BooleanVar(value=False)
        ttk.Checkbutton(settings_frame, text="启用代理池", variable=self.use_proxy).grid(row=3, column=4, sticky=tk.W, padx=8, pady=4)
        self.use_proxypool = tk.BooleanVar(value=False)
        ttk.Checkbutton(settings_frame, text="使用 proxypool 框架", variable=self.use_proxypool).grid(row=3, column=5, sticky=tk.W, padx=4, pady=4)

        ttk.Label(settings_frame, text="LLM 权重 (0-1):", style='Header.TLabel').grid(row=0, column=4, sticky=tk.W, padx=(12,0), pady=4)
        self.llm_weight = tk.DoubleVar(value=0.4)
        ttk.Spinbox(settings_frame, from_=0.0, to=1.0, increment=0.1, textvariable=self.llm_weight, width=6).grid(row=0, column=5, sticky=tk.W, padx=4, pady=4)
        ttk.Label(settings_frame, text="LLM 并发数:", style='Header.TLabel').grid(row=1, column=4, sticky=tk.W, padx=(12,0), pady=4)
        self.llm_threads = tk.IntVar(value=4)
        ttk.Spinbox(settings_frame, from_=1, to=10, textvariable=self.llm_threads, width=6).grid(row=1, column=5, sticky=tk.W, padx=4, pady=4)

        ttk.Button(settings_frame, text="💾 保存设置", command=self.save_config, style='Action.TButton').grid(row=4, column=1, sticky=tk.W, pady=8, padx=4)
        ttk.Button(settings_frame, text="🔌 测试 LLM", command=self.test_llm_connection, style='Action.TButton').grid(row=4, column=2, sticky=tk.W, pady=8, padx=4)
        ttk.Button(settings_frame, text="🌐 测试代理", command=self.test_proxies, style='Action.TButton').grid(row=4, column=3, sticky=tk.W, pady=8, padx=4)

        # --- Actions frame ---
        actions = ttk.Frame(self.main)
        actions.pack(fill=tk.X, padx=6, pady=8)
        self.start_btn = ttk.Button(actions, text="▶️ 开始采集并排行", command=self.start_scan, style='Primary.TButton')
        self.start_btn.pack(side=tk.LEFT, padx=(0,8))
        self.stop_btn = ttk.Button(actions, text="⏹️ 停止采集", command=self.stop_scan, state=tk.DISABLED, style='Action.TButton')
        self.stop_btn.pack(side=tk.LEFT, padx=(0,8))
        self.export_btn = ttk.Button(actions, text="📥 导出 CSV", command=self.export_csv, state=tk.DISABLED, style='Action.TButton')
        self.export_btn.pack(side=tk.LEFT, padx=(0,8))
        self.progress = ttk.Progressbar(actions, length=400, mode='determinate')
        self.progress.pack(side=tk.RIGHT, padx=(8,0))
        # 配置进度条样式
        style.configure("TProgressbar", background='#4CAF50', troughcolor='#e0e0e0', borderwidth=0, lightcolor='#4CAF50', darkcolor='#4CAF50')

        # --- Table frame ---
        table_frame = ttk.LabelFrame(self.main, text="📋 排行榜", padding=8)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=6, pady=6)
        cols = ("rank", "up_name", "rating", "videos", "views", "likes", "score", "llm_summary")
        self.tree = ttk.Treeview(table_frame, columns=cols, show="headings", height=12)
        # headings: show friendly Chinese label for rating
        headings = {"rank": "排名", "up_name": "UP主", "rating": "评级", "videos": "视频数", "views": "播放量", "likes": "收藏数", "score": "分数", "llm_summary": "评价"}
        for c in cols:
            self.tree.heading(c, text=headings.get(c, c))
            # narrow rating column
            if c == 'rating':
                self.tree.column(c, width=100, anchor=tk.CENTER)
            elif c == 'rank':
                self.tree.column(c, width=60, anchor=tk.CENTER)
            elif c == 'up_name':
                self.tree.column(c, width=150, anchor=tk.W)
            elif c == 'llm_summary':
                self.tree.column(c, width=300, anchor=tk.W)
            else:
                self.tree.column(c, width=120, anchor=tk.W)
        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.grid(row=0, column=0, sticky=tk.NSEW)
        vsb.grid(row=0, column=1, sticky=tk.NS)
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        # --- Log frame ---
        log_frame = ttk.LabelFrame(self.main, text="📝 运行日志", padding=8)
        log_frame.pack(fill=tk.BOTH, padx=6, pady=6)
        self.log_text = tk.Text(log_frame, height=8, wrap=tk.WORD, font=('Consolas', 9), bg='#1e1e1e', fg='#d4d4d4', insertbackground='#ffffff')
        lvsb = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=lvsb.set)
        self.log_text.grid(row=0, column=0, sticky=tk.NSEW)
        lvsb.grid(row=0, column=1, sticky=tk.NS)
        log_frame.rowconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)

        self.results = []
        # load saved config if exists
        try:
            self.load_config()
        except Exception:
            pass
        # stop event for canceling scans
        self._stop_event = threading.Event()

    def log(self, msg: str, level="info"):
        # thread-safe append with color coding
        def _append():
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.log_text.insert(tk.END, f"[{timestamp}] ", "timestamp")
            
            # 根据级别设置颜色
            if level == "error":
                self.log_text.insert(tk.END, f"{msg}\n", "error")
            elif level == "success":
                self.log_text.insert(tk.END, f"{msg}\n", "success")
            elif level == "warning":
                self.log_text.insert(tk.END, f"{msg}\n", "warning")
            else:
                self.log_text.insert(tk.END, f"{msg}\n", "info")
            
            self.log_text.see(tk.END)
            
            # 配置标签颜色
            self.log_text.tag_config("timestamp", foreground="#808080")
            self.log_text.tag_config("info", foreground="#d4d4d4")
            self.log_text.tag_config("success", foreground="#4CAF50")
            self.log_text.tag_config("warning", foreground="#FF9800")
            self.log_text.tag_config("error", foreground="#f44336")

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
        }
        try:
            with open(self.config_path(), "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)
            self.log("设置已保存", "success")
            messagebox.showinfo("设置", "设置已保存到 config.json")
        except Exception as e:
            self.log(f"保存设置失败: {e}", "error")
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
            self.log("已加载配置", "success")
        except Exception as e:
            self.log(f"加载配置失败: {e}", "warning")

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
            self.log(f"LLM 连接成功: {res.get('msg')}", "success")
            messagebox.showinfo("测试连接", "连接成功")
        else:
            self.log(f"LLM 连接失败: {res.get('msg')}", "error")
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
            self.log(f"已将 {len(good)} 个可用代理加入代理池", "success")
            messagebox.showinfo("测试代理", f"{len(good)} 个代理可用，已启用")
        else:
            self.log("没有可用的代理", "warning")
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
            for kw in keywords:
                for p in range(1, pages + 1):
                    if self._stop_event.is_set():
                        self.log("检测到停止信号，已中止采集循环")
                        break
                    try:
                        items = collect_by_keyword(kw, pages=1)
                        self.log(f"已检索关键词 '{kw}' 第 {p} 页，返回 {len(items)} 条结果")
                        if not items:
                            try:
                                last = get_last_response()
                                self.log(f"B站响应调试信息: {last}")
                                if isinstance(last, dict) and last.get('status_code') == 412:
                                    self.log("检测到 B站 安全拦截 (412)，当前 IP/请求被封。建议：使用有效的 B站 Cookie、代理或通过浏览器登录并抓取。")
                                    self.log("已停止采集以避免进一步封禁。若要继续，请配置 Cookie 或代理后重新开始。")
                                    self._stop_event.set()
                                    break
                            except Exception:
                                pass
                    except Exception as e:
                        items = []
                        self.log(f"关键词 '{kw}' 第 {p} 页检索出错: {e}")
                        self.log(traceback.format_exc())
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
                if self._stop_event.is_set():
                    break

            # aggregate by owner with per-category stats
            by_owner = {}
            for it in collected:
                owner = it.get('owner') or {}
                mid = owner.get('mid') or owner.get('mid')
                if not mid:
                    continue
                entry = by_owner.setdefault(mid, {"name": owner.get('name') or owner.get('uname') or str(mid), "mid": mid, "videos": [], "views_total": 0, "likes_total": 0, "by": {"abyss": {"count": 0, "views": 0, "likes": 0, "videos": []}, "battle": {"count": 0, "views": 0, "likes": 0, "videos": []}, "other": {"count": 0, "views": 0, "likes": 0, "videos": []}}})
                stat = it.get('stat') or {}
                views = int(stat.get('view', 0) or 0)
                likes = int(stat.get('like', 0) or stat.get('like') or 0)
                title = (it.get('title') or '')
                kw = (it.get('keyword') or '')
                cat = 'other'
                if '深渊' in kw or '深渊' in title:
                    cat = 'abyss'
                elif '记忆战场' in kw or '记忆战场' in title or '战场' in kw or '战场' in title:
                    cat = 'battle'
                entry['videos'].append({"bvid": it.get('bvid'), 'title': title, 'views': views, 'likes': likes, 'pubdate': it.get('pubdate'), 'cat': cat})
                entry['views_total'] += views
                entry['likes_total'] += likes
                entry['by'][cat]['count'] += 1
                entry['by'][cat]['views'] += views
                entry['by'][cat]['likes'] += likes
                entry['by'][cat]['videos'].append({"bvid": it.get('bvid'), 'title': title, 'views': views, 'likes': likes, 'pubdate': it.get('pubdate')})

            # scoring: create three leaderboards
            overall = []
            abyss = []
            battle = []
            for mid, v in by_owner.items():
                total_videos = len(v['videos'])
                total_views = v['views_total']
                total_likes = v['likes_total']
                overall_score = total_videos * 2.0 + total_views * 0.0001 + total_likes * 0.001
                abyss_score = v['by']['abyss']['count'] * 2.0 + v['by']['abyss']['views'] * 0.0001 + v['by']['abyss']['likes'] * 0.001
                battle_score = v['by']['battle']['count'] * 2.0 + v['by']['battle']['views'] * 0.0001 + v['by']['battle']['likes'] * 0.001
                base = {'mid': mid, 'name': v['name'], 'total_videos': total_videos, 'views': total_views, 'likes': total_likes, 'videos_list': v['videos'], 'by': v['by']}
                overall.append({**base, 'score': overall_score})
                abyss.append({**base, 'score': abyss_score})
                battle.append({**base, 'score': battle_score})

            overall.sort(key=lambda x: x['score'], reverse=True)
            abyss.sort(key=lambda x: x['score'], reverse=True)
            battle.sort(key=lambda x: x['score'], reverse=True)

            # optional LLM analysis for top N (use configured LLM settings)
            provider = self.provider.get()
            api_key = self.api_key.get().strip()
            api_url = self.api_url.get().strip() or None
            llm = None
            if self.use_llm.get() and provider != 'none':
                llm = LLMClient(provider=provider, endpoint=api_url, api_key=api_key, model=self.llm_model.get())

            # compute base scores already stored in each list under 'score'
            def normalize_scores(lst):
                vals = [x.get('score', 0) for x in lst]
                if not vals:
                    return {}
                mn = min(vals)
                mx = max(vals)
                if mx == mn:
                    return {x['mid']: 5.0 for x in lst}
                out = {}
                for x in lst:
                    out[x['mid']] = ((x.get('score', 0) - mn) / (mx - mn)) * 10.0
                return out

            overall_norm = normalize_scores(overall)
            abyss_norm = normalize_scores(abyss)
            battle_norm = normalize_scores(battle)

            # combine with LLM if available
            llm_weight = max(0.0, min(1.0, float(self.llm_weight.get())))
            def enrich_with_llm_and_combine(lst, lst_norm):
                for r in lst[:50]:
                    r['llm_score'] = None
                    r['llm_summary'] = ''
                if not llm:
                    # Local weighted rating when LLM is disabled.
                    # Weights:
                    # - published video count: 50% (增加寂灭时 +20% => 70%)
                    # - views: 无寂灭 30% / 有寂灭 20%
                    # - likes: 无寂灭 20% / 有寂灭 10%
                    counts_list = [ (x.get('total_videos') or len(x.get('videos_list') or [])) for x in lst ]
                    views_list = [ (x.get('views') or 0) for x in lst ]
                    likes_list = [ (x.get('likes') or 0) for x in lst ]

                    def norm_value(val, mn, mx):
                        try:
                            v = float(val)
                        except Exception:
                            v = 0.0
                        if mx == mn:
                            return 5.0
                        return ((v - mn) / (mx - mn)) * 10.0

                    cmin = min(counts_list) if counts_list else 0
                    cmax = max(counts_list) if counts_list else 0
                    vmin = min(views_list) if views_list else 0
                    vmax = max(views_list) if views_list else 0
                    lmin = min(likes_list) if likes_list else 0
                    lmax = max(likes_list) if likes_list else 0

                    def map_label(score):
                        if score >= 8.5:
                            return ("夯", 10.0)
                        if score >= 7.0:
                            return ("顶级", 8.0)
                        if score >= 5.5:
                            return ("人上人", 6.0)
                        if score >= 3.5:
                            return (("NPC", 4.0))
                        return (("拉完了", 2.0))

                    for r in lst:
                        counts_val = (r.get('total_videos') or len(r.get('videos_list') or []))
                        views_val = r.get('views') or 0
                        likes_val = r.get('likes') or 0
                        counts_n = norm_value(counts_val, cmin, cmax)
                        views_n = norm_value(views_val, vmin, vmax)
                        likes_n = norm_value(likes_val, lmin, lmax)

                        # detect presence of 寂灭 in any published title
                        has_jm = False
                        for vv in (r.get('videos_list') or []):
                            try:
                                t = (vv.get('title') or '')
                                if '寂灭' in t:
                                    has_jm = True
                                    break
                            except Exception:
                                continue

                        if has_jm:
                            w_counts, w_views, w_likes = 0.7, 0.2, 0.1
                        else:
                            w_counts, w_views, w_likes = 0.5, 0.3, 0.2

                        composite = counts_n * w_counts + views_n * w_views + likes_n * w_likes
                        label, val = map_label(composite)

                        r['llm_score'] = val
                        r['llm_summary'] = (f"本地评级({ '含寂灭' if has_jm else '无寂灭' }): {label} "
                                            f"(评分={composite:.2f}); counts={counts_val}({counts_n:.2f}), "
                                            f"views={views_val}({views_n:.2f}), likes={likes_val}({likes_n:.2f})")
                        try:
                            self.log(f"本地加权评级 - {r.get('name')} ({r.get('mid')}): {label}, score={composite:.2f}, counts={counts_val}, views={views_val}, likes={likes_val}")
                        except Exception:
                            pass
                        # use composite as final score so sorting follows the weighted rating
                        r['final_score'] = composite
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

            self.results_by_category = {"总榜": overall, "深渊榜": abyss, "战场榜": battle}
            self.results = self.results_by_category.get(self.leaderboard_var.get(), overall)
            self.root.after(0, self._update_table)
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

            # 添加交替行颜色
            tags = ('evenrow',) if idx % 2 == 0 else ('oddrow',)
            item = self.tree.insert("", tk.END, values=(idx, r.get("name"), label, r.get("total_videos") or r.get("videos") or 0, r.get("views") or 0, r.get("likes") or 0, round(r.get("score", 0), 2), r.get("llm_summary", "")), tags=tags)
        
        # 配置交替行颜色
        self.tree.tag_configure('evenrow', background='#f5f5f5')
        self.tree.tag_configure('oddrow', background='white')

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
        self.results = self.results_by_category.get(sel, self.results)
        self._update_table()


if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
