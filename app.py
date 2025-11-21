"""
Main GUI application (tkinter) for crawling B站并生成崩坏3 UP 主排行榜。
"""
import threading
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
    "崩坏3 作业",
    "崩坏3 无限",
    "崩坏3 寂灭",
    "崩坏3 红莲",
    "崩坏3 苦痛",
    "崩坏3 禁忌",
    "崩坏3 原罪",
    "崩坏3 乐土",
]


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

        ttk.Label(settings_frame, text="LLM API Key:").grid(row=0, column=2, sticky=tk.W, padx=(10,0))
        self.api_key = tk.StringVar(value="")
        ttk.Entry(settings_frame, textvariable=self.api_key, width=34, show='*').grid(row=0, column=3, sticky=tk.W)

        ttk.Label(settings_frame, text="LLM API URL:").grid(row=1, column=0, sticky=tk.W)
        self.api_url = tk.StringVar(value="")
        ttk.Entry(settings_frame, textvariable=self.api_url, width=50).grid(row=1, column=1, columnspan=3, sticky=tk.W, pady=2)

        ttk.Label(settings_frame, text="B站 Cookie:").grid(row=2, column=0, sticky=tk.W)
        self.bil_cookie = tk.StringVar(value="")
        ttk.Entry(settings_frame, textvariable=self.bil_cookie, width=70).grid(row=2, column=1, columnspan=3, sticky=tk.W)

        ttk.Label(settings_frame, text="代理池 (逗号分隔):").grid(row=3, column=0, sticky=tk.W)
        self.proxy_list = tk.StringVar(value="")
        ttk.Entry(settings_frame, textvariable=self.proxy_list, width=70).grid(row=3, column=1, columnspan=3, sticky=tk.W)
        self.use_proxy = tk.BooleanVar(value=False)
        ttk.Checkbutton(settings_frame, text="启用代理池", variable=self.use_proxy).grid(row=3, column=4, sticky=tk.W, padx=6)
        self.use_proxypool = tk.BooleanVar(value=False)
        ttk.Checkbutton(settings_frame, text="使用 proxypool 框架", variable=self.use_proxypool).grid(row=3, column=5, sticky=tk.W, padx=6)

        ttk.Button(settings_frame, text="保存设置", command=self.save_config).grid(row=4, column=1, sticky=tk.W, pady=6)
        ttk.Button(settings_frame, text="测试 LLM", command=self.test_llm_connection).grid(row=4, column=2, sticky=tk.W, pady=6, padx=4)
        ttk.Button(settings_frame, text="测试代理", command=self.test_proxies).grid(row=4, column=3, sticky=tk.W, pady=6)
        ttk.Label(settings_frame, text="LLM 权重 (0-1):").grid(row=0, column=4, sticky=tk.W, padx=(10,0))
        self.llm_weight = tk.DoubleVar(value=0.4)
        ttk.Spinbox(settings_frame, from_=0.0, to=1.0, increment=0.1, textvariable=self.llm_weight, width=6).grid(row=0, column=5, sticky=tk.W)

        # --- Actions frame ---
        actions = ttk.Frame(self.main)
        actions.pack(fill=tk.X, padx=4, pady=4)
        self.start_btn = ttk.Button(actions, text="开始采集并排行", command=self.start_scan)
        self.start_btn.pack(side=tk.LEFT, padx=(0,6))
        self.stop_btn = ttk.Button(actions, text="停止采集", command=self.stop_scan, state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT, padx=(0,6))
        self.export_btn = ttk.Button(actions, text="导出 CSV", command=self.export_csv, state=tk.DISABLED)
        self.export_btn.pack(side=tk.LEFT, padx=(0,6))
        self.progress = ttk.Progressbar(actions, length=360)
        self.progress.pack(side=tk.RIGHT)

        # --- Table frame ---
        table_frame = ttk.Frame(self.main)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)
        cols = ("rank", "up_name", "mid", "videos", "views", "likes", "score", "llm_summary")
        self.tree = ttk.Treeview(table_frame, columns=cols, show="headings")
        for c in cols:
            self.tree.heading(c, text=c)
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

        self.results = []
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
            "bili_cookie": self.bil_cookie.get(),
            "proxies": self.proxy_list.get(),
            "use_proxy": bool(self.use_proxy.get()),
            "use_proxypool": bool(self.use_proxypool.get()),
            "llm_weight": float(self.llm_weight.get()),
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
            self.bil_cookie.set(cfg.get("bili_cookie", ""))
            self.proxy_list.set(cfg.get("proxies", ""))
            self.use_proxy.set(cfg.get("use_proxy", False))
            self.use_proxypool.set(cfg.get("use_proxypool", False))
            try:
                self.llm_weight.set(float(cfg.get("llm_weight", 0.4)))
            except Exception:
                self.llm_weight.set(0.4)
            self.log("已加载配置")
        except Exception as e:
            self.log(f"加载配置失败: {e}")

    def test_llm_connection(self):
        provider = self.provider.get()
        api_key = self.api_key.get().strip()
        api_url = self.api_url.get().strip() or None
        if provider == "none":
            messagebox.showinfo("测试连接", "未启用 LLM（选择了 none）")
            return
        client = LLMClient(provider=provider, endpoint=api_url, api_key=api_key)
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
            if provider != 'none':
                llm = LLMClient(provider=provider, endpoint=api_url, api_key=api_key)

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
                    for r in lst:
                        r['final_score'] = lst_norm.get(r['mid'], 0.0)
                    return
                for r in lst[:50]:
                    try:
                        info = {"name": r['name'], 'mid': r['mid'], 'videos': r.get('total_videos'), 'views': r.get('views'), 'likes': r.get('likes'), 'top_videos': r.get('videos_list')[:3]}
                        out = llm.analyze_uploader(info, top_videos=r.get('videos_list')[:3])
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
                    except Exception as e:
                        r['llm_score'] = 5.0
                        r['llm_summary'] = f"LLM error: {e}"
                        self.log(f"LLM 分析 {r.get('name')} 出错: {e}")
                        self.log(traceback.format_exc())
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
        for idx, r in enumerate(self.results, start=1):
            self.tree.insert("", tk.END, values=(idx, r.get("name"), r.get("mid"), r.get("total_videos") or r.get("videos") or 0, r.get("views") or 0, r.get("likes") or 0, round(r.get("score", 0), 2), r.get("llm_summary", "")))

    def export_csv(self):
        if not self.results:
            messagebox.showinfo("提示", "没有可导出的结果")
            return
        path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV", "*.csv")])
        if not path:
            return
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(["rank", "up_name", "mid", "videos", "views", "likes", "score", "llm_summary"])
            for idx, r in enumerate(self.results, start=1):
                videos = r.get("total_videos") or r.get("videos") or (len(r.get("videos_list") or []))
                w.writerow([idx, r.get("name"), r.get("mid"), videos, r.get("views") or 0, r.get("likes") or 0, r.get("score"), r.get("llm_summary")])
        messagebox.showinfo("完成", f"已导出 {path}")

    def on_leaderboard_change(self):
        sel = self.leaderboard_var.get()
        self.results = self.results_by_category.get(sel, self.results)
        self._update_table()


if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
