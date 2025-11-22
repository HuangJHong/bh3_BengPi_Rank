"""
Simple LLM client supporting OpenAI-compatible API and Ollama local endpoint.
This is a minimal wrapper — extend prompt/temperature/streaming as needed.
"""
import os
import requests
from typing import Dict, Any


class LLMClient:
    def __init__(self, provider: str = "openai", endpoint: str = None, api_key: str = None, model: str = None):
        self.provider = provider
        self.endpoint = endpoint
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.model = model or "gpt-3.5-turbo"

    def analyze_uploader(self, uploader_info: Dict[str, Any], top_videos: list = None) -> Dict[str, Any]:
        """Return a short evaluation and numeric score (1-10).
        uploader_info: dict with keys 'name', 'mid', 'videos_summary', 'desc', 'comments_sample' etc.
        """
        name = uploader_info.get("name") or uploader_info.get("mid")
        prompt = f"请基于以下信息对UP主 '{name}' 做一个简短的评价（中文），并给出1到10的评分，最后输出一个一句话的推荐标签。\n信息：\n"
        for k, v in uploader_info.items():
            prompt += f"{k}: {v}\n"
        prompt += "\n请返回 JSON 格式：{\"score\": number, \"summary\": string, \"tag\": string}"

        if self.provider == "openai":
            return self._call_openai_chat(prompt)
        elif self.provider == "ollama":
            return self._call_ollama(prompt)
        else:
            return {"score": 5, "summary": "未配置 LLM。", "tag": "无"}

    def _call_openai_chat(self, prompt: str) -> Dict[str, Any]:
        # resolve URL: allow self.endpoint to be a full path or a base URL
        if self.endpoint:
            ep = self.endpoint.rstrip('/')
            if 'chat.completions' in ep:
                url = ep
            else:
                # ensure v1 path
                if ep.endswith('/v1') or ep.endswith('/v1/'):
                    url = ep.rstrip('/') + '/chat/completions'
                else:
                    url = ep + '/v1/chat/completions'
        else:
            url = "https://api.openai.com/v1/chat/completions"

        # default Authorization header uses Bearer token; many OpenAI-compatible services accept this
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        data = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 300,
            "temperature": 0.2,
        }
        r = requests.post(url, json=data, headers=headers, timeout=30)
        r.raise_for_status()
        # extract text content in a tolerant way
        j = r.json()
        # OpenAI-compatible response
        try:
            text = j["choices"][0]["message"]["content"]
        except Exception:
            # fallback: try other shapes
            text = j.get("output", j.get("text", ""))
            if isinstance(text, list):
                text = "\n".join([t.get("content", "") if isinstance(t, dict) else str(t) for t in text])
        parsed = self._parse_json_like(text)
        if isinstance(parsed, dict):
            parsed.setdefault('raw', text)
            return parsed
        return {'raw': text}

    def _call_ollama(self, prompt: str) -> Dict[str, Any]:
        # default ollama endpoint
        endpoint = self.endpoint or "http://127.0.0.1:11434/api/generate"
        data = {"model": self.model or "llama2", "prompt": prompt, "max_tokens": 300}
        r = requests.post(endpoint, json=data, timeout=30)
        r.raise_for_status()
        text = r.text
        parsed = self._parse_json_like(text)
        if isinstance(parsed, dict):
            parsed.setdefault('raw', text)
            return parsed
        return {'raw': text}

    def _parse_json_like(self, text: str) -> Dict[str, Any]:
        # try to extract a JSON object from model output heuristically
        import re, json
        if not text:
            return {"score": 6, "summary": "", "tag": "无"}
        m = re.search(r"\{[\s\S]*\}", text)
        if not m:
            # if not JSON, return the raw text as summary
            return {"score": 6, "summary": text.strip(), "tag": "无"}
        try:
            return json.loads(m.group(0))
        except Exception:
            return {"score": 6, "summary": text.strip(), "tag": "无"}

    def test_connection(self) -> Dict[str, Any]:
        """Test connectivity to the configured provider. Returns dict with 'ok' and 'msg'."""
        try:
            if self.provider == "openai":
                # allow custom endpoint for OpenAI-compatible providers
                if not (self.api_key or self.endpoint):
                    return {"ok": False, "msg": "未配置 API Key 或 endpoint"}
                # resolve URL same as _call_openai_chat
                if self.endpoint:
                    ep = self.endpoint.rstrip('/')
                    if 'chat.completions' in ep:
                        url = ep
                    else:
                        if ep.endswith('/v1') or ep.endswith('/v1/'):
                            url = ep.rstrip('/') + '/chat/completions'
                        else:
                            url = ep + '/v1/chat/completions'
                else:
                    url = "https://api.openai.com/v1/chat/completions"
                headers = {"Content-Type": "application/json"}
                if self.api_key:
                    headers["Authorization"] = f"Bearer {self.api_key}"
                data = {
                    "model": self.model,
                    "messages": [{"role": "user", "content": "测试连接"}],
                    "max_tokens": 1,
                }
                r = requests.post(url, json=data, headers=headers, timeout=10)
                if r.status_code == 200:
                    return {"ok": True, "msg": "OpenAI-compatible 服务连接成功"}
                return {"ok": False, "msg": f"HTTP {r.status_code}: {r.text[:200]}"}
            elif self.provider == "ollama":
                endpoint = self.endpoint or "http://127.0.0.1:11434/api/generate"
                data = {"model": self.model or "llama2", "prompt": "测试连接", "max_tokens": 1}
                r = requests.post(endpoint, json=data, timeout=10)
                if r.status_code == 200:
                    return {"ok": True, "msg": "Ollama 连接成功"}
                return {"ok": False, "msg": f"HTTP {r.status_code}: {r.text[:200]}"}
            else:
                return {"ok": False, "msg": "未知的 provider"}
        except Exception as e:
            return {"ok": False, "msg": str(e)}
        try:
            return json.loads(m.group(0))
        except Exception:
            return {"score": 6, "summary": text.strip(), "tag": "无"}
