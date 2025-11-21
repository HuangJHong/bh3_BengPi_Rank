# B站崩批统计排行榜 (bh3_Rank)

说明：这个项目用于通过关键词在哔哩哔哩（B站）检索与“崩坏3”作业相关的视频，汇总各 UP 主的相关视频数据并生成排行榜。项目提供一个简单的 GUI，可以设置关键词、时间区间、分类过滤，并支持通过 LLM（兼容 OpenAI API 或本地 Ollama）对 UP 主进行自动评价。

主要功能：
- 通过关键词检索视频（使用 B 站公开接口）
- 按 UP 主聚合视频数据并计算排行（播放、点赞等）
- 支持时间区间过滤
- 支持自定义关键词集合（默认为崩坏3 相关关键词）
- 使用 LLM 对 UP 主进行文本分析与评分（可选）
- GUI 界面（基于 `tkinter`）

快速开始
1. 创建虚拟环境并安装依赖：

```powershell
cd E:\PythonProject\bh3_Rank
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. 配置环境变量（可选 - 若使用 OpenAI）：

```powershell
$env:OPENAI_API_KEY = "your_openai_api_key"
```

3. 运行 GUI：

```powershell
python app.py
```

注意
- 本项目使用 B 站公开接口，若大量抓取请遵守网站使用条款并控制请求频率。
- Ollama 使用本地部署时，请在设置中指定正确的地址。
