# 🤖 AI Trade Assistant (智能交易助手)

**AI Trade Assistant** 是一个基于大型语言模型（LLM）的自动化 A 股量化投研与决策支持系统。本项目将**经典高阶量化指标（如 Hurst 指数、波动率分位、均值回归）、财务数据与大模型的深度逻辑推理能力**完美结合，并通过现代化的 Web UI 面板为交易者提供直观的决策参考。

通过首创的 **“漏斗式双模型降本架构”** 与 **动态模型竞技场**，本系统在保证极高质量研判的同时，大幅降低了 API 调用成本。

## ✨ 核心特性

* 🖥️ **全景可视化研判面板 (Web Dashboard)**：基于 Dash & Plotly 构建的交互式终端。一屏整合 K 线走势图、宏观大盘环境、核心财务指标、量化信号矩阵、最新新闻面以及 AI 深度逻辑推演，告别枯燥的纯文本阅读。
* 🧠 **漏斗式双模型筛选架构 (Cost-Effective Routing)**：支持“初筛+精决”双重过滤。使用免费/低成本基础模型（如 Flash/8B 级模型）进行海量粗筛；仅当触发非观望信号，或该股属于当前持仓时，才自动唤醒推理能力更强的高级模型（如 Pro/70B+ 级模型）进行深度复核。**成本直降 80%+**。
* 📊 **硬核量化与基本面特征融合**：自动抓取并计算 52 周极值、PE/PB 历史分位、动量效应、布林带偏离度、RSI、ADX，以及高阶统计套利指标（Hurst 指数、偏度、峰度）。整合新浪财务数据与同花顺机构业绩预测。
* 🌍 **宏观风控与动态护城河**：自动拉取上证指数（sh000001）趋势与每日“宏观财经早餐”。让 Agent 具备全局视野，在系统性风险（如大盘跌破 MA20 且 RSI 超卖）发生时，自动规避盲目抄底。
* ⚔️ **大模型竞技场 (Model Arena)**：内置 `model_arena.py` 测试台。支持将同一份复杂的研判 Prompt 并发分发给多个不同的 LLM（如 Gemini, Qwen, DeepSeek 等），直观对比各家大模型的投资逻辑与格式遵循能力。
* 💼 **智能持仓感知**：UI 直接集成持仓股数与成本录入。AI 决策将严格结合您的实际仓位与浮亏/浮盈状况，给出止盈、止损或加仓的个性化建议。

---

## 🚀 快速开始

### 1. 环境准备与依赖安装 (推荐使用 Conda)

强烈推荐使用 **Miniforge** 或 **Anaconda** 来管理项目的 Python 环境，以避免繁杂的依赖冲突。建议创建一个名为 `agent` 的专属环境：

```bash
# 1. 创建并激活名为 agent 的环境 (推荐 Python 3.9+)
conda create -n agent python=3.12
conda activate agent

# 2. 安装核心依赖
pip install dash dash-bootstrap-components plotly pandas numpy baostock akshare beautifulsoup4 python-dotenv json-repair google-genai openai efinance questionary

# 3. 安装 Playwright 及内置浏览器（用于新闻无头抓取）
pip install playwright
playwright install chromium

```

### 2. 模型与密钥配置 (.env)

本项目采用**动态解耦配置**，支持通过 OpenAI 兼容格式无缝接入任何本地开源模型（配合 LM Studio / Ollama）以及云端商业模型。

在项目根目录创建或修改 `.env` 文件。您可以随意配置您拥有的 API 资源，系统将自动在 Web 端生成对应的模型下拉框。

**配置说明：**

* `ACTIVE_MODELS`：全局开关，声明想要激活的模型 ID 列表（以逗号分隔）。只有在这里填写的模型，才会参与运行。
* `[ID]_TYPE`：设定为 `gemini` 调用谷歌原生 SDK，设定为 `openai` 则通过兼容协议调用第三方或本地模型。
* `[ID]_NAME`：在 Web 界面和日志表格中展示的友好名称。
* `[ID]_MODEL`：实际调用时请求底层的模型代号。

**`.env` 完整配置示例：**

```env
# ==========================================
# 模型注册表 (这里用下划线作为唯一ID)
# ==========================================
ACTIVE_MODELS="gemini_flash,gemini_pro,qwen_9b,qwen_35b"

# --- 1. Gemini Flash ---
gemini_flash_TYPE="gemini"
# 网页端下拉框显示的名称
gemini_flash_NAME="gemini-3.1-flash" 
# 实际调用的底层模型名
gemini_flash_MODEL="gemini-3.1-flash-lite-preview"
gemini_flash_API_KEY="你的_gemini_api_key"
gemini_flash_USE_TOOLS="false"

# --- 2. Gemini Pro ---
gemini_pro_TYPE="gemini"
gemini_pro_NAME="gemini-3.1-pro"
gemini_pro_MODEL="gemini-3.1-pro-preview"
gemini_pro_API_KEY="你的_gemini_api_key"
gemini_pro_USE_TOOLS="true"

# --- 3. 本地开源模型 1：Qwen 9B ---
qwen_9b_TYPE="openai"
qwen_9b_NAME="qwen3.5-9b"
qwen_9b_MODEL="qwen/qwen3.5-9b"
qwen_9b_API_KEY="lm-studio"
qwen_9b_BASE_URL="http://localhost:1234/v1"
qwen_9b_STRIP_THINK="true"

# --- 4. 本地开源模型 2：Qwen 35B ---
qwen_35b_TYPE="openai"
qwen_35b_NAME="qwen3.5-35b-a3b"
qwen_35b_MODEL="qwen/qwen3.5-35b-a3b"
qwen_35b_API_KEY="lm-studio"
qwen_35b_BASE_URL="http://localhost:1234/v1"
qwen_35b_STRIP_THINK="true"

```

### 3. 一键启动项目

**方式一：使用批处理脚本一键启动 (Windows 推荐)**

如果您使用的是 Windows 系统，并且已经按照第 1 步建立了名为 `agent` 的 Conda 环境，您只需在项目根目录双击 **`run.bat`** 文件即可一键启动整个 Web 交互界面。
*该脚本已内置了防乱码、自动切换路径、自动激活 `agent` 环境、环境检测以及报错拦截等防呆设计。*

**方式二：终端手动启动 (支持多种模式)**

如果您使用的是 macOS / Linux，或偏好命令行操作，请在确保激活环境后执行所需的模块脚本：

```bash
# 1. 确保已激活虚拟环境
conda activate agent

# 2. 启动交互式 Web 面板 (适合单票深度分析与可视化呈现)
# 启动后在浏览器访问 [http://127.0.0.1:8050](http://127.0.0.1:8050)
python app.py

# 3. 启动批量分析终端 (适合盘后批量扫盘与挖掘)
# 拥有交互式命令行菜单，支持手动输入指定股票池，或从全市场随机抽盲盒。
# 自动结合双筛漏斗架构进行批量测算，结果将汇总至每日的 Daily Table.csv 中。
python run_batch.py

# 4. 运行大模型竞技场 (并发测试多个模型的投资研判表现)
python model_arena.py

```

---

## 🗺️ 未来演进路线 (Roadmap)

* [x] **大模型竞技场**：引入多模型进行自动化对比，筛选最具“盘感”的交易模型。
* [x] **交互式终端升级**：从纯 CLI 脚本升级为现代化的可交互 Dashboard。
* [ ] **工程效率升级 (异步并发)**：针对批量扫盘脚本引入异步协程，将数百只股票的并行扫盘时间压缩至几分钟内。
* [ ] **认知架构升级 (交易记忆与反思)**：为 Agent 引入“短期记忆”。在新的决策循环中传入前一交易日的判断逻辑，让模型在面对暴涨暴跌时产生“反思纠错机制”。
* [ ] **自动回测闭环**：通过轻量级脚本自动读取历史 `Daily Table.csv` 决策日志，进行胜率与盈亏比的自动回测归因。

## ⚠️ 免责声明

本项目及代码仅供学习、技术研究与探讨 AI 在金融量化领域的应用。系统生成的任何输出（包括但不限于建议仓位、买入/卖出方向、目标价格等）**均不构成任何投资建议**。金融市场具有极高的风险，使用者需对自身账户的交易决策及盈亏负完全责任。