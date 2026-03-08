# 🤖 AI Trade Assistant

**AI Trade Assistant** 是一个基于大型语言模型（LLM）的自动化量化投研与决策支持系统，全面支持 A 股个股与 ETF 基金的深度解析。本项目将**经典高阶量化指标、财务数据/F10基金概况与大模型的深度逻辑推理能力**完美结合，并通过现代化的 Web UI 面板为交易者提供直观的决策参考。

通过 **“漏斗式双模型架构”** 与 **“多模型议事委员会 (MoA)”**，本系统在保证极高质量研判的同时，兼顾了 API 调用成本与多维度的逻辑碰撞。

## ✨ 核心特性

* 🖥️ **全景可视化研判面板 (Web Dashboard)**：基于 Dash & Plotly 构建的交互式终端。内置高级交互式 K 线图（支持动态切换 MA/BOLL 主图指标，以及 MACD/KDJ/RSI 副图指标）。一屏整合走势图、宏观大盘环境、核心指标、量化信号矩阵、最新新闻面以及 AI 深度逻辑推演。
* 🧠 **多模型议事与双筛架构 (MoA & Cost-Effective Routing)**：
  * **双模型漏斗**：支持“初筛+精决”过滤。使用免费/低成本基础模型进行海量粗筛，仅触发关键信号或持有持仓时唤醒高级模型，**成本直降 80%+**。
  * **AI 裁判委员会 (MoA)**：支持将同一份数据并发分发给多个顶级研究员模型（如 Gemini, Qwen），并由“投资总监（AI裁判）”进行交叉质证与最终拍板，有效消除单一模型的逻辑盲区与幻觉。
* 📈 **全市场 ETF 深度解析 (ETF Assistant)**：内置独立的 ETF 决策模块。深度解析 Mootdx F10 数据，自动透视基金份额变动、持有人结构、资产配置与重仓股明细，结合专属量化指标给出操作建议。
* 📊 **硬核量化与基本面特征融合**：自动抓取并计算 52 周极值、PE/PB 历史分位、动量效应，以及高阶统计套利指标（Hurst 指数、偏度、峰度）。个股深度整合新浪财务数据与同花顺机构业绩预测。
* 🌍 **宏观风控与动态护城河**：自动拉取上证指数趋势与每日“宏观财经早餐”。让 Agent 具备全局视野，在系统性风险发生时自动规避盲目抄底。
* 💼 **智能持仓与动态目标追踪**：UI 集成持仓股数与成本录入。AI 决策严格结合您的实际仓位，并在面板中自动计算、高亮显示目标价与止损价相对于买入成本的**动态百分比收益率**。

---

## 🚀 快速开始

### 1. 环境准备与依赖安装 (推荐使用 Conda)

强烈推荐使用 **Miniforge** 或 **Anaconda** 来管理项目的 Python 环境，以避免繁杂的依赖冲突。建议创建一个名为 `agent` 的专属环境：

```bash
# 1. 创建并激活名为 agent 的环境 (推荐 Python 3.9+)
conda create -n agent python=3.12
conda activate agent

# 2. 安装核心依赖
pip install dash dash-bootstrap-components plotly pandas numpy baostock akshare beautifulsoup4 python-dotenv json-repair google-genai openai efinance questionary mootdx

# 3. 安装 Playwright 及内置浏览器（用于新闻无头抓取）
pip install playwright
playwright install chromium

```

### 2. 模型与密钥配置 (.env)

本项目采用**动态解耦配置**，支持通过 OpenAI 兼容格式无缝接入任何本地开源模型（配合 LM Studio / Ollama）以及云端商业模型。

在项目根目录创建或修改 `.env` 文件。您可以随意配置您拥有的 API 资源，系统将自动在 Web 端生成对应的模型下拉框。

**配置示例：**

```env
# ==========================================
# 模型注册表 (这里用下划线作为唯一ID)
# ==========================================
ACTIVE_MODELS="gemini_flash,gemini_pro,qwen_9b"

# --- 1. Gemini Flash ---
gemini_flash_TYPE="gemini"
gemini_flash_NAME="gemini-3.1-flash" 
gemini_flash_MODEL="gemini-3.1-flash-lite-preview"
gemini_flash_API_KEY="你的_gemini_api_key"

# --- 2. Gemini Pro ---
gemini_pro_TYPE="gemini"
gemini_pro_NAME="gemini-3.1-pro"
gemini_pro_MODEL="gemini-3.1-pro-preview"
gemini_pro_API_KEY="你的_gemini_api_key"

# --- 3. 本地开源模型示例 ---
qwen_9b_TYPE="openai"
qwen_9b_NAME="qwen3.5-9b"
qwen_9b_MODEL="qwen/qwen3.5-9b"
qwen_9b_API_KEY="lm-studio"
qwen_9b_BASE_URL="http://localhost:1234/v1"

```

### 3. 一键启动项目

确保在终端中激活了 `agent` 虚拟环境，您可以根据需求启动不同的分析模块：

```bash
conda activate agent

# 1. 启动 A股个股交互式面板 (主程序)
# 启动后在浏览器访问 [http://127.0.0.1:8050](http://127.0.0.1:8050)
python app.py

# 2. 启动 ETF 基金专属决策面板 (新增)
# 自动抓取 ETF 份额与重仓明细，在浏览器访问 [http://127.0.0.1:8051](http://127.0.0.1:8051)
python etf_app.py

# 3. 启动批量分析终端 (适合盘后批量扫盘)
# 拥有交互式命令行菜单，结果将汇总至每日的 Daily Table.csv 中
python run_batch.py

# 4. 运行大模型竞技场 (并发测试多个模型的纯逻辑表现)
python model_arena.py

```

---

## 🗺️ 未来演进路线 (Roadmap)

* [x] **交互式终端升级**：从纯 CLI 脚本升级为现代化的可交互 Dashboard，并引入高级 K 线技术图表。
* [x] **大模型竞技场与 MoA 架构**：引入多模型进行自动化对比，支持委员会并发议事与裁判最终拍板。
* [x] **ETF 市场全覆盖**：打通 F10 数据，实现指数与行业 ETF 的自动化投研。
* [ ] **工程效率升级 (异步并发)**：针对批量扫盘脚本引入异步协程，将数百只股票的并行扫盘时间压缩至几分钟内。
* [ ] **认知架构升级 (交易记忆与反思)**：为 Agent 引入“短期记忆”。在新的决策循环中传入前一交易日的判断逻辑，让模型在面对暴涨暴跌时产生“反思纠错机制”。
* [ ] **自动回测闭环**：通过轻量级脚本自动读取历史决策日志，进行胜率与盈亏比的自动回测归因。

## ⚠️ 免责声明

本项目及代码仅供学习、技术研究与探讨 AI 在金融量化领域的应用。系统生成的任何输出（包括但不限于建议仓位、买入/卖出方向、目标价格等）**均不构成任何投资建议**。金融市场具有极高的风险，使用者需对自身账户的交易决策及盈亏负完全责任。