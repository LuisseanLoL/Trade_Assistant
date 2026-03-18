# -*- coding: utf-8 -*-
import os
import random
import glob
import pandas as pd
import questionary
from questionary import Choice
from dotenv import load_dotenv
from datetime import datetime, timedelta

# 🌟 直接引入核心引擎，彻底抛弃 worker.py
from src.core_analyzer import run_core_analysis
from src.LLM_chat import get_model_config

# 在所有逻辑开始前，强制加载根目录的 .env 文件
load_dotenv()

# ⚠️ 注意：如果你在国内环境运行，且需要访问外部 API，请确保正确配置了系统环境变量中的 HTTP_PROXY 和 HTTPS_PROXY。
# os.environ["HTTP_PROXY"] = "http://127.0.0.1:10809"
# os.environ["HTTPS_PROXY"] = "http://127.0.0.1:10809"

# 🎯 针对 A 股市场特色精选的 11 位默认参会大师
DEFAULT_AGENT_NAMES = [
    "A_Share_Hot_Money", "Richard_Wyckoff", "Jesse_Livermore", 
    "William_O'Neil", "Peter_Lynch", "George_Soros", "Howard_Marks",
    "Ray_Dalio", "Paul_Tudor_Jones", "Jim_Simons", "Charlie_Munger",
]

def load_portfolio(csv_path="portfolio.csv"):
    """
    读取持仓清单，返回字典格式：
    {'001914': {'position': 400, 'cost': 10.68}, ...}
    """
    portfolio = {}
    if os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path, dtype={'股票代码': str})
            for _, row in df.iterrows():
                code = str(row['股票代码']).strip()
                portfolio[code] = {
                    'position': float(row.get('持仓数量', 0)),
                    'cost': float(row.get('持仓成本', 0))
                }
            print(f"📦 成功加载本地持仓文件 '{csv_path}'，已识别 {len(portfolio)} 个标的持仓。")
        except Exception as e:
            print(f"⚠️ 读取持仓文件失败: {e}。将默认按空仓(0持仓)处理。")
    else:
        print(f"ℹ️ 未检测到本地持仓文件 '{csv_path}'。将默认按全量空仓(0持仓)处理。")
    return portfolio

def get_logical_date():
    """获取逻辑交易日：每天早上 9:00 以前，均归属于前一天的盘后分析"""
    now = datetime.now()
    if now.hour < 9:
        return (now - timedelta(days=1)).date()
    return now.date()

def get_agent_options():
    """动态获取 agents_text 目录下的所有 Agent 角色，并高亮/勾选默认大师"""
    agent_files = glob.glob("src/agents_text/*.txt")
    options = []
    for f in agent_files:
        name = os.path.basename(f).replace(".txt", "")
        
        # 判断当前大师是否在默认名单中
        is_default = name in DEFAULT_AGENT_NAMES
        
        # 使用 checked=is_default 来初始化勾选状态
        options.append(Choice(name.replace("_", " "), value=name, checked=is_default))
        
    return sorted(options, key=lambda x: x.title)

def main():
    # ==========================================
    # 1. 日期计算区 (使用逻辑交易日)
    # ==========================================
    current_date = get_logical_date()
    current_date_str = current_date.strftime("%Y-%m-%d")

    # ==========================================
    # 2. 交互式配置参数区 
    # ==========================================
    print("\n" + "="*50)
    print("🤖 AI Quant Agent - 批量分析启动终端")
    print("="*50 + "\n")

    is_random_mode = questionary.select(
        "【步骤 1】请选择批量选股模式：",
        choices=[
            Choice("1. 指定股票池 (手动输入需要分析的代码)", value=False),
            Choice("2. 随机抽盲盒 (从全市场自动抽取未分析标的)", value=True)
        ],
        pointer="👉"
    ).ask()

    if is_random_mode is None:
        print("👋 已取消运行。")
        return

    USE_RANDOM_BATCH = is_random_mode
    RANDOM_CSV_PATH = '主板股票代码.csv'  
    COLUMN_NAME = '股票代码'            
    SAMPLE_SIZE = 1                    
    SPECIFIC_BATCH = [] 

    if USE_RANDOM_BATCH:
        size_input = questionary.text("请输入需要随机抽取的股票数量:", default="5").ask()
        if size_input is None: return
        if size_input.strip().isdigit():
            SAMPLE_SIZE = int(size_input.strip())
    else:
        stocks_input = ""
        while not stocks_input:
            stocks_input = questionary.text("请输入股票代码 (多个代码用空格分隔):").ask()
            if stocks_input is None: return
            stocks_input = stocks_input.strip()
            if not stocks_input:
                print("⚠️ 股票代码不能为空，请重新输入！")
        SPECIFIC_BATCH = stocks_input.split()

    # --- 2.2 动态加载模型并配置双筛漏斗与 MoA 议事 ---
    model_configs = get_model_config()
    if not model_configs:
        print("\n❌ 未检测到可用的模型配置，请检查 .env 文件中的 ACTIVE_MODELS 字段！")
        return

    model_choices = [Choice(cfg['name'], value=mid) for mid, cfg in model_configs.items()]

    flash_model = questionary.select(
        "【步骤 2】请选择【第一阶段：初筛模型】(用于快速扫盘过滤，过滤无交易价值标的)：",
        choices=model_choices,
        pointer="👉"
    ).ask()
    if flash_model is None: return

    use_pro = questionary.confirm(
        "【步骤 3】是否启用【第二阶段：高级终审】？\n(仅当初筛给出买入/动作信号，或当前有持仓时才触发)", 
        default=True
    ).ask()
    if use_pro is None: return

    # 变量初始化
    pro_model = flash_model
    committee_model = flash_model 
    dual_filter = use_pro 
    use_moa = False
    committee_agents = []

    if use_pro:
        use_moa = questionary.confirm(
            "【步骤 3.1】终审机制：是否升级为【MoA 多大师联合议事】？\n(选No则使用单发大模型复盘)", 
            default=True
        ).ask()
        if use_moa is None: return

        if use_moa:
            agent_choices = get_agent_options()
            if not agent_choices:
                print("⚠️ 未在 agents_text/ 目录下检测到大师人设文件，已自动退化为单模型复盘模式。")
                use_moa = False
            else:
                committee_agents = questionary.checkbox(
                    "请选择【MoA：参会大师 (Agent 角色)】：",
                    choices=agent_choices
                ).ask()
                
                if committee_agents is None: return
                if not committee_agents:
                    print("⚠️ 未选择任何参会大师，已自动退化为单模型复盘模式。")
                    use_moa = False
            
            if use_moa:
                committee_model = questionary.select(
                    "请选择【MoA：议事会模型】(并发扮演上述大师角色)：",
                    choices=model_choices,
                    pointer="👉"
                ).ask()
                if committee_model is None: return

                pro_model = questionary.select(
                    "请选择【MoA：最终拍板裁判】(投资总监，建议用推理最强模型)：",
                    choices=model_choices,
                    pointer="👉"
                ).ask()
                if pro_model is None: return
        
        if not use_moa:
            pro_model = questionary.select(
                "请选择【单发：终审高级模型】(如 DeepSeek 或 Gemini-Pro)：",
                choices=model_choices,
                pointer="👉"
            ).ask()
            if pro_model is None: return

    print("\n" + "="*50 + "\n")

    # ==========================================
    # 3. 构建待分析的股票池 (Batch)
    # ==========================================
    if USE_RANDOM_BATCH:
        if os.path.exists(RANDOM_CSV_PATH):
            try:
                df = pd.read_csv(RANDOM_CSV_PATH, dtype=str)
                all_codes = df[COLUMN_NAME].astype(str).str.strip().tolist()
                
                # 🌟 新增逻辑：往前推 7 天，读取所有已处理过的 Daily Table 记录
                processed_codes = set()
                days_to_check = 7
                
                print(f"\n🔍 正在扫描近 {days_to_check} 天的运行记录，以剔除近期已处理标的...")
                for i in range(days_to_check):
                    check_date = current_date - timedelta(days=i)
                    check_date_str = check_date.strftime("%Y-%m-%d")
                    daily_table_path = f"output/{check_date_str}/Daily Table_{check_date_str}.csv"
                    
                    if os.path.exists(daily_table_path):
                        try:
                            df_daily = pd.read_csv(daily_table_path, dtype={'股票代码': str})
                            codes_in_file = set(df_daily['股票代码'].astype(str).str.strip())
                            processed_codes.update(codes_in_file)
                            # 可选：如果你想看到每一天具体排除了多少个，可以取消下面这行的注释
                            # print(f"  - 找到 {check_date_str} 的记录，包含 {len(codes_in_file)} 只股票")
                        except Exception as e:
                            print(f"⚠️ 读取 {check_date_str} 的 Daily Table 失败: {e}")
                
                if processed_codes:
                    print(f"✅ 成功提取近 {days_to_check} 天的历史记录，共排除了 {len(processed_codes)} 只不重复的股票。")
                else:
                    print("ℹ️ 未发现近期的历史处理记录。")
                
                # 将近一周已处理过的股票从全量股票池中剔除
                all_codes = [c for c in all_codes if c not in processed_codes]

                if not all_codes:
                    print("⚠️ 全量股票池中的所有股票在近期均已处理完毕！")
                    return

                actual_sample_size = min(SAMPLE_SIZE, len(all_codes))
                batch = random.sample(all_codes, actual_sample_size)
                print(f"🎲 [随机模式] 成功从全市场池中抽取了 {actual_sample_size} 只近期未处理过的标的。")
            except Exception as e:
                print(f"❌ 读取随机股票池失败: {e}。自动回退到指定股票池模式。")
                batch = SPECIFIC_BATCH
        else:
            print(f"⚠️ 找不到全量代码文件 '{RANDOM_CSV_PATH}'。自动回退到指定股票池模式。")
            batch = SPECIFIC_BATCH
    else:
        print("🎯 [指定模式] 将分析你手动输入的特定股票。")
        batch = SPECIFIC_BATCH

    portfolio_data = load_portfolio("portfolio.csv")

    print(f"\n=== 🚀 开始量化批量分析任务 ===")
    print(f"逻辑归属日期: {current_date_str}")
    print(f"初筛模型(Actor): {flash_model}")
    if use_pro:
        if use_moa:
            print(f"终审架构: 【漏斗触发 + MoA多大师议事】 | 议事模型: {committee_model} | 参会大师: {committee_agents} | 最终裁判(Judge): {pro_model}")
        else:
            print(f"终审架构: 【漏斗触发 + 单发复盘】 | 决策模型: {pro_model}")
    else:
        print(f"终审架构: 未启用 (仅初筛)")
    print(f"最终待分析股票池 ({len(batch)}只): {batch}")
    print("===================================\n")

    # ==========================================
    # 4. 循环执行区 (🌟 抛弃 worker，直连 core_analyzer)
    # ==========================================
    success_count = 0
    skip_count = 0

    for code in batch:
        pos_info = portfolio_data.get(code, {'position': 0.0, 'cost': 0.0})
        current_position = pos_info['position']
        current_cost = pos_info['cost']

        print(f">>> 开始处理股票: {code} | 当前持仓: {current_position} 股 | 成本: {current_cost} 元")
        
        try:
            # 直接调用核心分析引擎
            df_chart, s_name, s_price, parsed, disp_model, user_msg, res_text = run_core_analysis(
                stock_code=code,
                position=current_position,
                cost=current_cost,
                current_date_str=current_date_str,
                flash_model=flash_model,
                use_pro=use_pro,
                pro_model=pro_model,
                dual_filter=dual_filter,
                use_moa=use_moa,
                committee_agents=committee_agents,
                committee_model=committee_model
            )

            # 解析结果为空，说明触发了过滤/初筛（没有产生实质性交易价值）
            if not parsed:
                print(f"⚠️ 已跳过 {code}（可能由于初筛无交易价值或数据获取失败）。\n")
                skip_count += 1
            else:
                print(f"✅ {code} 处理成功并已落库。\n")
                success_count += 1

        except Exception as e:
            print(f"❌ 处理 {code} 时发生严重异常: {e}\n")
            skip_count += 1

    print(f"--- 🏁 批量处理完毕！成功: {success_count} 只，跳过/失败: {skip_count} 只 ---")

if __name__ == "__main__":
    main()