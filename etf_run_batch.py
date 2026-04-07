# -*- coding: utf-8 -*-
import logging

# 屏蔽底层 HTTP 库的日志，防止并发时终端被请求信息刷屏
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("google").setLevel(logging.WARNING)
logging.getLogger("google.genai").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)

import os
import random
import glob
import pandas as pd
import baostock as bs
import questionary
from questionary import Choice
from dotenv import load_dotenv
from datetime import datetime, timedelta

# 🌟 引入 ETF 核心引擎
from src.etf_core_analyzer import run_etf_core_analysis
from src.LLM_chat import get_model_config

# 强制加载根目录的 .env 文件
load_dotenv()

# 🎯 针对 ETF 市场特色精选的默认参会大师
DEFAULT_AGENT_NAMES = [
    "Ray_Dalio_ETF", "Richard_Wyckoff_ETF", "William_ONeil_ETF", 
    "Paul_Tudor_Jones_ETF", "Howard_Marks_ETF"
]

# 🎯 内置的核心 ETF 观测池
ETF_DICT = {
    "纳指": ("513100", 2013), "创业板": ("159915", 2011), "中证1000": ("512100", 2016),
    "黄金": ("518880", 2013), "豆粕": ("159985", 2019), "可转债": ("511380", 2020),
    "上证指数": ("510210", 2011), "沪深300": ("510300", 2012), "上证50": ("510050", 2005), 
    "中证500": ("510500", 2013), "中证2000": ("159531", 2023), "科创50": ("588000", 2020), 
    "恒生科技": ("513180", 2021), "标普500": ("513500", 2014), "日经225": ("513520", 2019), 
    "印度基金": ("164824", 2018), "中韩半导": ("513310", 2022), "德国DAX": ("513030", 2014),
    "华宝油气LOF": ("162411", 2012), "有色ETF": ("159980", 2019),
    "红利ETF": ("510880", 2007), "易方达原油LOF": ("161129", 2017),
    "国债ETF": ("511010", 2013), "中概互联": ("513050", 2017),
    "证券": ("512880", 2016), "半导体": ("512480", 2019), "基建": ("516970", 2021), 
    "基础化工": ("516020", 2021), "食品饮料": ("515170", 2021), "银行": ("512800", 2017), 
    "旅游": ("159766", 2021), "消费50": ("515650", 2019), "军工": ("512660", 2016), 
    "传媒": ("512980", 2018), "游戏": ("159869", 2021), "通信": ("515880", 2019), 
    "软件": ("159852", 2021), "农林牧渔": ("159825", 2020), "医药生物": ("512010", 2013), 
    "电子": ("515260", 2020), "计算机": ("512720", 2019), "有色金属": ("512400", 2017), 
    "工程机械": ("560280", 2023), "汽车": ("516110", 2021), "建筑材料": ("159745", 2021), 
    "房地产": ("512200", 2017), "煤炭": ("515220", 2020), "钢铁": ("515210", 2020), 
    "白酒": ("512690", 2019), "家用电器": ("159996", 2020), "交通运输": ("159662", 2022), 
    "电池ETF": ("159755", 2021), "电力ETF": ("159611", 2022), "石油石化": ("561360", 2023),
}

# 建立反向映射，方便打印时显示中文名称
CODE_TO_NAME = {v[0]: k for k, v in ETF_DICT.items()}

def load_portfolio(csv_path="portfolio_etf.csv"):
    """
    读取持仓清单，返回字典格式
    """
    portfolio = {}
    if os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path, dtype={'ETF代码': str, '股票代码': str})
            # 兼容列名
            code_col = 'ETF代码' if 'ETF代码' in df.columns else '股票代码'
            for _, row in df.iterrows():
                code = str(row[code_col]).strip()
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
    """动态获取 ETF_agents 目录下的所有 Agent 角色"""
    agent_files = glob.glob("src/agents_text/ETF_agents/*.txt")
    options = []
    for f in agent_files:
        name = os.path.basename(f).replace(".txt", "")
        is_default = name in DEFAULT_AGENT_NAMES
        options.append(Choice(name.replace("_", " "), value=name, checked=is_default))
    return sorted(options, key=lambda x: x.title)

def main():
    current_date = get_logical_date()
    current_date_str = current_date.strftime("%Y-%m-%d")

    print("\n" + "="*50)
    print("🤖 ETF AI Quant Agent - 批量分析启动终端")
    print("="*50 + "\n")

    batch_mode = questionary.select(
        "【步骤 1】请选择批量分析模式：",
        choices=[
            Choice("1. 跑满核心 ETF 池 (自动剔除近期已分析标的)", value="all"),
            Choice("2. 随机抽盲盒 (从核心池自动抽取 N 只未分析标的)", value="random"),
            Choice("3. 指定 ETF 代码 (手动输入)", value="specific")
        ],
        pointer="👉"
    ).ask()

    if batch_mode is None:
        print("👋 已取消运行。")
        return

    SAMPLE_SIZE = len(ETF_DICT)
    SPECIFIC_BATCH = [] 

    if batch_mode == "random":
        size_input = questionary.text("请输入需要随机抽取的 ETF 数量:", default="5").ask()
        if size_input is None: return
        if size_input.strip().isdigit():
            SAMPLE_SIZE = int(size_input.strip())
    elif batch_mode == "specific":
        stocks_input = ""
        while not stocks_input:
            stocks_input = questionary.text("请输入 ETF 代码 (多个代码用空格分隔):").ask()
            if stocks_input is None: return
            stocks_input = stocks_input.strip()
            if not stocks_input:
                print("⚠️ ETF代码不能为空，请重新输入！")
        SPECIFIC_BATCH = stocks_input.split()

    model_configs = get_model_config()
    if not model_configs:
        print("\n❌ 未检测到可用的模型配置，请检查 .env 文件中的 ACTIVE_MODELS 字段！")
        return

    model_choices = [Choice(cfg['name'], value=mid) for mid, cfg in model_configs.items()]

    flash_model = questionary.select(
        "【步骤 2】请选择【第一阶段：初筛模型】(用于快速扫盘过滤)：",
        choices=model_choices,
        pointer="👉"
    ).ask()
    if flash_model is None: return

    use_pro = questionary.confirm(
        "【步骤 3】是否启用【第二阶段：高级终审】？\n(仅当初筛给出买卖信号，或当前有持仓时才触发)", 
        default=True
    ).ask()
    if use_pro is None: return

    pro_model = flash_model
    committee_model = flash_model 
    dual_filter = False 
    use_moa = False
    committee_agents = []

    if use_pro:
        dual_filter = questionary.confirm(
            "【步骤 3.1】是否启用【双重筛选过滤】？\n(让基础模型先过一遍，只有明确交易方向才惊动终审模型/大师)", 
            default=True
        ).ask()
        if dual_filter is None: return

        use_moa = questionary.confirm(
            "【步骤 3.2】终审机制：是否升级为【MoA 多大师联合议事】？", 
            default=True
        ).ask()
        if use_moa is None: return

        if use_moa:
            agent_choices = get_agent_options()
            if not agent_choices:
                print("⚠️ 未在 src/agents_text/ETF_agents/ 目录下检测到大师文件，退化为单模型模式。")
                use_moa = False
            else:
                committee_agents = questionary.checkbox(
                    "请选择【MoA：参会大师 (Agent 角色)】：",
                    choices=agent_choices
                ).ask()
                
                if committee_agents is None: return
                if not committee_agents:
                    print("⚠️ 未选择任何大师，退化为单模型模式。")
                    use_moa = False
            
            if use_moa:
                committee_model = questionary.select(
                    "请选择【MoA：大师扮演模型】(并发处理大文件，如 Kimi/Qwen)：",
                    choices=model_choices,
                    pointer="👉"
                ).ask()
                if committee_model is None: return

                pro_model = questionary.select(
                    "请选择【MoA：最终拍板裁判】(投资总监，建议 DeepSeek-R1)：",
                    choices=model_choices,
                    pointer="👉"
                ).ask()
                if pro_model is None: return
        
        if not use_moa:
            pro_model = questionary.select(
                "请选择【单发：终审高级模型】：",
                choices=model_choices,
                pointer="👉"
            ).ask()
            if pro_model is None: return

    print("\n" + "="*50 + "\n")

    # ==========================================
    # 3. 构建待分析的 ETF 池
    # ==========================================
    all_codes = list(CODE_TO_NAME.keys())
    
    if batch_mode in ["all", "random"]:
        processed_codes = set()
        days_to_check = 7
        
        print(f"🔍 正在扫描 output_etf/ 目录下近 {days_to_check} 天的记录，剔除近期已处理标的...")
        for i in range(days_to_check):
            check_date = current_date - timedelta(days=i)
            check_date_str = check_date.strftime("%Y-%m-%d")
            # 适配 ETF 专属的输出文件命名
            daily_table_path = f"output_etf/{check_date_str}/ETF_Daily_Table_{check_date_str}.csv"
            
            if os.path.exists(daily_table_path):
                try:
                    df_daily = pd.read_csv(daily_table_path, dtype={'ETF代码': str})
                    codes_in_file = set(df_daily['ETF代码'].astype(str).str.strip())
                    processed_codes.update(codes_in_file)
                except Exception as e:
                    pass
        
        if processed_codes:
            print(f"✅ 成功排除了 {len(processed_codes)} 只近期已处理的 ETF。")
        
        all_codes = [c for c in all_codes if c not in processed_codes]

        if not all_codes:
            print("⚠️ 核心池中的所有 ETF 在近期均已分析完毕！歇一会儿吧！")
            return

        if batch_mode == "random":
            actual_sample_size = min(SAMPLE_SIZE, len(all_codes))
            batch = random.sample(all_codes, actual_sample_size)
            print(f"🎲 成功从剩余未处理池中随机抽取了 {actual_sample_size} 只 ETF。")
        else:
            batch = all_codes
            print(f"🎯 准备分析剩余未处理的全部 {len(batch)} 只 ETF。")
    else:
        batch = SPECIFIC_BATCH
        print("🎯 将分析您手动输入的特定 ETF 代码。")

    portfolio_data = load_portfolio("portfolio_etf.csv")

    print(f"\n=== 🚀 开始 ETF 量化批量任务 ===")
    print(f"逻辑归属日期: {current_date_str}")
    print(f"最终待分析池 ({len(batch)}只):")
    for b_code in batch:
        print(f"  - {CODE_TO_NAME.get(b_code, '未知ETF')} ({b_code})")
    print("===================================\n")

    # ==========================================
    # 4. 循环执行区 
    # ==========================================
    success_count = 0
    skip_count = 0

    print("🔌 正在建立 Baostock 全局数据连接 (部分底层数据依赖)...")
    bs.login()

    try:
        for code in batch:
            etf_name = CODE_TO_NAME.get(code, "ETF")
            pos_info = portfolio_data.get(code, {'position': 0.0, 'cost': 0.0})
            current_position = pos_info['position']
            current_cost = pos_info['cost']

            print(f">>> 开始处理: {etf_name} ({code}) | 持仓: {current_position} 份 | 成本: {current_cost} 元")
            
            try:
                # 适配 run_etf_core_analysis 的 8 个解包参数
                df_chart, s_name, s_price, parsed, disp_model, user_msg, res_text, deep_text = run_etf_core_analysis(
                    etf_code=code,
                    position=current_position,
                    cost=current_cost,
                    current_date_str=current_date_str,
                    flash_model=flash_model,
                    use_pro=use_pro,
                    pro_model=pro_model,
                    dual_filter=dual_filter,
                    use_moa=use_moa,
                    committee_agents=committee_agents,
                    committee_model=committee_model,
                    set_progress=print
                )

                if not parsed:
                    print(f"⚠️ 已跳过 {etf_name} ({code})（初筛无价值或数据获取失败）。\n")
                    skip_count += 1
                else:
                    print(f"✅ {etf_name} ({code}) 分析落库成功。\n")
                    success_count += 1

            except Exception as e:
                print(f"❌ 处理 {etf_name} ({code}) 时发生严重异常: {e}\n")
                skip_count += 1

    finally:
        bs.logout()
        print("🔌 数据连接已断开。")

    print(f"--- 🏁 ETF 批量处理完毕！成功: {success_count} 只，跳过/失败: {skip_count} 只 ---")

if __name__ == "__main__":
    main()