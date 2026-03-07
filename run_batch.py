# -*- coding: utf-8 -*-
import os
import random
import pandas as pd
import questionary
from questionary import Choice
from dotenv import load_dotenv
from datetime import datetime, timedelta

# 引入核心工作流和最新架构需要的工具
from src.worker import process, SHOULD_SKIP
from src.LLM_chat import get_model_config

# 在所有逻辑开始前，强制加载根目录的 .env 文件
load_dotenv()

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

def main():
    # ==========================================
    # 1. 日期计算区 (使用逻辑交易日)
    # ==========================================
    current_date = get_logical_date()
    end_date = current_date 
    end = end_date.isoformat().replace('-', '')
    start_date = end_date - timedelta(days=365)  
    beg = start_date.isoformat().replace('-', '')

    # ==========================================
    # 2. 交互式配置参数区 
    # ==========================================
    print("\n" + "="*50)
    print("🤖 AI Quant Agent - 批量分析启动终端")
    print("="*50 + "\n")

    # --- 2.1 选择运行模式 ---
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
        size_input = questionary.text(
            "请输入需要随机抽取的股票数量:", 
            default="5"
        ).ask()
        
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

    # --- 2.2 动态加载模型并配置双筛漏斗 ---
    model_configs = get_model_config()
    if not model_configs:
        print("\n❌ 未检测到可用的模型配置，请检查 .env 文件中的 ACTIVE_MODELS 字段！")
        return

    model_choices = [Choice(cfg['name'], value=mid) for mid, cfg in model_configs.items()]

    flash_model = questionary.select(
        "【步骤 2】请选择基础/初筛模型 (Flash Model)：",
        choices=model_choices,
        pointer="👉"
    ).ask()
    if flash_model is None: return

    use_pro = questionary.confirm(
        "【步骤 3】是否启用 Pro 高级模型进行深度测算？", 
        default=True
    ).ask()
    if use_pro is None: return

    pro_model = flash_model
    dual_filter = False

    if use_pro:
        pro_model = questionary.select(
            "【步骤 3.1】请选择高级复核模型 (Pro Model)：",
            choices=model_choices,
            pointer="👉"
        ).ask()
        if pro_model is None: return

        dual_filter = questionary.confirm(
            "【步骤 3.2】是否启用双重筛选 (仅空仓且初筛触发动作时才调用 Pro)(默认为是）？", 
            default=True
        ).ask()
        if dual_filter is None: return

    print("\n" + "="*50 + "\n")

    # ==========================================
    # 3. 构建待分析的股票池 (Batch)
    # ==========================================
    if USE_RANDOM_BATCH:
        if os.path.exists(RANDOM_CSV_PATH):
            try:
                df = pd.read_csv(RANDOM_CSV_PATH, dtype=str)
                all_codes = df[COLUMN_NAME].astype(str).str.strip().tolist()
                
                daily_table_path = f"output/{current_date}/Daily Table_{current_date}.csv"
                processed_codes = set()
                
                if os.path.exists(daily_table_path):
                    try:
                        df_daily = pd.read_csv(daily_table_path, dtype={'股票代码': str})
                        processed_codes = set(df_daily['股票代码'].astype(str).str.strip())
                        print(f"🔍 发现今日({current_date})已处理记录 {len(processed_codes)} 条，将在随机抽取时予以剔除。")
                    except Exception as e:
                        print(f"⚠️ 读取今日 Daily Table 失败: {e}")
                
                all_codes = [c for c in all_codes if c not in processed_codes]

                if not all_codes:
                    print("⚠️ 全量股票池中的所有股票今日均已处理完毕！")
                    return

                actual_sample_size = min(SAMPLE_SIZE, len(all_codes))
                batch = random.sample(all_codes, actual_sample_size)
                print(f"🎲 [随机模式] 成功从全市场池中抽取了 {actual_sample_size} 只未处理过的标的。")
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
    print(f"逻辑归属日期: {current_date}")
    print(f"初筛模型: {flash_model} | 高级模型: {pro_model if use_pro else '未启用'} | 双筛架构: {'启用' if dual_filter else '关闭'}")
    print(f"最终待分析股票池 ({len(batch)}只): {batch}")
    print("===================================\n")

    # ==========================================
    # 4. 循环执行区
    # ==========================================
    success_count = 0
    skip_count = 0

    for code in batch:
        pos_info = portfolio_data.get(code, {'position': 0.0, 'cost': 0.0})
        current_position = pos_info['position']
        current_cost = pos_info['cost']

        print(f">>> 开始处理股票: {code} | 当前持仓: {current_position} 股 | 成本: {current_cost} 元")
        
        try:
            # 调用最新的 worker.process，并传入模型解耦参数
            processed_result = process(
                stock_code=code,
                stock_position=current_position,
                stock_holding_cost=current_cost,
                beg=beg,
                end=end,
                current_date=current_date,
                flash_model=flash_model,
                use_pro=use_pro,
                pro_model=pro_model,
                dual_filter=dual_filter
            )

            if processed_result is SHOULD_SKIP:
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