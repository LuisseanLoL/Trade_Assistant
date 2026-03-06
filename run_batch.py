# -*- coding: utf-8 -*-
import os
import random
import pandas as pd
import questionary
from questionary import Choice  # 引入 Choice 对象进行强绑定
from dotenv import load_dotenv
from datetime import datetime, timedelta
from src.worker import process, SHOULD_SKIP

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
    # 2. 交互式配置参数区 (使用 questionary 上下键菜单)
    # ==========================================
    print("\n" + "="*45)
    print("🤖 AI Quant Agent - 交互式启动菜单")
    print("="*45 + "\n")

    # --- 选择运行模式 ---
    # 使用 Choice 对象，强绑定 UI 文本与后台的 True/False 逻辑
    is_random_mode = questionary.select(
        "【步骤 1】请选择选股模式：",
        choices=[
            Choice("1. 指定股票池 (手动输入需要分析的代码)", value=False),
            Choice("2. 随机抽盲盒 (从全市场自动抽取未分析标的)", value=True)
        ],
        pointer="👉"
    ).ask()

    # 如果用户按 Ctrl+C 退出
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
            default="1"
        ).ask()
        
        if size_input is None:
            print("👋 已取消运行。")
            return
            
        if size_input.strip().isdigit():
            SAMPLE_SIZE = int(size_input.strip())
    else:
        # 取消了默认值，强制要求用户输入代码
        stocks_input = ""
        while not stocks_input:
            stocks_input = questionary.text("请输入股票代码 (多个代码用空格分隔):").ask()
            
            if stocks_input is None:
                print("👋 已取消运行。")
                return
                
            stocks_input = stocks_input.strip()
            if not stocks_input:
                print("⚠️ 股票代码不能为空，请重新输入！")
                
        SPECIFIC_BATCH = stocks_input.split()

    # --- 选择调用模型 ---
    model_choice = questionary.select(
        "【步骤 2】请选择大模型底层架构：",
        choices=[
            Choice("1. Gemini (双模型漏斗架构: Flash粗筛 + Pro精决)", value='gemini'),
            Choice("2. ARK (火山引擎)", value='ark'),
            Choice("3. Local (本地开源模型)", value='local')
        ],
        pointer="👉"
    ).ask()

    if model_choice is None:
        print("👋 已取消运行。")
        return

    print("\n" + "="*45 + "\n")

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
                print(f"🎲 [随机模式] 成功从 '{RANDOM_CSV_PATH}' 中随机抽取了 {actual_sample_size} 只未处理过的股票。")
            except Exception as e:
                print(f"❌ 读取随机股票池失败: {e}。将自动回退到指定股票池模式。")
                batch = SPECIFIC_BATCH
        else:
            print(f"⚠️ 找不到全量代码文件 '{RANDOM_CSV_PATH}'。将自动回退到指定股票池模式。")
            batch = SPECIFIC_BATCH
    else:
        print("🎯 [指定模式] 将分析你手动输入的特定股票。")
        batch = SPECIFIC_BATCH

    portfolio_data = load_portfolio("portfolio.csv")

    print(f"\n=== 🚀 开始量化分析任务 ===")
    print(f"使用的模型: {model_choice.upper()}")
    print(f"逻辑归属日期: {current_date}")
    print(f"最终待分析股票池 ({len(batch)}只): {batch}")
    print("===================================\n")

    # ==========================================
    # 4. 循环执行区
    # ==========================================
    for code in batch:
        pos_info = portfolio_data.get(code, {'position': 0.0, 'cost': 0.0})
        current_position = pos_info['position']
        current_cost = pos_info['cost']

        print(f">>> 开始处理股票: {code} | 当前持仓: {current_position} 股 | 成本: {current_cost} 元")
        
        try:
            processed_result = process(
                stock_code=code,
                stock_position=current_position,
                stock_holding_cost=current_cost,
                beg=beg,
                end=end,
                current_date=current_date,
                model_choice=model_choice
            )

            if processed_result is SHOULD_SKIP:
                print(f"⚠️ 已跳过 {code}。\n")
            else:
                print(f"✅ {code} 处理成功。\n")

        except Exception as e:
            print(f"❌ 处理 {code} 时发生严重异常: {e}\n")

    print("--- 🏁 所有项目处理完毕 ---")

if __name__ == "__main__":
    main()