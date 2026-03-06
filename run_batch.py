# -*- coding: utf-8 -*-
import os
import random
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta
from src.worker import process, SHOULD_SKIP
import warnings
warnings.filterwarnings("ignore")

# 在所有逻辑开始前，强制加载根目录的 .env 文件
load_dotenv()

# ==========================================
# 代理设置 (可选)
# ==========================================
# os.environ["http_proxy"] = "http://127.0.0.1:7890"
# os.environ["https_proxy"] = "http://127.0.0.1:7890"

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
    # 1. 日期计算区 (提前计算日期以便读取对应的 Daily Table)
    # ==========================================
    current_date = get_logical_date()
    end_date = current_date 
    end = end_date.isoformat().replace('-', '')
    start_date = end_date - timedelta(days=365)  
    beg = start_date.isoformat().replace('-', '')

    # ==========================================
    # 2. 配置参数区 (随心切换模式)
    # ==========================================
    USE_RANDOM_BATCH = True   # 设置为 True 则开启随机抽盲盒，False 则使用下方的 SPECIFIC_BATCH
    RANDOM_CSV_PATH = '主板股票代码.csv'  
    COLUMN_NAME = '股票代码'            
    SAMPLE_SIZE = 1
    
    # 指定股票池（当随机模式不可用时的回退选项）                    
    SPECIFIC_BATCH = "001914 600325 002553 600809 603025".split() 
    model_choice = 'gemini'

    # ==========================================
    # 3. 构建待分析的股票池 (Batch)
    # ==========================================
    if USE_RANDOM_BATCH:
        if os.path.exists(RANDOM_CSV_PATH):
            try:
                df = pd.read_csv(RANDOM_CSV_PATH, dtype=str)
                all_codes = df[COLUMN_NAME].astype(str).str.strip().tolist()
                
                # ---------------- 【核心新增：剔除今日已处理的标的】 ----------------
                daily_table_path = f"output/{current_date}/Daily Table_{current_date}.csv"
                processed_codes = set()
                
                if os.path.exists(daily_table_path):
                    try:
                        df_daily = pd.read_csv(daily_table_path, dtype={'股票代码': str})
                        processed_codes = set(df_daily['股票代码'].astype(str).str.strip())
                        print(f"🔍 发现今日已处理记录 {len(processed_codes)} 条，将在随机抽取时予以剔除。")
                    except Exception as e:
                        print(f"⚠️ 读取今日 Daily Table 失败: {e}")
                
                # 过滤掉已经跑过的代码
                all_codes = [c for c in all_codes if c not in processed_codes]
                # ----------------------------------------------------------------

                if not all_codes:
                    print("⚠️ 全量股票池中的所有股票今日均已处理完毕！")
                    return

                actual_sample_size = min(SAMPLE_SIZE, len(all_codes))
                batch = random.sample(all_codes, actual_sample_size)
                print(f"\n🎲 [随机模式已开启] 成功从 '{RANDOM_CSV_PATH}' 中随机抽取了 {actual_sample_size} 只未处理过的股票。")
            except Exception as e:
                print(f"\n❌ 读取随机股票池失败: {e}。将自动回退到指定股票池模式。")
                batch = SPECIFIC_BATCH
        else:
            print(f"\n⚠️ 找不到全量代码文件 '{RANDOM_CSV_PATH}'。将自动回退到指定股票池模式。")
            batch = SPECIFIC_BATCH
    else:
        print("\n🎯 [指定模式已开启] 将分析预设的特定股票。")
        batch = SPECIFIC_BATCH

    # 加载持仓配置
    portfolio_data = load_portfolio("portfolio.csv")

    print(f"\n=== 🚀 启动量化分析 Worker ===")
    print(f"分析模型: {model_choice}")
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
                #移除了 cash 参数，保持与最新的 worker.py 匹配
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