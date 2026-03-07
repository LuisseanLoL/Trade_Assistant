# -*- coding: utf-8 -*-
import os
import random
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
import json_repair

# 导入新闻爬虫模块
from src.news_crawler import get_news_titles

def get_logical_date():
    """获取逻辑日期（凌晨算作前一天）"""
    now = datetime.now()
    if now.hour < 9: return (now - timedelta(days=1)).date()
    return now.date()

def fetch_news_safely(symbol, stock_name, current_date_str):
    """安全地并发获取新闻"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(get_news_titles, symbol=symbol, stock_name=stock_name, max_news=20, save_txt=True, current_date=current_date_str)
        return future.result()

def get_all_output_dates():
    """获取所有已输出过分析结果的日期文件夹"""
    dates = []
    if os.path.exists("output"):
        for folder_name in os.listdir("output"):
            if os.path.isdir(os.path.join("output", folder_name)) and os.path.exists(os.path.join("output", folder_name, f"Daily Table_{folder_name}.csv")):
                dates.append(folder_name)
    return sorted(dates, reverse=True)

def load_daily_table_by_date(date_str):
    """按日期加载当日的策略表并排序"""
    file_path = f"output/{date_str}/Daily Table_{date_str}.csv"
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path, dtype=str, on_bad_lines='skip')
            df['_conf_val'] = df['置信度'].str.replace('%', '', regex=False).astype(float).fillna(0)
            df['_action_rank'] = df['操作'].apply(lambda x: 0 if str(x).strip() == '买入' else 1)
            # 买入优先，同操作置信度高的优先
            df = df.sort_values(by=['_action_rank', '_conf_val'], ascending=[True, False]).drop(columns=['_conf_val', '_action_rank'])
            df['详情'] = '查看'
            return df.to_dict('records')
        except: pass
    return []

def get_random_unprocessed_stock():
    """获取一个今天还未分析过的随机股票代码"""
    current_date_str = get_logical_date().strftime("%Y-%m-%d")
    csv_path = '主板股票代码.csv'
    if not os.path.exists(csv_path): return None, "未找到 '主板股票代码.csv'"
    try:
        df = pd.read_csv(csv_path, dtype=str)
        all_codes = df['股票代码'].astype(str).str.strip().tolist()
        daily_table_path = f"output/{current_date_str}/Daily Table_{current_date_str}.csv"
        processed_codes = set(pd.read_csv(daily_table_path, dtype={'股票代码': str})['股票代码'].astype(str).str.strip()) if os.path.exists(daily_table_path) else set()
        unprocessed_codes = [c for c in all_codes if c not in processed_codes]
        if not unprocessed_codes: return None, "今日全部股票已分析完毕"
        return random.choice(unprocessed_codes), None
    except Exception as e: return None, str(e)

def parse_llm_json(result_text):
    """从 LLM 的回复中安全提取 JSON 配置"""
    res = {"action": "-", "expectation": "-", "pos_adv": "-", "confidence": "-", "buy_p": "-", "sell_p": "-", "stop_p": "-", "reasoning": result_text}
    try:
        c_text = result_text.replace("“", '"').replace("”", '"')
        s_idx, e_idx = c_text.find('{'), c_text.rfind('}')
        if s_idx != -1 and e_idx != -1:
            parsed = json_repair.loads(c_text[s_idx : e_idx + 1])
            res.update({
                "action": parsed.get("操作", "-"), "expectation": parsed.get("预期", "-"), "pos_adv": f"{parsed.get('建议仓位', 0)}%",
                "confidence": f"{parsed.get('置信度', 0) * 100:.0f}%", "buy_p": parsed.get('建议买入价'), "sell_p": parsed.get('目标卖出价'),
                "stop_p": parsed.get('建议止损价'), "reasoning": parsed.get('原因', '暂无深度逻辑')
            })
    except: pass
    return res