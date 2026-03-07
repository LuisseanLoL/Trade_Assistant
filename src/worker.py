# -*- coding: utf-8 -*-
import baostock as bs
from datetime import datetime, timedelta, time as dt_time
import pandas as pd
import os
import json
import re
import time
import json_repair
from src.data_crawler import get_stock_data
from src.news_crawler import get_news_titles
from src.LLM_chat import get_LLM_message, get_model_config

def get_logical_date():
    """获取逻辑交易日：每天早上 9:00 以前，均归属于前一天的盘后分析"""
    now = datetime.now()
    if now.hour < 9:
        return (now - timedelta(days=1)).date()
    return now.date()

current_date = get_logical_date()
end_date = current_date
end = end_date.isoformat().replace('-', '')
start_date = end_date - timedelta(days=365)
beg = start_date.isoformat().replace('-', '')

SHOULD_SKIP = object() 

def get_bs_code(symbol: str) -> str:
    """将股票代码转换为 baostock 需要的格式"""
    if symbol.startswith('6'): return f"sh.{symbol}"
    elif symbol.startswith('0') or symbol.startswith('3'): return f"sz.{symbol}"
    elif symbol.startswith('8') or symbol.startswith('4'): return f"bj.{symbol}"
    return symbol

def get_stock_name(stock_code: str) -> str:
    bs.login()
    bs_code = get_bs_code(stock_code)
    rs_basic = bs.query_stock_basic(code=bs_code)
    stock_name = "未知名称"
    if rs_basic.error_code == '0' and rs_basic.next():
        stock_name = rs_basic.get_row_data()[1]
    bs.logout()
    return stock_name

def get_baostock_k_data(stock_code: str, beg: str, end: str) -> pd.DataFrame:
    bs.login()
    bs_code = get_bs_code(stock_code)
    bs_start = f"{beg[:4]}-{beg[4:6]}-{beg[6:]}"
    bs_end = f"{end[:4]}-{end[4:6]}-{end[6:]}"
    
    rs = bs.query_history_k_data_plus(
        bs_code, "date,open,high,low,close,volume,amount,turn,pctChg",
        start_date=bs_start, end_date=bs_end, frequency="d", adjustflag="2"
    )
    
    data_list = []
    while (rs.error_code == '0') & rs.next(): data_list.append(rs.get_row_data())
    k_data = pd.DataFrame(data_list, columns=rs.fields)
    
    for col in ["open", "high", "low", "close", "volume", "amount", "turn", "pctChg"]:
        if col in k_data.columns: k_data[col] = pd.to_numeric(k_data[col], errors='coerce')
            
    k_data = k_data.rename(columns={'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'volume': '成交量', 'amount': '成交额', 'turn': '换手率', 'pctChg': '涨跌幅'})
    bs.logout()
    return k_data

# ================= 核心更新：引入解耦参数 =================
def process(stock_code = '600325',
            stock_position = 0,
            stock_holding_cost = 0,
            beg = beg,
            end = end,
            current_date = current_date,
            flash_model='gemini',
            use_pro=True,
            pro_model='gemini',
            dual_filter=True
            ):

    stock_name = get_stock_name(stock_code)
    stock_name = re.sub(r'[\\/*?:"<>|]', '', stock_name)
    logical_today_3pm = datetime.combine(current_date, dt_time(15, 0, 0))

    input_dir = f"input/{current_date}"
    os.makedirs(input_dir, exist_ok=True)
    filename_in = f"{stock_code}_{stock_name}_input_{current_date}.txt"
    filepath_in = os.path.join(input_dir, filename_in)

    def generate_user_message():
        data_input = get_stock_data(stock_code=stock_code, beg=beg, end=end, current_date=current_date)
        k_data = get_baostock_k_data(stock_code, beg=beg, end=end)
        last_30_days = k_data.tail(30)
        pd.set_option('display.max_columns', len(last_30_days.columns))
        last_30_days_str = last_30_days.to_string(index=False)
        news_titles = get_news_titles(symbol=stock_code, stock_name=stock_name, max_news=20, current_date=current_date)

        msg = f"""基于获得的以下数据和新闻消息，做出你的交易决策。\n\n{data_input}\n\n最近三十个交易日数据如下：\n{last_30_days_str}\n\n相关新闻如下：\n{news_titles}\n\n当前该股持仓：{stock_position} 股\n当前持仓成本: {stock_holding_cost} 元\n\n请记住，行动必须是买入、卖出、持有或观望。\n谨慎考虑交易决策：考虑当前股价是高位还是低位，在低位买入，高位卖出。\n考虑自己的持仓成本，在有足够浮盈的情况下考虑卖出收获现金实利。"""
        return msg

    if os.path.isfile(filepath_in):
        creation_time = datetime.fromtimestamp(os.path.getmtime(filepath_in))
        if creation_time < logical_today_3pm:  
            user_message = generate_user_message()
            with open(filepath_in, 'w', encoding='utf-8') as f: f.write(user_message)
        else:
            with open(filepath_in, 'r', encoding='utf-8') as f: user_message = f.read()
    else:
        user_message = generate_user_message()
        with open(filepath_in, 'w', encoding='utf-8') as f: f.write(user_message)

    with open('LLM system content.txt', 'r', encoding='utf-8') as file:
        system_content = file.read()
        
    run_pro = False
    result = ""

    # ================= 模型智能调度执行树 =================
    if use_pro and dual_filter:
        if float(stock_position) > 0:
            print(f"\n💼 [{stock_code}] 真实持仓，跳过初筛，直接触发 Pro 高级模型 ({pro_model})...")
            run_pro = True
        else:
            print(f"\n📡 [{stock_code}] 正在使用基础漏斗 {flash_model} 进行初筛...")
            result = get_LLM_message(system_content=system_content, user_message=user_message, model_choice=flash_model, model_tier='flash')
            try:
                temp_text = result.replace("“", '"').replace("”", '"')
                s_idx, e_idx = temp_text.find('{'), temp_text.rfind('}')
                if s_idx != -1 and e_idx != -1:
                    action = json_repair.loads(temp_text[s_idx : e_idx + 1]).get('操作', '')
                    if action in ['买入', '卖出', '持有']:
                        run_pro = True
                        print(f"🎯 初筛预警：发现疑似【{action}】信号！触发高级模型 ({pro_model}) 复核...")
                    else:
                        print(f"💤 初筛结果：【{action}】，暂无操作价值。")
            except Exception as e:
                 run_pro = True
                 print(f"⚠️ 初筛格式异常，强制触发高级模型 ({pro_model}) 容错...")
                 
    elif use_pro and not dual_filter:
        print(f"\n🎯 [{stock_code}] 模式设定为直接运行 Pro 高级模型 ({pro_model})...")
        run_pro = True
    else:
        print(f"\n📡 [{stock_code}] 模式设定为仅使用基础模型 ({flash_model})...")
        result = get_LLM_message(system_content=system_content, user_message=user_message, model_choice=flash_model, model_tier='flash')

    if run_pro:
         result = get_LLM_message(system_content=system_content, user_message=user_message, model_choice=pro_model, model_tier='pro')
         print("✅ 深度测算与复核完成！")

    output_dir = f"output/{current_date}"
    os.makedirs(output_dir, exist_ok=True)
    
    # 文件打上确切使用的模型标签
    model_tag = f"D-{flash_model}-{pro_model}" if (run_pro and dual_filter) else (pro_model if run_pro else flash_model)
    
    # 动态解析模型名称用于展示和存表
    configs = get_model_config()
    if model_tag.startswith("D-"):
        parts = model_tag.split("-")
        disp_model = f"{configs.get(parts[2], {}).get('name', parts[2])}(双筛)" if len(parts) >= 3 else model_tag
    else:
        disp_model = configs.get(model_tag, {}).get('name', model_tag)

    filename_out = f"{stock_code}_{stock_name}_output_{model_tag}_{current_date}.txt"
    filepath_out = os.path.join(output_dir, filename_out)

    with open(filepath_out, 'w', encoding='utf-8') as f: f.write(result)

    try:
        data_dir = f"log/stock_data/{current_date}"
        filename_data = f"{stock_code}_{stock_name}_data_{current_date}.csv"
        stock_price = pd.read_csv(os.path.join(data_dir, filename_data))['收盘'].iloc[-1]
    except:
        stock_price = 0

    # ================= 整理输出到表格 =================
    try:
        corrected_text = result.replace("“", '"').replace("”", '"')
        data = {}
        s_idx, e_idx = corrected_text.find('{'), corrected_text.rfind('}')
        if s_idx != -1 and e_idx != -1 and e_idx > s_idx:
            data = json_repair.loads(corrected_text[s_idx : e_idx + 1])

        if not data: return SHOULD_SKIP 
        
        parsed_data = data
        reward_risk_ratio_str = 'N/A'
        try:
            b_p, t_p, s_p = parsed_data.get('建议买入价'), parsed_data.get('目标卖出价'), parsed_data.get('建议止损价')
            if b_p and t_p and s_p and (b_p - s_p) > 0: reward_risk_ratio_str = f"{(t_p - b_p) / (b_p - s_p):.2f}:1"
        except: pass

        pos_adv = parsed_data.get('建议仓位')
        pos_str = f"{pos_adv}%" if pos_adv is not None else "N/A"
        conf = parsed_data.get('置信度')
        conf_str = f"{conf * 100:.0f}%" if conf is not None else "N/A"

        final_data = {
            "股票代码": stock_code, "股票名称": stock_name, "决策模型": disp_model, "当前价格": stock_price,
            "预期": parsed_data.get("预期", "N/A"), "操作": parsed_data.get("操作", "N/A"), "建议仓位": pos_str,      
            "置信度": conf_str, "建议买入价": b_p if b_p else "N/A", "目标卖出价": t_p if t_p else "N/A", "建议止损价": s_p if s_p else "N/A", "回报风险比": reward_risk_ratio_str
        }

        file_name = f"Daily Table_{current_date}.csv"
        file_path = os.path.join(output_dir, file_name)
        output_df = pd.DataFrame([final_data])

        for attempt in range(3):
            try:
                if not os.path.exists(file_path):
                    pd.DataFrame({"股票代码": [], "股票名称": [], "决策模型": [], "当前价格": [], "预期": [], "操作": [], "建议仓位": [], "置信度": [], "建议买入价": [], "目标卖出价": [], "建议止损价": [], "回报风险比": []}).to_csv(file_path, index=False, encoding='utf-8-sig') 
                output_df.to_csv(file_path, index=False, header=False, mode='a', encoding='utf-8-sig')
                break 
            except PermissionError as e:
                time.sleep(10)
            except Exception as e:
                break 
    except: return SHOULD_SKIP
    return 0