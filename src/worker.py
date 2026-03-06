# -*- coding: utf-8 -*-
import baostock as bs
from datetime import datetime, timedelta, time as dt_time
import pandas as pd
import os
import json
import re
import time
import textwrap
import json_repair
from src.data_crawler import get_stock_data
from src.news_crawler import get_news_titles
from src.LLM_chat import get_LLM_message

def get_logical_date():
    """获取逻辑交易日：每天早上 9:00 以前，均归属于前一天的盘后分析"""
    now = datetime.now()
    if now.hour < 9:
        return (now - timedelta(days=1)).date()
    return now.date()

# 获取逻辑日期
current_date = get_logical_date()
end_date = current_date
end = end_date.isoformat().replace('-', '')
start_date = end_date - timedelta(days=365)
beg = start_date.isoformat().replace('-', '')

# 定义一个常量或特定的返回值来表示“需要跳过”
SHOULD_SKIP = object() 

# --- 辅助函数 ---
def get_bs_code(symbol: str) -> str:
    """将股票代码转换为 baostock 需要的格式"""
    if symbol.startswith('6'): return f"sh.{symbol}"
    elif symbol.startswith('0') or symbol.startswith('3'): return f"sz.{symbol}"
    elif symbol.startswith('8') or symbol.startswith('4'): return f"bj.{symbol}"
    return symbol

def get_stock_name(stock_code: str) -> str:
    """获取股票名称"""
    bs.login()
    bs_code = get_bs_code(stock_code)
    rs_basic = bs.query_stock_basic(code=bs_code)
    stock_name = "未知名称"
    if rs_basic.error_code == '0' and rs_basic.next():
        stock_name = rs_basic.get_row_data()[1]
    bs.logout()
    return stock_name

def get_baostock_k_data(stock_code: str, beg: str, end: str) -> pd.DataFrame:
    """使用 Baostock 获取历史 K 线数据并翻译为中文列名"""
    bs.login()
    bs_code = get_bs_code(stock_code)
    bs_start = f"{beg[:4]}-{beg[4:6]}-{beg[6:]}"
    bs_end = f"{end[:4]}-{end[4:6]}-{end[6:]}"
    
    rs = bs.query_history_k_data_plus(
        bs_code,
        "date,open,high,low,close,volume,amount,turn,pctChg",
        start_date=bs_start, end_date=bs_end,
        frequency="d", adjustflag="2"
    )
    
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
        
    k_data = pd.DataFrame(data_list, columns=rs.fields)
    
    for col in ["open", "high", "low", "close", "volume", "amount", "turn", "pctChg"]:
        if col in k_data.columns:
            k_data[col] = pd.to_numeric(k_data[col], errors='coerce')
            
    k_data = k_data.rename(columns={
        'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低',
        'close': '收盘', 'volume': '成交量', 'amount': '成交额',
        'turn': '换手率', 'pctChg': '涨跌幅'
    })
    
    bs.logout()
    return k_data

def process(stock_code = '600325',
            stock_position = 0,
            stock_holding_cost = 0,
            beg = beg,
            end = end,
            current_date = current_date,
            model_choice='gemini'
            ):

    stock_name = get_stock_name(stock_code)
    stock_name = re.sub(r'[\\/*?:"<>|]', '', stock_name)  # 移除所有非法字符
    
    # 获取当前逻辑日期的下午三点，用于判断是否使用旧缓存
    logical_today_3pm = datetime.combine(current_date, dt_time(15, 0, 0))

    # 创建输入文件存储目录
    input_dir = f"input/{current_date}"
    os.makedirs(input_dir, exist_ok=True)
    filename_in = f"{stock_code}_{stock_name}_input_{current_date}.txt"
    filepath_in = os.path.join(input_dir, filename_in)

    # 抽取公共的 prompt 拼装函数，让代码更加整洁
    def generate_user_message():
        data_input = get_stock_data(stock_code=stock_code, beg=beg, end=end, current_date=current_date)
        k_data = get_baostock_k_data(stock_code, beg=beg, end=end)
        last_30_days = k_data.tail(30)
        pd.set_option('display.max_columns', len(last_30_days.columns))
        last_30_days_str = last_30_days.to_string(index=False)
        news_titles = get_news_titles(symbol=stock_code, stock_name=stock_name, max_news=20, current_date=current_date)

        if news_titles:
            print("\n获取到的新闻标题: ")
            print(news_titles)
        else:
            print("未获取到有效标题")

        msg = f"""基于获得的以下数据和新闻消息，做出你的交易决策。

        {data_input}

        最近三十个交易日数据如下：
        {last_30_days_str}

        相关新闻如下：
        {news_titles}

        当前持仓如下：
        投资组合：
        当前持仓：{stock_position} 股
        当前持仓成本: {stock_holding_cost} 元

        请记住，行动必须是买入、卖出、持有或观望。
        谨慎考虑交易决策：考虑当前股价是高位还是低位，在低位买入，高位卖出。
        考虑自己的持仓成本，在有足够浮盈的情况下考虑卖出收获现金实利。"""
        return msg

    # 1. 检查并生成 user_message
    if os.path.isfile(filepath_in):
        creation_timestamp = os.path.getmtime(filepath_in)
        creation_time = datetime.fromtimestamp(creation_timestamp)

        # 输入文件创建时间早于逻辑时间下午三点时，重新拉取最新数据
        if creation_time < logical_today_3pm:  
            print(f'已有输入文件创建时间({creation_time})早于逻辑节点({logical_today_3pm})，重新获取最新数据并生成文件') 
            user_message = generate_user_message()
            with open(filepath_in, 'w', encoding='utf-8') as f:
                f.write(user_message)
            print(f"输入文件已保存至: {filename_in}")
        else:
            with open(filepath_in, 'r', encoding='utf-8') as f:
                user_message = f.read()
            print(f"使用今日已有输入文件: {filename_in}")
    else:
        print('今日输入文件不存在，正在抓取数据创建...')
        user_message = generate_user_message()
        with open(filepath_in, 'w', encoding='utf-8') as f:
            f.write(user_message)
        print(f"输入文件已保存至: {filename_in}")

    with open('LLM system content.txt', 'r', encoding='utf-8') as file:
        system_content = file.read()
        
    need_pro = False
    result = ""

    # ================= 核心逻辑：智能模型调度 =================
    if float(stock_position) > 0:
        # 如果是真实持仓：直接绕过 Flash，直达 Pro 模型
        print(f"\n💼 [{stock_code}] 检测到真实持仓 ({stock_position} 股)！跳过初筛，直接触发 Pro 高级模型进行深度诊断...")
        need_pro = True
    else:
        # 如果不是持仓股：先用免费模型粗筛
        print(f"\n📡 [{stock_code}] 正在使用 {model_choice} (Flash Lite/免费初筛) 进行全盘扫描...")
        result = get_LLM_message(system_content=system_content, user_message=user_message, model_choice=model_choice, model_tier='flash')
        
        try:
            # 解析 Flash 初筛结果
            temp_text = result.replace("“", '"').replace("”", '"')
            s_idx = temp_text.find('{')
            e_idx = temp_text.rfind('}')
            if s_idx != -1 and e_idx != -1:
                temp_data = json_repair.loads(temp_text[s_idx : e_idx + 1])
                action = temp_data.get('操作', '')
                
                # 若 Flash 发现了潜在机会，激活 Pro 复核
                if action in ['买入', '卖出', '持有']:
                    need_pro = True
                    print(f"🎯 初筛预警：发现疑似【{action}】信号！触发 Pro 高级模型进行深度逻辑复核与精准测算...")
                else:
                    print(f"💤 初筛结果：【{action}】，暂无操作价值，已跳过 Pro 模型以节省 API 额度。")
        except Exception as e:
             # Flash JSON 崩溃保护机制
             need_pro = True
             print(f"⚠️ Flash 初筛模型输出格式异常，强制触发 Pro 高级模型进行修复与决策...")

    # ================= 触发 Pro 模型 =================
    if need_pro and model_choice == 'gemini':
         result = get_LLM_message(system_content=system_content, user_message=user_message, model_choice=model_choice, model_tier='pro')
         print("✅ Pro 模型深度测算与复核完成！")
         
    print(result)

    # 创建存储目录
    output_dir = f"output/{current_date}"
    os.makedirs(output_dir, exist_ok=True)

    # 保存文件
    filename_out = f"{stock_code}_{stock_name}_output_{model_choice}_{current_date}.txt"
    filepath_out = os.path.join(output_dir, filename_out)

    with open(filepath_out, 'w', encoding='utf-8') as f:
        f.write(result)
    print(f"输出文件已保存至: {filename_out}")

    # 获得股票当前价格以便写入表格
    data_dir = f"log/stock_data/{current_date}"
    filename_data = f"{stock_code}_{stock_name}_data_{current_date}.csv"
    filepath_data = os.path.join(data_dir, filename_data)
    df_data = pd.read_csv(filepath_data)
    stock_price = df_data['收盘'].iloc[-1]

    # ================= 整理输出到表格 =================
    try:
        corrected_text = result.replace("“", '"').replace("”", '"')
        data = {}
        try:
            start_index = corrected_text.find('{')
            end_index = corrected_text.rfind('}')

            if start_index != -1 and end_index != -1 and end_index > start_index:
                json_string = corrected_text[start_index : end_index + 1]
                data = json_repair.loads(json_string)
                print("✅ JSON 数据提取成功！")
            else:
                print("❌ 未能在文本中找到有效的JSON边界。")

        except json.JSONDecodeError as e:
            print(f"❌ 截取的字符串不是有效的JSON格式: {e}")
        except Exception as e:
            print(f"❌ 发生未知错误: {e}")

        if not data:
            print(f"错误：在处理股票 {stock_code} ({stock_name}) 时，未能从结果中提取到 JSON 数据块。")
            return SHOULD_SKIP 
        else:
            parsed_data = data

            # 清理不需要的字段长文本
            keys_to_delete = ['各种信号', '原因'] 
            for key in keys_to_delete:
                if key in parsed_data:
                    del parsed_data[key]

            # 计算回报风险比
            reward_risk_ratio_str = 'N/A'
            try:
                buy_price = parsed_data.get('建议买入价')
                target_price = parsed_data.get('目标卖出价')
                stop_price = parsed_data.get('建议止损价')
                
                if buy_price is not None and target_price is not None and stop_price is not None:
                    potential_reward = target_price - buy_price
                    potential_risk = buy_price - stop_price
                    
                    if potential_risk > 0:
                        reward_risk_ratio = potential_reward / potential_risk
                        reward_risk_ratio_str = f"{reward_risk_ratio:.2f}:1"
            except (ValueError, TypeError) as e:
                print(f"警告: 价格数据格式异常，跳过风险回报计算 - {e}。")
            except Exception as e:
                print(f"计算回报风险比时发生未知错误: {e}")

            # 提取建议仓位，格式化为百分比
            pos_adv = parsed_data.get('建议仓位')
            pos_str = f"{pos_adv}%" if pos_adv is not None else "N/A"

            final_data = {
                "股票代码": stock_code,
                "股票名称": stock_name,
                "当前价格": stock_price,
                "预期": parsed_data.get("预期", "N/A"),
                "操作": parsed_data.get("操作", "N/A"),
                "建议仓位": pos_str,      
                "置信度": parsed_data.get("置信度", "N/A"),
                "建议买入价": parsed_data.get("建议买入价") if parsed_data.get("建议买入价") is not None else "N/A",
                "目标卖出价": parsed_data.get("目标卖出价") if parsed_data.get("目标卖出价") is not None else "N/A",
                "建议止损价": parsed_data.get("建议止损价") if parsed_data.get("建议止损价") is not None else "N/A",
                "回报风险比": reward_risk_ratio_str
            }

            file_name = f"Daily Table_{current_date}.csv"
            file_path = os.path.join(output_dir, file_name)
            output_df = pd.DataFrame([final_data])

            # 写入机制 (防文件锁阻塞)
            max_retries = 3      
            retry_delay = 10     

            for attempt in range(max_retries):
                try:
                    if not os.path.exists(file_path):
                        header = {
                            "股票代码": [], "股票名称": [], "当前价格": [], "预期": [], "操作": [],
                            "建议仓位": [], "置信度": [], "建议买入价": [], "目标卖出价": [],
                            "建议止损价": [], "回报风险比": []
                        }
                        new_df = pd.DataFrame(header)
                        new_df.to_csv(file_path, index=False, encoding='utf-8-sig') 
                        print(f"Csv文件 '{file_name}' 已经成功创建。")
                        output_df.to_csv(file_path, index=False, header=False, mode='a', encoding='utf-8-sig')
                    else:
                        output_df.to_csv(file_path, index=False, header=False, mode='a', encoding='utf-8-sig')
                    
                    print('✅ 表格输出成功')
                    break 

                except PermissionError as e:
                    if attempt < max_retries - 1:
                        print(f"⚠️ 文件 '{file_name}' 正在被云同步或其它程序占用 (Permission denied)。")
                        print(f"等待 {retry_delay} 秒后进行第 {attempt + 1} 次重试...")
                        time.sleep(retry_delay)
                    else:
                        print(f"❌ 连续 {max_retries} 次尝试写入失败，请检查文件是否被永久锁定: {e}")
                except Exception as e:
                    print(f"❌ 写入表格时发生其他未知错误: {e}")
                    break 
                
    except IndexError:
        print(f"错误：在处理股票 {stock_code} ({stock_name}) 时，提取到的 JSON 数据列表为空。")
        return SHOULD_SKIP 
    except json.JSONDecodeError as e:
        print(f"错误：在处理股票 {stock_code} ({stock_name}) 时，解析 JSON 数据失败。请检查 JSON 格式。错误详情: {e}")
        return SHOULD_SKIP
    except KeyError as e:
        print(f"错误：在处理股票 {stock_code} ({stock_name}) 时，尝试访问 JSON 数据中不存在的键: {e}。")
        return SHOULD_SKIP 
    except Exception as e:
        print(f"在处理股票 {stock_code} ({stock_name}) 时发生意外错误: {e}")
        return SHOULD_SKIP
        
    return 0