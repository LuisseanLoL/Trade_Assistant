# -*- coding: utf-8 -*-
import efinance as ef
from datetime import datetime, timedelta
import pandas as pd
import akshare as ak
import json
import re
import os
from src.LLM_chat import get_LLM_message

# 获取日期
current_date = datetime.now().date()
# 结束日期
end_date = current_date # 默认获取当前日期
end = end_date.isoformat().replace('-', '')
# 开始日期
start_date = end_date - timedelta(days=60)  # 默认获取两个月的数据
beg = start_date.isoformat().replace('-', '')

# 定义一个常量或特定的返回值来表示“需要跳过”
SHOULD_SKIP = object() # 使用一个唯一的对象实例，避免与 None 或 False 混淆

def etf_process(
        etf_code = '588200',
        cash = 10000,
        stock_position = 0,
        stock_holding_cost = 0,
        beg = beg,
        end = end,
        current_date = current_date,
        model_choice='gemini'
):
    """
    获取etf历史数据, 计算技术指标, 获得近期新闻, 询问LLM助手并保存为CSV文件

    Args:
        etf_code: etf代码, 字符串
        cash: 现金量
        stock_position: 持仓量
        stock_holding_cost: 持仓成本
        beg: 开始日期, 格式: YYYYMMDD
        end: 结束日期, 格式: YYYYMMDD
        current_date: 当前日期, 格式: YYYY-MM-DD
        model_choice:'gemini','ark','local'
    """

    # 获取etf基本信息
    etf_base_info = ef.fund.get_base_info(etf_code)

    # 获取近30交易日K线数据
    etf_k_data = ef.stock.get_quote_history(stock_codes=etf_code,beg=beg,end=end) 
    # 1. 取出后30项
    etf_k_data = etf_k_data[-30:]
    # 2. 重置索引并丢弃旧索引
    etf_k_data = etf_k_data.reset_index(drop=True)

    etf_name = etf_k_data.iat[0, 0]

    # etf持仓
    year = end[:4]
    etf_portfolio_hold = ak.fund_portfolio_hold_em(symbol=etf_code, date=year)
    new_column_names = {'占净值比例': '占净值比例:%', '持股数': '持股数:万股', '持仓市值': '持仓市值:万元'}
    etf_portfolio_hold = etf_portfolio_hold.rename(columns=new_column_names)
    etf_portfolio_hold.drop('季度', axis=1, inplace=True)

    user_message = f"""基于获得的以下数据和你搜集到的新闻消息，做出你的交易决策。

{etf_base_info.to_string(dtype=False)}

最近三十个交易日数据如下：
{etf_k_data.to_string(index=False)}

该etf持仓情况如下:
{etf_portfolio_hold.to_string(index=False)}

当前持仓如下：
投资组合：
现金：{cash:.2f}
当前持仓：{stock_position} 股
当前持仓成本: {stock_holding_cost} 元

请记住，行动必须是买入、卖出或持有。
你只能在有可用现金的情况下买入。
你只能在投资组合中有可售股票的情况下卖出。
谨慎考虑交易决策：考虑当前股价是高位还是低位，在低位买入，高位卖出。
考虑自己的持仓成本，在有足够浮盈的情况下考虑卖出收获现金实利."""

    print(user_message)

    # 输出prompt文件

    # 创建存储目录
    input_dir = f"input/{current_date}"
    os.makedirs(input_dir, exist_ok=True)

    # 保存文件
    filename_in = f"{etf_code}_{etf_name}_input_{current_date}.txt"
    filepath_in = os.path.join(input_dir, filename_in)
    with open(filepath_in, 'w', encoding='utf-8') as f:
        f.write(user_message)
    print(f"输入文件已保存至: {filename_in}")

    with open('LLM system content.txt', 'r', encoding='utf-8') as file:
    # 读取整个文件内容
        system_content = file.read()

    result = get_LLM_message(system_content=system_content, user_message=user_message, model_choice=model_choice)
    print(result)

    # 创建存储目录
    output_dir = f"output/{current_date}"
    os.makedirs(output_dir, exist_ok=True)

    # 保存文件
    filename_out = f"{etf_code}_{etf_name}_output_{current_date}.txt"
    filepath_out = os.path.join(output_dir, filename_out)

    with open(filepath_out, 'w', encoding='utf-8') as f:
        f.write(result)
    print(f"输出文件已保存至: {filename_out}")

    # 整理成表格
    try:
        # 得到JSON字符串
        data = re.findall(r'```json\s*(\{.*?\})\s*```', result, re.DOTALL)

        # Check if any JSON block was found
        if not data:
            print(f"错误：在处理股票 {etf_code} ({etf_name}) 时，未能从结果中提取到 JSON 数据块。")
            # If running in a loop for multiple stocks, you might want to 'continue' here
            return SHOULD_SKIP # <--- 返回特定信号

        else:
            # 处理json数据
            parsed_data = json.loads(data[0])

            # 清理parsed data
            keys_to_delete = ['头寸数量', '可接受价格', '各种信号', '原因', '预期']
            for key in keys_to_delete:
                if key in parsed_data:
                    del parsed_data[key]

            # 加入股票信息
            tickercode = {'股票代码':etf_code}
            tickername = {'股票名称':etf_name}
            # Combine dictionaries, ensuring identifiers come first
            final_data = {**tickercode, **tickername, **parsed_data}

            # 保存文件路径
            file_name = f"Comparison Table_{current_date}.csv"
            file_path = os.path.join(output_dir, file_name)
            # Prepare DataFrame for output
            output_df = pd.DataFrame([final_data])

            if not os.path.exists(file_path):
                # 如果文件不存在，创建它
                try:
                    header = {
                        "股票代码": [],
                        "股票名称": [],
                        "操作": [],
                        "置信度": []
                    }
                    new_df = pd.DataFrame(header)
                    new_df.to_csv(file_path, index=False)
                    print(f"Csv文件 '{file_name}' 已经成功创建。")

                    output_df.to_csv(file_path, index=False, header=False, mode='a')
                    print('表格输出成功')

                except IOError:
                    print(f"无法创建文件 '{file_name}'。请检查权限和路径是否正确。")
            else:
                output_df.to_csv(file_path, index=False, header=False, mode='a')
                print('表格输出成功')
                
    # --- Exception Handling for JSON extraction/parsing ---
    except IndexError:
        # This might occur if re.findall returns an empty list and we try data_list[0]
        # The 'if not data_list:' check above makes this less likely, but it's good practice.
        print(f"错误：在处理股票 {etf_code} ({etf_name}) 时，提取到的 JSON 数据列表为空。")
        return SHOULD_SKIP # <--- 返回特定信号

    except json.JSONDecodeError as e:
        # This occurs if the string inside data_list[0] is not valid JSON
        print(f"错误：在处理股票 {etf_code} ({etf_name}) 时，解析 JSON 数据失败。请检查 JSON 格式。错误详情: {e}")
        # Optional: print the problematic string
        # if data_list: print(f"问题数据: {data_list[0]}")
        return SHOULD_SKIP # <--- 返回特定信号

    except KeyError as e:
        # This occurs if a key you try to delete doesn't exist (handled by the 'if key in parsed_data' check now)
        # If you remove the check, this handler becomes important.
        print(f"错误：在处理股票 {etf_code} ({etf_name}) 时，尝试访问 JSON 数据中不存在的键: {e}。")
        return SHOULD_SKIP # <--- 返回特定信号

    except Exception as e:
        # Catch any other unexpected errors during the try block
        print(f"在处理股票 {etf_code} ({etf_name}) 时发生意外错误: {e}")
        return SHOULD_SKIP # <--- 返回特定信号
        
    return 0