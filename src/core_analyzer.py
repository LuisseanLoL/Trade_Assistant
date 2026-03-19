# -*- coding: utf-8 -*-
import os
import re
import concurrent.futures
import time
import pandas as pd
from datetime import datetime, timedelta
import json_repair
import baostock as bs

from src.data_crawler import get_stock_data, get_stock_name_bs, get_chart_data, get_ths_fund_flow, get_30m_chart_data, format_large_number
from src.LLM_chat import get_LLM_message, get_model_config
from src.utils import fetch_news_safely, parse_llm_json

def run_core_analysis(
    stock_code, position, cost, current_date_str,
    flash_model, use_pro, pro_model, dual_filter,
    use_moa, committee_agents, committee_model=None
):
    """
    统一的核心分析引擎，供 UI (app.py) 和 批处理 (worker.py) 调用。
    返回: df_chart, stock_name, stock_price, parsed_json, disp_model, user_msg, result_text
    """
    # 如果没有指定议事模型，默认用初筛模型
    if not committee_model:
        committee_model = flash_model
        
    c_date = datetime.strptime(current_date_str, "%Y-%m-%d").date() # 加上 .date() 剥离多余的时间信息
    end = c_date.strftime("%Y%m%d") # 直接使用 strftime，比 isoformat 替换更安全
    beg = (c_date - timedelta(days=720)).strftime("%Y%m%d")

    # ================= 统一接管 BaoStock 登录 =================
    bs.login()
    try:
        s_name = get_stock_name_bs(stock_code)
        safe_s_name = re.sub(r'[\\/:*?"<>|]', '', s_name) 
        
        # 1. 组装数据 (统一使用 app.py 的最新逻辑)
        df_chart = get_chart_data(stock_code, beg, end)
        df_30m = get_30m_chart_data(stock_code, beg, end)

        try:
            fund_df = get_ths_fund_flow(stock_code)
            if not fund_df.empty:
                df_chart = pd.merge(df_chart, fund_df, on='date', how='left')
        except Exception as e:
            print(f"合并资金数据到日常图表失败: {e}")

        s_price = df_chart['close'].iloc[-1] if not df_chart.empty else 0

        in_str = get_stock_data(stock_code=stock_code, beg=beg, end=end, current_date=current_date_str)
        
    finally:
        # ================= 统一接管 BaoStock 登出 =================
        bs.logout()
        
    # 新闻获取不需要 bs 登录，可以放在外面
    news_titles = fetch_news_safely(stock_code, safe_s_name, current_date_str)
    
    if not df_chart.empty:
        df_monthly_tmp = df_chart.copy()
        df_monthly_tmp['date'] = pd.to_datetime(df_monthly_tmp['date'])
        df_monthly_tmp['year_month'] = df_monthly_tmp['date'].dt.to_period('M')
        df_monthly = df_monthly_tmp.groupby('year_month').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).reset_index()
        df_monthly.rename(columns={'year_month': 'date'}, inplace=True)
        df_monthly['date'] = df_monthly['date'].astype(str)
        # ======== 新增：压缩与简化月K线价格和交易量 ========
        # 1. 将开盘、最高、最低、收盘价保留2位小数
        for col in ['open', 'high', 'low', 'close']:
            if col in df_monthly.columns:
                df_monthly[col] = df_monthly[col].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else x)
        
        # 2. 调用已导入的 format_large_number 将交易量转换为 万/亿
        if 'volume' in df_monthly.columns:
            df_monthly['volume'] = df_monthly['volume'].apply(format_large_number)
        # ====================================================

        monthly_str = df_monthly.tail(24).to_markdown(index=False)
    else:
        monthly_str = "暂无"

    daily_str = df_chart.tail(24).to_markdown(index=False) if not df_chart.empty else "暂无"
    df_30m_str = df_30m.tail(24).to_markdown(index=False) if not df_30m.empty else "暂无"
    if not df_30m.empty:
        df_30m_tmp = df_30m.copy()
        
        # 格式化 time 列：取字符串的第8-9位(时)和第10-11位(分)，中间加冒号
        if 'time' in df_30m_tmp.columns:
            df_30m_tmp['time'] = df_30m_tmp['time'].astype(str).apply(
                lambda x: f"{x[8:10]}:{x[10:12]}" if len(x) >= 12 else x
            )
            
        # 格式化 amount 列：调用刚才导入的 format_large_number
        if 'amount' in df_30m_tmp.columns:
            df_30m_tmp['amount'] = df_30m_tmp['amount'].apply(format_large_number)
            
        df_30m_str = df_30m_tmp.tail(24).to_markdown(index=False)
    else:
        df_30m_str = "暂无"

    user_msg = f"""基于获得的以下数据和新闻消息，做出你的交易决策。

{in_str}

最近二十四个 30分钟 K线数据如下：
{df_30m_str}

最近二十四个交易日数据如下：
{daily_str}

最近二十四个月 K线数据如下：
{monthly_str}

相关新闻如下：
{news_titles}

当前该股仓位：{position} %
当前持仓成本: {cost} 元

请记住，行动必须是买入、卖出、持有或观望。
请严格结合你的专属交易哲学，从上述客观数据中提取核心矛盾，并给出带有明确止损止盈点位的决策。"""

    os.makedirs(f"input/{current_date_str}", exist_ok=True)
    with open(f"input/{current_date_str}/{stock_code}_{safe_s_name}_input_{current_date_str}.txt", 'w', encoding='utf-8') as f: 
        f.write(user_msg)

    try:
        with open('LLM system content.txt', 'r', encoding='utf-8') as f: sys_content = f.read()
    except: 
        sys_content = "你是一个专业的量化交易AI..."

    run_pro = False
    res_text = ""
    model_tag = flash_model
    
    # 2. 第一阶段：过滤/初筛阶段
    if use_pro and dual_filter:
        if float(position) > 0: 
            run_pro = True
        else:
            res_text = get_LLM_message(system_content=sys_content, user_message=user_msg, model_id=flash_model)
            try:
                c_text = res_text.replace("“", '"').replace("”", '"') # type: ignore
                s_idx, e_idx = c_text.find('{'), c_text.rfind('}')
                if s_idx != -1 and e_idx != -1:
                    action_result = json_repair.loads(c_text[s_idx : e_idx + 1]).get('操作', '') # type: ignore
                    if action_result in ['买入', '卖出', '持有']: run_pro = True
            except: 
                run_pro = True 
    elif use_pro and not dual_filter:
        run_pro = True
    else:
        res_text = get_LLM_message(system_content=sys_content, user_message=user_msg, model_id=flash_model)

    # 3. 第二阶段：高级决议阶段 (MoA vs 单模型)
    if run_pro: 
        if use_moa and committee_agents:
            committee_results = {}
            format_idx = sys_content.find("【决策过程与输出规范】")
            format_rules = sys_content[format_idx:] if format_idx != -1 else sys_content
            
            def agent_task(agent_name):
                try:
                    with open(f"src/agents_text/{agent_name}.txt", "r", encoding="utf-8") as f:
                        agent_persona = f.read()
                    agent_sys_content = f"{agent_persona}\n\n====================\n以下是系统级硬性约束，你必须严格遵守：\n{format_rules}"
                    return get_LLM_message(system_content=agent_sys_content, user_message=user_msg, model_id=committee_model)
                except Exception as e:
                    return f"该大师 ({agent_name}) 分析失败：{e}"

            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                futures = {executor.submit(agent_task, agent_name): agent_name for agent_name in committee_agents}
                for future in concurrent.futures.as_completed(futures):
                    agent_name = futures[future]
                    try:
                        committee_results[agent_name] = future.result()
                    except Exception as e:
                        committee_results[agent_name] = f"该大师分析失败：{e}"
            
            judge_msg = f"{user_msg}\n\n=================================\n【投资总监（AI裁判）专属决议指令】\n以上是客观标的数据。以下是多位顶尖投资大师（不同交易流派的 Agent）针对该数据给出的独立分析和 JSON 报告：\n\n"
            for agent_name, res in committee_results.items():
                display_name = agent_name.replace("_", " ")
                judge_msg += f"--- 投资大师：{display_name} 的意见 ---\n{res}\n\n"
                
            judge_msg += "作为量化基金的投资总监，你拥有最终拍板权。请严格按照以下【核心裁判原则】进行综合决策：\n1. 事实核查先行（零容忍数据幻觉）：必须先核对大师引用的数据是否与上文提供的【客观标的数据】完全一致。对于任何基于虚构数据得出的结论，必须直接一票否决。\n2. 寻找非共识的正确与流派交叉验证：重点审视大师之间的【分歧点】。例如，当价值派（如巴菲特）与趋势派（如利弗莫尔）在特定点位达成共识时，该决策置信度极高；若出现严重分歧，需判断当前市场环境更适用哪种流派。\n3. 拒绝无效瘫痪（果断决策）：不要因为存在分歧就本能地退缩到‘观望’。在剔除幻觉意见后，评估盈亏比，勇敢给出具体的买入/卖出、观望指令和点位。\n4. 资金面数据的辩证看待：资金流向是重要的辅助验证工具，但【绝非所有策略的硬性前提】。如果是左侧深度价值潜伏，主力资金未明显介入甚至流出是完全正常的；如果是右侧主升浪突破，则需要资金合力。切勿因为缺乏明显的资金净流入，就教条式地否决优秀的左侧或长线基本面机会。\n\n请给出最终决策。你必须在 JSON 的 '原因' 字段中分段输出：\n【事实核查与幻觉剔除】：简述是否有大师引用了错误数据。\n【大师观点交锋】：简述各流派有效观点的交锋与共鸣点。\n【总监拍板逻辑】：详细说明你最终的综合裁决理由。\n注意：你的输出必须是一个单一的、严格符合原定系统提示词规范的 JSON 对象！\n"

            print(f"⏳ 开始呼叫总监模型: {pro_model}...")
            start_time = time.time()
            res_text = get_LLM_message(system_content=sys_content, user_message=judge_msg, model_id=pro_model)
            print(f"✅ 总监模型返回，纯 API 耗时: {time.time() - start_time:.2f} 秒")
            model_tag = f"MoA-{len(committee_agents)}大师-{pro_model}"
            
        else:
            res_text = get_LLM_message(system_content=sys_content, user_message=user_msg, model_id=pro_model)
            model_tag = f"D-{flash_model}-{pro_model}" if dual_filter else pro_model

    # 4. 落地保存
    model_configs = get_model_config()
    if model_tag.startswith("MoA-"):
        parts = model_tag.split("-")
        disp_model = f"【决议】{model_configs.get(parts[2], {}).get('name', parts[2])}" if len(parts) >= 3 else model_tag
    elif model_tag.startswith("D-"):
        parts = model_tag.split("-")
        disp_model = f"{model_configs.get(parts[2], {}).get('name', parts[2])}(双筛)" if len(parts) >= 3 else model_tag
    else:
        disp_model = model_configs.get(model_tag, {}).get('name', model_tag)

    os.makedirs(f"output/{current_date_str}", exist_ok=True)
    with open(f"output/{current_date_str}/{stock_code}_{safe_s_name}_output_{model_tag}_{current_date_str}.txt", 'w', encoding='utf-8') as f: 
        f.write(res_text) # type: ignore

    parsed = parse_llm_json(res_text)

    buy_p, sell_p, stop_p = parsed.get("buy_p") or parsed.get("建议买入价"), parsed.get("sell_p") or parsed.get("目标卖出价"), parsed.get("stop_p") or parsed.get("建议止损价")
    
    rr_str = 'N/A'
    try:
        if buy_p and sell_p and stop_p and float(buy_p) - float(stop_p) > 0: 
            rr_str = f"{(float(sell_p) - float(buy_p)) / (float(buy_p) - float(stop_p)):.2f}:1"
    except: pass

    # 写 CSV 记录
    csv_path = f"output/{current_date_str}/Daily Table_{current_date_str}.csv"
    pd.DataFrame([{
        "股票代码": stock_code, "股票名称": s_name, "决策模型": disp_model, "当前价格": s_price, 
        "预期": parsed.get("expectation") or parsed.get("预期", "N/A"), 
        "操作": parsed.get("action") or parsed.get("操作", "N/A"), 
        "建议仓位": parsed.get("pos_adv") or parsed.get("建议仓位", "N/A"), 
        "置信度": parsed.get("confidence") or parsed.get("置信度", "N/A"), 
        "建议买入价": str(buy_p) if buy_p else "-", 
        "目标卖出价": str(sell_p) if sell_p else "-", 
        "建议止损价": str(stop_p) if stop_p else "-", 
        "回报风险比": rr_str
    }]).to_csv(csv_path, index=False, header=not os.path.exists(csv_path), mode='a', encoding='utf-8-sig')

    return df_chart, s_name, s_price, parsed, disp_model, user_msg, res_text