# -*- coding: utf-8 -*-
import os
import re
import concurrent.futures
import time
import pandas as pd
import glob  # <--- 新增导入
from datetime import datetime, timedelta
import json_repair
import baostock as bs

from src.data_crawler import get_stock_data, get_stock_name_bs, get_chart_data, get_ths_fund_flow, get_30m_chart_data, format_large_number
from src.LLM_chat import get_LLM_message, get_model_config
from src.utils import fetch_news_safely, parse_llm_json, get_all_output_dates # <--- 新增 get_all_output_dates
from src.financial_analyzer import process_pipeline

def run_core_analysis(
    stock_code, position, cost, current_date_str,
    flash_model, use_pro, pro_model, dual_filter,
    use_moa, committee_agents, committee_model=None,
    set_progress=None
):
    """
    统一的核心分析引擎，供 UI (app.py) 和 批处理 (worker.py) 调用。
    """
    if not committee_model:
        committee_model = flash_model
        
    c_date = datetime.strptime(current_date_str, "%Y-%m-%d").date() 
    end = c_date.strftime("%Y%m%d") 
    beg = (c_date - timedelta(days=720)).strftime("%Y%m%d")

    if set_progress: set_progress("🔍 步骤 1/5: 正在连接数据源获取基础行情与量价数据...")
    # 修改为直接调用：
    s_name = get_stock_name_bs(stock_code)
    safe_s_name = re.sub(r'[\\/:*?"<>|]', '', s_name) 
    
    df_chart = get_chart_data(stock_code, beg, end)
    df_30m = get_30m_chart_data(stock_code, beg, end)

    try:
        fund_df = get_ths_fund_flow(stock_code, current_date_str)
        if not fund_df.empty:
            df_chart = pd.merge(df_chart, fund_df, on='date', how='left')
    except Exception as e:
        print(f"合并资金数据到日常图表失败: {e}")

    s_price = df_chart['close'].iloc[-1] if not df_chart.empty else 0

    in_str = get_stock_data(stock_code=stock_code, beg=beg, end=end, current_date=current_date_str)
        
    if set_progress: set_progress("📰 步骤 2/5: 正在全网抓取最新市场新闻与宏观情绪...")
    news_titles = fetch_news_safely(stock_code, safe_s_name, current_date_str)
    
    if not df_chart.empty:
        df_monthly_tmp = df_chart.copy()
        df_monthly_tmp['date'] = pd.to_datetime(df_monthly_tmp['date'])
        df_monthly_tmp['year_month'] = df_monthly_tmp['date'].dt.to_period('M')
        df_monthly = df_monthly_tmp.groupby('year_month').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).reset_index()
        df_monthly.rename(columns={'year_month': 'date'}, inplace=True)
        df_monthly['date'] = df_monthly['date'].astype(str)
        
        for col in ['open', 'high', 'low', 'close']:
            if col in df_monthly.columns:
                df_monthly[col] = df_monthly[col].apply(lambda x: f"{float(x):.2f}" if pd.notna(x) else x)
        
        if 'volume' in df_monthly.columns:
            df_monthly['volume'] = df_monthly['volume'].apply(format_large_number)
            
        monthly_str = df_monthly.tail(24).to_markdown(index=False)
    else:
        monthly_str = "暂无"

    daily_str = df_chart.tail(24).to_markdown(index=False) if not df_chart.empty else "暂无"
    df_30m_str = df_30m.tail(24).to_markdown(index=False) if not df_30m.empty else "暂无"
    if not df_30m.empty:
        df_30m_tmp = df_30m.copy()
        
        if 'time' in df_30m_tmp.columns:
            df_30m_tmp['time'] = df_30m_tmp['time'].astype(str).apply(
                lambda x: f"{x[8:10]}:{x[10:12]}" if len(x) >= 12 else x
            )
            
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
    
    if set_progress: set_progress(f"🧠 步骤 3/5: 正在请求基础模型 ({flash_model}) 进行初筛逻辑推演...")
    
    # === 阶段 1：初筛过滤器 ===
    print(f"⏳ 开始呼叫基础模型 ({flash_model}) 进行初筛逻辑推演...")
    filter_start_time = time.time()
    
    if use_pro and dual_filter:
        if float(position) > 0: 
            run_pro = True
            print("   💡 当前持仓大于0，跳过 API 请求，直接进入高级决议圈...")
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
        print("   💡 未开启双筛，跳过 API 请求，直接进入高级决议圈...")
    else:
        res_text = get_LLM_message(system_content=sys_content, user_message=user_msg, model_id=flash_model)

    filter_cost_time = time.time() - filter_start_time
    print(f"✅ 基础模型初筛执行完毕，耗时: {filter_cost_time:.2f} 秒\n")

    # === 阶段 2：准备进入高级决议圈的数据 (财报 + 历史记忆) ===
    history_str = ""
    if run_pro:
        if set_progress: set_progress("📊 阶段触发: 进入高级决议圈，正在进行深度财报研读与历史记忆检索...")
        print(f"\n🎯 [{stock_code}] {s_name} 进入高级决议圈，触发大模型深度财报研读...")
        
        # 2.1 财报检索
        try:
            match = re.search(r"最新财务报告期:\s*([0-9\-]+)", in_str)
            if match:
                report_date_raw = match.group(1).replace("-", "").strip() 
                fin_summary = process_pipeline(stock_code, safe_s_name, report_date_raw)
                
                if fin_summary:
                    user_msg += f"\n\n=================================\n### 【大模型深度财报解析与战略一致性校验】\n{fin_summary}\n"
            else:
                print("   ⚠️ 无法在原文中提取到财报日期，跳过财报解析。")
        except Exception as e:
            print(f"   ⚠️ 财报研读模块调用失败，已跳过: {e}")
            
        # === 阶段 2.2：🌟 检索历史决策记忆库 (增强版) 🌟 ===
        all_dates = get_all_output_dates()
        # 过滤出早于当前日期的所有记录
        past_dates = [d for d in all_dates if d < current_date_str]
        
        history_texts = []
        # 优化点：从取最近3条 ([:3]) 修改为取最近 10 条，以获得更连贯的策略视野
        # 如果你想找“更早期”的，可以调整这里的逻辑，比如取 [5:15] 查看更早之前的
        for d in past_dates[:10]: 
            out_fs = glob.glob(f"output/{d}/{stock_code}_*_output_*_{d}.txt")
            if out_fs:
                try:
                    # 优先按文件修改时间排序以确保读取的是该日最终决策
                    latest_file = max(out_fs, key=os.path.getmtime)
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        h_out = f.read()
                    h_parsed = parse_llm_json(h_out)
                    reasoning = h_parsed.get("reasoning") or h_parsed.get("原因")
                    action = h_parsed.get("action") or h_parsed.get("操作")
                    if reasoning and reasoning not in ["-", "暂无深度逻辑"]:
                        # 截取推演逻辑的前 200 字防止上下文过载
                        short_reasoning = (reasoning[:200] + '..') if len(reasoning) > 200 else reasoning
                        history_texts.append(f"▶【日期：{d} | 历史动作：{action}】\n推演逻辑：{short_reasoning}")
                except Exception:
                    pass

        if history_texts:
            history_str = "\n\n".join(history_texts)
            print(f"   💡 成功提取到 {len(history_texts)} 条历史决策记忆，已注入 AI 裁判上下文。")

    # === 阶段 3：高级决议阶段 (MoA vs 单模型) ===
    if run_pro: 
        if use_moa and committee_agents:
            if set_progress: set_progress(f"👥 步骤 4/5: 正在呼叫 {len(committee_agents)} 位投资大师模型并发分析...")
            committee_results = {}
            format_idx = sys_content.find("【决策过程与输出规范】")
            format_rules = sys_content[format_idx:] if format_idx != -1 else sys_content
            
            def agent_task(agent_name):
                try:
                    with open(f"src/agents_text/{agent_name}.txt", "r", encoding="utf-8") as f:
                        agent_persona = f.read()
                    agent_sys_content = f"{agent_persona}\n\n====================\n以下是系统级硬性约束，你必须严格遵守：\n{format_rules}"
                    # 提示：传递给大师的 user_msg 是纯客观数据，没有历史记忆干扰，保证其视角的独立性
                    return get_LLM_message(system_content=agent_sys_content, user_message=user_msg, model_id=committee_model)
                except Exception as e:
                    return f"该大师 ({agent_name}) 分析失败：{e}"

            print(f"⏳ 开始呼叫 {len(committee_agents)} 位投资大师模型并发分析 (底层模型: {committee_model})...")
            committee_start_time = time.time()

            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                futures = {executor.submit(agent_task, agent_name): agent_name for agent_name in committee_agents}
                for future in concurrent.futures.as_completed(futures):
                    agent_name = futures[future]
                    display_name = agent_name.replace("_", " ")
                    try:
                        committee_results[agent_name] = future.result()
                        print(f"   ✅ 大师 [{display_name}] 意见已送达！")
                    except Exception as e:
                        committee_results[agent_name] = f"该大师分析失败：{e}"
                        print(f"   ❌ 大师 [{display_name}] 分析失败！异常: {e}")
            
            committee_cost_time = time.time() - committee_start_time
            print(f"✅ 大师团所有成员分析完毕，并发总耗时: {committee_cost_time:.2f} 秒\n")
            
            judge_msg = f"{user_msg}\n\n=================================\n【投资总监（AI裁判）专属决议指令】\n以上是客观标的数据。以下是多位顶尖投资大师（不同交易流派的 Agent）针对该数据给出的独立分析和 JSON 报告：\n\n"
            for agent_name, res in committee_results.items():
                display_name = agent_name.replace("_", " ")
                judge_msg += f"--- 投资大师：{display_name} 的意见 ---\n{res}\n\n"
                
            judge_msg += "作为量化基金的投资总监，你拥有最终拍板权。请严格按照以下【核心裁判原则】进行综合决策：\n1. 事实核查先行（零容忍数据幻觉）：必须先核对大师引用的数据是否与上文提供的【客观标的数据】完全一致。对于任何基于虚构数据得出的结论，必须直接一票否决。\n2. 寻找非共识的正确与流派交叉验证：重点审视大师之间的【分歧点】。例如，当价值派（如巴菲特）与趋势派（如利弗莫尔）在特定点位达成共识时，该决策置信度极高；若出现严重分歧，需判断当前市场环境更适用哪种流派。\n3. 拒绝无效瘫痪（果断决策）：不要因为存在分歧就本能地退缩到‘观望’。在剔除幻觉意见后，评估盈亏比，勇敢给出具体的买入/卖出、观望指令和点位。\n4. 资金面数据的辩证看待：资金流向是重要的辅助验证工具，但【绝非所有策略的硬性前提】。如果是左侧深度价值潜伏，主力资金未明显介入甚至流出是完全正常的；如果是右侧主升浪突破，则需要资金合力。切勿因为缺乏明显的资金净流入，就教条式地否决优秀的左侧或长线基本面机会。\n\n"

            # 🌟 在最后关头注入历史记忆，仅供总监（裁判）参考
            if history_str:
                judge_msg += f"=================================\n【总监个人历史记忆库】\n系统调取了你（总监）前几日对该股做出的深度推演，请以此作为连贯性参考：\n{history_str}\n\n（特别注意：请对照最新大师意见与今日最新盘面，审视原逻辑是否被证伪。保持你投资思路的连贯性；但若行情发生根本反转，请果断进行战略纠错！）\n\n"

            judge_msg += "请给出最终决策。你必须在 JSON 的 '原因' 字段中分段输出：\n【事实核查与幻觉剔除】：简述是否有大师引用了错误数据。\n【大师观点交锋】：简述各流派有效观点的交锋与共鸣点。\n【总监拍板逻辑】：结合最新盘面与你的历史记忆，详细说明最终裁决理由。\n注意：你的输出必须是一个单一的、严格符合原定系统提示词规范的 JSON 对象！\n"

            if set_progress: set_progress(f"⚖️ 步骤 5/5: 正在呼叫总监模型 ({pro_model}) 进行最终综合裁决...")
            print(f"⏳ 开始呼叫总监模型: {pro_model}...")
            start_time = time.time()
            res_text = get_LLM_message(system_content=sys_content, user_message=judge_msg, model_id=pro_model)
            print(f"✅ 总监模型返回，纯 API 耗时: {time.time() - start_time:.2f} 秒")
            model_tag = f"MoA-{len(committee_agents)}大师-{pro_model}"
            
        else:
            final_user_msg = user_msg
            # 🌟 如果未开启多大师议事，但在使用单体 Pro 模型，依然注入记忆
            if history_str:
                final_user_msg += f"\n\n=================================\n### 【总监个人历史记忆库】\n前几日你对该股的决策与逻辑如下：\n{history_str}\n\n（特别注意：请结合今日最新盘面，评估你的原逻辑是否被证伪，保持投资体系的连贯性，或在变盘时果断纠错。）\n"

            if set_progress: set_progress(f"⚖️ 步骤 4/4: 正在呼叫 Pro 模型 ({pro_model}) 进行深度推演...")
            res_text = get_LLM_message(system_content=sys_content, user_message=final_user_msg, model_id=pro_model)
            model_tag = f"D-{flash_model}-{pro_model}" if dual_filter else pro_model

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