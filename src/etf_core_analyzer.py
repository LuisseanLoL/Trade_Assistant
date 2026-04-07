# -*- coding: utf-8 -*-
import os
import re
import concurrent.futures
import threading
import time
import pandas as pd
import glob
from datetime import datetime, timedelta
import json_repair

# 导入内部模块
from src.etf_data_crawler import get_etf_data_context
from src.news_crawler import get_news_titles
from src.LLM_chat import get_LLM_message, get_model_config
from src.utils import parse_llm_json

def run_etf_core_analysis(
    etf_code, position, cost, current_date_str,
    flash_model, use_pro, pro_model, dual_filter,
    use_moa, committee_agents, committee_model=None,
    set_progress=None
):
    """
    ETF 统一核心分析引擎，供 UI (etf_app.py) 的异步任务调用。
    与个股 core_analyzer 架构对齐，支持 MoA 议事与历史记忆注入。
    """
    if not committee_model:
        committee_model = flash_model
        
    c_date = datetime.strptime(current_date_str, "%Y-%m-%d").date() 
    end = c_date.strftime("%Y%m%d") 
    beg = (c_date - timedelta(days=720)).strftime("%Y%m%d")
    
    etf_code = str(etf_code).strip()

    # ==========================================
    # 阶段 1：数据获取与上下文拼装
    # ==========================================
    if set_progress: set_progress(f"🔍 步骤 1/5: 正在获取 ETF [{etf_code}] 基础档案与量价数据...")
    
    # 核心：调用 ETF 专属的爬虫引擎
    in_str = get_etf_data_context(etf_code=etf_code, beg=beg, end=end, current_date=current_date_str)
    
    # 从 context 中提取真实基金名称，方便后续日志保存
    s_name = "ETF"
    if "基金名称:" in in_str:
        try:
            s_name = in_str.split("基金名称:")[1].split('\n')[0].strip()
        except: pass
    safe_s_name = re.sub(r'[\\/:*?"<>|]', '', s_name) 

    # 读取刚才由 get_etf_data_context 生成的 CSV，用于返回给前端画图
    csv_path = f"log/etf_data/{current_date_str}/{etf_code}_indicators_{current_date_str}.csv"
    df_chart = pd.DataFrame()
    s_price = 0.0
    if os.path.exists(csv_path):
        df_chart = pd.read_csv(csv_path)
        # 兼容 utils.create_advanced_kline_fig 所需的列名
        df_chart = df_chart.rename(columns={'日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume'})
        if not df_chart.empty:
            s_price = df_chart['close'].iloc[-1]

    if set_progress: set_progress("📰 步骤 2/5: 正在全网抓取 ETF 标的最新市场新闻...")
    # ETF 新闻抓取
    news_text = get_news_titles(symbol=etf_code, stock_name=s_name, max_news=20, save_txt=True, current_date=current_date_str)

    # 构建 ETF 专属 User Prompt
    user_msg = f"""基于获得的以下 ETF 数据和你搜集到的新闻消息，做出你的交易决策。

{in_str}

相关新闻如下：
{news_text}

当前该ETF持仓：{position} 份
当前持仓成本: {cost} 元

请记住，行动必须是买入、卖出、持有或观望。
谨慎考虑交易决策：考虑当前价格是高位还是低位，在低位买入，高位卖出。
考虑自己的持仓成本，在有足够浮盈的情况下考虑卖出收获现金实利。"""

    os.makedirs(f"input_etf/{current_date_str}", exist_ok=True)
    with open(f"input_etf/{current_date_str}/{etf_code}_{safe_s_name}_input_{current_date_str}.txt", 'w', encoding='utf-8') as f: 
        f.write(user_msg)

    # ==========================================
    # 🌟 并发启动：大模型底层资产穿透与战略评估
    # ==========================================
    deep_analysis_result = ["暂未生成深度分析报告"]
    def generate_deep_analysis():
        try:
            sys_p = "你是一名顶尖的ETF投研专家，擅长底层资产穿透与宏观大周期定调。"
            
            clean_msg = user_msg.split("请记住，行动必须是买入")[0].strip()
            
            user_p = f"请基于以下ETF客观数据与新闻，撰写一份专业的《大模型底层资产穿透与战略评估报告》（支持Markdown排版，字数约800-1000字）。\n\n" \
                     f"要求重点分析：\n" \
                     f"1. 【底层重仓资产穿透】：分析前十大重仓股的业务共性及其所处的产业生命周期。\n" \
                     f"2. 【宏观大盘与行业共振】：结合当前宏观数据评估该ETF所处赛道的战略胜率。\n" \
                     f"3. 【资金博弈动向】：深度解读近期场内份额申赎变动背后的机构与国家队意图。\n" \
                     f"4. 【综合战略定调】：给出明确的长线配置建议。\n\n" \
                     f"【🛑极度重要的强制规范】：\n" \
                     f"不要输出任何 JSON 代码！不要输出买卖信号字典！必须且只能输出一篇结构流畅、文笔专业的 Markdown 中文分析长文。\n\n" \
                     f"输入数据如下：\n{clean_msg}"
                     
            # 🌟 核心修复：必须加上 schema=None 彻底解除 JSON 格式束缚
            res = get_LLM_message(system_content=sys_p, user_message=user_p, model_id=pro_model, schema=None)
            
            deep_analysis_result[0] = res
            with open(f"output_etf/{current_date_str}/{etf_code}_{safe_s_name}_deep_analysis_{current_date_str}.md", 'w', encoding='utf-8') as f:
                f.write(res)
        except Exception as e:
            deep_analysis_result[0] = f"深度分析生成失败: {e}"

    deep_thread = threading.Thread(target=generate_deep_analysis)
    deep_thread.start()

    # 加载系统提示词
    try:
        with open('ETF LLM system content.txt', 'r', encoding='utf-8') as f: 
            sys_content = f.read()
    except: 
        sys_content = "你是一个专业的 ETF 量化交易AI..."

    run_pro = False
    res_text = ""
    model_tag = flash_model
    
    # ==========================================
    # 阶段 2：初筛过滤器 (Flash 模型)
    # ==========================================
    if set_progress: set_progress(f"🧠 步骤 3/5: 正在请求基础模型 ({flash_model}) 进行初筛逻辑推演...")
    print(f"⏳ 开始呼叫基础模型 ({flash_model}) 进行初筛逻辑推演...")
    filter_start_time = time.time()
    
    if use_pro and dual_filter:
        if float(position) > 0: 
            run_pro = True
            print("   💡 当前持仓大于0，跳过 API 请求，直接进入高级决议圈...")
        else:
            res_text = get_LLM_message(system_content=sys_content, user_message=user_msg, model_id=flash_model)
            try:
                c_text = res_text.replace("“", '"').replace("”", '"')
                s_idx, e_idx = c_text.find('{'), c_text.rfind('}')
                if s_idx != -1 and e_idx != -1:
                    action_result = json_repair.loads(c_text[s_idx : e_idx + 1]).get('操作', '')
                    if action_result in ['买入', '卖出', '持有']: run_pro = True
            except: 
                run_pro = True 
    elif use_pro and not dual_filter:
        run_pro = True
        print("   💡 未开启双筛，跳过 API 请求，直接进入高级决议圈...")
    else:
        res_text = get_LLM_message(system_content=sys_content, user_message=user_msg, model_id=flash_model)

    print(f"✅ 基础模型初筛执行完毕，耗时: {time.time() - filter_start_time:.2f} 秒\n")

    # ==========================================
    # 阶段 3：检索历史决策记忆库
    # ==========================================
    history_str = ""
    if run_pro:
        if set_progress: set_progress("📊 阶段触发: 进入高级决议圈，正在进行历史档案记忆检索...")
        print(f"\n🎯 [{etf_code}] {s_name} 进入高级决议圈...")
        
        # 跨文件夹检索针对此 ETF 的历史输出
        all_etf_files = glob.glob(f"output_etf/*/{etf_code}_*_output_*.txt")
        valid_histories = []
        
        for filepath in all_etf_files:
            folder_date = os.path.basename(os.path.dirname(filepath))
            if folder_date < current_date_str:
                valid_histories.append({
                    'date': folder_date,
                    'path': filepath,
                    'mtime': os.path.getmtime(filepath)
                })
        
        valid_histories.sort(key=lambda x: (x['date'], x['mtime']), reverse=True)
        history_texts = []
        
        # 提取最近 3 次的逻辑
        for hist in valid_histories[:3]:
            try:
                with open(hist['path'], 'r', encoding='utf-8') as f:
                    h_out = f.read()
                h_parsed = parse_llm_json(h_out)
                reasoning = h_parsed.get("reasoning") or h_parsed.get("原因")
                action = h_parsed.get("action") or h_parsed.get("操作")
                
                if reasoning and reasoning not in ["-", "暂无深度逻辑"]:
                    short_reasoning = (reasoning[:200] + '..') if len(reasoning) > 200 else reasoning
                    history_texts.append(f"▶【历史时间：{hist['date']} | 历史动作：{action}】\n推演逻辑：{short_reasoning}")
            except: pass
                
        if history_texts:
            history_texts.reverse() # 时间正序给总监看
            history_str = "\n\n".join(history_texts)
            print(f"   💡 成功提取到 {len(history_texts)} 条针对该 ETF 的精准历史决策记忆！")

    # ==========================================
    # 阶段 4：高级决议阶段 (多智能体 MoA vs 单模型)
    # ==========================================
    if run_pro: 
        if use_moa and committee_agents:
            if set_progress: set_progress(f"👥 步骤 4/5: 正在呼叫 {len(committee_agents)} 位投资大师模型并发分析...")
            committee_results = {}
            
            # 提取 JSON 格式约束给大师 Agent
            format_idx = sys_content.find("【决策过程与输出规范】")
            format_rules = sys_content[format_idx:] if format_idx != -1 else sys_content
            
            def agent_task(agent_name):
                try:
                    with open(f"src/agents_text/ETF_agents/{agent_name}.txt", "r", encoding="utf-8") as f:
                        agent_persona = f.read()
                    agent_sys_content = f"{agent_persona}\n\n====================\n以下是系统级硬性约束，你必须严格遵守：\n{format_rules}"
                    return get_LLM_message(system_content=agent_sys_content, user_message=user_msg, model_id=committee_model)
                except Exception as e:
                    return f"该大师 ({agent_name}) 分析失败：{e}"

            print(f"⏳ 开始呼叫 {len(committee_agents)} 位投资大师并发分析...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
                futures = {executor.submit(agent_task, agent_name): agent_name for agent_name in committee_agents}
                for future in concurrent.futures.as_completed(futures):
                    agent_name = futures[future]
                    try:
                        committee_results[agent_name] = future.result()
                        print(f"   ✅ 大师 [{agent_name.replace('_', ' ')}] 意见已送达！")
                    except Exception as e:
                        committee_results[agent_name] = f"分析失败：{e}"
            
            # 构建总监裁判提示词 
            judge_msg = f"{user_msg}\n\n"
            judge_msg += "=================================\n"
            judge_msg += "【投资总监（AI裁判）专属决议指令】\n"
            judge_msg += "以上是客观标的数据。以下是你的多位顶级研究员（不同流派的投资大师）针对该 ETF 给出的独立分析和 JSON 报告：\n\n"
            
            for agent_name, res in committee_results.items():
                display_name = agent_name.replace("_", " ")
                judge_msg += f"--- 投资大师：{display_name} 的意见 ---\n{res}\n\n"
                
            judge_msg += "作为量化基金的投资总监，你拥有最终拍板权。请严格按照以下【核心裁判原则】进行综合决策：\n"
            judge_msg += "1. 事实核查先行（零容忍数据幻觉）：必须先核对大师引用的数据是否与上文提供的【客观标的数据】（尤其是估值分位、宏观定调、场内申赎份额变动）完全一致。对于虚构数据的结论，直接一票否决。\n"
            judge_msg += "2. 流派交叉验证与非共识捕捉：重点审视大师之间的【分歧点】与【共振点】。例如，当宏观派（如达利欧）定调周期向好，且趋势派（如欧奈尔）也提示放量突破时，置信度极高；若逆向派（马克斯）提示处于低估击球区，但风控派（琼斯）警告跌破200日均线，你需要明确裁决：当前是采取左侧分批网格低吸，还是等待右侧趋势确认。\n"
            judge_msg += "3. 申赎资金的终极底牌：ETF 场内份额的净申赎是看透主力意图的底牌。在左侧阴跌中看到连续大额净申购是主力（国家队/机构）托底护盘；在右侧暴涨中看到净赎回是主力套现。切勿忽略这一核心数据的指引。\n"
            judge_msg += "4. 拒绝无效瘫痪（果断决策）：不要因为大师存在分歧就本能地退缩到‘观望’。在剔除幻觉意见后，评估盈亏比，勇敢给出具体的买入/卖出、观望指令和明确的点位。\n\n"

            if history_str:
                judge_msg += f"=================================\n【总监个人历史记忆库】\n系统调取了你（总监）前几日对该 ETF 做出的深度推演，请以此作为连贯性参考：\n{history_str}\n\n（特别注意：请对照最新大师意见与今日最新盘面，审视你原先的宏观定调和策略逻辑是否被证伪。保持体系的连贯性；若周期发生根本反转，请果断纠错！）\n\n"

            judge_msg += "请给出最终决策。你必须在 JSON 的 '原因' 字段中分段输出：\n"
            judge_msg += "【事实核查与幻觉剔除】：简述是否有大师引用了错误数据。\n"
            judge_msg += "【流派观点交锋】：简述各流派有效观点的分歧与共鸣点。\n"
            judge_msg += "【总监拍板逻辑】：结合最新盘面（申赎动向/宏观环境）与你的历史记忆，详细说明最终裁决理由。\n"
            judge_msg += "注意：你的输出必须是一个单一的、严格符合原定系统提示词规范的 JSON 对象！\n"

            if set_progress: set_progress(f"⚖️ 步骤 5/5: 正在呼叫总监模型 ({pro_model}) 进行最终综合裁决...")
            print(f"⏳ 开始呼叫总监模型: {pro_model}...")
            res_text = get_LLM_message(system_content=sys_content, user_message=judge_msg, model_id=pro_model)
            model_tag = f"MoA-{len(committee_agents)}大师-{pro_model}"
            
        else:
            final_user_msg = user_msg
            if history_str:
                final_user_msg += f"\n\n=================================\n### 【总监个人历史记忆库】\n前几日你对该标的的决策与逻辑如下：\n{history_str}\n\n（特别注意：请结合今日最新盘面，评估原逻辑是否被证伪，保持体系连贯或果断纠错。）\n"

            if set_progress: set_progress(f"⚖️ 步骤 4/4: 正在呼叫 Pro 模型 ({pro_model}) 进行深度推演...")
            res_text = get_LLM_message(system_content=sys_content, user_message=final_user_msg, model_id=pro_model)
            model_tag = f"D-{flash_model}-{pro_model}" if dual_filter else pro_model

    # ==========================================
    # 阶段 5：解析、落盘与返回
    # ==========================================
    model_configs = get_model_config()
    if model_tag.startswith("MoA-"):
        parts = model_tag.split("-")
        disp_model = f"【决议】{model_configs.get(parts[2], {}).get('name', parts[2])}" if len(parts) >= 3 else model_tag
    elif model_tag.startswith("D-"):
        parts = model_tag.split("-")
        disp_model = f"{model_configs.get(parts[2], {}).get('name', parts[2])}(双筛)" if len(parts) >= 3 else model_tag
    else:
        disp_model = model_configs.get(model_tag, {}).get('name', model_tag)

    os.makedirs(f"output_etf/{current_date_str}", exist_ok=True)
    with open(f"output_etf/{current_date_str}/{etf_code}_{safe_s_name}_output_{model_tag}_{current_date_str}.txt", 'w', encoding='utf-8') as f: 
        f.write(res_text) 

    parsed = parse_llm_json(res_text)

    buy_p = parsed.get("buy_p") or parsed.get("建议买入价")
    sell_p = parsed.get("sell_p") or parsed.get("目标卖出价")
    stop_p = parsed.get("stop_p") or parsed.get("建议止损价")
    
    rr_str = 'N/A'
    try:
        if buy_p and sell_p and stop_p and float(buy_p) - float(stop_p) > 0: 
            rr_str = f"{(float(sell_p) - float(buy_p)) / (float(buy_p) - float(stop_p)):.2f}:1"
    except: pass

    csv_path = f"output_etf/{current_date_str}/ETF_Daily_Table_{current_date_str}.csv"
    pd.DataFrame([{
        "ETF代码": etf_code, "ETF名称": s_name, "决策模型": disp_model, "当前价格": s_price, 
        "预期": parsed.get("expectation") or parsed.get("预期", "N/A"), 
        "操作": parsed.get("action") or parsed.get("操作", "N/A"), 
        "建议仓位": parsed.get("pos_adv") or parsed.get("建议仓位", "N/A"), 
        "置信度": parsed.get("confidence") or parsed.get("置信度", "N/A"), 
        "建议买入价": str(buy_p) if buy_p else "-", 
        "目标卖出价": str(sell_p) if sell_p else "-", 
        "建议止损价": str(stop_p) if stop_p else "-", 
        "回报风险比": rr_str
    }]).to_csv(csv_path, index=False, header=not os.path.exists(csv_path), mode='a', encoding='utf-8-sig')

    # 🌟 等待深度研读后台线程执行完毕
    deep_thread.join()

    return df_chart, s_name, s_price, parsed, disp_model, user_msg, res_text, deep_analysis_result[0]