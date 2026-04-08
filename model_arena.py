# -*- coding: utf-8 -*-
import os
import time
import logging
import re
import json
import random
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from dotenv import load_dotenv
import pandas as pd
import baostock as bs

# 屏蔽底层日志
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
load_dotenv()

from src.LLM_chat import get_LLM_message, get_model_config
from src.data_crawler import get_stock_data, get_stock_name_bs, get_chart_data, get_30m_chart_data
from src.utils import fetch_news_safely, parse_llm_json
from src.financial_analyzer import download_specific_report_api, slice_financial_report_pdf

# ==========================================
# 阶段 1：全局数据统一获取 (主进程防重复请求)
# ==========================================
def prepare_global_context(stock_code, current_date_str):
    print(f"\n📥 [Arena] 正在统一拉取 [{stock_code}] 全局上下文数据...")
    bs.login()
    try:
        c_date = datetime.strptime(current_date_str, "%Y-%m-%d").date() 
        end = c_date.strftime("%Y%m%d") 
        beg = (c_date - timedelta(days=720)).strftime("%Y%m%d")
        
        s_name = get_stock_name_bs(stock_code)
        safe_s_name = re.sub(r'[\\/:*?"<>|]', '', s_name)
        
        df_chart = get_chart_data(stock_code, beg, end)
        df_30m = get_30m_chart_data(stock_code, beg, end)
        in_str = get_stock_data(stock_code=stock_code, beg=beg, end=end, current_date=current_date_str)
        news_titles = fetch_news_safely(stock_code, safe_s_name, current_date_str)
        
        daily_str = df_chart.tail(24).to_markdown(index=False) if not df_chart.empty else "暂无"
        df_30m_str = df_30m.tail(24).to_markdown(index=False) if not df_30m.empty else "暂无"
        
        user_msg = f"""基于获得的以下数据和新闻消息，做出你的交易决策。\n\n{in_str}\n\n最近二十四个 30分钟 K线数据如下：\n{df_30m_str}\n\n最近二十四个交易日数据如下：\n{daily_str}\n\n相关新闻如下：\n{news_titles}\n\n当前该股仓位：0 %\n当前持仓成本: 0 元\n\n请记住，行动必须是买入、卖出、持有或观望。"""

        # 财报智能切片
        curr_fin_text, prev_fin_text = "", ""
        match = re.search(r"最新财务报告期:\s*([0-9\-]+)", in_str)
        if match:
            report_date_raw = match.group(1).replace("-", "").strip()
            year = report_date_raw[:4]
            mmdd = report_date_raw[4:8]
            prev_year = str(int(year) - 1)
            
            curr_cat = "年报" if mmdd == "1231" else ("半年报" if mmdd == "0630" else ("一季报" if mmdd == "0331" else "三季报"))
            is_annual = True if curr_cat in ["年报", "半年报"] else False
            
            pdf_dir = "log/financial_pdfs"
            curr_pdf = download_specific_report_api(stock_code, year, curr_cat, pdf_dir)
            curr_fin_text = slice_financial_report_pdf(curr_pdf, is_annual) if curr_pdf else ""
            
            prev_pdf = download_specific_report_api(stock_code, prev_year, "年报", pdf_dir)
            prev_fin_text = slice_financial_report_pdf(prev_pdf, True) if prev_pdf else ""

        print("✅ [Arena] 全局数据准备完毕，进入高并发沙盒！")
        return user_msg, curr_fin_text, prev_fin_text, s_name
    finally:
        bs.logout()

# ==========================================
# 阶段 2：单模型全管线突围 (含防并发风暴机制)
# ==========================================
def process_single_model_pipeline(model_id, display_name, user_msg, curr_fin, prev_fin, s_name, agents, sys_content):
    # 🌟 防并发风暴 (Jitter)：随机延迟 0-3 秒起步，避免所有进程在同一毫秒砸向 API 
    time.sleep(random.uniform(0.1, 3.0))
    print(f"⏳ [{display_name}] 启动推演链路...")
    
    start_time = time.time()
    trace_log = {"filter": "", "financial": "", "moa": {}, "judge": ""}
    
    try:
        # 1. 基础初筛
        trace_log["filter"] = get_LLM_message(system_content=sys_content, user_message=user_msg, model_id=model_id)
        
        # 2. 财报研读
        if curr_fin:
            from src.financial_analyzer import generate_report_summary_with_llm
            os.environ["FINANCIAL_MODEL"] = model_id 
            fin_summary = generate_report_summary_with_llm(curr_fin, prev_fin, "财报", s_name)
            trace_log["financial"] = fin_summary
            user_msg += f"\n\n=================================\n### 【深度财报解析】\n{fin_summary}\n"

        # 3. 大师议事 (MoA) - 内部继续线程并发控制
        import concurrent.futures
        format_idx = sys_content.find("【决策过程与输出规范】")
        format_rules = sys_content[format_idx:] if format_idx != -1 else sys_content
        
        def agent_task(agent_name):
            with open(f"src/agents_text/{agent_name}.txt", "r", encoding="utf-8") as f:
                agent_persona = f.read()
            agent_sys = f"{agent_persona}\n\n====================\n以下是系统级硬性约束：\n{format_rules}"
            time.sleep(random.uniform(0.1, 1.0)) # 大师内部抖动
            return get_LLM_message(system_content=agent_sys, user_message=user_msg, model_id=model_id)

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(agents), 3)) as executor:
            futures = {executor.submit(agent_task, ag): ag for ag in agents}
            for future in concurrent.futures.as_completed(futures):
                trace_log["moa"][futures[future]] = future.result()

        # 4. 总监裁决
        judge_msg = f"{user_msg}\n\n【大师意见】\n"
        for ag, res in trace_log["moa"].items():
            judge_msg += f"--- {ag} ---\n{res}\n\n"
        judge_msg += "作为投资总监，请结合上述信息给出最终裁判意见，必须输出严格的 JSON。"
        
        trace_log["judge"] = get_LLM_message(system_content=sys_content, user_message=judge_msg, model_id=model_id)
        
        # 收尾解析
        cost_time = time.time() - start_time
        parsed = parse_llm_json(trace_log["judge"])
        action = parsed.get("action") or parsed.get("操作", "解析失败")
        confidence = parsed.get("confidence") or parsed.get("置信度", "N/A")
        
        print(f"✅ [{display_name}] 冲线完成! 耗时: {cost_time:.1f}s | 决策: {action}")
        return {
            "Model": display_name, "Status": "Success", "Time(s)": round(cost_time, 2), 
            "Action": action, "Confidence": confidence, "Trace": trace_log
        }
    except Exception as e:
        print(f"❌ [{display_name}] 发生致命异常: {e}")
        return {"Model": display_name, "Status": "Failed", "Time(s)": round(time.time() - start_time, 2), "Action": "Error", "Confidence": "N/A", "Trace": trace_log}

# ==========================================
# 阶段 3：超可读战报与资产沉淀
# ==========================================
def generate_enterprise_report(results, run_dir, stock_code, target_date):
    """生成带折叠面板的可读 Markdown 及结构化 JSON 存档"""
    md_path = os.path.join(run_dir, "Arena_Leaderboard.md")
    json_path = os.path.join(run_dir, "Arena_Trace_Data.json")
    
    # --- 1. 结构化存档 ---
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
        
    # --- 2. Markdown 生成 ---
    valid_results = [r for r in results if r["Status"] == "Success"]
    
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# 🏆 AI Quant 竞技场 - 全管线横向评测\n\n")
        f.write(f"**测试标的:** `{stock_code}` | **逻辑日期:** `{target_date}`\n\n")
        
        # 总览表格
        f.write("## 🏁 0. 最终决策总览\n\n")
        df_summary = pd.DataFrame([{k: v for k, v in r.items() if k != "Trace"} for r in results]).sort_values(by="Time(s)")
        f.write(df_summary.to_markdown(index=False) + "\n\n")
        
        if not valid_results:
            f.write("> ⚠️ 所有参赛模型均运行失败，无阶段比对数据。\n")
            return
            
        # -- 横向切片 1：财报提取 --
        f.write("---\n## 📖 1. 财报研读能力校验 (Financial Analysis)\n")
        f.write("> 评估标准：提取上期承诺是否精准？业绩归因是否客观？\n\n")
        for res in valid_results:
            f.write(f"### 🤖 {res['Model']}\n")
            f.write("<details>\n<summary>👉 点击展开查看完整 JSON 分析</summary>\n\n")
            f.write("```json\n" + res["Trace"].get("financial", "暂无输出") + "\n```\n\n</details>\n\n")
            
        # -- 横向切片 2：MoA 人格分裂度 --
        f.write("---\n## 🎭 2. MoA 大师视角演绎 (Persona Fidelity)\n")
        f.write("> 评估标准：在完全相同的客观数据下，不同大模型对不同流派风格的遵循度和推演深度。\n\n")
        agents = valid_results[0]["Trace"]["moa"].keys() if valid_results else []
        for ag in agents:
            f.write(f"### 🧙‍♂️ 大师: {ag.replace('_', ' ')}\n")
            for res in valid_results:
                parsed_ag = parse_llm_json(res["Trace"]["moa"].get(ag, "{}"))
                reason = parsed_ag.get("reasoning") or parsed_ag.get("原因", "*(未有效提取到逻辑)*")
                f.write(f"- **{res['Model']}**: {reason}\n")
            f.write("\n")
            
        # -- 横向切片 3：总监裁决逻辑 --
        f.write("---\n## ⚖️ 3. 投资总监终审 (Director Judgment)\n")
        f.write("> 评估标准：总监是否成功融合了上方的大师分歧？抗幻觉能力如何？\n\n")
        for res in valid_results:
            f.write(f"### 🤖 {res['Model']} (决策: **{res['Action']}**)\n")
            parsed_judge = parse_llm_json(res["Trace"].get("judge", "{}"))
            reason = parsed_judge.get("reasoning") or parsed_judge.get("原因", "*(未提取到总监逻辑)*")
            f.write(f"> {reason}\n\n")
            f.write("<details>\n<summary>🔍 查看总监输出源码</summary>\n\n")
            f.write("```json\n" + res["Trace"].get("judge", "") + "\n```\n\n</details>\n\n")
            
    print(f"\n📄 精装多维战报已生成: {md_path}")
    print(f"💾 原始资产已归档至: {json_path}")

def run_pipeline_arena(stock_code, target_date, test_models, agents, max_workers=4):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join("arena_results", f"Pipeline_Run_{timestamp}")
    os.makedirs(run_dir, exist_ok=True)

    with open('LLM system content.txt', 'r', encoding='utf-8') as f:
        sys_content = f.read()

    # 主进程备料
    user_msg, curr_fin, prev_fin, s_name = prepare_global_context(stock_code, target_date)
    configs = get_model_config()
    results = []

    print(f"\n🚀 统一数据下发完毕，进入高并发推演沙盒 (最大进程数: {max_workers})...")
    
    # 核心并发执行
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for model_id in test_models:
            display_name = configs.get(model_id, {}).get('name', model_id)
            future = executor.submit(
                process_single_model_pipeline, 
                model_id, display_name, user_msg, curr_fin, prev_fin, s_name, agents, sys_content
            )
            futures[future] = display_name
            
        for future in as_completed(futures):
            results.append(future.result())

    # 生成专业报告
    generate_enterprise_report(results, run_dir, stock_code, target_date)

if __name__ == "__main__":
    # --- 竞技场配置区域 ---
    TARGET_STOCK = "002714"          # 测试标的
    TARGET_DATE = "2026-04-08"       # 测试逻辑日期
    
    # 精选两位流派迥异的大师，既能测试 MoA 冲突处理，又控制 Token 消耗
    MOA_AGENTS = ["Richard_Wyckoff", "Charlie_Munger"] 
    
    MODELS_TO_TEST = [
        "gemini_flash",
        "gemini_pro",
        "deepseek_v32",
        "kimi_k25",
        "glm_5",
    ]
    
    # ⚠️ 启动竞技场 (建议 max_workers 设为 3-5 即可，太高容易被各大厂同时触发 Rate Limit，且可能导致 Baostock 熔断)
    run_pipeline_arena(TARGET_STOCK, TARGET_DATE, MODELS_TO_TEST, MOA_AGENTS, max_workers=1)