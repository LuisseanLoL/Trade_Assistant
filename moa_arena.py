import os
import json
import time
from dotenv import load_dotenv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# 在所有逻辑开始前，强制加载根目录的 .env 文件
load_dotenv()

from src.LLM_chat import get_LLM_message, get_model_config

def process_single_model(model_id, display_name, system_content, user_message, run_dir, role_prefix=""):
    """
    单个模型的工作线程任务，新增 role_prefix 用于区分研究员和裁判的输出文件
    """
    safe_filename = f"{role_prefix}{display_name.replace(':', '_').replace('/', '_')}"
    print(f"⏳ [{role_prefix}{display_name} ({model_id})] 已启动，正在思考与生成...")
    
    start_time = time.time()
    result_text = ""
    try:
        # 调用底层 API
        result_text = get_LLM_message(
            system_content=system_content,
            user_message=user_message,
            model_id=model_id
        )
        
        cost_time = time.time() - start_time
        print(f"✅ [{role_prefix}{display_name}] 完成! 耗时: {cost_time:.2f}s")
        
        # 清洗与保存结果
        output_path = os.path.join(run_dir, f"{safe_filename}.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            try:
                clean_result = result_text.strip()
                if clean_result.startswith('```json'):
                    clean_result = clean_result[7:-3].strip()
                elif clean_result.startswith('```'):
                    clean_result = clean_result[3:-3].strip()

                json_result = json.loads(clean_result)
                json.dump(json_result, f, ensure_ascii=False, indent=4)
                
            except json.JSONDecodeError:
                # 发生幻觉或格式崩溃，回退保存为纯文本
                f.write(result_text)
                print(f"⚠️ [{role_prefix}{display_name}] 警告: 输出格式非标准 JSON，已回退保存为纯文本。")
                
    except Exception as e:
        print(f"❌ [{role_prefix}{display_name}] 运行失败: {e}")
        result_text = f"该模型运行失败: {e}"

    # 返回结果供后续 MoA 裁判环节使用
    return model_id, display_name, result_text


def run_moa_arena(system_file, input_file, researchers, judges, output_dir="arena_results", max_workers=8):
    """
    运行 MoA (多模型议事) 竞技场，分两阶段测试裁判模型的能力
    """
    print("📥 正在读取 System Prompt 和 User Input...")
    try:
        with open(system_file, 'r', encoding='utf-8') as f:
            system_content = f.read()
        with open(input_file, 'r', encoding='utf-8') as f:
            base_user_message = f.read()
    except FileNotFoundError as e:
        print(f"❌ 文件读取失败: {e}")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(output_dir, f"MoA_run_{timestamp}")
    os.makedirs(run_dir, exist_ok=True)
    
    configs = get_model_config()
    if not configs:
        print("❌ 未在环境变量中找到模型配置！")
        return

    # 验证参赛模型
    valid_researchers = [m for m in researchers if m in configs]
    valid_judges = [m for m in judges if m in configs]

    if not valid_researchers or not valid_judges:
        print("❌ 缺少有效的研究员或裁判模型，请检查配置！")
        return

    print(f"🚀 欢迎来到 MoA Model Arena!")
    print(f"👨‍💻 研究员阵容 ({len(valid_researchers)}): {valid_researchers}")
    print(f"⚖️ 裁判员阵容 ({len(valid_judges)}): {valid_judges}")
    print("-" * 50)
    
    # ================= 阶段 1：获取研究员独立意见 =================
    print("▶️ [Phase 1] 启动研究员并发推演...")
    researcher_results = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for mid in valid_researchers:
            disp_name = configs[mid].get('name', mid)
            future = executor.submit(process_single_model, mid, disp_name, system_content, base_user_message, run_dir, "R_")
            futures[future] = disp_name
            
        for future in as_completed(futures):
            try:
                m_id, d_name, r_text = future.result()
                researcher_results[d_name] = r_text
            except Exception as exc:
                print(f"🚨 研究员线程引发异常: {exc}")

    # ================= 阶段 2：组装裁判提示词并启动裁判 =================
    print("-" * 50)
    print("▶️ [Phase 2] 正在组装案件卷宗，呼叫裁判委员会...")
    
    # 构建 Meta-Prompt
    judge_msg = f"{base_user_message}\n\n"
    judge_msg += "=================================\n"
    judge_msg += "【投资总监（AI裁判）专属决议指令】\n"
    judge_msg += "以上是客观标的数据。以下是你的多位顶级研究员（不同AI模型）针对该数据给出的独立分析和 JSON 报告：\n\n"
    
    for r_name, r_content in researcher_results.items():
        judge_msg += f"--- 研究员模型：{r_name} 的意见 ---\n{r_content}\n\n"
        
    judge_msg += "作为量化基金的投资总监，你拥有最终拍板权。请严格按照以下【核心裁判原则】进行综合决策：\n"
    judge_msg += "1. 事实核查先行（零容忍数据幻觉）：必须先核对研究员引用的数据是否与上文提供的【客观标的数据】完全一致。对于任何基于虚构数据得出的结论，必须直接一票否决。\n"
    judge_msg += "2. 寻找非共识的正确：重点审视研究员之间的【分歧点】。如果少数派指出了隐含的风控隐患，且多数派未能有效应对，应果断采纳少数派意见。\n"
    judge_msg += "3. 拒绝无效瘫痪（果断决策）：不要因为存在分歧就本能地退缩到‘观望’。在剔除幻觉意见后，评估盈亏比，勇敢给出具体的买入/卖出、观望指令和点位。\n\n"
    judge_msg += "请给出最终决策。你必须在 JSON 的 '原因' 字段中分段输出：\n"
    judge_msg += "【事实核查与幻觉剔除】：简述是否有研究员引用了错误数据。\n"
    judge_msg += "【共识与核心分歧】：简述各方有效观点的交锋点。\n"
    judge_msg += "【总监拍板逻辑】：详细说明你最终支持哪一方的深度理由。\n"
    judge_msg += "注意：你的输出必须是一个单一的、严格符合原定系统提示词规范的 JSON 对象！\n"

    # 并发运行裁判模型
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for mid in valid_judges:
            disp_name = configs[mid].get('name', mid)
            future = executor.submit(process_single_model, mid, disp_name, system_content, judge_msg, run_dir, "Judge_")
            futures[future] = disp_name
            
        for future in as_completed(futures):
            try:
                future.result() 
            except Exception as exc:
                print(f"🚨 裁判线程引发异常: {exc}")

    print("-" * 50)
    print(f"🎉 MoA 评测并发执行完成！所有报告（R_研究员 与 Judge_裁判）已保存至: {run_dir}")

if __name__ == "__main__":
    # --- 配置区域 ---
    sys_prompt_file = "LLM system content.txt"
    user_input_file = r"input\2026-03-08\600732_爱旭股份_input_2026-03-08.txt" 
    
    # 设定在第一阶段干活的“研究员”模型（建议异构，比如一大一小，一开源一闭源）
    researcher_models = [
        "gemini_pro",
        "deepseek_v32",
        "kimi_k25",
        "glm_5"
    ]
    
    # 设定在第二阶段进行拍板的“裁判”模型（建议使用推理能力最强的超大杯模型）
    judge_models = [
        "gemini_pro",
        "deepseek_v32",
        "kimi_k25",
        "glm_5"
    ]
    
    # 启动 MoA 竞技场
    run_moa_arena(
        system_file=sys_prompt_file, 
        input_file=user_input_file, 
        researchers=researcher_models, 
        judges=judge_models, 
        max_workers=4
    )