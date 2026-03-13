import os
import json
import time
import glob
from dotenv import load_dotenv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# 在所有逻辑开始前，强制加载根目录的 .env 文件
load_dotenv()

# 导入你新修改的 LLM_chat 函数
from src.LLM_chat import get_LLM_message, get_model_config

# ==============================================================================
# 通用 JSON 输出规范 (自动附加在每个角色提示词末尾，确保程序解析不崩溃)
# ==============================================================================
COMMON_OUTPUT_FORMAT = """
【决策过程与输出规范】
1. 综合全盘信息，确定你当前采取的时间周期与交易策略。
2. 即使操作决定是“观望”或“持有”，也必须在“预期”中给出明确的多空态度。
3. 请以严格的 JSON 格式输出，内容必须包含：
   - "操作": "买入" | "卖出" | "持有" | "观望"
   - "建议仓位": <0到100之间的整数，代表建议配置的资金百分比>
   - "置信度": <0到1之间的浮点数，代表对该决策的把握程度>
   - "建议买入价": <如买入或逢低建仓的建议挂单价，若无计划可为null>
   - "目标卖出价": <止盈目标价，数值>
   - "建议止损价": <绝对止损价，数值，严控风险>
   - "各种信号": <必须包含 情绪分析、估值分析、基本面分析、技术分析、量化分析 五个维度的对象，每个维度需包含"信号"、"解析"及"置信度">
   - "原因": <深度解释你的决策逻辑及核心驱动力>
   - "预期": "强烈看多" | "偏多" | "震荡" | "偏空" | "强烈看空"

【硬性交易规则】
- 只有在投资组合中当前持有该股票（持仓>0）的情况下才能下达“卖出”或“持有”指令。
"""

def load_personas_from_dir(directory_path="src/agents_text", common_suffix=COMMON_OUTPUT_FORMAT):
    """
    动态从指定文件夹读取所有的角色 txt 文件，并附加统一的输出格式
    """
    personas_dict = {}
    if not os.path.exists(directory_path):
        print(f"❌ 错误: 找不到角色文件夹 '{directory_path}'，请创建并放入 .txt 文件。")
        return personas_dict

    txt_files = glob.glob(os.path.join(directory_path, "*.txt"))
    if not txt_files:
        print(f"⚠️ 警告: 文件夹 '{directory_path}' 中没有找到任何 .txt 文件。")
        return personas_dict

    for filepath in txt_files:
        # 使用不带后缀的文件名作为角色名
        persona_name = os.path.splitext(os.path.basename(filepath))[0]
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            # 自动拼接统一的 JSON 输出规范
            personas_dict[persona_name] = content + "\n\n" + common_suffix
            print(f"✅ 成功加载角色: {persona_name}")
        except Exception as e:
            print(f"❌ 读取角色文件 {filepath} 失败: {e}")
            
    return personas_dict


def process_single_model_and_persona(persona_name, model_id, display_name, system_content, user_message, run_dir):
    """
    单个模型+单个角色的工作线程任务
    """
    safe_model_name = f"{display_name.replace(':', '_').replace('/', '_')}"
    safe_persona_name = f"{persona_name.replace(' ', '_')}"
    # 文件命名融合角色和模型名称
    file_name = f"{safe_persona_name}_{safe_model_name}"
    
    print(f"⏳ [{persona_name} | {display_name}] 已启动，正在思考与生成...")
    
    start_time = time.time()
    try:
        # 调用底层 API
        result = get_LLM_message(
            system_content=system_content,
            user_message=user_message,
            model_id=model_id
        )
        
        cost_time = time.time() - start_time
        print(f"✅ [{persona_name} | {display_name}] 完成! 耗时: {cost_time:.2f}s")
        
        # 清洗与保存结果
        output_path = os.path.join(run_dir, f"{file_name}.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            try:
                clean_result = result.strip()
                if clean_result.startswith('```json'):
                    clean_result = clean_result[7:-3].strip()
                elif clean_result.startswith('```'):
                    clean_result = clean_result[3:-3].strip()

                json_result = json.loads(clean_result)
                json.dump(json_result, f, ensure_ascii=False, indent=4)
                
            except json.JSONDecodeError:
                # 发生幻觉或格式崩溃，回退保存为纯文本
                f.write(result)
                print(f"⚠️ [{persona_name} | {display_name}] 警告: 输出格式非标准 JSON，已回退保存为纯文本。")
                
    except Exception as e:
        print(f"❌ [{persona_name} | {display_name}] 运行失败: {e}")


def run_model_arena(personas_dict, input_file, test_models=None, output_dir="arena_results", max_workers=8):
    """
    运行 Model Arena 进行多模型、多角色的并发对比测试
    """
    print("\n📥 正在读取 User Input...")
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            user_message = f.read()
    except FileNotFoundError as e:
        print(f"❌ 文件读取失败: {e}")
        return

    # 准备输出目录
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(output_dir, f"run_{timestamp}")
    os.makedirs(run_dir, exist_ok=True)
    
    # 从系统配置中拉取模型注册表
    configs = get_model_config()
    if not configs:
        print("❌ 未在环境变量中找到 ACTIVE_MODELS 配置！请检查 .env 文件。")
        return
        
    # 确定参赛阵容
    if test_models is None:
        test_queue = list(configs.keys())
    else:
        test_queue = [m for m in test_models if m in configs]
        missing = [m for m in test_models if m not in configs]
        if missing:
            print(f"⚠️ 警告: 以下模型未在 .env 中配置，将被跳过参赛: {missing}")

    if not test_queue or not personas_dict:
        print("❌ 没有可用的模型或角色进行测试！")
        return

    total_tasks = len(personas_dict) * len(test_queue)
    print(f"\n🚀 欢迎来到 Model Arena 交叉测试!")
    print(f"🎭 参赛角色数: {len(personas_dict)}")
    print(f"🤖 参赛模型数: {len(test_queue)}")
    print(f"📊 总测试任务数: {total_tasks}")
    print(f"⚡ 启动并发测试，最大线程数: {max_workers}")
    print("-" * 50)
    
    # 使用 ThreadPoolExecutor 并发执行双重循环
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        # 外层循环：遍历不同角色
        for persona_name, system_content in personas_dict.items():
            # 内层循环：遍历不同模型
            for model_id in test_queue:
                model_info = configs[model_id]
                display_name = model_info.get('name', model_id)
                
                future = executor.submit(
                    process_single_model_and_persona, 
                    persona_name, model_id, display_name, system_content, user_message, run_dir
                )
                futures[future] = f"[{persona_name} | {display_name}]"
            
        # 等待并收集结果
        for future in as_completed(futures):
            task_name = futures[future]
            try:
                future.result() 
            except Exception as exc:
                print(f"🚨 {task_name} 线程执行引发异常: {exc}")

    print("-" * 50)
    print(f"🎉 交叉评测执行完成！所有报告已保存至: {run_dir}")

if __name__ == "__main__":
    # 1. 从文件夹动态加载所有角色提示词
    test_personas = load_personas_from_dir("src/agents_text")
    
    if test_personas:
        # 2. 配置输入文件和参赛模型
        user_input_file = r"input\2026-03-10\002624_完美世界_input_2026-03-10.txt" 
        
        models_to_test = [
        # "gemini_flash",
        "gemini_pro",
        # "qwen_9b",
        # "qwen_35b",
        # "kimi_k25",
        # "deepseek_v32",
        # "glm_5",
        # "minimax_m25"
    ] # 可以根据你的 .env 随意增减
        
        # 3. 启动竞技场
        run_model_arena(test_personas, user_input_file, test_models=models_to_test, max_workers=4)