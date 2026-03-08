import os
import json
import time
from dotenv import load_dotenv
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# 在所有逻辑开始前，强制加载根目录的 .env 文件
load_dotenv()

# 导入你新修改的 LLM_chat 函数
from src.LLM_chat import get_LLM_message, get_model_config

def process_single_model(model_id, display_name, system_content, user_message, run_dir):
    """
    单个模型的工作线程任务
    """
    safe_filename = f"{display_name.replace(':', '_').replace('/', '_')}"
    print(f"⏳ [{display_name} ({model_id})] 已启动，正在思考与生成...")
    
    start_time = time.time()
    try:
        # 调用底层 API
        result = get_LLM_message(
            system_content=system_content,
            user_message=user_message,
            model_id=model_id
        )
        
        cost_time = time.time() - start_time
        print(f"✅ [{display_name}] 完成! 耗时: {cost_time:.2f}s")
        
        # 清洗与保存结果
        output_path = os.path.join(run_dir, f"{safe_filename}.json")
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
                print(f"⚠️ [{display_name}] 警告: 输出格式非标准 JSON，已回退保存为纯文本。")
                
    except Exception as e:
        print(f"❌ [{display_name}] 运行失败: {e}")

def run_model_arena(system_file, input_file, test_models=None, output_dir="arena_results", max_workers=8):
    """
    运行 Model Arena 进行多模型并发对比测试
    :param test_models: list, 需要参赛的 model_id 列表。
    :param max_workers: int, 最大并发线程数。
    """
    # 1. 读取统一的提示词和输入
    print("📥 正在读取 System Prompt 和 User Input...")
    try:
        with open(system_file, 'r', encoding='utf-8') as f:
            system_content = f.read()
        with open(input_file, 'r', encoding='utf-8') as f:
            user_message = f.read()
    except FileNotFoundError as e:
        print(f"❌ 文件读取失败: {e}")
        return

    # 2. 准备输出目录
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(output_dir, f"run_{timestamp}")
    os.makedirs(run_dir, exist_ok=True)
    
    # 3. 从系统配置中拉取模型注册表
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

    if not test_queue:
        print("❌ 没有可用的模型进行测试！")
        return

    print(f"🚀 欢迎来到 Model Arena! 本次共有 {len(test_queue)} 个模型参赛。")
    print(f"⚡ 启动并发测试，最大线程数: {max_workers}")
    print("-" * 50)
    
    # 4. 使用 ThreadPoolExecutor 并发执行
    # 为了避免过多并发触发 API 限流 (Rate Limit)，可以通过 max_workers 控制并发量
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务到线程池
        futures = {}
        for model_id in test_queue:
            model_info = configs[model_id]
            display_name = model_info.get('name', model_id)
            
            future = executor.submit(
                process_single_model, 
                model_id, display_name, system_content, user_message, run_dir
            )
            futures[future] = display_name
            
        # 等待任务完成并收集结果（as_completed 会在任务完成时立即 yield）
        for future in as_completed(futures):
            display_name = futures[future]
            try:
                # 捕获线程内可能抛出的未处理异常
                future.result() 
            except Exception as exc:
                print(f"🚨 [{display_name}] 线程执行引发异常: {exc}")

    print("-" * 50)
    print(f"🎉 评测并发执行完成！所有报告已保存至: {run_dir}")

if __name__ == "__main__":
    # --- 配置区域 ---
    sys_prompt_file = "LLM system content.txt"
    user_input_file = r"input\2026-03-08\002170_芭田股份_input_2026-03-08.txt" 
    
    models_to_test = [
        "gemini_flash",
        "gemini_pro",
        "qwen_9b",
        "qwen_35b",
        "kimi_k25",
        "deepseek_v32",
        "glm_5",
        "minimax_m25"
    ]
    
    # 启动竞技场 (推荐线程数设置在 5-10 之间，避免被厂商 API 限流)
    run_model_arena(sys_prompt_file, user_input_file, test_models=models_to_test, max_workers=8)