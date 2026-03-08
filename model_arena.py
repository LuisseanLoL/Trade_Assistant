import os
import json
import time
from dotenv import load_dotenv
from datetime import datetime

# 在所有逻辑开始前，强制加载根目录的 .env 文件
load_dotenv()

# 导入你新修改的 LLM_chat 函数
from src.LLM_chat import get_LLM_message, get_model_config

def run_model_arena(system_file, input_file, test_models=None, output_dir="arena_results"):
    """
    运行 Model Arena 进行多模型对比测试
    :param test_models: list, 需要参赛的 model_id 列表。如果为 None，则测试 .env 中 ACTIVE_MODELS 配置的所有模型。
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

    # 2. 准备输出目录（按时间戳新建文件夹，避免覆盖）
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
        # 如果不指定，直接全员参战
        test_queue = list(configs.keys())
    else:
        # 过滤出合法配置的模型
        test_queue = [m for m in test_models if m in configs]
        missing = [m for m in test_models if m not in configs]
        if missing:
            print(f"⚠️ 警告: 以下模型未在 .env 中配置，将被跳过参赛: {missing}")

    if not test_queue:
        print("❌ 没有可用的模型进行测试！")
        return

    print(f"🚀 欢迎来到 Model Arena! 本次共有 {len(test_queue)} 个模型参赛。")
    print("-" * 50)
    
    # 4. 循环测试并保存结果
    for model_id in test_queue:
        # 获取前端显示的友好名称，用来做文件名
        model_info = configs[model_id]
        display_name = model_info.get('name', model_id)
        # 替换特殊字符以确保可以作为合法的文件名
        safe_filename = f"{display_name.replace(':', '_').replace('/', '_')}"
        
        print(f"\n[{display_name} ({model_id})] 正在思考与生成...")
        start_time = time.time()
        
        try:
            # 【关键更新】现在只需传入 model_id，底层 API 鉴权和参数组装全部由 LLM_chat 自动完成
            result = get_LLM_message(
                system_content=system_content,
                user_message=user_message,
                model_id=model_id
            )
            
            cost_time = time.time() - start_time
            print(f"✅ [{display_name}] 完成! 耗时: {cost_time:.2f}s")
            
            # 5. 清洗与保存结果
            output_path = os.path.join(run_dir, f"{safe_filename}.json")
            with open(output_path, 'w', encoding='utf-8') as f:
                # 尝试将字符串解析为 JSON，保存为排版优美的格式
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

    print("-" * 50)
    print(f"🎉 评测完成！所有报告已保存至: {run_dir}")

if __name__ == "__main__":
    # --- 配置区域 ---
    
    # 1. 指定输入文件的路径
    sys_prompt_file = "LLM system content.txt"
    user_input_file = r"input\2026-03-08\600830_香溢融通_input_2026-03-08.txt" # 改成你实际想测试的 input 路径
    
    # 2. 指定参赛阵容（填写你在 .env 中 ACTIVE_MODELS 里定义的 ID 即可）
    # 设置为 None 即可一键跑完所有注册的模型
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
    
    # 启动竞技场
    run_model_arena(sys_prompt_file, user_input_file, test_models=models_to_test)