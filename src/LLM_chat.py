from openai import OpenAI
import re
import time
import os
from google import genai
from google.genai import types

output_schema = {
  "type": "json_schema",
  "json_schema": {
    "schema": {
        "type": "object",
        "properties": {
        "操作": {
            "type": "string",
            "enum": ["买入", "卖出", "持有", "观望"],
            "description": "交易操作类型：买入(建仓/加仓)，卖出(平仓/减仓)，持有(有持仓且看多)，观望(无持仓且等待)"
        },
        "建议仓位": {
            "type": ["number", "null"],
            "minimum": 0,
            "maximum": 100,
            "description": "建议配置的资金百分比(0-100的数字)，如建议半仓则为50，空仓或观望为0"
        },
        "置信度": {
            "type": "number",
            "minimum": 0.0,
            "maximum": 1.0,
            "description": "综合决策的置信度，0到1之间的小数"
        },
        "建议买入价": {
            "type": ["number", "null"],
            "minimum": 0.0,
            "description": "建议的买入价格，若无买入计划可为null"
        },
        "目标卖出价": {
            "type": ["number", "null"],
            "minimum": 0.0,
            "description": "短期目标止盈价格，若不适用可为null"
        },
        "建议止损价": {
            "type": ["number", "null"],
            "minimum": 0.0,
            "description": "建议的止损价格，严格控制风险"
        },
        "各种信号": {
            "type": "object",
            "properties": {
            "情绪分析": {
                "type": "object",
                "properties": {
                "信号": {"type": "string", "enum": ["看多", "看空", "中性"]},
                "解析": {"type": "string"},
                "置信度": {"type": "number", "minimum": 0.0, "maximum": 1.0}
                },
                "required": ["信号", "解析", "置信度"]
            },
            "估值分析": {
                "type": "object",
                "properties": {
                "信号": {"type": "string", "enum": ["看多", "看空", "中性"]},
                "解析": {"type": "string"},
                "置信度": {"type": "number", "minimum": 0.0, "maximum": 1.0}
                },
                "required": ["信号", "解析", "置信度"]
            },
            "基本面分析": {
                "type": "object",
                "properties": {
                "信号": {"type": "string", "enum": ["看多", "看空", "中性"]},
                "解析": {"type": "string"},
                "置信度": {"type": "number", "minimum": 0.0, "maximum": 1.0}
                },
                "required": ["信号", "解析", "置信度"]
            },
            "技术分析": {
                "type": "object",
                "properties": {
                "信号": {"type": "string", "enum": ["看多", "看空", "中性"]},
                "解析": {"type": "string"},
                "置信度": {"type": "number", "minimum": 0.0, "maximum": 1.0}
                },
                "required": ["信号", "解析", "置信度"]
            },
            "量化分析": {
                "type": "object",
                "properties": {
                "信号": {"type": "string", "enum": ["看多", "看空", "中性"]},
                "解析": {"type": "string", "description": "综合解读ADX、均值回归、动量、波动率及赫斯特指数等数据"},
                "置信度": {"type": "number", "minimum": 0.0, "maximum": 1.0}
                },
                "required": ["信号", "解析", "置信度"]
            }
            },
            "required": ["估值分析", "基本面分析", "技术分析", "情绪分析", "量化分析"]
        },
        "原因": {
            "type": "string",
            "description": "深度解释决策逻辑，如何综合评估各项信号与风控限制"
        },
        "预期": {
            "type": "string",
            "enum": ["强烈看多", "偏多", "震荡", "偏空", "强烈看空"],
            "description": "对未来走势的明确方向预期"
        }
        },
        "required": ["操作", "建议仓位", "置信度", "建议买入价", "目标卖出价", "建议止损价", "各种信号", "原因", "预期"]
    }
  }
}

# 读取系统提示词和用户输入
with open('LLM system content.txt', 'r', encoding='utf-8') as file:
    system_content = file.read()

with open(r'test\input.txt', 'r', encoding='utf-8') as file:
    user_message = file.read()


def gemini_chat(
        system_content=system_content, 
        user_message=user_message,
        model_tier='flash'
):
    """处理 Google Gemini 模型的请求，包含高负载重试机制"""
    if model_tier == 'pro':
        model = os.getenv("gemini_pro_model", "gemini-3.1-pro-preview")
        api_key = os.getenv("gemini_pro_api_key")
        tools = [
            types.Tool(googleSearch=types.GoogleSearch()),
        ]
    else:
        model = os.getenv("gemini_flash_model", "gemini-3.1-flash-lite-preview")
        api_key = os.getenv("gemini_flash_api_key")
        tools = []  # Flash 版本不使用工具

    client = genai.Client(api_key=api_key)
    
    generate_content_config = types.GenerateContentConfig(
        response_mime_type="text/plain",
        system_instruction=system_content,
        temperature=0.8,
        tools=tools
    )

    # ================= 核心新增：重试机制 =================
    max_retries = 3
    retry_delay = 30

    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model=model,
                contents=user_message,
                config=generate_content_config
            )
            return response.text
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⚠️ Gemini API 调用异常 (503高负载或网络错误) - 第 {attempt + 1} 次尝试失败: {e}")
                print(f"⏳ 等待 {retry_delay} 秒后进行第 {attempt + 2} 次重试...")
                time.sleep(retry_delay)
            else:
                print(f"❌ Gemini API 连续 {max_retries} 次调用失败，放弃当前请求: {e}")
                raise e  # 将错误向上抛出，交由 worker.py 外层的 try-except 捕获，从而安全跳过该只股票


def openai_chat(
        system_content=system_content, 
        user_message=user_message,
        schema=output_schema,
        api_key=None,
        base_url=None,
        model=None,
        strip_think=False
):
    """统一处理所有兼容 OpenAI 接口规范的模型 (如 Ark, Local, DeepSeek 等)"""
    client = OpenAI(base_url=base_url, api_key=api_key)
    messages = [
        {"role": "system", "content": system_content},
        {"role": "user", "content": user_message}
    ]
    
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        response_format=schema
    )

    result = response.choices[0].message.content
    
    # 清理深度思考模型的 <think> 标签
    if strip_think:
        result = re.sub(r'<think>.*?</think>', '', str(result), flags=re.DOTALL)
        
    return result


def get_LLM_message(
        system_content=system_content, 
        user_message=user_message,
        model_choice='gemini',
        model_tier='flash'
):
    """
    主路由函数，根据选择分发到对应的模型 API
    model_choice: 'gemini', 'ark', 'local'
    model_tier: 'flash' (初筛), 'pro' (精决)
    """
    
    # 统一使用 output_schema
    schema = output_schema

    if model_choice == 'gemini':
        response = gemini_chat(
            system_content=system_content, 
            user_message=user_message, 
            model_tier=model_tier
        )
        
    elif model_choice == 'ark':
        # 火山引擎：环境变量读取，并提供默认值以防报错
        response = openai_chat(
            system_content=system_content, 
            user_message=user_message, 
            schema=schema,
            api_key=os.getenv("ark_api_key"),
            base_url=os.getenv("ark_base_url", "https://ark.cn-beijing.volces.com/api/v3"),
            model=os.getenv("ark_model"),
            strip_think=False
        )
        
    elif model_choice == 'local':
        # 本地模型：读取环境变量，并开启清除 <think> 标签功能
        response = openai_chat(
            system_content=system_content, 
            user_message=user_message, 
            schema=schema,
            api_key=os.getenv("local_api_key"),
            base_url=os.getenv("local_url"),
            model=os.getenv("local_model"),
            strip_think=True 
        )
        
    else:
        raise ValueError(f"不支持或未知的模型: {model_choice}。请选择 'gemini', 'ark', 'local' 其中的一个。")

    return response