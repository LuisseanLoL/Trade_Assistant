from openai import OpenAI
import re
import copy
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


def get_model_config():
    """从环境变量动态加载所有注册的模型配置"""
    active_models_str = os.getenv("ACTIVE_MODELS", "")
    if not active_models_str:
        return {}
    
    model_ids = [m.strip() for m in active_models_str.split(",") if m.strip()]
    configs = {}
    
    for mid in model_ids:
        configs[mid] = {
            "id": mid,
            "type": os.getenv(f"{mid}_TYPE", "openai").lower(),
            "name": os.getenv(f"{mid}_NAME", mid),
            "model": os.getenv(f"{mid}_MODEL", ""),
            "api_key": os.getenv(f"{mid}_API_KEY", ""),
            "base_url": os.getenv(f"{mid}_BASE_URL", None),
            # 新增下面这行：读取是否为 Vertex 的标识
            "is_vertex": os.getenv(f"{mid}_IS_VERTEX", "false").lower() == "true",
            "strip_think": os.getenv(f"{mid}_STRIP_THINK", "false").lower() == "true",
            "use_tools": os.getenv(f"{mid}_USE_TOOLS", "false").lower() == "true"
        }
    return configs

def gemini_chat(system_content, user_message, model, api_key, is_vertex=False, use_tools=False, schema=None):
    """处理 Google Gemini 模型"""
    client = genai.Client(api_key=api_key, vertexai=is_vertex)
    tools = [types.Tool(googleSearch=types.GoogleSearch())] if use_tools else None
    
    config_kwargs = {
        "system_instruction": system_content,
        "temperature": 1.0,
        "top_p": 1.0,
    }
    
    if schema:
        config_kwargs["response_mime_type"] = "application/json"
        
        # 【核心修复】：将 OpenAI 格式的 Schema 动态翻译为 Gemini 严格格式
        gemini_schema = copy.deepcopy(schema["json_schema"]["schema"])
        
        def adapt_schema_for_gemini(node):
            if isinstance(node, dict):
                if 'type' in node:
                    t = node['type']
                    # 1. 处理 ["number", "null"] 这种列表形式
                    if isinstance(t, list):
                        if 'null' in t:
                            node['nullable'] = True
                            t = [x for x in t if x != 'null'][0] # 取出真正的数据类型
                        else:
                            t = t[0]
                            
                    # 2. Gemini 强制要求 type 为大写，例如 'NUMBER', 'STRING'
                    if isinstance(t, str):
                        node['type'] = t.upper()
                        
                # 递归处理所有子节点
                for k, v in node.items():
                    adapt_schema_for_gemini(v)
            elif isinstance(node, list):
                for item in node:
                    adapt_schema_for_gemini(item)
                    
        # 执行转换
        adapt_schema_for_gemini(gemini_schema)
        config_kwargs["response_schema"] = gemini_schema
    else:
        config_kwargs["response_mime_type"] = "text/plain"

    if tools:
        config_kwargs["tools"] = tools

    generate_content_config = types.GenerateContentConfig(**config_kwargs)

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
                print(f"⚠️ Gemini API 调用异常 - 第 {attempt + 1} 次尝试失败: {e}")
                time.sleep(retry_delay)
            else:
                raise e

def openai_chat(system_content, user_message, schema, api_key, base_url, model, strip_think):
    """统一处理所有兼容 OpenAI 接口规范的模型"""
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
    if strip_think:
        result = re.sub(r'<think>.*?</think>', '', str(result), flags=re.DOTALL)
    return result

def get_LLM_message(system_content, user_message, model_id):
    """
    主路由函数：通过传入的 model_id，动态分发到对应的 API 请求逻辑
    """
    configs = get_model_config()
    if model_id not in configs:
        raise ValueError(f"未找到模型配置: {model_id}。请检查 .env 中的 ACTIVE_MODELS。")
        
    config = configs[model_id]
    
    if config['type'] == 'gemini':
        return gemini_chat(
            system_content=system_content, 
            user_message=user_message, 
            model=config['model'],
            api_key=config['api_key'],
            is_vertex=config['is_vertex'], # 【核心修改】：将参数传给处理函数
            use_tools=config['use_tools'],
            schema=output_schema
        )
        
    elif config['type'] == 'openai':
        return openai_chat(
            system_content=system_content, 
            user_message=user_message, 
            schema=output_schema,
            api_key=config['api_key'],
            base_url=config['base_url'],
            model=config['model'],
            strip_think=config['strip_think']
        )
    else:
        raise ValueError(f"不支持的模型类型: {config['type']}")