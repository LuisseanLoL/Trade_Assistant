from volcenginesdkarkruntime import Ark
from openai import OpenAI
import re
import base64
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

backtest_schema = {
  "type": "json_schema",
  "json_schema": {
    "schema": {
        "type": "object",
        "properties": {
        "操作": {
            "type": "string",
            "enum": ["买入", "卖出", "不操作"],
            "description": "交易操作类型"
        },
        "置信度": {
            "type": "number",
            "minimum": 0.0,
            "maximum": 1.0,
            "exclusiveMaximum": 1.0
        },
        "头寸数量": {
            "type": "integer",
            "minimum": 0,
            "multipleOf": 100
        },
        "可接受价格": {
            "type": "number",
            "minimum": 0.0
        },
        },
        "required": ["操作", "置信度", "头寸数量","可接受价格"]
    }
  }
}

with open('LLM system content.txt', 'r', encoding='utf-8') as file:
    # 读取整个文件内容
    system_content = file.read()

with open(r'test\\input.txt', 'r', encoding='utf-8') as file:
    # 读取整个文件内容
    user_message = file.read()

def gemini_chat(
        system_content=system_content, 
        user_message=user_message,
        model_tier='flash'  # 新增参数，默认使用免费的 flash
):
    
    # 核心修改：根据层级自动选择模型
    if model_tier == 'pro':
        model = os.getenv("gemini_pro_model", "gemini-3.1-pro-preview")
        api_key = os.getenv("gemini_pro_api_key")
    else:
        model = os.getenv("gemini_flash_model", "gemini-3.1-flash-lite-preview")
        api_key = os.getenv("gemini_flash_api_key")

    client = genai.Client(api_key=api_key)
    contents = user_message

    generate_content_config = types.GenerateContentConfig(
        response_mime_type="text/plain",
        system_instruction=system_content,
        temperature = 0.8
    )

    response = client.models.generate_content(
        model=model,
        contents=contents,
        config=generate_content_config
    )
    return response.text

def ARK_chat(
        system_content=system_content, 
        user_message=user_message,
        schema=output_schema
):
    client = Ark(api_key='b7bfbd95-1765-43fa-9ee2-e0cfd13e3fec')
    completion = client.bot_chat.completions.create(
        model='bot-20250214192429-94sbw',
        messages = [
            {"role": "system", "content": f"{system_content}"},
            {"role": "user", "content": f"{user_message}"},
        ],
        response_format=schema
    )
    result = completion.choices[0].message.content
    return result

def local_chat(
        system_content=system_content, 
        user_message=user_message,
        schema=output_schema
):
    client = OpenAI(base_url=os.getenv("local_url"), api_key=os.getenv("local_api_key"))
    message = [
                    {"role": "system", "content": system_content},
                    {"role": "user", "content": user_message}
                ]
    response = client.chat.completions.create(
        model=os.getenv("local_model"),
        messages = message,
        response_format=schema
    )

    result = response.choices[0].message.content
    cleaned_content = re.sub(r'<think>.*?</think>', '', str(result), flags=re.DOTALL)
    # print(cleaned_content)
    return cleaned_content

def get_LLM_message(
        system_content=system_content, 
        user_message=user_message,
        model_choice='gemini',
        model_tier='flash',  # 新增参数透传
        backtest=None
):
    """
    model_choice: 'gemini', 'ark', 'local'
    model_tier: 'flash' (初筛), 'pro' (精决)
    """

    if backtest == None:
        schema = output_schema
    else:
        schema = backtest_schema

    if model_choice == 'gemini':
        response = gemini_chat(system_content=system_content, user_message=user_message, model_tier=model_tier)
    elif model_choice == 'ark':
        response = ARK_chat(system_content=system_content, user_message=user_message, schema=schema)
    elif model_choice == 'local':
        response = local_chat(system_content=system_content, user_message=user_message, schema=schema)
    else:
        raise ValueError(f"不支持或未知的模型: {model_choice}。请选择'gemini','ark','local'其中的一个模型。")

    return response