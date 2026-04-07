# -*- coding: utf-8 -*-
import dash
from dash import dcc, html, Input, Output, State, dash_table, callback_context
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
import os
import re
import glob
import random
import logging
import json
from dash import DiskcacheManager
import diskcache
from dotenv import load_dotenv

# 屏蔽底层 HTTP 库的 INFO 级别请求日志，避免大模型 API 刷屏
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("google").setLevel(logging.WARNING)
logging.getLogger("google.genai").setLevel(logging.WARNING)
logging.getLogger("openai").setLevel(logging.WARNING)

# 加载环境变量
load_dotenv()

# ================= 导入内部模块 =================
from src.etf_core_analyzer import run_etf_core_analysis
from src.LLM_chat import get_model_config
from src.utils import (
    get_logical_date, 
    parse_llm_json,
    create_advanced_kline_fig
)
from src.ui_components import get_index_kline_fig

# ==========================================
# 辅助函数：专门针对 ETF 的读写与解析
# ==========================================
def get_all_etf_output_dates():
    """获取所有已输出过分析结果的 ETF 日期文件夹"""
    dates = []
    if os.path.exists("output_etf"):
        for folder_name in os.listdir("output_etf"):
            if os.path.isdir(os.path.join("output_etf", folder_name)) and os.path.exists(os.path.join("output_etf", folder_name, f"ETF_Daily_Table_{folder_name}.csv")):
                dates.append(folder_name)
    return sorted(dates, reverse=True)

def load_etf_daily_table_by_date(date_str):
    """按日期加载当日的 ETF 策略表并排序"""
    file_path = f"output_etf/{date_str}/ETF_Daily_Table_{date_str}.csv"
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path, dtype=str, on_bad_lines='skip')
            if '置信度' in df.columns:
                df['_conf_val'] = df['置信度'].str.replace('%', '', regex=False).astype(float).fillna(0)
            else:
                df['_conf_val'] = 0
            if '操作' in df.columns:
                df['_action_rank'] = df['操作'].apply(lambda x: 0 if str(x).strip() == '买入' else 1)
            else:
                df['_action_rank'] = 1
            df = df.sort_values(by=['_action_rank', '_conf_val'], ascending=[True, False]).drop(columns=['_conf_val', '_action_rank'])
            df['详情'] = '查看'
            return df.to_dict('records')
        except: pass
    return []

def build_etf_macro_ui(etf_context_str):
    """针对 ETF 数据流构建宏观大盘面板"""
    trend, rsi, z_score = "-", "-", "-"
    if "情绪指标(RSI14):" in etf_context_str: rsi = etf_context_str.split("情绪指标(RSI14):")[1].split('\n')[0].strip()
    if "均线偏离(Z-Score):" in etf_context_str: z_score = etf_context_str.split("均线偏离(Z-Score):")[1].split('\n')[0].strip()
    if "赫斯特指数:" in etf_context_str: trend = etf_context_str.split("赫斯特指数:")[1].split('\n')[0].strip()

    trend_color, rsi_color, z_color = "#2d3748", "#2d3748", "#2d3748"
    try:
        rsi_num = float(re.sub(r'[^\d\.-]', '', rsi.split('(')[0]))
        if rsi_num > 70: rsi_color = "#f03e3e"
        elif rsi_num < 30: rsi_color = "#2f9e44"
    except: pass
    try:
        z_num = float(re.sub(r'[^\d\.-]', '', z_score.split('(')[0]))
        if z_num > 2: z_color = "#f03e3e"
        elif z_num < -2: z_color = "#2f9e44"
    except: pass

    def mini_kpi(label, val, color="#495057"):
        return html.Div([
            html.Div(label, style={"fontSize": "0.7rem", "color": "#868e96"}),
            html.Div(val.split('(')[0].strip() if '(' in val else val, style={"fontSize": "0.95rem", "fontWeight": "bold", "color": color})
        ], style={"backgroundColor": "#f8f9fa", "padding": "4px", "borderRadius": "4px", "textAlign": "center"})

    return html.Div([
        html.Div(dcc.Graph(figure=get_index_kline_fig(), config={'displayModeBar': False, 'responsive': True}, style={"position": "absolute", "top": 0, "left": 0, "width": "100%", "height": "100%"}), style={"flexGrow": 1, "position": "relative", "minHeight": "200px"}),
        html.Div([
            dbc.Row([dbc.Col(mini_kpi("上证指数", "参考图表", "#f03e3e"), width=6, className="pe-1"), dbc.Col(mini_kpi("赫斯特趋势", trend, trend_color), width=6, className="ps-1")], className="mb-1"),
            dbc.Row([dbc.Col(mini_kpi("RSI情绪", rsi, rsi_color), width=6, className="pe-1"), dbc.Col(mini_kpi("偏离度", z_score, z_color), width=6, className="ps-1")]),
        ], style={"flexShrink": 0, "marginTop": "10px"})
    ], style={"height": "100%", "minHeight": "320px", "display": "flex", "flexDirection": "column"})

def build_etf_fin_quant_ui(etf_context_str, news_text):
    """针对 ETF 数据流构建基金指标与量化信号 UI"""
    import json
    import json_repair

    # ==========================================
    # 1. 动态解析 ETF 核心档案 (双列网格布局)
    # ==========================================
    profile_dict = {}
    if "【1." in etf_context_str and "基金概况】" in etf_context_str:
        try:
            # 截取 【1. xxx 基金概况】 到下一个 ====== 之间的内容
            block = etf_context_str.split("基金概况】")[1].split("======")[0].strip()
            for line in block.split('\n'):
                if ':' in line or '：' in line:
                    parts = line.replace('：', ':').split(':', 1)
                    if len(parts) == 2:
                        k, v = parts[0].strip(), parts[1].strip()
                        if k and v:
                            profile_dict[k] = v
        except Exception as e:
            print("解析基金概况报错:", e)
            
    def f_item(label, val):
        return html.Div([
            html.Div(label, style={"color": "#868e96", "fontSize": "0.7rem", "whiteSpace": "nowrap"}),
            html.Div(val, style={"color": "#2d3748", "fontWeight": "bold", "fontSize": "0.8rem", "wordBreak": "break-all"})
        ], className="col-6 mb-2", style={"textAlign": "left"}) 

    profile_items = [f_item(k, v) for k, v in profile_dict.items()]
    if not profile_items:
        profile_items = [html.Div("暂无档案数据", style={"color": "#adb5bd", "fontSize": "0.8rem", "padding": "10px"})]

    fin_ui = html.Div([
        html.Div([
            html.H6("ETF 核心概况", style={"fontSize": "0.75rem", "fontWeight": "bold", "color": "#495057", "marginBottom": "8px", "paddingBottom": "4px", "borderBottom": "1px solid #e9ecef"}),
            dbc.Row(profile_items, className="gx-2 mb-0")
        ], style={"backgroundColor": "#f8f9fa", "padding": "12px", "borderRadius": "6px", "marginBottom": "6px"}),
    ])

    # ==========================================
    # 2. 深度解析量化信号矩阵 (带具体指标副标题)
    # ==========================================
    quant_ui_elements = []
    if "【量化策略信号矩阵】" in etf_context_str:
        try:
            json_str = etf_context_str.split("【量化策略信号矩阵】")[1].split("相关新闻")[0].strip()
            s_idx = json_str.find('{')
            e_idx = json_str.rfind('}')
            if s_idx != -1 and e_idx != -1:
                signals = json_repair.loads(json_str[s_idx:e_idx+1])
                
                # 🌟 修复点：转为列表，利用 enumerate 提前判断是否是最后一项
                signal_items = list(signals.items())
                for i, (category, data) in enumerate(signal_items):
                    sig = data.get("信号", "-")
                    conf = data.get("置信度", "-")
                    details = data.get("具体指标", {})
                    
                    color = "#495057"
                    if sig in ["看多", "bullish"]: color = "#f03e3e"
                    elif sig in ["看空", "bearish"]: color = "#2f9e44"
                    
                    detail_texts = [f"{dk}: {dv}" for dk, dv in details.items()]
                    detail_str = " | ".join(detail_texts) if detail_texts else "无细节指标"
                    
                    # 如果是最后一项，去掉底边框
                    border_style = "none" if i == len(signal_items) - 1 else "1px dashed #dee2e6"
                    
                    quant_ui_elements.append(
                        html.Div([
                            html.Div([
                                html.Span(category, style={"fontWeight": "bold", "fontSize": "0.8rem", "color": "#343a40"}),
                                html.Span(f"{sig} ({conf})", style={"fontWeight": "bold", "fontSize": "0.8rem", "color": color})
                            ], style={"display": "flex", "justifyContent": "space-between", "marginBottom": "3px"}),
                            html.Div(detail_str, style={"fontSize": "0.7rem", "color": "#868e96", "lineHeight": "1.3", "textAlign": "left"})
                        ], style={"padding": "8px 6px", "borderBottom": border_style})
                    )
        except Exception as e: 
            print("解析量化JSON报错:", e)
    
    # 兼容老的纯文本格式历史记录
    if not quant_ui_elements and "【6. 最新交易日量化信号】" in etf_context_str:
        block = etf_context_str.split("【6. 最新交易日量化信号】")[1].split("相关新闻")[0].strip()
        lines = [line for line in block.split('\n') if ':' in line and '信号矩阵' not in line]
        for i, line in enumerate(lines):
            k, v = line.split(':', 1)
            # 同样修复这里的边界判断
            border_style = "none" if i == len(lines) - 1 else "1px dashed #dee2e6"
            quant_ui_elements.append(
                html.Div([
                    html.Div(k.strip(), style={"color": "#495057", "fontSize": "0.75rem", "fontWeight": "bold"}),
                    html.Div(v.strip().split('(')[0].strip() if '(' in v else v.strip(), style={"color": "#4c6ef5", "fontWeight": "bold", "fontSize": "0.75rem"})
                ], style={"display": "flex", "justifyContent": "space-between", "padding": "8px 6px", "borderBottom": border_style})
            )

    if not quant_ui_elements:
        quant_ui_elements = [html.Div("暂无量化数据", style={"color": "#adb5bd", "fontSize": "0.8rem", "padding": "10px"})]

    quant_ui = html.Div(quant_ui_elements, style={"backgroundColor": "#f8f9fa", "padding": "4px 8px", "borderRadius": "6px", "marginTop": "2px"})

    # ==========================================
    # 3. 格式化新闻流
    # ==========================================
    formatted_news = []
    if news_text:
        for line in news_text.split('\n'):
            line = line.strip()
            if line and "【" not in line: 
                formatted_news.append(html.Div(["• ", line], style={"marginBottom": "6px", "lineHeight": "1.45"}))
    if not formatted_news: 
        formatted_news = [html.Div("暂无新闻数据", style={"color": "#adb5bd", "fontSize": "0.8rem"})]

    return fin_ui, quant_ui, formatted_news

# ==========================================
# 界面构建
# ==========================================
BG_COLOR = "#f5f6fa"
CARD_STYLE = {"backgroundColor": "#ffffff", "border": "none", "borderRadius": "6px", "boxShadow": "0 1px 6px rgba(0, 0, 0, 0.04)", "marginBottom": "10px"}
SIDEBAR_STYLE = {"backgroundColor": "#ffffff", "height": "100vh", "padding": "15px", "borderRight": "1px solid #ebedf2", "position": "fixed", "width": "280px", "top": 0, "left": 0, "zIndex": 1000}
CONTENT_STYLE = {"marginLeft": "280px", "padding": "15px", "backgroundColor": BG_COLOR, "minHeight": "100vh", "position": "relative"}

MODEL_CONFIGS = get_model_config()
if not MODEL_CONFIGS:
    MODEL_OPTIONS = [{'label': '未检测到模型', 'value': 'none'}]
    default_flash_model, default_pro_model = 'none', 'none'
else:
    MODEL_OPTIONS = [{'label': cfg['name'], 'value': mid} for mid, cfg in MODEL_CONFIGS.items()]
    default_flash_model = MODEL_OPTIONS[0]['value'] if len(MODEL_OPTIONS) > 0 else None
    default_pro_model = MODEL_OPTIONS[1]['value'] if len(MODEL_OPTIONS) > 1 else default_flash_model

def get_agent_options():
    agent_files = glob.glob("src/agents_text/ETF_agents/*.txt")
    options = []
    for f in agent_files:
        name = os.path.basename(f).replace(".txt", "")
        options.append({'label': name.replace("_", " "), 'value': name})
    return sorted(options, key=lambda x: x['label'])

AGENT_OPTIONS = get_agent_options()
default_agent_names = ["Ray_Dalio_ETF", "Richard_Wyckoff_ETF", "William_ONeil_ETF", "Paul_Tudor_Jones_ETF", "Howard_Marks_ETF"]
default_agents = [opt['value'] for opt in AGENT_OPTIONS if opt['value'] in default_agent_names]

cache = diskcache.Cache("./cache_etf")
background_callback_manager = DiskcacheManager(cache)

app = dash.Dash(
    __name__, 
    external_stylesheets=[dbc.themes.LUMEN, dbc.icons.FONT_AWESOME], 
    prevent_initial_callbacks="initial_duplicate",
    background_callback_manager=background_callback_manager
)
app.title = "ETF AI Trade Assistant"

sidebar = html.Div([
    html.Div([html.I(className="fa-solid fa-chart-pie me-2", style={"color": "#4a5568", "fontSize": "1.3rem"}), html.Span("ETF Trade Assistant", style={"fontWeight": "900", "fontSize": "1.1rem", "color": "#2d3748", "letterSpacing": "-0.5px"})], className="d-flex align-items-center mb-4"),
    
    html.Div([
        html.H6("ETF 配置", className="text-muted fw-bold mb-2", style={"fontSize": "0.8rem", "letterSpacing": "1px"}),
        html.Label("ETF 代码", className="small fw-bold text-secondary mb-1"),
        dbc.InputGroup([dbc.Input(id="input-stock-code", type="text", placeholder="输入代码(如: 510300)...", size="sm"), dbc.Button(html.I(className="fa-solid fa-dice"), id="btn-random", color="light", title="随机主流 ETF", size="sm")], className="mb-2"),
        
        html.Label("当前持仓", className="small fw-bold text-secondary mb-1 mt-1"),
        dbc.Input(id="input-position", type="number", value=0, className="mb-2", size="sm"),
        html.Label("持仓成本", className="small fw-bold text-secondary mb-1"),
        dbc.Input(id="input-cost", type="number", value=0, className="mb-3", size="sm"),

        html.H6("流水线模型配置", className="text-muted fw-bold mb-2", style={"fontSize": "0.8rem", "letterSpacing": "1px"}),
        html.Label("1. 基础/初筛模型", className="small fw-bold text-secondary mb-1"),
        dbc.Select(id="dropdown-flash-model", options=MODEL_OPTIONS, value=default_flash_model, className="mb-2", size="sm"),
        
        dbc.Checklist(options=[{"label": "2. 启用 Pro 模型 (或裁判)", "value": 1}], value=[1], id="switch-use-pro", switch=True, className="mb-1 text-secondary small fw-bold"),
        html.Label("选择 Pro / 裁判模型", className="small fw-bold text-secondary mb-1"),
        dbc.Select(id="dropdown-pro-model", options=MODEL_OPTIONS, value=default_pro_model, className="mb-2", size="sm"),

        dbc.Checklist(options=[{"label": "3. 启用双重筛选过滤", "value": 1}], value=[], id="switch-dual-filter", switch=True, className="mb-2 text-secondary small fw-bold"),

        html.Hr(style={"margin": "10px 0", "opacity": "0.15"}),
        html.H6("多大师议事会 (MoA)", className="text-muted fw-bold mb-2", style={"fontSize": "0.8rem", "letterSpacing": "1px", "color": "#e64980"}),
        dbc.Checklist(options=[{"label": "启用 AI 裁判委员会", "value": 1}], value=[1], id="switch-use-moa", switch=True, className="mb-1 text-secondary small fw-bold"),
        html.Label("选择参会大师 (建议宏观/趋势流派)", className="small fw-bold text-secondary mb-1"),
        dcc.Dropdown(id="dropdown-committee-agents", options=AGENT_OPTIONS, value=default_agents, multi=True, placeholder="选择研究员模型...", className="mb-3", style={"fontSize": "0.8rem"}),

        dbc.Button("开始分析 ETF", id="btn-analyze", color="primary", className="w-100 fw-bold", size="sm", style={"borderRadius": "4px", "backgroundColor": "#4c6ef5", "border": "none"}),
        html.Div(id="random-msg", className="mt-2 small text-danger")
    ], style={"height": "calc(100vh - 80px)", "overflowY": "auto", "overflowX": "hidden"})
], style=SIDEBAR_STYLE)

def create_stat_card(title, value_id, color, value_size="1.0rem"):
    return dbc.Col(html.Div([
        html.Div(title, className="text-muted small fw-bold mb-0", style={"fontSize": "0.7rem", "whiteSpace": "nowrap"}), 
        html.Div("-", id=value_id, className="fw-bold", style={"fontSize": value_size, "color": color, "whiteSpace": "nowrap", "overflow": "hidden", "textOverflow": "ellipsis"})
    ], style={"backgroundColor": "#ffffff", "borderRadius": "6px", "boxShadow": "0 1px 4px rgba(0, 0, 0, 0.03)", "padding": "8px", "height": "100%", "minWidth": "90px"}), className="col px-1")

content = html.Div([
    # 全屏遮罩层
    html.Div(
        id="loading-overlay", style={"display": "none"},
        children=[
            dbc.Spinner(color="primary", spinner_style={"width": "3.5rem", "height": "3.5rem", "marginBottom": "20px"}),
            html.Div(id="progress-msg", style={"fontSize": "1.1rem", "color": "#4c6ef5", "fontWeight": "bold", "textAlign": "center", "letterSpacing": "0.5px"})
        ]
    ),

    html.Div([
        html.Div([
            html.Div([
                html.H5("ETF 智能决策面板", className="fw-bold mb-1", style={"color": "#2d3748", "fontSize": "1.1rem", "whiteSpace": "nowrap"}),
                html.P(f"日期: {get_logical_date().strftime('%Y-%m-%d')}", className="text-muted small mb-0", style={"fontSize": "0.75rem", "whiteSpace": "nowrap"})
            ], style={"marginRight": "15px", "display": "flex", "flexDirection": "column", "justifyContent": "center"}),
            html.Div([
                dbc.Row([
                    create_stat_card("ETF 标的", "out-stock-name", "#2d3748", value_size="0.85rem"),
                    create_stat_card("策略动作", "out-action", "#4c6ef5"), 
                    create_stat_card("方向预期", "out-expectation", "#845ef7"), 
                    create_stat_card("建议仓位", "out-position", "#f59f00"), 
                    create_stat_card("AI 置信度", "out-confidence", "#e64980"),
                    create_stat_card("建议买点", "out-buy-price", "#be4bdb"), 
                    create_stat_card("目标卖点", "out-sell-price", "#f03e3e"), 
                    create_stat_card("建议止损", "out-stop-price", "#37b24d"),
                    create_stat_card("决策模型", "out-model-name", "#20c997", value_size="0.8rem"),
                    dbc.Col(
                        dbc.Button([html.I(className="fa-solid fa-clock-rotate-left me-2"), "历史档案"], 
                                   id="btn-open-history", color="outline-primary", size="sm", className="fw-bold mt-1"),
                        width="auto", className="px-2 d-flex align-items-center"
                    )
                ], className="flex-nowrap", style={"overflowX": "auto", "margin": 0})
            ], style={"flexGrow": 1, "overflow": "hidden"})
        ], className="d-flex align-items-center mb-2", style={"width": "100%"}),
        
        dbc.Card([
            dbc.CardBody([
                html.Div([
                    html.I(className="fa-solid fa-compass me-2", style={"color": "#4c6ef5"}),
                    html.Span("ETF 周期与策略定调：", className="fw-bold", style={"color": "#495057", "fontSize": "0.9rem"}),
                    html.Span("-", id="out-cycle-strategy", style={"color": "#d6336c", "fontWeight": "bold", "fontSize": "0.9rem", "marginLeft": "5px"})
                ])
            ], style={"padding": "10px 15px"})
        ], style=CARD_STYLE, className="mb-2"),

        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardBody([html.H6("ETF 实时走势与决策基准", className="fw-bold mb-1", style={"color": "#495057", "fontSize": "0.85rem"}), dcc.Graph(id="main-chart", style={"height": "360px"})], style={"padding": "10px"})], style=CARD_STYLE), width=9),
            dbc.Col(dbc.Card([dbc.CardBody([html.H6([html.I(className="fa-solid fa-globe-asia me-2"), "宏观大盘环境"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem"}), html.Div(id="out-macro", style={"height": "360px"})], style={"padding": "10px"})], style=CARD_STYLE), width=3),
        ], className="gx-2"),

        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardBody([
                html.H6([html.I(className="fa-solid fa-file-invoice-dollar me-2"), "ETF 核心档案"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem"}), 
                html.Div(id="out-financial", style={"height": "320px", "overflowY": "auto", "overflowX": "hidden"})
            ], style={"padding": "10px", "height": "360px"})], style=CARD_STYLE), width=3),
            
            dbc.Col(dbc.Card([dbc.CardBody([
                html.H6([html.I(className="fa-solid fa-robot me-2"), "量化信号矩阵"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem"}), 
                html.Div(id="out-quant", style={"height": "320px", "overflowY": "auto", "overflowX": "hidden"})
            ], style={"padding": "10px", "height": "360px"})], style=CARD_STYLE), width=2),
            
            dbc.Col(dbc.Card([dbc.CardBody([
                html.H6([html.I(className="fa-solid fa-brain me-2"), "AI 深度逻辑推演"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem", "paddingBottom": "4px"}), 
                dcc.Markdown(id="out-reasoning", style={
                    "height": "320px", "overflowY": "auto", "fontSize": "0.85rem", "color": "#343a40", "whiteSpace": "pre-wrap", 
                    "lineHeight": "1.65", "textAlign": "justify", "backgroundColor": "#f8f9fa", "padding": "12px", "borderRadius": "6px"
                })
            ], style={"padding": "10px", "height": "360px"})], style=CARD_STYLE), width=4),
            
            dbc.Col(dbc.Card([dbc.CardBody([
                html.H6([html.I(className="fa-solid fa-newspaper me-2"), "消息面动态"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem", "paddingBottom": "4px"}), 
                html.Div(id="out-news", style={
                    "height": "320px", "overflowY": "auto", "overflowX": "hidden", "fontSize": "0.8rem", "color": "#495057", "whiteSpace": "pre-wrap",
                    "backgroundColor": "#f8f9fa", "padding": "12px", "borderRadius": "6px"
                })
            ], style={"padding": "10px", "height": "360px"})], style=CARD_STYLE), width=3),
        ], className="gx-2")
    ]),
], style=CONTENT_STYLE)

# 历史记录抽屉
history_drawer = dbc.Offcanvas(
    [
        html.P("选择历史交易日查看当天的详细分析记录 (点击表格 '详情' 自动载入面板)：", className="text-muted small mb-2"),
        dbc.Tabs(id="date-tabs", active_tab=get_all_etf_output_dates()[0] if get_all_etf_output_dates() else "", 
                 children=[dbc.Tab(label=date, tab_id=date) for date in get_all_etf_output_dates()[:20]], className="mb-2"), 
        dash_table.DataTable(
            id='daily-table',
            columns=[{"name": i, "id": i} for i in ["ETF代码", "ETF名称", "决策模型", "预期", "操作", "建议仓位", "置信度", "建议买入价", "目标卖出价", "建议止损价", "详情"]],
            style_table={'overflowX': 'auto', 'minWidth': '100%'}, 
            style_cell={'backgroundColor': '#ffffff', 'color': '#495057', 'textAlign': 'center', 'border': 'none', 'borderBottom': '1px solid #f1f3f5', 'padding': '10px', 'fontSize': '0.85rem'},
            style_header={'backgroundColor': '#f8f9fa', 'fontWeight': 'bold', 'color': '#868e96', 'borderBottom': '2px solid #e9ecef', 'padding': '10px'},
            style_data_conditional=[
                {'if': {'filter_query': '{操作} = "买入"'}, 'color': '#f03e3e', 'fontWeight': 'bold'},
                {'if': {'filter_query': '{操作} = "卖出"'}, 'color': '#2f9e44', 'fontWeight': 'bold'},
                {'if': {'column_id': '详情'}, 'cursor': 'pointer', 'color': '#4c6ef5', 'fontWeight': 'bold', 'backgroundColor': '#f0f4ff'},
                {'if': {'column_id': '详情', 'state': 'active'}, 'backgroundColor': '#dce4ff', 'border': '1px solid #4c6ef5'}
            ],
            page_size=10
        )
    ],
    id="offcanvas-history", title="📜 ETF 历史决策档案库", is_open=False, placement="bottom", 
    style={"height": "65vh", "boxShadow": "0 -5px 25px rgba(0,0,0,0.15)"}
)

app.layout = html.Div([sidebar, content, history_drawer])

# ==========================================
# 核心回调逻辑 
# ==========================================

@app.callback(
    Output("offcanvas-history", "is_open"),
    [Input("btn-open-history", "n_clicks"), Input("daily-table", "active_cell")],
    [State("offcanvas-history", "is_open")], prevent_initial_call=True
)
def toggle_history(n_clicks, active_cell, is_open):
    ctx = dash.callback_context
    if not ctx.triggered: return is_open
    if ctx.triggered[0]['prop_id'].split('.')[0] == "daily-table":
        if active_cell and active_cell['column_id'] == '详情': return False
        return is_open
    return not is_open

@app.callback([Output("input-stock-code", "value", allow_duplicate=True), Output("random-msg", "children")], [Input("btn-random", "n_clicks")], prevent_initial_call=True)
def handle_random_pick(n_clicks):
    if not n_clicks: return dash.no_update, ""
    ETF_POOL = ['510300', '510500', '513100', '518880', '159915', '512100', '512880', '512690']
    return random.choice(ETF_POOL), "已随机填入热门ETF代码！"

@app.callback(
    [Output("switch-use-moa", "value"), Output("switch-dual-filter", "value")],
    [Input("switch-use-moa", "value"), Input("switch-dual-filter", "value")], prevent_initial_call=True
)
def sync_exclusive_switches(moa_val, dual_val):
    ctx = dash.callback_context
    if not ctx.triggered: return dash.no_update, dash.no_update
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    if trigger_id == "switch-use-moa": return ([1], []) if moa_val else ([], dual_val)
    elif trigger_id == "switch-dual-filter": return ([], [1]) if dual_val else (moa_val, [])
    return dash.no_update, dash.no_update

@app.callback(Output("daily-table", "data"), [Input("date-tabs", "active_tab")])
def update_table(active_tab): return load_etf_daily_table_by_date(active_tab) if active_tab else []

@app.callback(
    [Output("main-chart", "figure"), Output("out-stock-name", "children"),
     Output("out-action", "children"), Output("out-expectation", "children"), Output("out-position", "children"), Output("out-confidence", "children"),
     Output("out-buy-price", "children"), Output("out-sell-price", "children"), Output("out-stop-price", "children"), Output("out-model-name", "children"),
     Output("out-cycle-strategy", "children"), 
     Output("out-reasoning", "children"), Output("out-news", "children"), 
     Output("out-macro", "children"), Output("out-financial", "children"), Output("out-quant", "children"), 
     Output("date-tabs", "children"), Output("date-tabs", "active_tab", allow_duplicate=True), Output("daily-table", "data", allow_duplicate=True)], 
    [Input("btn-analyze", "n_clicks"), Input("daily-table", "active_cell")],
    [State("input-stock-code", "value"), 
     State("dropdown-flash-model", "value"), State("switch-use-pro", "value"), State("dropdown-pro-model", "value"), State("switch-dual-filter", "value"),
     State("switch-use-moa", "value"), State("dropdown-committee-agents", "value"), 
     State("input-position", "value"), State("input-cost", "value"), State("daily-table", "derived_viewport_data"), State("date-tabs", "active_tab")],
    prevent_initial_call=True, background=True, manager=background_callback_manager,
    running=[
        (Output("btn-analyze", "disabled"), True, False), 
        (Output("btn-analyze", "children"), html.Span([html.I(className="fa-solid fa-spinner fa-spin me-2"), "推演中..."]), "开始分析 ETF"), 
        (Output("loading-overlay", "style"), {"display": "flex", "position": "fixed", "top": 0, "left": "280px", "width": "calc(100% - 280px)", "height": "100vh", "backgroundColor": "rgba(255, 255, 255, 0.85)", "backdropFilter": "blur(4px)", "zIndex": 9999, "flexDirection": "column", "justifyContent": "center", "alignItems": "center"}, {"display": "none"}),
    ],
    progress=[Output("progress-msg", "children")], progress_default="准备启动 ETF 分析引擎..."
)
def unified_action_handler(set_progress, n_clicks, active_cell, stock_code, flash_model, use_pro_switch, pro_model, dual_filter_switch, moa_switch, committee_agents, position, cost, table_data, active_tab):
    ctx = dash.callback_context
    if not ctx.triggered: return [dash.no_update] * 19
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    use_pro, dual_filter, use_moa = bool(use_pro_switch), bool(dual_filter_switch), bool(moa_switch)
    committee_agents = committee_agents if committee_agents else []

    def get_display_model_name(tag):
        if not tag: return "-"
        if tag.startswith("MoA-"): return f"【决议】{MODEL_CONFIGS.get(tag.split('-')[2], {}).get('name', tag.split('-')[2])}" if len(tag.split("-")) >= 3 else tag
        if tag.startswith("D-"): return f"{MODEL_CONFIGS.get(tag.split('-')[2], {}).get('name', tag.split('-')[2])}(双筛)" if len(tag.split("-")) >= 3 else tag
        return MODEL_CONFIGS.get(tag, {}).get('name', tag)

    def format_dynamic_color(text, is_action=True):
        if not text: return "-"
        text_str = str(text)
        if is_action: return html.Span(text_str, style={"color": "#f03e3e"}) if text_str == "买入" else (html.Span(text_str, style={"color": "#2f9e44"}) if text_str == "卖出" else text_str)
        else:
            if "看多" in text_str: return html.Span(text_str, style={"color": "#f03e3e"})
            elif "看空" in text_str: return html.Span(text_str, style={"color": "#2f9e44"})
        return text_str
    
    def get_price_display(target_p, buy_p):
        if not target_p or str(target_p) == "-": return "-"
        if not buy_p or str(buy_p) == "-": return str(target_p)
        try:
            t_val, b_val = float(target_p), float(buy_p)
            if b_val > 0:
                pct = (t_val - b_val) / b_val * 100
                return html.Span([str(t_val), html.Span(f" ({'+' if pct > 0 else ''}{pct:.2f}%)", style={"fontSize": "0.75rem", "opacity": "0.85", "marginLeft": "2px"})])
        except: pass
        return str(target_p)
    
    # 历史档案点击
    if trigger_id == 'daily-table':
        if not active_cell or active_cell['column_id'] != '详情': return [dash.no_update] * 19
        set_progress("正在从本地读取 ETF 历史决策日志与行情数据...")
        
        row_data = table_data[active_cell['row']]
        h_stock, h_date, h_stock_name = row_data['ETF代码'], active_tab, row_data.get('ETF名称', '未知')
        
        in_fs, out_fs = glob.glob(f"input_etf/{h_date}/{h_stock}_*_input_{h_date}.txt"), glob.glob(f"output_etf/{h_date}/{h_stock}_*_output_*_{h_date}.txt")
        if not in_fs or not out_fs: 
            return go.Figure(), f"{h_stock_name} ({h_stock})", "-", "-", "-", "-", "-", "-", "-", "-", "-", "未能找到历史文本文件！", "-", "-", "-", "-", dash.no_update, dash.no_update, dash.no_update
        
        try: m_tag = os.path.basename(out_fs[0]).split('_output_')[1].rsplit('_', 1)[0]
        except: m_tag = "-"
        
        with open(in_fs[0], 'r', encoding='utf-8') as f: h_in = f.read()
        with open(out_fs[0], 'r', encoding='utf-8') as f: h_out = f.read()
        
        try: news_t = h_in.split("相关新闻如下：")[1].split("当前该ETF持仓：")[0].strip()
        except: news_t = "历史新闻提取失败"

        macro_ui = build_etf_macro_ui(h_in)
        fin_ui, quant_ui, news_ui = build_etf_fin_quant_ui(h_in, news_t)
        parsed = parse_llm_json(h_out)
        
        fig = go.Figure()
        csv_path = f"log/etf_data/{h_date}/{h_stock}_indicators_{h_date}.csv"
        if os.path.exists(csv_path):
            df_chart = pd.read_csv(csv_path).rename(columns={'日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume'})
            fig = create_advanced_kline_fig(df_chart)

            # 🌟 修改点 1：增加中文键值的兼容获取
            buy_p = parsed.get("buy_p") or parsed.get("建议买入价")
            sell_p = parsed.get("sell_p") or parsed.get("目标卖出价")
            stop_p = parsed.get("stop_p") or parsed.get("建议止损价")

            if buy_p and str(buy_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(buy_p), line_dash="dot", line_color="#be4bdb", annotation_text="买点", row=1, col=1)
            if sell_p and str(sell_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(sell_p), line_dash="dot", line_color="#f03e3e", annotation_text="目标", row=1, col=1)
            if stop_p and str(stop_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(stop_p), line_dash="dot", line_color="#37b24d", annotation_text="止损", row=1, col=1)
        
        buy_p = parsed.get("buy_p") or parsed.get("建议买入价")
        sell_p = parsed.get("sell_p") or parsed.get("目标卖出价")
        stop_p = parsed.get("stop_p") or parsed.get("建议止损价")
        return fig, f"{h_stock_name} ({h_stock})", format_dynamic_color(parsed.get("action"), True), format_dynamic_color(parsed.get("expectation"), False), parsed.get("pos_adv"), parsed.get("confidence"), str(buy_p) if buy_p else "-", get_price_display(sell_p, buy_p), get_price_display(stop_p, buy_p), get_display_model_name(m_tag), parsed.get("cycle_strategy", "-"), parsed.get("reasoning"), news_ui, macro_ui, fin_ui, quant_ui, dash.no_update, dash.no_update, dash.no_update

    # 新分析
    if not stock_code: return [dash.no_update] * 19
    c_date = get_logical_date().strftime("%Y-%m-%d")
    
    # 核心解耦调用
    df_chart, s_name, s_price, parsed, disp_model, user_msg, res_text = run_etf_core_analysis(
        etf_code=stock_code, position=position, cost=cost, current_date_str=c_date,
        flash_model=flash_model, use_pro=use_pro, pro_model=pro_model, dual_filter=dual_filter,
        use_moa=use_moa, committee_agents=committee_agents, committee_model=flash_model,
        set_progress=set_progress
    )

    set_progress("分析完毕！正在渲染数据与交互图表...")

    fig = go.Figure()
    if not df_chart.empty:
        fig = create_advanced_kline_fig(df_chart)

        buy_p = parsed.get("buy_p") or parsed.get("建议买入价")
        sell_p = parsed.get("sell_p") or parsed.get("目标卖出价")
        stop_p = parsed.get("stop_p") or parsed.get("建议止损价")

        if buy_p and str(buy_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(buy_p), line_dash="dot", line_color="#be4bdb", annotation_text="买点", row=1, col=1)
        if sell_p and str(sell_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(sell_p), line_dash="dot", line_color="#f03e3e", annotation_text="目标", row=1, col=1)
        if stop_p and str(stop_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(stop_p), line_dash="dot", line_color="#37b24d", annotation_text="止损", row=1, col=1)

    try: news_t = user_msg.split("相关新闻如下：")[1].split("当前该ETF持仓：")[0].strip()
    except: news_t = "无新闻数据"

    macro_ui = build_etf_macro_ui(user_msg)
    fin_ui, quant_ui, news_ui = build_etf_fin_quant_ui(user_msg, news_t)
    
    buy_p = parsed.get("buy_p") or parsed.get("建议买入价")
    sell_p = parsed.get("sell_p") or parsed.get("目标卖出价")
    stop_p = parsed.get("stop_p") or parsed.get("建议止损价")

    return (
        fig, f"{s_name} ({stock_code.strip()})", format_dynamic_color(parsed.get("action") or parsed.get("操作"), True), 
        format_dynamic_color(parsed.get("expectation") or parsed.get("预期"), False), parsed.get("pos_adv") or parsed.get("建议仓位"), 
        parsed.get("confidence") or parsed.get("置信度"), str(buy_p) if buy_p else "-", get_price_display(sell_p, buy_p), get_price_display(stop_p, buy_p), 
        disp_model, parsed.get("cycle_strategy") or parsed.get("周期与策略", "-"), parsed.get("reasoning") or parsed.get("原因"), 
        news_ui, macro_ui, fin_ui, quant_ui, [dbc.Tab(label=date, tab_id=date) for date in get_all_etf_output_dates()[:20]], c_date, load_etf_daily_table_by_date(c_date)
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=8051)