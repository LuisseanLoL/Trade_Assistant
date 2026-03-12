# -*- coding: utf-8 -*-
import dash
from dash import dcc, html, Input, Output, State, dash_table, callback_context
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
import os
import re
import json_repair
import glob
import random
import concurrent.futures  # 新增并发库用于多模型议事
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 导入 src 模块
from src.etf_data_crawler import get_etf_data_context
from src.news_crawler import get_news_titles
from src.LLM_chat import get_LLM_message, get_model_config
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
            df['_conf_val'] = df['置信度'].str.replace('%', '', regex=False).astype(float).fillna(0)
            df['_action_rank'] = df['操作'].apply(lambda x: 0 if str(x).strip() == '买入' else 1)
            df = df.sort_values(by=['_action_rank', '_conf_val'], ascending=[True, False]).drop(columns=['_conf_val', '_action_rank'])
            df['详情'] = '查看'
            return df.to_dict('records')
        except: pass
    return []

def build_etf_macro_ui(etf_context_str):
    """针对 ETF 数据流构建宏观大盘面板 (复用 ui_components.py 的图表)"""
    trend, rsi, z_score = "-", "-", "-"
    
    # 从 ETF 的上下文文本中提取量化指标
    if "情绪指标(RSI14):" in etf_context_str:
        rsi = etf_context_str.split("情绪指标(RSI14):")[1].split('\n')[0].strip()
    if "均线偏离(Z-Score):" in etf_context_str:
        z_score = etf_context_str.split("均线偏离(Z-Score):")[1].split('\n')[0].strip()
    if "赫斯特指数:" in etf_context_str:
        trend = etf_context_str.split("赫斯特指数:")[1].split('\n')[0].strip()

    # 颜色逻辑
    trend_color = "#2d3748"
    rsi_color = "#2d3748"
    try:
        rsi_num = float(re.sub(r'[^\d\.-]', '', rsi.split('(')[0]))
        if rsi_num > 70: rsi_color = "#f03e3e"
        elif rsi_num < 30: rsi_color = "#2f9e44"
    except: pass
    
    z_color = "#2d3748"
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
        html.Div(
            dcc.Graph(figure=get_index_kline_fig(), config={'displayModeBar': False, 'responsive': True}, style={"position": "absolute", "top": 0, "left": 0, "width": "100%", "height": "100%"}),
            style={"flexGrow": 1, "position": "relative", "minHeight": "200px"}
        ),
        html.Div([
            dbc.Row([dbc.Col(mini_kpi("上证指数", "参考图表", "#f03e3e"), width=6, className="pe-1"), dbc.Col(mini_kpi("赫斯特趋势", trend, trend_color), width=6, className="ps-1")], className="mb-1"),
            dbc.Row([dbc.Col(mini_kpi("RSI情绪", rsi, rsi_color), width=6, className="pe-1"), dbc.Col(mini_kpi("偏离度", z_score, z_color), width=6, className="ps-1")]),
        ], style={"flexShrink": 0, "marginTop": "10px"})
    ], style={"height": "100%", "minHeight": "360px", "display": "flex", "flexDirection": "column"})

def build_etf_fin_quant_ui(etf_context_str, news_text):
    """针对 ETF 数据流构建基金指标与量化信号 UI"""
    # 提取基本信息
    fund_name = "未知名称"
    if "基金名称:" in etf_context_str:
        fund_name = etf_context_str.split("基金名称:")[1].split('\n')[0].strip()
        
    scale = "-"
    if "基金规模:" in etf_context_str:
        scale = etf_context_str.split("基金规模:")[1].split('\n')[0].strip()
        
    benchmark = "-"
    if "跟踪标的:" in etf_context_str:
        benchmark = etf_context_str.split("跟踪标的:")[1].split('\n')[0].strip()

    def f_item(label, val):
        return html.Div([
            html.Div(label, style={"color": "#868e96", "fontSize": "0.65rem", "whiteSpace": "nowrap"}),
            html.Div(val, style={"color": "#2d3748", "fontWeight": "bold", "fontSize": "0.85rem", "whiteSpace": "nowrap", "overflow": "hidden", "textOverflow": "ellipsis"})
        ], className="col-12 mb-2", style={"textAlign": "left"})

    fin_ui = html.Div([
        html.Div([
            html.H6("ETF 核心概况", style={"fontSize": "0.75rem", "fontWeight": "bold", "color": "#495057", "marginBottom": "4px", "paddingBottom": "2px", "borderBottom": "1px solid #e9ecef"}),
            dbc.Row([f_item("ETF 名称", fund_name)], className="gx-1 mb-0"),
            dbc.Row([f_item("跟踪标的", benchmark)], className="gx-1 mb-0"),
            dbc.Row([f_item("基金规模", scale)], className="gx-1 mb-0")
        ], style={"backgroundColor": "#f8f9fa", "padding": "6px", "borderRadius": "6px", "marginBottom": "6px"}),
    ])

    # 提取量化信号 (简化展示)
    quant_items = []
    if "【6. 最新交易日量化信号】" in etf_context_str:
        block = etf_context_str.split("【6. 最新交易日量化信号】")[1].strip()
        for line in block.split('\n'):
            if ':' in line:
                k, v = line.split(':', 1)
                quant_items.append((k.strip(), v.strip()))

    quant_ui = html.Div([
        html.Div([
            html.Div(k, style={"color": "#495057", "fontSize": "0.75rem", "fontWeight": "bold"}),
            html.Div(v.split('(')[0].strip() if '(' in v else v, style={"color": "#4c6ef5", "fontWeight": "bold", "fontSize": "0.75rem"})
        ], style={"display": "flex", "justifyContent": "space-between", "borderBottom": "none" if i == len(quant_items) - 1 else "1px dashed #dee2e6", "padding": "6px 4px"})
        for i, (k, v) in enumerate(quant_items)
    ], style={"backgroundColor": "#f8f9fa", "padding": "8px 10px", "borderRadius": "6px", "marginTop": "2px"})

    # 新闻 UI
    formatted_news = []
    if news_text:
        for line in news_text.split('\n'):
            line = line.strip()
            if line and "【" not in line:  # 过滤掉宏观标题标签
                formatted_news.append(html.Div(["• ", line], style={"marginBottom": "4px", "lineHeight": "1.3"}))
    if not formatted_news: formatted_news = "暂无新闻数据"

    return fin_ui, quant_ui, formatted_news

# ==========================================
# 界面构建
# ==========================================
BG_COLOR = "#f5f6fa"
CARD_STYLE = {"backgroundColor": "#ffffff", "border": "none", "borderRadius": "6px", "boxShadow": "0 1px 6px rgba(0, 0, 0, 0.04)", "marginBottom": "10px"}
SIDEBAR_STYLE = {"backgroundColor": "#ffffff", "height": "100vh", "padding": "15px", "borderRight": "1px solid #ebedf2", "position": "fixed", "width": "280px", "top": 0, "left": 0, "zIndex": 1000}
CONTENT_STYLE = {"marginLeft": "280px", "padding": "15px", "backgroundColor": BG_COLOR, "minHeight": "100vh"}

MODEL_CONFIGS = get_model_config()
if not MODEL_CONFIGS:
    MODEL_OPTIONS = [{'label': '未检测到模型', 'value': 'none'}]
    default_flash_model, default_pro_model = 'none', 'none'
else:
    MODEL_OPTIONS = [{'label': cfg['name'], 'value': mid} for mid, cfg in MODEL_CONFIGS.items()]
    default_flash_model = MODEL_OPTIONS[0]['value'] if len(MODEL_OPTIONS) > 0 else None
    default_pro_model = MODEL_OPTIONS[1]['value'] if len(MODEL_OPTIONS) > 1 else default_flash_model

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUMEN, dbc.icons.FONT_AWESOME], prevent_initial_callbacks="initial_duplicate")
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

        html.H6("模型解耦配置", className="text-muted fw-bold mb-2", style={"fontSize": "0.8rem", "letterSpacing": "1px"}),
        html.Label("1. 基础/初筛模型", className="small fw-bold text-secondary mb-1"),
        dbc.Select(id="dropdown-flash-model", options=MODEL_OPTIONS, value=default_flash_model, className="mb-2", size="sm"),
        
        dbc.Checklist(options=[{"label": "2. 启用 Pro 模型 (或裁判)", "value": 1}], value=[1], id="switch-use-pro", switch=True, className="mb-1 text-secondary small fw-bold"),
        html.Label("选择 Pro / 裁判模型", className="small fw-bold text-secondary mb-1"),
        dbc.Select(id="dropdown-pro-model", options=MODEL_OPTIONS, value=default_pro_model, className="mb-2", size="sm"),

        dbc.Checklist(options=[{"label": "3. 启用双重筛选", "value": 1}], value=[1], id="switch-dual-filter", switch=True, className="mb-2 text-secondary small fw-bold"),

        # --- 【新增】多模型议事配置 ---
        html.Hr(style={"margin": "10px 0", "opacity": "0.15"}),
        html.H6("多模型议事 (MoA)", className="text-muted fw-bold mb-2", style={"fontSize": "0.8rem", "letterSpacing": "1px", "color": "#e64980"}),
        dbc.Checklist(options=[{"label": "启用 AI 裁判委员会", "value": 1}], value=[], id="switch-use-moa", switch=True, className="mb-1 text-secondary small fw-bold"),
        html.Label("选择参会模型 (建议 2-4 个)", className="small fw-bold text-secondary mb-1"),
        dcc.Dropdown(id="dropdown-committee-models", options=MODEL_OPTIONS, multi=True, placeholder="选择研究员模型...", className="mb-3", style={"fontSize": "0.8rem"}),

        dbc.Button("开始分析 ETF", id="btn-analyze", color="primary", className="w-100 fw-bold", size="sm", style={"borderRadius": "4px", "backgroundColor": "#4c6ef5", "border": "none"}),
        html.Div(id="random-msg", className="mt-2 small text-danger")
    ], style={"height": "calc(100vh - 80px)", "overflowY": "auto"})
], style=SIDEBAR_STYLE)

def create_stat_card(title, value_id, color):
    return dbc.Col(html.Div([
        html.Div(title, className="text-muted small fw-bold mb-0", style={"fontSize": "0.7rem", "whiteSpace": "nowrap"}), 
        html.Div("-", id=value_id, className="fw-bold", style={"fontSize": "1.0rem", "color": color})
    ], style={"backgroundColor": "#ffffff", "borderRadius": "6px", "boxShadow": "0 1px 4px rgba(0, 0, 0, 0.03)", "padding": "8px", "height": "100%", "minWidth": "90px"}), className="col px-1")

content = html.Div([
    dcc.Loading(id="loading-main", type="circle", color="#4c6ef5", children=[
        html.Div([
            html.Div([
                html.H5("ETF 智能决策面板", className="fw-bold mb-1", style={"color": "#2d3748", "fontSize": "1.1rem", "whiteSpace": "nowrap"}),
                html.P(f"日期: {get_logical_date().strftime('%Y-%m-%d')}", className="text-muted small mb-0", style={"fontSize": "0.75rem", "whiteSpace": "nowrap"})
            ], style={"marginRight": "15px", "display": "flex", "flexDirection": "column", "justifyContent": "center"}),
            html.Div([
                dbc.Row([
                    create_stat_card("ETF 标的", "out-stock-name", "#2d3748"),
                    create_stat_card("策略动作", "out-action", "#4c6ef5"), 
                    create_stat_card("方向预期", "out-expectation", "#845ef7"), 
                    create_stat_card("建议仓位", "out-position", "#f59f00"), 
                    create_stat_card("AI 置信度", "out-confidence", "#e64980"),
                    create_stat_card("建议买点", "out-buy-price", "#be4bdb"), 
                    create_stat_card("目标卖点", "out-sell-price", "#f03e3e"), 
                    create_stat_card("建议止损", "out-stop-price", "#37b24d"),
                    create_stat_card("决策模型", "out-model-name", "#20c997")
                ], className="flex-nowrap", style={"overflowX": "auto", "margin": 0})
            ], style={"flexGrow": 1, "overflow": "hidden"})
        ], className="d-flex align-items-center mb-2", style={"width": "100%"}),
        
        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardBody([html.H6("ETF 实时走势与决策基准", className="fw-bold mb-1", style={"color": "#495057", "fontSize": "0.85rem"}), dcc.Graph(id="main-chart", style={"height": "460px"})], style={"padding": "10px"})], style=CARD_STYLE), width=9),
            dbc.Col(dbc.Card([dbc.CardBody([html.H6([html.I(className="fa-solid fa-globe-asia me-2"), "宏观大盘环境"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem"}), html.Div(id="out-macro", style={"height": "460px"})], style={"padding": "10px"})], style=CARD_STYLE), width=3),
        ], className="gx-2"),

        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardBody([
                html.H6([html.I(className="fa-solid fa-file-invoice-dollar me-2"), "ETF 核心指标"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem"}), 
                html.Div(id="out-financial", style={"height": "240px", "overflowY": "auto", "overflowX": "hidden"})
            ], style={"padding": "10px", "height": "280px"})], style=CARD_STYLE), width=3),
            
            dbc.Col(dbc.Card([dbc.CardBody([
                html.H6([html.I(className="fa-solid fa-robot me-2"), "量化信号矩阵"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem"}), 
                html.Div(id="out-quant", style={"height": "240px", "overflowY": "auto", "overflowX": "hidden"})
            ], style={"padding": "10px", "height": "280px"})], style=CARD_STYLE), width=2),
            
            dbc.Col(dbc.Card([dbc.CardBody([
                html.H6([html.I(className="fa-solid fa-brain me-2"), "AI 深度逻辑推演"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem", "paddingBottom": "4px"}), 
                dcc.Markdown(id="out-reasoning", style={
                    "height": "200px", "overflowY": "auto", 
                    "fontSize": "0.8rem", "color": "#343a40", "whiteSpace": "pre-wrap", 
                    "lineHeight": "1.65", "textAlign": "justify", 
                    "backgroundColor": "#f8f9fa", "padding": "10px", "borderRadius": "6px"
                })
            ], style={"padding": "10px", "height": "280px"})], style=CARD_STYLE), width=4),
            
            dbc.Col(dbc.Card([dbc.CardBody([
                html.H6([html.I(className="fa-solid fa-newspaper me-2"), "消息面动态"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem", "paddingBottom": "4px"}), 
                html.Div(id="out-news", style={
                    "height": "200px", "overflowY": "auto", "overflowX": "hidden", 
                    "fontSize": "0.75rem", "color": "#495057", "whiteSpace": "pre-wrap",
                    "backgroundColor": "#f8f9fa", "padding": "10px", "borderRadius": "6px"
                })
            ], style={"padding": "10px", "height": "280px"})], style=CARD_STYLE), width=3),
        ], className="gx-2")
    ]),

    html.H6("历史决策日志", className="fw-bold mt-2 mb-2", style={"color": "#2d3748", "fontSize": "0.95rem"}),
    dbc.Card([
        dbc.CardBody([
            dbc.Tabs(id="date-tabs", active_tab=get_all_etf_output_dates()[0] if get_all_etf_output_dates() else "", children=[dbc.Tab(label=date, tab_id=date) for date in get_all_etf_output_dates()[:5]], className="mb-2"), 
            dash_table.DataTable(
                id='daily-table',
                columns=[{"name": i, "id": i} for i in ["ETF代码", "ETF名称", "决策模型", "预期", "操作", "建议仓位", "置信度", "建议买入价", "目标卖出价", "建议止损价", "详情"]],
                style_table={'overflowX': 'auto', 'minWidth': '100%'}, 
                style_cell={'backgroundColor': '#ffffff', 'color': '#495057', 'textAlign': 'center', 'border': 'none', 'borderBottom': '1px solid #f1f3f5', 'padding': '8px', 'fontSize': '0.8rem'},
                style_header={'backgroundColor': '#f8f9fa', 'fontWeight': 'bold', 'color': '#868e96', 'borderBottom': '2px solid #e9ecef', 'padding': '8px'},
                style_data_conditional=[
                    {'if': {'filter_query': '{操作} = "买入"'}, 'color': '#f03e3e', 'fontWeight': 'bold'},
                    {'if': {'filter_query': '{操作} = "卖出"'}, 'color': '#2f9e44', 'fontWeight': 'bold'},
                    {'if': {'column_id': '详情'}, 'cursor': 'pointer', 'color': '#4c6ef5', 'fontWeight': 'bold', 'backgroundColor': '#f0f4ff'},
                    {'if': {'column_id': '详情', 'state': 'active'}, 'backgroundColor': '#dce4ff', 'border': '1px solid #4c6ef5'}
                ],
                page_size=5
            )
        ], style={"padding": "10px"})
    ], style=CARD_STYLE)
], style=CONTENT_STYLE)

app.layout = html.Div([sidebar, content])

# ==========================================
# 核心回调逻辑 
# ==========================================
@app.callback([Output("input-stock-code", "value", allow_duplicate=True), Output("random-msg", "children")], [Input("btn-random", "n_clicks")], prevent_initial_call=True)
def handle_random_pick(n_clicks):
    if not n_clicks: return dash.no_update, ""
    # 随机主流宽基/行业 ETF
    ETF_POOL = ['510300', '510500', '513100', '518880', '159915', '512100', '512880', '512690']
    return random.choice(ETF_POOL), "已随机填入热门ETF代码！"

@app.callback(Output("daily-table", "data"), [Input("date-tabs", "active_tab")])
def update_table(active_tab): return load_etf_daily_table_by_date(active_tab) if active_tab else []

@app.callback(
    [Output("main-chart", "figure"), Output("out-stock-name", "children"),
     Output("out-action", "children"), Output("out-expectation", "children"), Output("out-position", "children"), Output("out-confidence", "children"),
     Output("out-buy-price", "children"), Output("out-sell-price", "children"), Output("out-stop-price", "children"), Output("out-model-name", "children"),
     Output("out-reasoning", "children"), Output("out-news", "children"), 
     Output("out-macro", "children"), Output("out-financial", "children"), Output("out-quant", "children"), 
     Output("date-tabs", "children"), Output("date-tabs", "active_tab", allow_duplicate=True), Output("daily-table", "data", allow_duplicate=True)], 
    [Input("btn-analyze", "n_clicks"), Input("daily-table", "active_cell")],
    [State("input-stock-code", "value"), 
     State("dropdown-flash-model", "value"), State("switch-use-pro", "value"), State("dropdown-pro-model", "value"), State("switch-dual-filter", "value"),
     State("switch-use-moa", "value"), State("dropdown-committee-models", "value"), # 新增 MoA 状态获取
     State("input-position", "value"), State("input-cost", "value"), State("daily-table", "derived_viewport_data"), State("date-tabs", "active_tab")],
    prevent_initial_call=True
)
def unified_action_handler(n_clicks, active_cell, stock_code, flash_model, use_pro_switch, pro_model, dual_filter_switch, moa_switch, committee_models, position, cost, table_data, active_tab):
    ctx = dash.callback_context
    if not ctx.triggered: return [dash.no_update] * 18
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    use_pro = bool(use_pro_switch)
    dual_filter = bool(dual_filter_switch)
    use_moa = bool(moa_switch)
    committee_models = committee_models if committee_models else []

    # 解析模型展示名称（兼容 MoA 标签）
    def get_display_model_name(tag):
        if not tag: return "-"
        if tag.startswith("MoA-"):
            parts = tag.split("-")
            if len(parts) >= 3:
                p_name = MODEL_CONFIGS.get(parts[2], {}).get('name', parts[2])
                return f"【决议】{p_name}"
        if tag.startswith("D-"):
            parts = tag.split("-")
            if len(parts) >= 3:
                p_name = MODEL_CONFIGS.get(parts[2], {}).get('name', parts[2])
                return f"{p_name}(双筛)"
        return MODEL_CONFIGS.get(tag, {}).get('name', tag)

    def format_dynamic_color(text, is_action=True):
        if not text: return "-"
        text_str = str(text)
        if is_action:
            if text_str == "买入": return html.Span(text_str, style={"color": "#f03e3e"})
            elif text_str == "卖出": return html.Span(text_str, style={"color": "#2f9e44"})
        else:
            if "强烈看多" in text_str or text_str == "看多": return html.Span(text_str, style={"color": "#f03e3e"})
            elif "偏多" in text_str: return html.Span(text_str, style={"color": "#ffb8b8"})
            elif "强烈看空" in text_str or text_str == "看空": return html.Span(text_str, style={"color": "#2f9e44"})
            elif "偏空" in text_str: return html.Span(text_str, style={"color": "#c3ffcd"})
        return text_str
    
    # ================= 分支 1：点击历史记录表查看详情 =================
    if trigger_id == 'daily-table':
        if not active_cell or active_cell['column_id'] != '详情': return [dash.no_update] * 18
        row_data = table_data[active_cell['row']]
        h_stock, h_date, h_stock_name = row_data['ETF代码'], active_tab, row_data.get('ETF名称', '未知')
        
        in_fs, out_fs = glob.glob(f"input_etf/{h_date}/{h_stock}_*_input_{h_date}.txt"), glob.glob(f"output_etf/{h_date}/{h_stock}_*_output_*_{h_date}.txt")
        if not in_fs or not out_fs: 
            return go.Figure(), f"{h_stock_name} ({h_stock})", "-", "-", "-", "-", "-", "-", "-", "-", "未能找到历史文本文件！", "-", "-", "-", "-", dash.no_update, dash.no_update, dash.no_update
        
        try: m_tag = os.path.basename(out_fs[0]).split('_output_')[1].rsplit('_', 1)[0]
        except: m_tag = "-"
        disp_model = get_display_model_name(m_tag)

        with open(in_fs[0], 'r', encoding='utf-8') as f: h_in = f.read()
        with open(out_fs[0], 'r', encoding='utf-8') as f: h_out = f.read()
        
        # 尝试提取之前保存的 ETF 数据与新闻 (Prompt 结构)
        try:
            news_t = h_in.split("相关新闻如下：")[1].split("当前持仓如下：")[0].strip()
        except:
            news_t = "历史新闻提取失败"

        macro_ui = build_etf_macro_ui(h_in)
        fin_ui, quant_ui, news_ui = build_etf_fin_quant_ui(h_in, news_t)
        parsed = parse_llm_json(h_out)
        
        # 重建 K线图
        fig = go.Figure()
        csv_path = f"log/etf_data/{h_date}/{h_stock}_indicators_{h_date}.csv"
        if os.path.exists(csv_path):
            df_chart = pd.read_csv(csv_path)
            # 兼容 utils.create_advanced_kline_fig 所需的列名
            df_chart = df_chart.rename(columns={'日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume'})
            fig = create_advanced_kline_fig(df_chart)
        
            # 叠加策略基准线
            buy_p, sell_p, stop_p = parsed["buy_p"], parsed["sell_p"], parsed["stop_p"]
            if buy_p and str(buy_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(buy_p), line_dash="dot", line_color="#be4bdb", annotation_text="买点", row=1, col=1)
            if sell_p and str(sell_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(sell_p), line_dash="dot", line_color="#f03e3e", annotation_text="目标", row=1, col=1)
            if stop_p and str(stop_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(stop_p), line_dash="dot", line_color="#37b24d", annotation_text="止损", row=1, col=1)
        
        return fig, f"{h_stock_name} ({h_stock})", format_dynamic_color(parsed["action"], True), format_dynamic_color(parsed["expectation"], False), parsed["pos_adv"], parsed["confidence"], str(parsed["buy_p"]) if parsed["buy_p"] else "-", str(parsed["sell_p"]) if parsed["sell_p"] else "-", str(parsed["stop_p"]) if parsed["stop_p"] else "-", disp_model, parsed["reasoning"], news_ui, macro_ui, fin_ui, quant_ui, dash.no_update, dash.no_update, dash.no_update

    # ================= 分支 2：点击“开始分析”获取新决策 =================
    if not stock_code: return [dash.no_update] * 18
    c_date = get_logical_date()
    c_str, end_date = c_date.strftime("%Y-%m-%d"), c_date.strftime("%Y%m%d")
    beg_date = (c_date - timedelta(days=720)).strftime("%Y%m%d") # 抓取更长时间数据给图表
    stock_code = stock_code.strip()
    s_name = "ETF" # 默认占位符，如果未提取出
    
    # 1. 抓取 ETF 核心上下文与数据 (etf_data_crawler 引擎)
    in_str = get_etf_data_context(etf_code=stock_code, beg=beg_date, end=end_date, current_date=c_str)
    
    # 从 context 中提取真实基金名称
    if "基金名称:" in in_str:
        s_name = in_str.split("基金名称:")[1].split('\n')[0].strip()
    safe_s_name = re.sub(r'[\\/:*?"<>|]', '', s_name) 

    # 2. 抓取新闻与宏观早餐 (news_crawler)
    news_text = get_news_titles(symbol=stock_code, stock_name=s_name, max_news=20, save_txt=True, current_date=c_str)

    # 3. 读取刚刚由 get_etf_data_context 生成的 CSV 绘制高级图表
    fig = go.Figure()
    csv_path = f"log/etf_data/{c_str}/{stock_code}_indicators_{c_str}.csv"
    if os.path.exists(csv_path):
        df_chart = pd.read_csv(csv_path)
        df_chart = df_chart.rename(columns={'日期': 'date', '开盘': 'open', '最高': 'high', '最低': 'low', '收盘': 'close', '成交量': 'volume'})
        fig = create_advanced_kline_fig(df_chart)

    # 4. 构建 ETF 专属 Prompt
    user_msg = f"""基于获得的以下 ETF 数据和你搜集到的新闻消息，做出你的交易决策。

{in_str}

相关新闻如下：
{news_text}

当前持仓如下：
当前该ETF持仓：{position} 份
当前持仓成本: {cost} 元

请记住，行动必须是买入、卖出、持有或观望。
谨慎考虑交易决策：考虑当前价格是高位还是低位，在低位买入，高位卖出。
考虑自己的持仓成本，在有足够浮盈的情况下考虑卖出收获现金实利。"""

    os.makedirs(f"input_etf/{c_str}", exist_ok=True)
    with open(f"input_etf/{c_str}/{stock_code}_{safe_s_name}_input_{c_str}.txt", 'w', encoding='utf-8') as f: f.write(user_msg)

    # 加载系统提示词
    try:
        with open('ETF LLM system content.txt', 'r', encoding='utf-8') as f: sys_content = f.read()
    except: sys_content = "你是一个专业的量化交易AI..."
        
    run_pro = False
    res_text = ""
    
    # 核心解耦过滤逻辑
    if use_pro and dual_filter:
        if float(position) > 0: 
            run_pro = True
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
    else:
        res_text = get_LLM_message(system_content=sys_content, user_message=user_msg, model_id=flash_model)

    # 【高级决议阶段：单模型 vs. 多模型议事(MoA)】
    if run_pro: 
        if use_moa and len(committee_models) > 0:
            print(f"🚀 触发 MoA 议事机制，正在并发呼叫委员会模型: {committee_models}...")
            committee_results = {}
            # 并发请求参会模型获取独立意见
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(committee_models)) as executor:
                futures = {executor.submit(get_LLM_message, system_content=sys_content, user_message=user_msg, model_id=mid): mid for mid in committee_models}
                for future in concurrent.futures.as_completed(futures):
                    mid = futures[future]
                    try:
                        committee_results[mid] = future.result()
                    except Exception as e:
                        print(f"⚠️ {mid} 议事失败: {e}")
                        committee_results[mid] = f"该模型分析失败：{e}"
            
            # 组装 Meta-Prompt (裁判提示词)
            judge_msg = f"{user_msg}\n\n"
            judge_msg += "=================================\n"
            judge_msg += "【投资总监（AI裁判）专属决议指令】\n"
            judge_msg += "以上是客观标的数据。以下是你的多位顶级研究员（不同AI模型）针对该数据给出的独立分析和 JSON 报告：\n\n"
            
            for mid, res in committee_results.items():
                m_name = MODEL_CONFIGS.get(mid, {}).get('name', mid)
                judge_msg += f"--- 研究员模型：{m_name} 的意见 ---\n{res}\n\n"
                
            judge_msg += "作为量化基金的投资总监，你拥有最终拍板权。请严格按照以下【核心裁判原则】进行综合决策：\n"
            judge_msg += "1. 事实核查先行（零容忍数据幻觉）：必须先核对研究员引用的数据是否与上文提供的【客观标的数据】完全一致。对于任何基于虚构数据得出的结论，必须直接一票否决。\n"
            judge_msg += "2. 寻找非共识的正确：重点审视研究员之间的【分歧点】。如果少数派指出了隐含的风控隐患，且多数派未能有效应对，应果断采纳少数派意见。\n"
            judge_msg += "3. 拒绝无效瘫痪（果断决策）：不要因为存在分歧就本能地退缩到‘观望’。在剔除幻觉意见后，评估盈亏比，勇敢给出具体的买入/卖出、观望指令和点位。\n\n"
            judge_msg += "请给出最终决策。你必须在 JSON 的 '原因' 字段中分段输出：\n"
            judge_msg += "【事实核查与幻觉剔除】：简述是否有研究员引用了错误数据。\n"
            judge_msg += "【共识与核心分歧】：简述各方有效观点的交锋点。\n"
            judge_msg += "【总监拍板逻辑】：详细说明你最终支持哪一方的深度理由。\n"
            judge_msg += "注意：你的输出必须是一个单一的、严格符合原定系统提示词规范的 JSON 对象！\n"

            # 呼叫裁判模型进行最终裁决
            print(f"⚖️ 正在请求裁判模型 [{pro_model}] 进行最终综合拍板...")
            res_text = get_LLM_message(system_content=sys_content, user_message=judge_msg, model_id=pro_model)
            model_tag = f"MoA-{len(committee_models)}议事-{pro_model}"
            
        else:
            # 原有的单发 Pro 模型逻辑
            res_text = get_LLM_message(system_content=sys_content, user_message=user_msg, model_id=pro_model)
            model_tag = f"D-{flash_model}-{pro_model}" if dual_filter else pro_model
    else:
        model_tag = flash_model
            
    disp_model = get_display_model_name(model_tag)
    
    os.makedirs(f"output_etf/{c_str}", exist_ok=True)
    with open(f"output_etf/{c_str}/{stock_code}_{safe_s_name}_output_{model_tag}_{c_str}.txt", 'w', encoding='utf-8') as f: f.write(res_text)

    # 5. 解析 LLM JSON 以及重构 UI 
    parsed = parse_llm_json(res_text)
    macro_ui = build_etf_macro_ui(in_str)
    fin_ui, quant_ui, news_ui = build_etf_fin_quant_ui(in_str, news_text)

    # 叠加策略基准线
    buy_p, sell_p, stop_p = parsed["buy_p"], parsed["sell_p"], parsed["stop_p"]
    if buy_p and str(buy_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(buy_p), line_dash="dot", line_color="#be4bdb", annotation_text="买点", row=1, col=1)
    if sell_p and str(sell_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(sell_p), line_dash="dot", line_color="#f03e3e", annotation_text="目标", row=1, col=1)
    if stop_p and str(stop_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(stop_p), line_dash="dot", line_color="#37b24d", annotation_text="止损", row=1, col=1)

    # 保存日志记录表
    pd.DataFrame([{
        "ETF代码": stock_code, "ETF名称": s_name, "决策模型": disp_model, "预期": parsed["expectation"], "操作": parsed["action"], "建议仓位": parsed["pos_adv"], "置信度": parsed["confidence"], "建议买入价": str(buy_p) if buy_p else "-", "目标卖出价": str(sell_p) if sell_p else "-", "建议止损价": str(stop_p) if stop_p else "-"
    }]).to_csv(f"output_etf/{c_str}/ETF_Daily_Table_{c_str}.csv", index=False, header=not os.path.exists(f"output_etf/{c_str}/ETF_Daily_Table_{c_str}.csv"), mode='a', encoding='utf-8-sig')

    return fig, f"{s_name} ({stock_code})", format_dynamic_color(parsed["action"], True), format_dynamic_color(parsed["expectation"], False), parsed["pos_adv"], parsed["confidence"], str(buy_p) if buy_p else "-", str(sell_p) if sell_p else "-", str(stop_p) if stop_p else "-", disp_model, parsed["reasoning"], news_ui, macro_ui, fin_ui, quant_ui, [dbc.Tab(label=date, tab_id=date) for date in get_all_etf_output_dates()[:5]], c_str, load_etf_daily_table_by_date(c_str)

if __name__ == '__main__':
    # 为了避免与主股票程序端口冲突，您可以更换运行端口
    app.run(host='0.0.0.0',debug=True, port=8051)