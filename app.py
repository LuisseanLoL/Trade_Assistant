# -*- coding: utf-8 -*-
import dash
from dash import dcc, html, Input, Output, State, dash_table, callback_context
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
from datetime import datetime, timedelta
import os
import glob
import logging
from dotenv import load_dotenv

# 屏蔽底层 HTTP 库的 INFO 级别请求日志，避免大模型 API 刷屏
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

# 加载环境变量
load_dotenv()

# ================= 导入内部模块 =================
# 注意：大部分数据爬取逻辑已下放到 core_analyzer 中，app.py 仅保留历史记录查看所需的基础绘图库
from src.data_crawler import get_chart_data  
from src.LLM_chat import get_model_config
from src.utils import (
    get_logical_date, 
    get_all_output_dates, 
    load_daily_table_by_date, 
    get_random_unprocessed_stock, 
    parse_llm_json,
    create_advanced_kline_fig,
)
from src.ui_components import parse_and_build_macro_ui, parse_and_build_fin_and_quant_ui
# 🌟 引入全新重构的分析引擎
from src.core_analyzer import run_core_analysis

# ==========================================
# 界面构建
# ==========================================
BG_COLOR = "#f5f6fa"
CARD_STYLE = {"backgroundColor": "#ffffff", "border": "none", "borderRadius": "6px", "boxShadow": "0 1px 6px rgba(0, 0, 0, 0.04)", "marginBottom": "10px"}
SIDEBAR_STYLE = {"backgroundColor": "#ffffff", "height": "100vh", "padding": "15px", "borderRight": "1px solid #ebedf2", "position": "fixed", "width": "280px", "top": 0, "left": 0, "zIndex": 1000}
CONTENT_STYLE = {"marginLeft": "280px", "padding": "15px", "backgroundColor": BG_COLOR, "minHeight": "100vh"}

# ================= 动态选项池定义 =================
MODEL_CONFIGS = get_model_config()
if not MODEL_CONFIGS:
    MODEL_OPTIONS = [{'label': '未检测到模型，请检查 .env', 'value': 'none'}]
    default_flash_model = 'none'
    default_pro_model = 'none'
else:
    MODEL_OPTIONS = [{'label': cfg['name'], 'value': mid} for mid, cfg in MODEL_CONFIGS.items()]
    default_flash_model = MODEL_OPTIONS[0]['value'] if len(MODEL_OPTIONS) > 0 else None
    default_pro_model = MODEL_OPTIONS[1]['value'] if len(MODEL_OPTIONS) > 1 else default_flash_model

# 动态获取 Agents 列表
def get_agent_options():
    agent_files = glob.glob("agents_text/*.txt")
    options = []
    for f in agent_files:
        name = os.path.basename(f).replace(".txt", "")
        options.append({'label': name.replace("_", " "), 'value': name})
    return sorted(options, key=lambda x: x['label'])

AGENT_OPTIONS = get_agent_options()

# 针对 A 股市场特色精选的 7 位默认参会大师
default_agent_names = [
    "A_Share_Hot_Money", "Richard_Wyckoff", "Jesse_Livermore", 
    "Cathie_Wood", "Peter_Lynch", "Stanley_Druckenmiller", "Warren_Buffett"
]
default_agents = [opt['value'] for opt in AGENT_OPTIONS if opt['value'] in default_agent_names]

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUMEN, dbc.icons.FONT_AWESOME], prevent_initial_callbacks="initial_duplicate")
app.title = "AI Trade Assistant"

sidebar = html.Div([
    html.Div([html.I(className="fa-solid fa-chart-line me-2", style={"color": "#4a5568", "fontSize": "1.3rem"}), html.Span("AI Trade Assistant", style={"fontWeight": "900", "fontSize": "1.1rem", "color": "#2d3748", "letterSpacing": "-0.5px"})], className="d-flex align-items-center mb-4"),
    
    html.Div([
        # --- 标的配置 ---
        html.H6("标的配置", className="text-muted fw-bold mb-2", style={"fontSize": "0.8rem", "letterSpacing": "1px"}),
        html.Label("股票代码", className="small fw-bold text-secondary mb-1"),
        dbc.InputGroup([dbc.Input(id="input-stock-code", type="text", placeholder="输入代码...", size="sm"), dbc.Button(html.I(className="fa-solid fa-dice"), id="btn-random", color="light", title="随机抽取", size="sm")], className="mb-2"),
        
        html.Label("当前仓位", className="small fw-bold text-secondary mb-1 mt-1"),
        dbc.InputGroup([
            dbc.Input(id="input-position", type="number", value=0, min=0, max=100, step=1, size="sm"),
            dbc.InputGroupText("%", style={"fontSize": "0.8rem", "padding": "0.25rem 0.5rem", "backgroundColor": "#f8f9fa"})
        ], className="mb-2"),
        html.Label("持仓成本", className="small fw-bold text-secondary mb-1"),
        dbc.Input(id="input-cost", type="number", value=0, className="mb-3", size="sm"),

        # --- 模型架构解耦配置 ---
        html.H6("流水线模型配置", className="text-muted fw-bold mb-2", style={"fontSize": "0.8rem", "letterSpacing": "1px"}),
        html.Label("1. 基础/初筛模型 (Actor)", className="small fw-bold text-secondary mb-1"),
        dbc.Select(id="dropdown-flash-model", options=MODEL_OPTIONS, value=default_flash_model, className="mb-2", size="sm"),
        
        dbc.Checklist(options=[{"label": "2. 启用 Pro 模型 (或裁判)", "value": 1}], value=[1], id="switch-use-pro", switch=True, className="mb-1 text-secondary small fw-bold"),
        html.Label("选择 Pro / 裁判模型 (Judge)", className="small fw-bold text-secondary mb-1"),
        dbc.Select(id="dropdown-pro-model", options=MODEL_OPTIONS, value=default_pro_model, className="mb-2", size="sm"),

        dbc.Checklist(options=[{"label": "3. 启用双重筛选过滤", "value": 1}], value=[], id="switch-dual-filter", switch=True, className="mb-2 text-secondary small fw-bold"),

        # --- 多 Agent 议事配置 ---
        html.Hr(style={"margin": "10px 0", "opacity": "0.15"}),
        html.H6("多大师议事会 (MoA)", className="text-muted fw-bold mb-2", style={"fontSize": "0.8rem", "letterSpacing": "1px", "color": "#e64980"}),
        dbc.Checklist(options=[{"label": "启用 AI 裁判委员会", "value": 1}], value=[1], id="switch-use-moa", switch=True, className="mb-1 text-secondary small fw-bold"),
        html.Label("选择参会大师 (Agent 角色)", className="small fw-bold text-secondary mb-1"),
        dcc.Dropdown(id="dropdown-committee-agents", options=AGENT_OPTIONS, value=default_agents, multi=True, placeholder="选择大师角色...", className="mb-3", style={"fontSize": "0.8rem"}),

        dbc.Button("开始分析", id="btn-analyze", color="primary", className="w-100 fw-bold", size="sm", style={"borderRadius": "4px", "backgroundColor": "#4c6ef5", "border": "none"}),
        html.Div(id="random-msg", className="mt-2 small text-danger")
    ], style={"height": "calc(100vh - 80px)", "overflowY": "auto", "overflowX": "hidden"})
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
                html.H5("股票智能决策面板", className="fw-bold mb-1", style={"color": "#2d3748", "fontSize": "1.1rem", "whiteSpace": "nowrap"}),
                html.P(f"日期: {get_logical_date().strftime('%Y-%m-%d')}", className="text-muted small mb-0", style={"fontSize": "0.75rem", "whiteSpace": "nowrap"})
            ], style={"marginRight": "15px", "display": "flex", "flexDirection": "column", "justifyContent": "center"}),
            html.Div([
                dbc.Row([
                    create_stat_card("分析标的", "out-stock-name", "#2d3748"),
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
            dbc.Col(dbc.Card([dbc.CardBody([html.H6("实时走势与决策标线", className="fw-bold mb-1", style={"color": "#495057", "fontSize": "0.85rem"}), dcc.Graph(id="main-chart", style={"height": "460px"})], style={"padding": "10px"})], style=CARD_STYLE), width=9),
            dbc.Col(dbc.Card([dbc.CardBody([html.H6([html.I(className="fa-solid fa-globe-asia me-2"), "宏观大盘环境"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem"}), html.Div(id="out-macro", style={"height": "460px"})], style={"padding": "10px"})], style=CARD_STYLE), width=3),
        ], className="gx-2"),

        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardBody([
                html.H6([html.I(className="fa-solid fa-file-invoice-dollar me-2"), "核心财务指标"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem"}), 
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
            dbc.Tabs(id="date-tabs", active_tab=get_all_output_dates()[0] if get_all_output_dates() else "", children=[dbc.Tab(label=date, tab_id=date) for date in get_all_output_dates()[:5]], className="mb-2"), 
            dash_table.DataTable(
                id='daily-table',
                columns=[{"name": i, "id": i} for i in ["股票代码", "股票名称", "决策模型", "当前价格", "预期", "操作", "建议仓位", "置信度", "建议买入价", "目标卖出价", "建议止损价", "回报风险比", "详情"]],
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
    code, err = get_random_unprocessed_stock()
    return (dash.no_update, err) if err else (code, "已随机填入代码！")

@app.callback(
    [Output("switch-use-moa", "value"), Output("switch-dual-filter", "value")],
    [Input("switch-use-moa", "value"), Input("switch-dual-filter", "value")],
    prevent_initial_call=True
)
def sync_exclusive_switches(moa_val, dual_val):
    """确保 'MoA 大师议事' 和 '双重筛选' 逻辑互斥"""
    ctx = dash.callback_context
    if not ctx.triggered: return dash.no_update, dash.no_update
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    if trigger_id == "switch-use-moa":
        if moa_val: return [1], []
        else: return [], dual_val
    elif trigger_id == "switch-dual-filter":
        if dual_val: return [], [1]
        else: return moa_val, []
    return dash.no_update, dash.no_update

@app.callback(Output("daily-table", "data"), [Input("date-tabs", "active_tab")])
def update_table(active_tab): return load_daily_table_by_date(active_tab) if active_tab else []

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
     State("switch-use-moa", "value"), State("dropdown-committee-agents", "value"), 
     State("input-position", "value"), State("input-cost", "value"), State("daily-table", "derived_viewport_data"), State("date-tabs", "active_tab")],
    prevent_initial_call=True
)
def unified_action_handler(n_clicks, active_cell, stock_code, flash_model, use_pro_switch, pro_model, dual_filter_switch, moa_switch, committee_agents, position, cost, table_data, active_tab):
    ctx = dash.callback_context
    if not ctx.triggered: return [dash.no_update] * 18
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    use_pro = bool(use_pro_switch)
    dual_filter = bool(dual_filter_switch)
    use_moa = bool(moa_switch)
    committee_agents = committee_agents if committee_agents else []

    # UI 辅助函数
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
    
    def get_price_display(target_p, buy_p):
        if not target_p or str(target_p) == "-": return "-"
        if not buy_p or str(buy_p) == "-": return str(target_p)
        try:
            t_val, b_val = float(target_p), float(buy_p)
            if b_val > 0:
                pct = (t_val - b_val) / b_val * 100
                sign = "+" if pct > 0 else ""
                return html.Span([str(t_val), html.Span(f" ({sign}{pct:.2f}%)", style={"fontSize": "0.75rem", "opacity": "0.85", "marginLeft": "2px"})])
        except Exception: pass
        return str(target_p)
    
    # ================= 分支 1：点击历史记录表查看详情 =================
    if trigger_id == 'daily-table':
        if not active_cell or active_cell['column_id'] != '详情': return [dash.no_update] * 18
        row_data = table_data[active_cell['row']]
        h_stock, h_date, h_stock_name = row_data['股票代码'], active_tab, row_data.get('股票名称', '未知')
        
        in_fs, out_fs = glob.glob(f"input/{h_date}/{h_stock}_*_input_{h_date}.txt"), glob.glob(f"output/{h_date}/{h_stock}_*_output_*_{h_date}.txt")
        if not in_fs or not out_fs: 
            return go.Figure(), f"{h_stock_name} ({h_stock})", "-", "-", "-", "-", "-", "-", "-", "-", "未能找到历史文本文件！", "-", "-", "-", "-", dash.no_update, dash.no_update, dash.no_update
        
        try: m_tag = os.path.basename(out_fs[0]).split('_output_')[1].rsplit('_', 1)[0]
        except: m_tag = "-"
        disp_model = get_display_model_name(m_tag)

        with open(in_fs[0], 'r', encoding='utf-8') as f: h_in = f.read()
        with open(out_fs[0], 'r', encoding='utf-8') as f: h_out = f.read()
        
        macro_ui = parse_and_build_macro_ui(h_in)
        fin_ui, quant_ui, news_t = parse_and_build_fin_and_quant_ui(h_in)
        parsed = parse_llm_json(h_out)
        
        beg, end = (datetime.strptime(h_date, "%Y-%m-%d") - timedelta(days=180)).strftime("%Y%m%d"), h_date.replace('-', '')
        
        # 历史记录回看需要调用爬虫复现图表
        df_chart = get_chart_data(h_stock, beg, end)
        fig = create_advanced_kline_fig(df_chart)
        
        if not df_chart.empty:
            buy_p, sell_p, stop_p = parsed.get("buy_p"), parsed.get("sell_p"), parsed.get("stop_p")
            if buy_p and str(buy_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(buy_p), line_dash="dot", line_color="#be4bdb", annotation_text="买点", row=1, col=1)
            if sell_p and str(sell_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(sell_p), line_dash="dot", line_color="#f03e3e", annotation_text="目标", row=1, col=1)
            if stop_p and str(stop_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(stop_p), line_dash="dot", line_color="#37b24d", annotation_text="止损", row=1, col=1)
        
        buy_p, sell_p, stop_p = parsed.get("buy_p"), parsed.get("sell_p"), parsed.get("stop_p")
        sell_display = get_price_display(sell_p, buy_p)
        stop_display = get_price_display(stop_p, buy_p)
        buy_display = str(buy_p) if buy_p else "-"
        
        return fig, f"{h_stock_name} ({h_stock})", format_dynamic_color(parsed.get("action"), True), format_dynamic_color(parsed.get("expectation"), False), parsed.get("pos_adv"), parsed.get("confidence"), buy_display, sell_display, stop_display, disp_model, parsed.get("reasoning"), news_t, macro_ui, fin_ui, quant_ui, dash.no_update, dash.no_update, dash.no_update

    # ================= 分支 2：点击“开始分析”触发核心引擎 =================
    if not stock_code: return [dash.no_update] * 18
    c_date = get_logical_date()
    c_str = c_date.strftime("%Y-%m-%d")
    stock_code = stock_code.strip()

    # 🌟 直接调用 core_analyzer.py 中统一的核心方法
    df_chart, s_name, s_price, parsed, disp_model, user_msg, res_text = run_core_analysis(
        stock_code=stock_code,
        position=position,
        cost=cost,
        current_date_str=c_str,
        flash_model=flash_model,
        use_pro=use_pro,
        pro_model=pro_model,
        dual_filter=dual_filter,
        use_moa=use_moa,
        committee_agents=committee_agents,
        committee_model=flash_model  # 在 UI 中默认复用初筛模型作为议事模型
    )

    # UI 绘图与解析
    fig = create_advanced_kline_fig(df_chart)
    buy_p, sell_p, stop_p = parsed.get("buy_p") or parsed.get("建议买入价"), parsed.get("sell_p") or parsed.get("目标卖出价"), parsed.get("stop_p") or parsed.get("建议止损价")
    
    if not df_chart.empty:
        if buy_p and str(buy_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(buy_p), line_dash="dot", line_color="#be4bdb", annotation_text="买点", row=1, col=1)
        if sell_p and str(sell_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(sell_p), line_dash="dot", line_color="#f03e3e", annotation_text="目标", row=1, col=1)
        if stop_p and str(stop_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(stop_p), line_dash="dot", line_color="#37b24d", annotation_text="止损", row=1, col=1)

    # 渲染子 UI 组件
    macro_ui = parse_and_build_macro_ui(user_msg)
    fin_ui, quant_ui, news_t = parse_and_build_fin_and_quant_ui(user_msg)

    # 动态数值展示
    sell_display = get_price_display(sell_p, buy_p)
    stop_display = get_price_display(stop_p, buy_p)
    buy_display = str(buy_p) if buy_p else "-"

    return (
        fig, 
        f"{s_name} ({stock_code})", 
        format_dynamic_color(parsed.get("action") or parsed.get("操作"), True), 
        format_dynamic_color(parsed.get("expectation") or parsed.get("预期"), False), 
        parsed.get("pos_adv") or parsed.get("建议仓位"), 
        parsed.get("confidence") or parsed.get("置信度"), 
        buy_display, 
        sell_display, 
        stop_display, 
        disp_model, 
        parsed.get("reasoning") or parsed.get("原因"), 
        news_t, 
        macro_ui, 
        fin_ui, 
        quant_ui, 
        [dbc.Tab(label=date, tab_id=date) for date in get_all_output_dates()[:5]], 
        c_str, 
        load_daily_table_by_date(c_str)
    )

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=8050)