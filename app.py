# -*- coding: utf-8 -*-
import dash
from dash import dcc, html, Input, Output, State, dash_table, callback_context
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timedelta
import os
import re
import json_repair
import glob
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 导入 src 模块
from src.data_crawler import get_stock_data, get_bs_code, get_stock_name_bs, get_chart_data
from src.LLM_chat import get_LLM_message, get_model_config
from src.utils import (
    get_logical_date, 
    fetch_news_safely, 
    get_all_output_dates, 
    load_daily_table_by_date, 
    get_random_unprocessed_stock, 
    parse_llm_json
)
from src.ui_components import parse_and_build_macro_ui, parse_and_build_fin_and_quant_ui

# ==========================================
# 界面构建
# ==========================================
BG_COLOR = "#f5f6fa"
CARD_STYLE = {"backgroundColor": "#ffffff", "border": "none", "borderRadius": "6px", "boxShadow": "0 1px 6px rgba(0, 0, 0, 0.04)", "marginBottom": "10px"}
SIDEBAR_STYLE = {"backgroundColor": "#ffffff", "height": "100vh", "padding": "15px", "borderRight": "1px solid #ebedf2", "position": "fixed", "width": "240px", "top": 0, "left": 0, "zIndex": 1000}
CONTENT_STYLE = {"marginLeft": "240px", "padding": "15px", "backgroundColor": BG_COLOR, "minHeight": "100vh"}

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

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.LUMEN, dbc.icons.FONT_AWESOME], prevent_initial_callbacks="initial_duplicate")
app.title = "AI Trade Assistant"

sidebar = html.Div([
    html.Div([html.I(className="fa-solid fa-chart-line me-2", style={"color": "#4a5568", "fontSize": "1.3rem"}), html.Span("AI Trade Assistant", style={"fontWeight": "900", "fontSize": "1.1rem", "color": "#2d3748", "letterSpacing": "-0.5px"})], className="d-flex align-items-center mb-4"),
    
    html.Div([
        # --- 标的配置 ---
        html.H6("标的配置", className="text-muted fw-bold mb-2", style={"fontSize": "0.8rem", "letterSpacing": "1px"}),
        html.Label("股票代码", className="small fw-bold text-secondary mb-1"),
        dbc.InputGroup([dbc.Input(id="input-stock-code", type="text", placeholder="输入代码...", size="sm"), dbc.Button(html.I(className="fa-solid fa-dice"), id="btn-random", color="light", title="随机抽取", size="sm")], className="mb-2"),
        
        html.Label("当前持仓", className="small fw-bold text-secondary mb-1 mt-1"),
        dbc.Input(id="input-position", type="number", value=0, className="mb-2", size="sm"),
        html.Label("持仓成本", className="small fw-bold text-secondary mb-1"),
        dbc.Input(id="input-cost", type="number", value=0, className="mb-3", size="sm"),

        # --- 模型架构解耦配置 ---
        html.H6("模型解耦配置", className="text-muted fw-bold mb-2", style={"fontSize": "0.8rem", "letterSpacing": "1px"}),
        
        html.Label("1. 基础/初筛模型", className="small fw-bold text-secondary mb-1"),
        dbc.Select(id="dropdown-flash-model", options=MODEL_OPTIONS, value=default_flash_model, className="mb-2", size="sm"),
        
        dbc.Checklist(options=[{"label": "2. 启用 Pro 高级模型", "value": 1}], value=[1], id="switch-use-pro", switch=True, className="mb-1 text-secondary small fw-bold"),
        html.Label("选择 Pro 模型", className="small fw-bold text-secondary mb-1"),
        dbc.Select(id="dropdown-pro-model", options=MODEL_OPTIONS, value=default_pro_model, className="mb-2", size="sm"),

        dbc.Checklist(options=[{"label": "3. 启用双重筛选", "value": 1}], value=[1], id="switch-dual-filter", switch=True, className="mb-3 text-secondary small fw-bold"),

        dbc.Button("开始分析", id="btn-analyze", color="primary", className="w-100 fw-bold", size="sm", style={"borderRadius": "4px", "backgroundColor": "#4c6ef5", "border": "none"}),
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
            dbc.Col(dbc.Card([dbc.CardBody([html.H6("实时走势与决策标线 (近半年)", className="fw-bold mb-1", style={"color": "#495057", "fontSize": "0.85rem"}), dcc.Graph(id="main-chart", style={"height": "280px"})], style={"padding": "10px"})], style=CARD_STYLE), width=9),
            dbc.Col(dbc.Card([dbc.CardBody([html.H6([html.I(className="fa-solid fa-globe-asia me-2"), "宏观大盘环境"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem"}), html.Div(id="out-macro", style={"height": "280px"})], style={"padding": "10px"})], style=CARD_STYLE), width=3),
        ], className="gx-2"),

        dbc.Row([
            dbc.Col(dbc.Card([dbc.CardBody([html.H6([html.I(className="fa-solid fa-file-invoice-dollar me-2"), "核心财务指标"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem"}), html.Div(id="out-financial", style={"height": "280px", "overflow": "hidden"})], style={"padding": "10px"})], style=CARD_STYLE), width=3),
            dbc.Col(dbc.Card([dbc.CardBody([html.H6([html.I(className="fa-solid fa-robot me-2"), "量化信号矩阵"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem"}), html.Div(id="out-quant", style={"height": "280px", "overflow": "auto"})], style={"padding": "10px"})], style=CARD_STYLE), width=2),
            dbc.Col(dbc.Card([dbc.CardBody([html.H6([html.I(className="fa-solid fa-brain me-2"), "AI 深度逻辑推演"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem"}), html.Div(id="out-reasoning", style={"height": "280px", "overflow-y": "auto", "fontSize": "0.8rem", "color": "#495057", "whiteSpace": "pre-wrap", "lineHeight": "1.5"})], style={"padding": "10px"})], style=CARD_STYLE), width=4),
            dbc.Col(dbc.Card([dbc.CardBody([html.H6([html.I(className="fa-solid fa-newspaper me-2"), "消息面动态"], className="fw-bold mb-1 text-secondary", style={"fontSize": "0.85rem"}), html.Div(id="out-news", style={"height": "280px", "overflow-y": "auto", "fontSize": "0.75rem", "color": "#868e96", "whiteSpace": "pre-wrap"})], style={"padding": "10px"})], style=CARD_STYLE), width=3),
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
                    {
                        'if': {'column_id': '详情'},
                        'cursor': 'pointer',
                        'color': '#4c6ef5',
                        'fontWeight': 'bold',
                        'backgroundColor': '#f0f4ff',
                    },
                    {
                        'if': {'column_id': '详情', 'state': 'active'},
                        'backgroundColor': '#dce4ff',
                        'border': '1px solid #4c6ef5'
                    }
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
     State("input-position", "value"), State("input-cost", "value"), State("daily-table", "data"), State("date-tabs", "active_tab")],
    prevent_initial_call=True
)
def unified_action_handler(n_clicks, active_cell, stock_code, flash_model, use_pro_switch, pro_model, dual_filter_switch, position, cost, table_data, active_tab):
    ctx = dash.callback_context
    if not ctx.triggered: return [dash.no_update] * 18
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    use_pro = bool(use_pro_switch)
    dual_filter = bool(dual_filter_switch)

    layout_cfg = dict(template="plotly_white", margin=dict(l=30, r=20, t=10, b=10), hovermode="x unified", xaxis_rangeslider_visible=False, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', xaxis_type='category')
    fig = go.Figure(layout=layout_cfg)

    def get_display_model_name(tag):
        if not tag: return "-"
        if tag.startswith("D-"):
            parts = tag.split("-")
            if len(parts) >= 3:
                p_name = MODEL_CONFIGS.get(parts[2], {}).get('name', parts[2])
                return f"{p_name}(双筛)"
        return MODEL_CONFIGS.get(tag, {}).get('name', tag)

    # 动态渲染颜色的辅助函数
    def format_dynamic_color(text, is_action=True):
        if not text: return "-"
        text_str = str(text)
        
        if is_action:
            if text_str == "买入":
                return html.Span(text_str, style={"color": "#f03e3e"})  # 红色
            elif text_str == "卖出":
                return html.Span(text_str, style={"color": "#2f9e44"})  # 绿色
        else:
            # 处理“方向预期”的颜色分级
            if "强烈看多" in text_str or text_str == "看多":
                return html.Span(text_str, style={"color": "#f03e3e"})  # 红色
            elif "偏多" in text_str:
                return html.Span(text_str, style={"color": "#ffb8b8"})  # 淡红色
            elif "强烈看空" in text_str or text_str == "看空":
                return html.Span(text_str, style={"color": "#2f9e44"})  # 绿色
            elif "偏空" in text_str:
                return html.Span(text_str, style={"color": "#c3ffcd"})  # 淡绿色
                
        return text_str
    
    if trigger_id == 'daily-table':
        if not active_cell or active_cell['column_id'] != '详情': return [dash.no_update] * 18
        row_data = table_data[active_cell['row']]
        h_stock, h_date, h_stock_name = row_data['股票代码'], active_tab, row_data.get('股票名称', '未知')
        
        in_fs, out_fs = glob.glob(f"input/{h_date}/{h_stock}_*_input_{h_date}.txt"), glob.glob(f"output/{h_date}/{h_stock}_*_output_*_{h_date}.txt")
        if not in_fs or not out_fs: return fig, f"{h_stock_name} ({h_stock})", "-", "-", "-", "-", "-", "-", "-", "-", "未能找到历史文本文件！", "-", "-", "-", "-", dash.no_update, dash.no_update, dash.no_update
        
        try:
            m_tag = os.path.basename(out_fs[0]).split('_output_')[1].rsplit('_', 1)[0]
        except:
            m_tag = "-"
        disp_model = get_display_model_name(m_tag)

        with open(in_fs[0], 'r', encoding='utf-8') as f: h_in = f.read()
        with open(out_fs[0], 'r', encoding='utf-8') as f: h_out = f.read()
        
        macro_ui = parse_and_build_macro_ui(h_in)
        fin_ui, quant_ui, news_t = parse_and_build_fin_and_quant_ui(h_in)
        parsed = parse_llm_json(h_out)
        
        beg, end = (datetime.strptime(h_date, "%Y-%m-%d") - timedelta(days=180)).strftime("%Y%m%d"), h_date.replace('-', '')
        df_chart = get_chart_data(h_stock, beg, end)
        
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.75, 0.25])
        if not df_chart.empty:
            df_chart['date'] = df_chart['date'].astype(str)
            fig.add_trace(go.Candlestick(x=df_chart['date'], open=df_chart['open'], high=df_chart['high'], low=df_chart['low'], close=df_chart['close'], increasing_line_color='#f03e3e', decreasing_line_color='#2f9e44'), row=1, col=1)
            colors = ['#f03e3e' if row['close'] >= row['open'] else '#2f9e44' for _, row in df_chart.iterrows()]
            fig.add_trace(go.Bar(x=df_chart['date'], y=df_chart['volume'], marker_color=colors, opacity=0.7), row=2, col=1)
            buy_p, sell_p, stop_p = parsed["buy_p"], parsed["sell_p"], parsed["stop_p"]
            if buy_p and str(buy_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(buy_p), line_dash="dot", line_color="#be4bdb", annotation_text="买点", row=1, col=1)
            if sell_p and str(sell_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(sell_p), line_dash="dot", line_color="#f03e3e", annotation_text="目标", row=1, col=1)
            if stop_p and str(stop_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(stop_p), line_dash="dot", line_color="#37b24d", annotation_text="止损", row=1, col=1)
        fig.update_layout(**layout_cfg)
        fig.update_xaxes(type='category', tickmode='auto', nticks=12)
        
        return fig, f"{h_stock_name} ({h_stock})", format_dynamic_color(parsed["action"], True), format_dynamic_color(parsed["expectation"], False), parsed["pos_adv"], parsed["confidence"], str(parsed["buy_p"]) if parsed["buy_p"] else "-", str(parsed["sell_p"]) if parsed["sell_p"] else "-", str(parsed["stop_p"]) if parsed["stop_p"] else "-", disp_model, parsed["reasoning"], news_t, macro_ui, fin_ui, quant_ui, dash.no_update, dash.no_update, dash.no_update

    if not stock_code: return [dash.no_update] * 18
    c_date = get_logical_date()
    c_str, end, beg = c_date.strftime("%Y-%m-%d"), c_date.isoformat().replace('-', ''), (c_date - timedelta(days=180)).isoformat().replace('-', '')
    stock_code = stock_code.strip()
    s_name = get_stock_name_bs(stock_code)
    safe_s_name = re.sub(r'[\\/:*?"<>|]', '', s_name) 
    
    df_chart = get_chart_data(stock_code, beg, end)
    s_price = df_chart['close'].iloc[-1] if not df_chart.empty else 0
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.75, 0.25])
    if not df_chart.empty:
        df_chart['date'] = df_chart['date'].astype(str)
        fig.add_trace(go.Candlestick(x=df_chart['date'], open=df_chart['open'], high=df_chart['high'], low=df_chart['low'], close=df_chart['close'], increasing_line_color='#f03e3e', decreasing_line_color='#2f9e44'), row=1, col=1)
        fig.add_trace(go.Bar(x=df_chart['date'], y=df_chart['volume'], marker_color=['#f03e3e' if row['close'] >= row['open'] else '#2f9e44' for _, row in df_chart.iterrows()], opacity=0.7), row=2, col=1)
    fig.update_layout(**layout_cfg)
    fig.update_xaxes(type='category', tickmode='auto', nticks=12)

    in_str = get_stock_data(stock_code=stock_code, beg=beg, end=end, current_date=c_str)
    news_titles = fetch_news_safely(stock_code, safe_s_name, c_str)
    user_msg = f"""基于获得的以下数据和新闻消息，做出你的交易决策。\n\n{in_str}\n\n最近三十个交易日数据如下：\n{df_chart.tail(30).to_string(index=False) if not df_chart.empty else "暂无"}\n\n相关新闻如下：\n{news_titles}\n\n当前该股持仓：{position} 股\n当前持仓成本: {cost} 元\n\n请记住，行动必须是买入、卖出、持有或观望。\n谨慎考虑交易决策：考虑当前股价是高位还是低位，在低位买入，高位卖出。\n考虑自己的持仓成本，在有足够浮盈的情况下考虑卖出收获现金实利。"""

    os.makedirs(f"input/{c_str}", exist_ok=True)
    with open(f"input/{c_str}/{stock_code}_{safe_s_name}_input_{c_str}.txt", 'w', encoding='utf-8') as f: f.write(user_msg)

    try:
        with open('LLM system content.txt', 'r', encoding='utf-8') as f: sys_content = f.read()
    except: sys_content = "你是一个专业的量化交易AI..."
        
    run_pro = False
    res_text = ""
    
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

    if run_pro: 
        res_text = get_LLM_message(system_content=sys_content, user_message=user_msg, model_id=pro_model)
    
    model_tag = f"D-{flash_model}-{pro_model}" if (run_pro and dual_filter) else (pro_model if run_pro else flash_model)
    disp_model = get_display_model_name(model_tag)
    
    os.makedirs(f"output/{c_str}", exist_ok=True)
    with open(f"output/{c_str}/{stock_code}_{safe_s_name}_output_{model_tag}_{c_str}.txt", 'w', encoding='utf-8') as f: f.write(res_text)

    parsed = parse_llm_json(res_text)
    macro_ui = parse_and_build_macro_ui(user_msg)
    fin_ui, quant_ui, news_t = parse_and_build_fin_and_quant_ui(user_msg)

    buy_p, sell_p, stop_p = parsed["buy_p"], parsed["sell_p"], parsed["stop_p"]
    if buy_p and str(buy_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(buy_p), line_dash="dot", line_color="#be4bdb", annotation_text="买点", row=1, col=1)
    if sell_p and str(sell_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(sell_p), line_dash="dot", line_color="#f03e3e", annotation_text="目标", row=1, col=1)
    if stop_p and str(stop_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(stop_p), line_dash="dot", line_color="#37b24d", annotation_text="止损", row=1, col=1)

    rr_str = 'N/A'
    try:
        if buy_p and sell_p and stop_p and float(buy_p) - float(stop_p) > 0: rr_str = f"{(float(sell_p) - float(buy_p)) / (float(buy_p) - float(stop_p)):.2f}:1"
    except: pass

    pd.DataFrame([{
        "股票代码": stock_code, "股票名称": s_name, "决策模型": disp_model, "当前价格": s_price, "预期": parsed["expectation"], "操作": parsed["action"], "建议仓位": parsed["pos_adv"], "置信度": parsed["confidence"], "建议买入价": str(buy_p) if buy_p else "-", "目标卖出价": str(sell_p) if sell_p else "-", "建议止损价": str(stop_p) if stop_p else "-", "回报风险比": rr_str
    }]).to_csv(f"output/{c_str}/Daily Table_{c_str}.csv", index=False, header=not os.path.exists(f"output/{c_str}/Daily Table_{c_str}.csv"), mode='a', encoding='utf-8-sig')

    return fig, f"{s_name} ({stock_code})", format_dynamic_color(parsed["action"], True), format_dynamic_color(parsed["expectation"], False), parsed["pos_adv"], parsed["confidence"], str(buy_p) if buy_p else "-", str(sell_p) if sell_p else "-", str(stop_p) if stop_p else "-", disp_model, parsed["reasoning"], news_t, macro_ui, fin_ui, quant_ui, [dbc.Tab(label=date, tab_id=date) for date in get_all_output_dates()[:5]], c_str, load_daily_table_by_date(c_str)

if __name__ == '__main__':
    app.run(debug=True, port=8050)