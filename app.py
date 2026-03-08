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
import concurrent.futures  # 新增并发库用于多模型议事
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
    parse_llm_json,
    create_advanced_kline_fig,
)
from src.ui_components import parse_and_build_macro_ui, parse_and_build_fin_and_quant_ui

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
        html.H6("流水线模型配置", className="text-muted fw-bold mb-2", style={"fontSize": "0.8rem", "letterSpacing": "1px"}),
        
        html.Label("1. 基础/初筛模型", className="small fw-bold text-secondary mb-1"),
        dbc.Select(id="dropdown-flash-model", options=MODEL_OPTIONS, value=default_flash_model, className="mb-2", size="sm"),
        
        dbc.Checklist(options=[{"label": "2. 启用 Pro 模型 (或裁判)", "value": 1}], value=[1], id="switch-use-pro", switch=True, className="mb-1 text-secondary small fw-bold"),
        html.Label("选择 Pro / 裁判模型", className="small fw-bold text-secondary mb-1"),
        dbc.Select(id="dropdown-pro-model", options=MODEL_OPTIONS, value=default_pro_model, className="mb-2", size="sm"),

        dbc.Checklist(options=[{"label": "3. 启用双重筛选过滤", "value": 1}], value=[1], id="switch-dual-filter", switch=True, className="mb-2 text-secondary small fw-bold"),

        # --- 【新增】多模型议事配置 ---
        html.Hr(style={"margin": "10px 0", "opacity": "0.15"}),
        html.H6("多模型议事 (MoA)", className="text-muted fw-bold mb-2", style={"fontSize": "0.8rem", "letterSpacing": "1px", "color": "#e64980"}),
        dbc.Checklist(options=[{"label": "启用 AI 裁判委员会", "value": 1}], value=[], id="switch-use-moa", switch=True, className="mb-1 text-secondary small fw-bold"),
        html.Label("选择参会模型 (建议 2-4 个)", className="small fw-bold text-secondary mb-1"),
        dcc.Dropdown(id="dropdown-committee-models", options=MODEL_OPTIONS, multi=True, placeholder="选择研究员模型...", className="mb-3", style={"fontSize": "0.8rem"}),

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
    
    def get_price_display(target_p, buy_p):
        """计算目标价/止损价相对于买入价的百分比，并格式化输出"""
        if not target_p or str(target_p) == "-": return "-"
        if not buy_p or str(buy_p) == "-": return str(target_p)
        
        try:
            t_val, b_val = float(target_p), float(buy_p)
            if b_val > 0:
                pct = (t_val - b_val) / b_val * 100
                sign = "+" if pct > 0 else ""
                # 主数字保持原样，百分比缩小字号并降低透明度，更具高级感
                return html.Span([
                    str(t_val),
                    html.Span(f" ({sign}{pct:.2f}%)", style={"fontSize": "0.75rem", "opacity": "0.85", "marginLeft": "2px"})
                ])
        except Exception:
            pass
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
        df_chart = get_chart_data(h_stock, beg, end)
        
        fig = create_advanced_kline_fig(df_chart)
        if not df_chart.empty:
            buy_p, sell_p, stop_p = parsed["buy_p"], parsed["sell_p"], parsed["stop_p"]
            if buy_p and str(buy_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(buy_p), line_dash="dot", line_color="#be4bdb", annotation_text="买点", row=1, col=1)
            if sell_p and str(sell_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(sell_p), line_dash="dot", line_color="#f03e3e", annotation_text="目标", row=1, col=1)
            if stop_p and str(stop_p).replace('.', '', 1).isdigit(): fig.add_hline(y=float(stop_p), line_dash="dot", line_color="#37b24d", annotation_text="止损", row=1, col=1)
        
        buy_p, sell_p, stop_p = parsed.get("buy_p"), parsed.get("sell_p"), parsed.get("stop_p")
        
        # 使用辅助函数生成带百分比的显示组件
        sell_display = get_price_display(sell_p, buy_p)
        stop_display = get_price_display(stop_p, buy_p)
        buy_display = str(buy_p) if buy_p else "-"
        
        return fig, f"{h_stock_name} ({h_stock})", format_dynamic_color(parsed["action"], True), format_dynamic_color(parsed["expectation"], False), parsed["pos_adv"], parsed["confidence"], buy_display, sell_display, stop_display, disp_model, parsed["reasoning"], news_t, macro_ui, fin_ui, quant_ui, dash.no_update, dash.no_update, dash.no_update

    # ================= 分支 2：点击“开始分析”获取新决策 =================
    if not stock_code: return [dash.no_update] * 18
    c_date = get_logical_date()
    c_str, end, beg = c_date.strftime("%Y-%m-%d"), c_date.isoformat().replace('-', ''), (c_date - timedelta(days=720)).isoformat().replace('-', '')
    stock_code = stock_code.strip()
    s_name = get_stock_name_bs(stock_code)
    safe_s_name = re.sub(r'[\\/:*?"<>|]', '', s_name) 
    
    df_chart = get_chart_data(stock_code, beg, end)
    s_price = df_chart['close'].iloc[-1] if not df_chart.empty else 0
    fig = create_advanced_kline_fig(df_chart)

    in_str = get_stock_data(stock_code=stock_code, beg=beg, end=end, current_date=c_str)
    news_titles = fetch_news_safely(stock_code, safe_s_name, c_str)
    
    if not df_chart.empty:
        df_monthly_tmp = df_chart.copy()
        df_monthly_tmp['date'] = pd.to_datetime(df_monthly_tmp['date'])
        df_monthly_tmp['year_month'] = df_monthly_tmp['date'].dt.to_period('M')
        df_monthly = df_monthly_tmp.groupby('year_month').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).reset_index()
        df_monthly.rename(columns={'year_month': 'date'}, inplace=True)
        df_monthly['date'] = df_monthly['date'].astype(str)
        monthly_str = df_monthly.tail(20).to_string(index=False)
    else:
        monthly_str = "暂无"

    daily_str = df_chart.tail(20).to_string(index=False) if not df_chart.empty else "暂无"

    user_msg = f"""基于获得的以下数据和新闻消息，做出你的交易决策。

{in_str}

最近二十个交易日数据如下：
{daily_str}

最近二十个月K线数据如下：
{monthly_str}

相关新闻如下：
{news_titles}

当前该股持仓：{position} 股
当前持仓成本: {cost} 元

请记住，行动必须是买入、卖出、持有或观望。
谨慎考虑交易决策：考虑当前股价是高位还是低位，在低位买入，高位卖出。
考虑自己的持仓成本，在有足够浮盈的情况下考虑卖出收获现金实利。"""

    os.makedirs(f"input/{c_str}", exist_ok=True)
    with open(f"input/{c_str}/{stock_code}_{safe_s_name}_input_{c_str}.txt", 'w', encoding='utf-8') as f: f.write(user_msg)

    try:
        with open('LLM system content.txt', 'r', encoding='utf-8') as f: sys_content = f.read()
    except: sys_content = "你是一个专业的量化交易AI..."
        
    run_pro = False
    res_text = ""
    
    # 【一、 过滤/初筛阶段】
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

    # 【二、 高级决议阶段：单模型 vs. 多模型议事(MoA)】
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
            judge_msg += "1. 逻辑至上，拒绝盲从：金融市场真理往往掌握在少数人手中。绝对不要机械地‘少数服从多数’！你要寻找的是‘谁的底层逻辑更无懈可击’，‘谁捕捉到了当前主导定价的核心矛盾’。\n"
            judge_msg += "2. 交叉质证与证伪：重点审视研究员之间的【分歧点】。如果少数派指出了致命的风控隐患（如隐含的估值陷阱、量价背离、均值回归的极值点），且多数派未能有效应对，你应该果断采纳少数派意见，甚至一票否决多数派的狂热。\n"
            judge_msg += "3. 混沌期的防守哲学：如果研究员意见严重撕裂（如 2:2 对立），且双方都没有压倒性的逻辑证据，或者面临极高的不确定性，你可以直接判定为‘观望’以保护本金，或给出极低的试错仓位，切勿强行折中。\n\n"
            judge_msg += "请给出最终的决策、点位和仓位。你必须在 JSON 的 '原因' 字段中分段输出：\n"
            judge_msg += "【委员会共识与分歧】：简述各方观点的核心交锋点。\n"
            judge_msg += "【投资总监拍板逻辑】：详细说明你最终支持哪一方（或推翻所有人）的深度理由，以及该决策背后的盈亏比考量。\n"
            judge_msg += "注意：你的输出必须是一个单一的、严格符合原定系统提示词规范的 JSON 对象！\n"

            # 呼叫裁判模型进行最终裁决
            print(f"⚖️ 正在请求裁判模型 [{pro_model}] 进行最终综合拍板...")
            res_text = get_LLM_message(system_content=sys_content, user_message=judge_msg, model_id=pro_model)
            model_tag = f"MoA-{len(committee_models)}议事-{pro_model}"
            
        else:
            # 原有的单发 Pro 模型逻辑
            res_text = get_LLM_message(system_content=sys_content, user_message=user_msg, model_id=pro_model)
            model_tag = f"D-{flash_model}-{pro_model}" if dual_filter else pro_model
            
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

    # 在最终 return 之前，使用辅助函数生成带百分比的显示组件
    sell_display = get_price_display(sell_p, buy_p)
    stop_display = get_price_display(stop_p, buy_p)
    buy_display = str(buy_p) if buy_p else "-"

    return fig, f"{s_name} ({stock_code})", format_dynamic_color(parsed["action"], True), format_dynamic_color(parsed["expectation"], False), parsed["pos_adv"], parsed["confidence"], buy_display, sell_display, stop_display, disp_model, parsed["reasoning"], news_t, macro_ui, fin_ui, quant_ui, [dbc.Tab(label=date, tab_id=date) for date in get_all_output_dates()[:5]], c_str, load_daily_table_by_date(c_str)

if __name__ == '__main__':
    app.run(debug=True, port=8050)