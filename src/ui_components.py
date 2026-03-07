# -*- coding: utf-8 -*-
from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import glob
import re
import json_repair

def get_index_kline_fig():
    """生成大盘走势的迷你 K 线图"""
    files = glob.glob("log/index_data/sh000001_daily_*.csv")
    fig = go.Figure()
    fig.update_layout(template="plotly_white", margin=dict(l=0, r=0, t=5, b=0), height=120, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', xaxis=dict(visible=False, type='category'), yaxis=dict(visible=False))
    if files:
        try:
            df = pd.read_csv(max(files)).tail(60) 
            df['date'] = df['date'].astype(str)
            fig.add_trace(go.Candlestick(x=df['date'], open=df['open'], high=df['high'], low=df['low'], close=df['close'], increasing_line_color='#f03e3e', decreasing_line_color='#2f9e44'))
            fig.update_layout(xaxis_rangeslider_visible=False)
        except: pass
    return fig

def parse_and_build_macro_ui(input_text):
    """解析大盘宏观文本并构建对应的 UI 面板"""
    macro_dict = {}
    if "【宏观大盘环境】" in input_text:
        block = input_text.split("【宏观大盘环境】")[1].split("======")[0]
        for line in block.split('\n'):
            if ':' in line or '：' in line:
                k, v = re.split(r'[:：]', line, 1)
                macro_dict[k.strip()] = v.strip()
    
    idx_val = macro_dict.get("上证指数", "-").split('(')[0].strip()
    trend = macro_dict.get("大盘趋势", "-").split('(')[0].strip()
    rsi = macro_dict.get("情绪指标(RSI14)", "-").split('(')[0].strip()
    z_score = macro_dict.get("偏离度(Z-Score)", "-").split('(')[0].strip()
    
    # --- 计算大盘动态颜色 ---
    trend_color = "#2d3748"
    if "多头" in trend or "偏多" in trend:
        trend_color = "#f03e3e" # 红色
    elif "空头" in trend or "偏空" in trend:
        trend_color = "#2f9e44" # 绿色
        
    rsi_color = "#2d3748"
    try:
        rsi_num = float(re.sub(r'[^\d\.-]', '', rsi))
        if rsi_num > 70: rsi_color = "#f03e3e"
        elif rsi_num < 30: rsi_color = "#2f9e44"
    except: pass
    
    z_color = "#2d3748"
    try:
        z_num = float(re.sub(r'[^\d\.-]', '', z_score))
        if z_num > 2: z_color = "#f03e3e"
        elif z_num < -2: z_color = "#2f9e44"
    except: pass
    
    def mini_kpi(label, val, color="#495057"):
        return html.Div([
            html.Div(label, style={"fontSize": "0.7rem", "color": "#868e96"}),
            html.Div(val, style={"fontSize": "0.95rem", "fontWeight": "bold", "color": color})
        ], style={"backgroundColor": "#f8f9fa", "padding": "4px", "borderRadius": "4px", "textAlign": "center"})

    return html.Div([
        dcc.Graph(figure=get_index_kline_fig(), config={'displayModeBar': False}),
        html.Div([
            dbc.Row([dbc.Col(mini_kpi("上证指数", idx_val, "#f03e3e"), width=6, className="pe-1"), dbc.Col(mini_kpi("大盘趋势", trend, trend_color), width=6, className="ps-1")], className="mb-1"),
            dbc.Row([dbc.Col(mini_kpi("RSI情绪", rsi, rsi_color), width=6, className="pe-1"), dbc.Col(mini_kpi("偏离度", z_score, z_color), width=6, className="ps-1")]),
        ], className="mt-1")
    ], style={"height": "260px", "display": "flex", "flexDirection": "column", "justifyContent": "space-between"})

def parse_and_build_fin_and_quant_ui(input_text):
    """解析个股财务与量化数据并构建 UI 面板"""
    fin_dict = {}
    quant_dict = {}
    news_text = "暂无新闻数据"
    
    lines = input_text.split('\n')
    for line in lines:
        if ':' in line or '：' in line:
            k, v = re.split(r'[:：]', line, 1)
            fin_dict[k.strip()] = v.strip()
            
    try:
        s_idx, e_idx = input_text.find('量化策略信号矩阵:\n{'), input_text.find('}\n滚动市盈率')
        if s_idx != -1 and e_idx != -1:
            quant_dict = json_repair.loads(input_text[s_idx + 9 : e_idx + 1])
    except: pass
    
    if "相关新闻如下：" in input_text:
        try: news_text = input_text.split("相关新闻如下：")[1].split("当前该股持仓：")[0].strip()
        except: pass

    def format_market_cap(val_str):
        try: return f"{float(val_str) / 100000000:.2f}亿"
        except: return val_str

    def get_color(val_str, color_type):
        try:
            num = float(re.sub(r'[^\d\.-]', '', val_str))
            if color_type == 'growth':
                return "#f03e3e" if num > 0 else "#2f9e44" if num < 0 else "#2d3748"
            elif color_type == 'percentile':
                # 估值分位低(<30)代表低估看多，用红色；高(>70)代表高估看跌，用绿色
                return "#f03e3e" if num < 30 else "#2f9e44" if num > 70 else "#2d3748"
        except: pass
        return "#2d3748"

    def f_item(label, key, color_type='neutral'):
        val = fin_dict.get(key, "-")
        if key == "总市值": val = format_market_cap(val)
        c = get_color(val, color_type) if color_type != 'neutral' else "#2d3748"
        return html.Div([
            html.Div(label, style={"color": "#868e96", "fontSize": "0.65rem", "whiteSpace": "nowrap"}),
            html.Div(val, style={"color": c, "fontWeight": "bold", "fontSize": "0.8rem", "whiteSpace": "nowrap"})
        ], className="col-4 mb-1")

    fin_ui = html.Div([
        html.Div([
            html.H6("估值与规模", style={"fontSize": "0.7rem", "fontWeight": "bold", "color": "#495057", "marginBottom": "4px"}),
            dbc.Row([f_item("总市值", "总市值"), f_item("PE(TTM)", "滚动市盈率 P/E(TTM)"), f_item("PE分位", "市盈率(PE)历史分位", 'percentile'), f_item("PB", "市净率 P/B"), f_item("PB分位", "市净率(PB)历史分位", 'percentile'), f_item("PS", "市销率 P/S")], className="gx-1 mb-0"),
        ], style={"backgroundColor": "#f8f9fa", "padding": "6px", "borderRadius": "4px", "marginBottom": "8px"}),
        
        html.Div([
            html.H6("盈利与成长", style={"fontSize": "0.7rem", "fontWeight": "bold", "color": "#495057", "marginBottom": "4px"}),
            dbc.Row([
                f_item("ROE", "净资产收益率(ROE)", 'growth'), f_item("毛利率", "毛利率", 'growth'), f_item("净利率", "销售净利率", 'growth'), 
                f_item("营收同比", "营业总收入增长率", 'growth'), f_item("净利同比", "净利润增长率", 'growth'), f_item("负债率", "资产负债率")
            ], className="gx-1 mb-0"),
        ], style={"backgroundColor": "#f8f9fa", "padding": "6px", "borderRadius": "4px"})
    ])
    
    quant_ui = html.Div([
        html.Div([
            html.Div(k, style={"color": "#495057", "fontSize": "0.75rem", "fontWeight": "bold"}),
            html.Div([
                html.Span(f"{v.get('信号', '-')} ", style={"color": "#37b24d" if v.get('信号')=='看空' else "#f03e3e" if v.get('信号') in ['看多','买入'] else "#868e96", "fontWeight": "bold", "fontSize": "0.75rem"}),
                html.Span(f"({v.get('置信度', '-')})", style={"color": "#adb5bd", "fontSize": "0.7rem"})
            ])
        ], style={"display": "flex", "justifyContent": "space-between", "borderBottom": "1px solid #f1f3f5", "padding": "4px 0"})
        for k, v in quant_dict.items()
    ], style={"padding": "0 2px"})
    
    # 格式化新闻文本
    formatted_news = []
    if news_text and news_text != "暂无新闻数据":
        for line in news_text.split('\n'):
            line = line.strip()
            if line:
                # 给每条新闻加个小圆点前缀和底边距
                formatted_news.append(html.Div(["• ", line], style={"marginBottom": "6px", "lineHeight": "1.4"}))
    else:
        formatted_news = "暂无新闻数据"
        
    return fin_ui, quant_ui, formatted_news