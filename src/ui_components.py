# -*- coding: utf-8 -*-
from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import glob
import re
import json_repair

# 导入在 utils.py 里写好的高级画图引擎
from src.utils import create_advanced_kline_fig

def get_index_kline_fig():
    """生成大盘走势的完整指标 K 线图"""
    files = glob.glob("log/index_data/sh000001_daily_*.csv")
    if files:
        try:
            df = pd.read_csv(max(files)).tail(120) 
            fig = create_advanced_kline_fig(df)
            
            # 【取消固定高度】：去掉 height 限制，开启 autosize，让外层 CSS 容器接管大小
            fig.update_layout(
                autosize=True,
                showlegend=False, 
                margin=dict(l=30, r=10, t=25, b=0) 
            )
            
            fig.update_xaxes(showticklabels=False)
            
            if len(fig.layout.updatemenus) >= 2:
                fig.layout.updatemenus[0].font.size = 9
                fig.layout.updatemenus[1].font.size = 9
                fig.layout.updatemenus[0].x = 0
                fig.layout.updatemenus[0].y = 1.10 
                fig.layout.updatemenus[1].x = 0.52 
                fig.layout.updatemenus[1].y = 1.10 
                
            return fig
        except Exception as e: 
            print(f"大盘绘图错误: {e}")
            pass
            
    return go.Figure()

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

    # 【终极自适应方案】：使用 position: relative + absolute 强制 Plotly 填满 Flex 剩余空间
    return html.Div([
        
        # 上半部分：图表区（自适应撑满）
        html.Div(
            dcc.Graph(
                figure=get_index_kline_fig(), 
                # 必须开启 responsive=True 让 Plotly 监听容器形变
                config={'displayModeBar': False, 'responsive': True}, 
                # 绝对定位，强制宽高 100% 贴合父容器
                style={"position": "absolute", "top": 0, "left": 0, "width": "100%", "height": "100%"}
            ),
            # flexGrow: 1 负责抢占剩下的所有高度，position: relative 为内部的绝对定位提供锚点
            style={"flexGrow": 1, "position": "relative", "minHeight": "200px"}
        ),
        
        # 下半部分：固定数据区（不参与压缩）
        html.Div([
            dbc.Row([dbc.Col(mini_kpi("上证指数", idx_val, "#f03e3e"), width=6, className="pe-1"), dbc.Col(mini_kpi("大盘趋势", trend, trend_color), width=6, className="ps-1")], className="mb-1"),
            dbc.Row([dbc.Col(mini_kpi("RSI情绪", rsi, rsi_color), width=6, className="pe-1"), dbc.Col(mini_kpi("偏离度", z_score, z_color), width=6, className="ps-1")]),
        ], style={"flexShrink": 0, "marginTop": "10px"})
        
    # 外层卡片高度设为 100%（跟随你在 app.py 里的 400px），并使用 flex 纵向排布
    ], style={"height": "100%", "minHeight": "360px", "display": "flex", "flexDirection": "column"})

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
                return "#f03e3e" if num < 30 else "#2f9e44" if num > 70 else "#2d3748"
        except: pass
        return "#2d3748"

    def f_item(label, key, color_type='neutral'):
        val = fin_dict.get(key, "-")
        if key == "总市值": val = format_market_cap(val)
        c = get_color(val, color_type) if color_type != 'neutral' else "#2d3748"
        return html.Div([
            html.Div(label, style={"color": "#868e96", "fontSize": "0.65rem", "whiteSpace": "nowrap"}),
            # 【瘦身】：去掉了原有的 marginTop，让数值和标题贴得更紧
            html.Div(val, style={"color": c, "fontWeight": "bold", "fontSize": "0.85rem", "whiteSpace": "nowrap"})
        # 【瘦身】：将底部的 margin 从 mb-2 压缩到了 mb-1
        ], className="col-4 mb-1", style={"textAlign": "center"})

    fin_ui = html.Div([
        html.Div([
            # 【瘦身】：标题底部间距变小
            html.H6("估值与规模", style={"fontSize": "0.75rem", "fontWeight": "bold", "color": "#495057", "marginBottom": "4px", "paddingBottom": "2px", "borderBottom": "1px solid #e9ecef"}),
            dbc.Row([f_item("总市值", "总市值"), f_item("PE(TTM)", "滚动市盈率 P/E(TTM)"), f_item("PE分位", "市盈率(PE)历史分位", 'percentile')], className="gx-1 mb-0"),
            dbc.Row([f_item("PB", "市净率 P/B"), f_item("PB分位", "市净率(PB)历史分位", 'percentile'), f_item("PS", "市销率 P/S")], className="gx-1 mb-0"),
        # 【瘦身】：背景块的 padding 从 10px 降到 6px，底部 margin 从 10px 降到 6px
        ], style={"backgroundColor": "#f8f9fa", "padding": "6px", "borderRadius": "6px", "marginBottom": "6px"}),
        
        html.Div([
            html.H6("盈利与成长", style={"fontSize": "0.75rem", "fontWeight": "bold", "color": "#495057", "marginBottom": "4px", "paddingBottom": "2px", "borderBottom": "1px solid #e9ecef"}),
            dbc.Row([f_item("ROE", "净资产收益率(ROE)", 'growth'), f_item("毛利率", "毛利率", 'growth'), f_item("净利率", "销售净利率", 'growth')], className="gx-1 mb-0"),
            dbc.Row([f_item("营收同比", "营业总收入增长率", 'growth'), f_item("净利同比", "净利润增长率", 'growth'), f_item("负债率", "资产负债率")], className="gx-1 mb-0")
        ], style={"backgroundColor": "#f8f9fa", "padding": "6px", "borderRadius": "6px"})
    ])
    
    # 将字典转为 list 方便判断最后一项
    quant_items = list(quant_dict.items())
    
    quant_ui = html.Div([
        html.Div([
            html.Div(k, style={"color": "#495057", "fontSize": "0.75rem", "fontWeight": "bold"}),
            html.Div([
                html.Span(f"{v.get('信号', '-')} ", style={"color": "#37b24d" if v.get('信号')=='看空' else "#f03e3e" if v.get('信号') in ['看多','买入'] else "#868e96", "fontWeight": "bold", "fontSize": "0.75rem"}),
                html.Span(f"({v.get('置信度', '-')})", style={"color": "#adb5bd", "fontSize": "0.7rem"})
            ])
        ], style={
            "display": "flex", "justifyContent": "space-between", 
            # 最后一项去掉底边框，其余使用精致的虚线
            "borderBottom": "none" if i == len(quant_items) - 1 else "1px dashed #dee2e6", 
            "padding": "6px 4px"
        })
        for i, (k, v) in enumerate(quant_items)
    # 外层套上与财务指标一致的浅灰色背景和圆角
    ], style={"backgroundColor": "#f8f9fa", "padding": "8px 10px", "borderRadius": "6px", "marginTop": "2px"})
    
    formatted_news = []
    if news_text and news_text != "暂无新闻数据":
        for line in news_text.split('\n'):
            line = line.strip()
            if line:
                formatted_news.append(html.Div(["• ", line], style={"marginBottom": "4px", "lineHeight": "1.3"})) # 压缩新闻行距
    else:
        formatted_news = "暂无新闻数据"
        
    return fin_ui, quant_ui, formatted_news