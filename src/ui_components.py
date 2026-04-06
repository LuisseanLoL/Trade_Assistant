# -*- coding: utf-8 -*-
from dash import dcc, html
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import pandas as pd
import glob
import re
import json_repair
import plotly.subplots as sp

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
            
            if len(fig.layout.updatemenus) >= 2: # type: ignore
                fig.layout.updatemenus[0].font.size = 9 # type: ignore
                fig.layout.updatemenus[1].font.size = 9 # type: ignore
                fig.layout.updatemenus[0].x = 0 # type: ignore
                fig.layout.updatemenus[0].y = 1.10  # type: ignore
                fig.layout.updatemenus[1].x = 0.52  # type: ignore
                fig.layout.updatemenus[1].y = 1.10  # type: ignore
                
            return fig
        except Exception as e: 
            print(f"大盘绘图错误: {e}")
            pass
            
    return go.Figure()

def get_mini_index_fig():
    """专为大盘面板定制的轻量级 K 线图（修复 A 股红绿习惯）"""
    files = glob.glob("log/index_data/sh000001_daily_*.csv")
    if files:
        try:
            df = pd.read_csv(max(files)).tail(120) 
            df['date'] = df['date'].astype(str)
            
            fig = sp.make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.75, 0.25], vertical_spacing=0.02)
            
            # 成交量颜色 (红涨绿跌)
            colors = ['#2f9e44' if row['close'] < row['open'] else '#f03e3e' for _, row in df.iterrows()]
            
            # 🌟 修复 1：强制设定 K 线的 A 股红绿配色
            fig.add_trace(go.Candlestick(
                x=df['date'], open=df['open'], high=df['high'], low=df['low'], close=df['close'], 
                increasing_line_color='#f03e3e', increasing_fillcolor='#f03e3e', # 涨为红
                decreasing_line_color='#2f9e44', decreasing_fillcolor='#2f9e44', # 跌为绿
                name='上证'
            ), row=1, col=1)
            
            df['MA20'] = df['close'].rolling(20).mean()
            fig.add_trace(go.Scatter(
                x=df['date'], y=df['MA20'], line=dict(color='#4c6ef5', width=1.5), name='MA20'
            ), row=1, col=1)
            
            fig.add_trace(go.Bar(
                x=df['date'], y=df['volume'], marker_color=colors, name='成交量'
            ), row=2, col=1)
            
            fig.update_layout(
                autosize=True, showlegend=False, 
                margin=dict(l=35, r=10, t=10, b=0),
                xaxis_rangeslider_visible=False,
                plot_bgcolor="#ffffff", paper_bgcolor="#ffffff"
            )
            
            fig.update_xaxes(type='category', showticklabels=False, row=1, col=1)
            fig.update_xaxes(type='category', showticklabels=False, row=2, col=1)
            fig.update_yaxes(showgrid=True, gridcolor='#f1f3f5')
            
            return fig
        except Exception as e: 
            print(f"大盘绘图错误: {e}")
            pass
            
    return go.Figure()

def parse_and_build_macro_ui(input_text):
    """解析大盘宏观文本并构建对应的 UI 面板（修复截断与逻辑）"""
    
    def safe_extract(pattern, default="-"):
        match = re.search(pattern, input_text)
        return match.group(1).strip() if match else default

    idx_val  = safe_extract(r"上证指数\s*([\d\.]+)")
    trend    = safe_extract(r"趋势与量能[：:]\s*(.*?)[（\(；。]")
    vol_ratio = safe_extract(r"均量的\s*([\d\.]+%?)")
    
    pe_pct   = safe_extract(r"近10年\s*([\d\.]+)%\s*分位")
    pe_status= safe_extract(r"分位，(.*?)）")
    pe_combined = f"{pe_pct}%" + (f"({pe_status})" if pe_status != "-" else "")

    rsi      = safe_extract(r"RSI14=([\d\.]+)")
    bond     = safe_extract(r"十年期国债收益率[：:]\s*([\d\.]+%?)")
    if_change = safe_extract(r"IF主力基差.*?较昨日([^\)）;；]+)")

    # 🌟 修复 2.1：精简长线牛熊的文本，防止截断
    bull_match = re.search(r"牛熊分界线.*?[上下]", input_text)
    if bull_match:
        bull_bear = bull_match.group(0).replace("(MA200)", "").replace("（MA200）", "")
    else:
        bull_bear = "-"

    # 🌟 修复 2.2：修复布林带空间的提取逻辑，处理复杂括号
    bb_match = re.search(r"布林极限%b.*?[\(（](.*?)[,，;；\n]", input_text)
    if bb_match:
        bb_status = bb_match.group(1).rstrip(')）').strip()
        if '(' in bb_status and ')' not in bb_status: bb_status += ')'
        if '（' in bb_status and '）' not in bb_status: bb_status += '）'
    else:
        bb_status = "-"

    # --- 颜色计算引擎 ---
    def smart_color(text, red_words, green_words):
        if any(w in text for w in red_words): return "#f03e3e"
        if any(w in text for w in green_words): return "#2f9e44"
        return "#2d3748"

    trend_color = smart_color(trend, ["多头", "偏多", "向上"], ["空头", "偏空", "向下", "跌破"])
    bb_color    = smart_color(bb_status, ["下轨", "支撑", "超卖"], ["上轨", "压力", "超买"])
    bb_color    = "#e64980" if "支撑" in bb_status else bb_color 
    bull_color  = smart_color(bull_bear, ["之上", "多头"], ["之下", "空头"])
    if_color    = smart_color(if_change, ["走强", "收敛", "做多"], ["走弱", "扩大", "做空"])
    pe_color    = "#f59f00" if "高估" in pe_status else "#f03e3e" if "低估" in pe_status else "#2d3748"
    
    # 🌟 修复 3：量能比以 100% 为分水岭
    vol_color = "#2d3748"
    try:
        val = float(vol_ratio.strip('%'))
        if val >= 100: vol_color = "#f03e3e"  # 正常放量及以上显示红色
        elif val < 100: vol_color = "#2f9e44" # 缩量显示绿色
    except: pass

    rsi_color = "#2d3748"
    try:
        rsi_num = float(rsi)
        if rsi_num > 70: rsi_color = "#f03e3e"
        elif rsi_num < 30: rsi_color = "#2f9e44"
    except: pass

    # --- UI 构建部分保持不变 ---
    def mini_kpi(label, val, color="#495057"):
        return html.Div([
            html.Div(label, style={"fontSize": "0.65rem", "color": "#868e96", "whiteSpace": "nowrap"}),
            html.Div(val, style={"fontSize": "0.8rem", "fontWeight": "bold", "color": color, "whiteSpace": "nowrap", "overflow": "hidden", "textOverflow": "ellipsis"})
        ], style={"backgroundColor": "#f8f9fa", "padding": "4px", "borderRadius": "4px", "textAlign": "center"})

    return html.Div([
        html.Div(
            dcc.Graph(
                figure=get_mini_index_fig(), 
                config={'displayModeBar': False, 'responsive': True}, 
                style={"position": "absolute", "top": 0, "left": 0, "width": "100%", "height": "100%"}
            ),
            style={"flexGrow": 1, "position": "relative", "minHeight": "180px"}
        ),
        
        html.Div([
            dbc.Row([
                dbc.Col(mini_kpi("上证指数", idx_val, "#f03e3e"), width=4, className="px-1"), 
                dbc.Col(mini_kpi("大盘趋势", trend, trend_color), width=4, className="px-1"),
                dbc.Col(mini_kpi("量能均值比", vol_ratio, vol_color), width=4, className="px-1")
            ], className="mb-1 mx-0"),
            dbc.Row([
                dbc.Col(mini_kpi("长线牛熊", bull_bear, bull_color), width=4, className="px-1"),
                dbc.Col(mini_kpi("布林空间", bb_status, bb_color), width=4, className="px-1"), 
                dbc.Col(mini_kpi("RSI情绪", rsi, rsi_color), width=4, className="px-1")
            ], className="mb-1 mx-0"),
            dbc.Row([
                dbc.Col(mini_kpi("A股估值水位", pe_combined, pe_color), width=4, className="px-1"),
                dbc.Col(mini_kpi("期指边际变化", if_change, if_color), width=4, className="px-1"),
                dbc.Col(mini_kpi("10年期国债", bond, "#845ef7"), width=4, className="px-1")
            ], className="mx-0")
        ], style={"flexShrink": 0, "marginTop": "8px"})
        
    ], style={"height": "100%", "minHeight": "360px", "display": "flex", "flexDirection": "column"})

def parse_and_build_fin_and_quant_ui(input_text):
    """解析个股财务与量化数据并构建 UI 面板"""
    
    # 0. 全局清洗不可见字符（保留，防止外部数据源污染）
    input_text = input_text.replace('\xa0', ' ').replace('\u200b', '').replace('\u2028', '\n')
    
    fin_dict = {}
    quant_dict = {}
    news_text = "暂无新闻数据"
    
    # 1. 解析财务指标
    lines = input_text.split('\n')
    for line in lines:
        if ':' in line or '：' in line:
            k, v = re.split(r'[:：]', line, 1)
            fin_dict[k.strip()] = v.strip()
            
    # 2. 【终极修复】解析量化矩阵：利用固定标题作为锚点，提取两个标题之间的完整文本块
    try:
        start_flag = "【量化策略信号矩阵】"
        end_flag = "【核心财务指标】"
        
        if start_flag in input_text and end_flag in input_text:
            # 截取两个标题中间的所有内容
            block = input_text.split(start_flag)[1].split(end_flag)[0]
            
            # 找到最外层的正反大括号
            s_idx = block.find('{')
            e_idx = block.rfind('}')
            
            if s_idx != -1 and e_idx != -1:
                json_str = block[s_idx : e_idx + 1]
                quant_dict = json_repair.loads(json_str)
    except Exception as e: 
        print(f"量化矩阵解析异常: {e}")
        pass
    
    # 3. 解析新闻模块
    if "相关新闻如下：" in input_text:
        try: 
            news_part = input_text.split("相关新闻如下：")[1]
            if "当前该股仓位：" in news_part:
                news_text = news_part.split("当前该股仓位：")[0].strip()
            else:
                news_text = news_part.split("当前该股持仓：")[0].strip()
        except: pass

    # --- 以下构建 UI 逻辑 ---
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
            html.Div(val, style={"color": c, "fontWeight": "bold", "fontSize": "0.8rem", "whiteSpace": "nowrap", "overflow": "hidden", "textOverflow": "ellipsis"})
        ], className="col-4 mb-1", style={"textAlign": "center"})

    # 1. 核心财务指标面板 (扩容优化)
    report_date = fin_dict.get("最新财务报告期", "-")
    if len(report_date) == 8:
        report_date = f"{report_date[:4]}-{report_date[4:6]}-{report_date[6:]}"

    fin_ui = html.Div([
        html.Div([
            html.Div(f"财报期: {report_date}", style={"fontSize": "0.65rem", "float": "right", "color": "#4c6ef5", "fontWeight": "bold", "backgroundColor": "#edf2ff", "padding": "2px 6px", "borderRadius": "4px", "marginTop": "-2px"}),
            html.H6("估值与规模", style={"fontSize": "0.75rem", "fontWeight": "bold", "color": "#495057", "marginBottom": "4px", "paddingBottom": "2px", "borderBottom": "1px solid #e9ecef"}),
            dbc.Row([f_item("总市值", "总市值"), f_item("PE(TTM)", "滚动市盈率 P/E(TTM)"), f_item("PE分位", "市盈率(PE)历史分位", 'percentile')], className="gx-1 mb-0"),
            dbc.Row([f_item("PB", "市净率 P/B"), f_item("PB分位", "市净率(PB)历史分位", 'percentile'), f_item("PS", "市销率 P/S")], className="gx-1 mb-0"),
            dbc.Row([f_item("股息率", "股息率(TTM)"), f_item("营收", "营业总收入"), f_item("净利润", "净利润")], className="gx-1 mb-0")
        ], style={"backgroundColor": "#f8f9fa", "padding": "6px", "borderRadius": "6px", "marginBottom": "6px"}),
        
        html.Div([
            html.H6("盈利与营运", style={"fontSize": "0.75rem", "fontWeight": "bold", "color": "#495057", "marginBottom": "4px", "paddingBottom": "2px", "borderBottom": "1px solid #e9ecef"}),
            dbc.Row([f_item("ROE", "净资产收益率(ROE)", 'growth'), f_item("毛利率", "毛利率", 'growth'), f_item("净利率", "销售净利率", 'growth')], className="gx-1 mb-0"),
            dbc.Row([f_item("营收同比", "营业总收入增长率", 'growth'), f_item("净利同比", "净利润增长率", 'growth'), f_item("负债率", "资产负债率")], className="gx-1 mb-0"),
            dbc.Row([f_item("存货周转", "存货周转天数"), f_item("应收周转", "应收账款周转天数"), f_item("流动比率", "流动比率")], className="gx-1 mb-0")
        ], style={"backgroundColor": "#f8f9fa", "padding": "6px", "borderRadius": "6px"})
    ])
    
    # 2. 量化信号矩阵 (折叠面板优化)
    if not quant_dict:
        quant_ui = html.Div("暂无量化信号数据", style={"color": "#adb5bd", "fontSize": "0.8rem", "textAlign": "center", "marginTop": "20px"})
    else:
        quant_items = list(quant_dict.items()) # type: ignore
        quant_ui = html.Div([
            html.Details([
                html.Summary([
                    html.Span(k, style={"color": "#495057", "fontSize": "0.75rem", "fontWeight": "bold"}),
                    html.Div([
                        html.Span(f"{v.get('信号', '-')} ", style={"color": "#37b24d" if v.get('信号')=='看空' else "#f03e3e" if v.get('信号') in ['看多','买入'] else "#868e96", "fontWeight": "bold", "fontSize": "0.75rem"}),
                        html.Span(f"({v.get('置信度', '-')})", style={"color": "#adb5bd", "fontSize": "0.7rem"})
                    ], style={"display": "inline-block", "float": "right"})
                ], style={
                    "borderBottom": "1px dashed #dee2e6" if i < len(quant_items) - 1 else "none", 
                    "padding": "6px 4px", "cursor": "pointer", "outline": "none"
                }),
                html.Div([
                    html.Div([
                        html.Span(dk, style={"color": "#868e96", "fontSize": "0.7rem"}),
                        html.Span(str(dv), style={"color": "#343a40", "fontSize": "0.7rem", "fontWeight": "bold", "float": "right"})
                    ], style={"padding": "3px 0", "borderBottom": "1px solid #f1f3f5" if j < len(v.get('具体指标', {})) - 1 else "none"})
                    for j, (dk, dv) in enumerate(v.get('具体指标', {}).items())
                ], style={"backgroundColor": "#ffffff", "padding": "6px 8px", "margin": "4px 0", "borderRadius": "4px", "boxShadow": "inset 0 0 4px rgba(0,0,0,0.02)"})
            ], style={"marginBottom": "2px"})
            for i, (k, v) in enumerate(quant_items)
        ], style={"backgroundColor": "#f8f9fa", "padding": "4px 8px", "borderRadius": "6px", "marginTop": "2px"})
    
    # 3. 消息面动态
    formatted_news = []
    if news_text and news_text != "暂无新闻数据":
        for line in news_text.split('\n'):
            line = line.strip()
            if line:
                formatted_news.append(html.Div(["• ", line], style={"marginBottom": "4px", "lineHeight": "1.3"})) 
    else:
        formatted_news = "暂无新闻数据"
        
    return fin_ui, quant_ui, formatted_news
