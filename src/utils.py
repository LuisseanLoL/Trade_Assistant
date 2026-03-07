# -*- coding: utf-8 -*-
import os
import random
import pandas as pd
from datetime import datetime, timedelta
import concurrent.futures
import json_repair
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# 导入新闻爬虫模块
from src.news_crawler import get_news_titles

def get_logical_date():
    """获取逻辑日期（凌晨算作前一天）"""
    now = datetime.now()
    if now.hour < 9: return (now - timedelta(days=1)).date()
    return now.date()

def fetch_news_safely(symbol, stock_name, current_date_str):
    """安全地并发获取新闻"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(get_news_titles, symbol=symbol, stock_name=stock_name, max_news=20, save_txt=True, current_date=current_date_str)
        return future.result()

def get_all_output_dates():
    """获取所有已输出过分析结果的日期文件夹"""
    dates = []
    if os.path.exists("output"):
        for folder_name in os.listdir("output"):
            if os.path.isdir(os.path.join("output", folder_name)) and os.path.exists(os.path.join("output", folder_name, f"Daily Table_{folder_name}.csv")):
                dates.append(folder_name)
    return sorted(dates, reverse=True)

def load_daily_table_by_date(date_str):
    """按日期加载当日的策略表并排序"""
    file_path = f"output/{date_str}/Daily Table_{date_str}.csv"
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path, dtype=str, on_bad_lines='skip')
            df['_conf_val'] = df['置信度'].str.replace('%', '', regex=False).astype(float).fillna(0)
            df['_action_rank'] = df['操作'].apply(lambda x: 0 if str(x).strip() == '买入' else 1)
            # 买入优先，同操作置信度高的优先
            df = df.sort_values(by=['_action_rank', '_conf_val'], ascending=[True, False]).drop(columns=['_conf_val', '_action_rank'])
            df['详情'] = '查看'
            return df.to_dict('records')
        except: pass
    return []

def get_random_unprocessed_stock():
    """获取一个今天还未分析过的随机股票代码"""
    current_date_str = get_logical_date().strftime("%Y-%m-%d")
    csv_path = '主板股票代码.csv'
    if not os.path.exists(csv_path): return None, "未找到 '主板股票代码.csv'"
    try:
        df = pd.read_csv(csv_path, dtype=str)
        all_codes = df['股票代码'].astype(str).str.strip().tolist()
        daily_table_path = f"output/{current_date_str}/Daily Table_{current_date_str}.csv"
        processed_codes = set(pd.read_csv(daily_table_path, dtype={'股票代码': str})['股票代码'].astype(str).str.strip()) if os.path.exists(daily_table_path) else set()
        unprocessed_codes = [c for c in all_codes if c not in processed_codes]
        if not unprocessed_codes: return None, "今日全部股票已分析完毕"
        return random.choice(unprocessed_codes), None
    except Exception as e: return None, str(e)

def parse_llm_json(result_text):
    """从 LLM 的回复中安全提取 JSON 配置"""
    res = {"action": "-", "expectation": "-", "pos_adv": "-", "confidence": "-", "buy_p": "-", "sell_p": "-", "stop_p": "-", "reasoning": result_text}
    try:
        c_text = result_text.replace("“", '"').replace("”", '"')
        s_idx, e_idx = c_text.find('{'), c_text.rfind('}')
        if s_idx != -1 and e_idx != -1:
            parsed = json_repair.loads(c_text[s_idx : e_idx + 1])
            res.update({
                "action": parsed.get("操作", "-"), "expectation": parsed.get("预期", "-"), "pos_adv": f"{parsed.get('建议仓位', 0)}%",
                "confidence": f"{parsed.get('置信度', 0) * 100:.0f}%", "buy_p": parsed.get('建议买入价'), "sell_p": parsed.get('目标卖出价'),
                "stop_p": parsed.get('建议止损价'), "reasoning": parsed.get('原因', '暂无深度逻辑')
            })
    except: pass
    return res

def calculate_technical_indicators(df):
    """
    计算所有技术指标（MA, BOLL, MACD, KDJ, RSI）
    返回带有新指标列的 DataFrame
    """
    if df.empty:
        return df
        
    df = df.copy()
    
    # 1. 均线系统 (MA)
    df['MA10'] = df['close'].rolling(window=10).mean()
    df['MA20'] = df['close'].rolling(window=20).mean()
    df['MA60'] = df['close'].rolling(window=60).mean()
    
    # 2. 布林带系统 (BOLL)
    df['BB_mid'] = df['MA20']
    df['BB_std'] = df['close'].rolling(window=20).std()
    df['BB_up'] = df['BB_mid'] + 2 * df['BB_std']
    df['BB_low'] = df['BB_mid'] - 2 * df['BB_std']
    
    # 3. MACD
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['MACD_DIF'] = exp1 - exp2
    df['MACD_DEA'] = df['MACD_DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_hist'] = (df['MACD_DIF'] - df['MACD_DEA']) * 2
    
    # 4. KDJ (9, 3, 3)
    low_list = df['low'].rolling(window=9, min_periods=1).min()
    high_list = df['high'].rolling(window=9, min_periods=1).max()
    rsv = (df['close'] - low_list) / (high_list - low_list) * 100
    df['K'] = rsv.ewm(com=2, adjust=False).mean()
    df['D'] = df['K'].ewm(com=2, adjust=False).mean()
    df['J'] = 3 * df['K'] - 2 * df['D']
    
    # 5. RSI (6, 12, 24)
    delta = df['close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    for period in [6, 12, 24]:
        roll_up = up.ewm(span=period, adjust=False).mean()
        roll_down = down.ewm(span=period, adjust=False).mean()
        # 避免除以0
        rs = roll_up / roll_down.replace(0, 0.001) 
        df[f'RSI{period}'] = 100.0 - (100.0 / (1.0 + rs))
        
    return df

def create_advanced_kline_fig(df):
    """
    生成包含 K线、均线/布林带、成交量、MACD/KDJ/RSI 切换的标准化高级图表
    """
    # 采用 3 行布局：主图(50%)、成交量(20%)、副图(30%)
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.03, 
        row_heights=[0.5, 0.2, 0.3]
    )
    
    layout_cfg = dict(
        template="plotly_white", margin=dict(l=30, r=20, t=50, b=10), 
        hovermode="x unified", xaxis_rangeslider_visible=False, 
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', 
        xaxis_type='category',
        showlegend=True,  # 开启全局图例
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10)),
        hoverlabel=dict(bgcolor="rgba(255, 255, 255, 0.95)", bordercolor="#ced4da", font=dict(color="#495057", size=12))
    )
    
    if df.empty:
        fig.update_layout(**layout_cfg)
        return fig
        
    df = df.copy()
    df['date'] = df['date'].astype(str)
    
    # 自动计算所有技术指标
    df = calculate_technical_indicators(df)
    
    # ====== 1. 主图 K 线 (Trace 0) ======
    fig.add_trace(go.Candlestick(x=df['date'], open=df['open'], high=df['high'], low=df['low'], close=df['close'], increasing_line_color='#f03e3e', decreasing_line_color='#2f9e44', name='K线', showlegend=False), row=1, col=1)
    
    # ====== 2. 成交量 (Trace 1) ======
    colors = ['#f03e3e' if row['close'] >= row['open'] else '#2f9e44' for _, row in df.iterrows()]
    fig.add_trace(go.Bar(x=df['date'], y=df['volume'], marker_color=colors, opacity=0.7, name='成交量', showlegend=False), row=2, col=1)
    
    # ====== 主图切换指标：均线 (Trace 2, 3, 4) ======
    fig.add_trace(go.Scatter(x=df['date'], y=df['MA10'], mode='lines', line=dict(color='#fcc419', width=1), name='MA10'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['date'], y=df['MA20'], mode='lines', line=dict(color='#e64980', width=1), name='MA20'), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['date'], y=df['MA60'], mode='lines', line=dict(color='#339af0', width=1), name='MA60'), row=1, col=1)

    
    # ====== 主图切换指标：布林带 (Trace 5, 6, 7) ======
    fig.add_trace(go.Scatter(x=df['date'], y=df['BB_up'], mode='lines', line=dict(color='#adb5bd', width=1, dash='dash'), name='BB Up', visible=False), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['date'], y=df['BB_mid'], mode='lines', line=dict(color='#868e96', width=1), name='BB Mid', visible=False), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['date'], y=df['BB_low'], mode='lines', line=dict(color='#adb5bd', width=1, dash='dash'), name='BB Low', visible=False), row=1, col=1)
    
    # ====== 副图切换指标：MACD (Trace 8, 9, 10 - 默认显示) ======
    macd_colors = ['#f03e3e' if val >= 0 else '#2f9e44' for val in df['MACD_hist']]
    fig.add_trace(go.Bar(x=df['date'], y=df['MACD_hist'], marker_color=macd_colors, name='MACD柱'), row=3, col=1)
    fig.add_trace(go.Scatter(x=df['date'], y=df['MACD_DIF'], mode='lines', line=dict(color='#4c6ef5', width=1), name='DIF'), row=3, col=1)
    fig.add_trace(go.Scatter(x=df['date'], y=df['MACD_DEA'], mode='lines', line=dict(color='#f59f00', width=1), name='DEA'), row=3, col=1)
    
    # ====== 副图切换指标：KDJ (Trace 11, 12, 13 - 默认隐藏) ======
    fig.add_trace(go.Scatter(x=df['date'], y=df['K'], mode='lines', line=dict(color='#20c997', width=1), name='K', visible=False), row=3, col=1)
    fig.add_trace(go.Scatter(x=df['date'], y=df['D'], mode='lines', line=dict(color='#be4bdb', width=1), name='D', visible=False), row=3, col=1)
    fig.add_trace(go.Scatter(x=df['date'], y=df['J'], mode='lines', line=dict(color='#f03e3e', width=1), name='J', visible=False), row=3, col=1)
    
    # ====== 副图切换指标：RSI (Trace 14, 15, 16 - 默认隐藏) ======
    fig.add_trace(go.Scatter(x=df['date'], y=df['RSI6'], mode='lines', line=dict(color='#845ef7', width=1), name='RSI6', visible=False), row=3, col=1)
    fig.add_trace(go.Scatter(x=df['date'], y=df['RSI12'], mode='lines', line=dict(color='#ff922b', width=1), name='RSI12', visible=False), row=3, col=1)
    fig.add_trace(go.Scatter(x=df['date'], y=df['RSI24'], mode='lines', line=dict(color='#3bc9db', width=1), name='RSI24', visible=False), row=3, col=1)
    
    fig.update_layout(**layout_cfg)
    fig.update_xaxes(type='category', tickmode='auto', nticks=12)
    
    # 使用 restyle 方法实现两个独立的前端按钮组（不互相干扰）
    fig.update_layout(
        updatemenus=[
            # 按钮组1：主图指标切换 (x=0 靠左)
            dict(
                type="buttons", direction="right",
                buttons=list([
                    dict(label="均线", method="restyle", args=["visible", [True, True, True, False, False, False], [2, 3, 4, 5, 6, 7]]),
                    dict(label="布林带", method="restyle", args=["visible", [False, False, False, True, True, True], [2, 3, 4, 5, 6, 7]]),
                    dict(label="纯K线", method="restyle", args=["visible", [False, False, False, False, False, False], [2, 3, 4, 5, 6, 7]])
                ]),
                showactive=True, x=0, xanchor="left", y=1.12, yanchor="top",
                pad={"r": 5, "t": 0}, bgcolor="rgba(255,255,255,0.85)", font=dict(size=10, color="#495057")
            ),
            # 按钮组2：副图指标切换 (x=0.25 紧挨在右侧)
            dict(
                type="buttons", direction="right",
                buttons=list([
                    dict(label="MACD", method="restyle", args=["visible", [True, True, True, False, False, False, False, False, False], [8, 9, 10, 11, 12, 13, 14, 15, 16]]),
                    dict(label="KDJ", method="restyle", args=["visible", [False, False, False, True, True, True, False, False, False], [8, 9, 10, 11, 12, 13, 14, 15, 16]]),
                    dict(label="RSI", method="restyle", args=["visible", [False, False, False, False, False, False, True, True, True], [8, 9, 10, 11, 12, 13, 14, 15, 16]])
                ]),
                showactive=True, x=0.28, xanchor="left", y=1.12, yanchor="top",
                pad={"r": 5, "t": 0}, bgcolor="rgba(255,255,255,0.85)", font=dict(size=10, color="#495057")
            )
        ]
    )
    return fig