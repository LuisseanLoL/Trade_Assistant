# -*- coding: utf-8 -*-
import dash
from dash import dcc, html, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime, timedelta
import os
import json_repair
import concurrent.futures  # 用于隔离 Playwright
import glob
import time

# 导入您原本的 src 工具模块
from src.data_crawler import get_stock_data, get_bs_code
from src.news_crawler import get_news_titles
from src.LLM_chat import get_LLM_message
import baostock as bs

# ==========================================
# 辅助函数 
# ==========================================
def get_logical_date():
    now = datetime.now()
    if now.hour < 9:
        return (now - timedelta(days=1)).date()
    return now.date()

def get_stock_name_bs(stock_code):
    bs.login()
    bs_code = get_bs_code(stock_code)
    rs_basic = bs.query_stock_basic(code=bs_code)
    stock_name = "未知名称"
    if rs_basic.error_code == '0' and rs_basic.next():
        stock_name = rs_basic.get_row_data()[1]
    bs.logout()
    return stock_name

def get_chart_data(stock_code, beg, end):
    bs.login()
    bs_code = get_bs_code(stock_code)
    bs_start = f"{beg[:4]}-{beg[4:6]}-{beg[6:]}"
    bs_end = f"{end[:4]}-{end[4:6]}-{end[6:]}"
    
    rs = bs.query_history_k_data_plus(
        bs_code, "date,open,high,low,close,volume",
        start_date=bs_start, end_date=bs_end, frequency="d", adjustflag="2"
    )
    
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
        
    df = pd.DataFrame(data_list, columns=rs.fields)
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    bs.logout()
    return df

def fetch_news_safely(symbol, stock_name, current_date_str):
    """【核心修复】将 Playwright 放在独立线程中运行，绕过 Dash 的 asyncio 冲突"""
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            get_news_titles, 
            symbol=symbol, 
            stock_name=stock_name, 
            max_news=20, 
            save_txt=True, 
            current_date=current_date_str
        )
        return future.result()

def load_all_daily_tables():
    """读取所有历史的 Daily Table 并在前端表格展示"""
    all_files = glob.glob("output/*/Daily Table_*.csv")
    df_list = []
    for file in all_files:
        try:
            df_list.append(pd.read_csv(file, dtype=str))
        except Exception:
            pass
    if df_list:
        final_df = pd.concat(df_list, ignore_index=True)
        # 反转顺序，让最新的在最上面
        final_df = final_df.iloc[::-1]
        return final_df.to_dict('records')
    return []

# ==========================================
# Dash 界面构建
# ==========================================
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.CYBORG])
app.title = "Trade Assistant"

navbar = dbc.NavbarSimple(brand="🤖 Trade Assistant", color="dark", dark=True, fluid=True)

control_panel = dbc.Card([
    dbc.CardHeader("⚙️ 策略控制台"),
    dbc.CardBody([
        html.Label("股票代码:"),
        dbc.Input(id="input-stock-code", type="text", placeholder="例如: 600519", value="600519", className="mb-3"),
        html.Label("底层模型架构:"),
        dcc.Dropdown(
            id="dropdown-model",
            options=[
                {'label': 'Gemini (Flash粗筛 + Pro精决)', 'value': 'gemini'},
                {'label': 'ARK (火山引擎)', 'value': 'ark'},
                {'label': 'Local (本地开源模型)', 'value': 'local'}
            ],
            value='gemini', className="mb-3 text-dark"
        ),
        html.Label("当前持仓数量:"),
        dbc.Input(id="input-position", type="number", value=0, className="mb-3"),
        html.Label("持仓成本价:"),
        dbc.Input(id="input-cost", type="number", value=0, className="mb-4"),
        dbc.Button("🚀 立即分析", id="btn-analyze", color="primary", className="w-100"),
    ])
])

ai_summary_cards = dbc.Row([
    dbc.Col(dbc.Card([dbc.CardHeader("交易操作"), dbc.CardBody(html.H3(id="out-action", className="text-info"))])),
    dbc.Col(dbc.Card([dbc.CardHeader("建议仓位"), dbc.CardBody(html.H3(id="out-position", className="text-warning"))])),
    dbc.Col(dbc.Card([dbc.CardHeader("操作区间"), dbc.CardBody(html.H5(id="out-price-range", className="text-success"))])),
    dbc.Col(dbc.Card([dbc.CardHeader("置信度"), dbc.CardBody(html.H3(id="out-confidence", className="text-primary"))])),
], className="mb-4")

app.layout = dbc.Container([
    navbar,
    html.Br(),
    dbc.Row([
        dbc.Col([control_panel], width=3),
        dbc.Col([
            dcc.Loading(
                id="loading-main", type="default",
                children=[
                    ai_summary_cards,
                    dbc.Card([
                        dbc.CardHeader("📈 行情图表与 AI 决策点位"),
                        dbc.CardBody(dcc.Graph(id="main-chart", style={"height": "550px"}))
                    ], className="mb-4"),
                    dbc.Row([
                        dbc.Col(dbc.Card([
                            dbc.CardHeader("🧠 AI 深度逻辑解析"),
                            dbc.CardBody(html.Div(id="out-reasoning", style={"white-space": "pre-wrap", "height": "350px", "overflow-y": "auto"}))
                        ]), width=7),
                        dbc.Col(dbc.Card([
                            dbc.CardHeader("📰 市场动态与新闻"),
                            dbc.CardBody(html.Div(id="out-news", style={"white-space": "pre-wrap", "height": "350px", "overflow-y": "auto", "font-size": "0.9em"}))
                        ]), width=5),
                    ])
                ]
            )
        ], width=9)
    ]),
    html.Br(),
    # 底部追加 Data Grid 表格
    dbc.Row(dbc.Col(dbc.Card([
        dbc.CardHeader("🗂️ 历史回测记录表 (Daily Table)"),
        dbc.CardBody(
            dash_table.DataTable(
                id='daily-table',
                data=load_all_daily_tables(),
                columns=[{"name": i, "id": i} for i in [
                    "股票代码", "股票名称", "当前价格", "预期", "操作", 
                    "建议仓位", "置信度", "建议买入价", "目标卖出价", "建议止损价", "回报风险比"
                ]],
                style_table={'overflowX': 'auto'},
                style_cell={'backgroundColor': '#222', 'color': 'white', 'textAlign': 'center', 'border': '1px solid #444'},
                style_header={'backgroundColor': '#333', 'fontWeight': 'bold'},
                sort_action="native",
                page_size=10
            )
        )
    ])))
], fluid=True)

# ==========================================
# 核心回调逻辑 
# ==========================================
@app.callback(
    [Output("main-chart", "figure"),
     Output("out-action", "children"),
     Output("out-position", "children"),
     Output("out-price-range", "children"),
     Output("out-confidence", "children"),
     Output("out-reasoning", "children"),
     Output("out-news", "children"),
     Output("daily-table", "data")], # 每次分析完刷新底部的表格
    [Input("btn-analyze", "n_clicks")],
    [State("input-stock-code", "value"),
     State("dropdown-model", "value"),
     State("input-position", "value"),
     State("input-cost", "value")],
    prevent_initial_call=True
)
def run_analysis(n_clicks, stock_code, model_choice, position, cost):
    if not stock_code:
        return dash.no_update
        
    current_date = get_logical_date()
    current_date_str = current_date.strftime("%Y-%m-%d")
    end = current_date.isoformat().replace('-', '')
    start_date = current_date - timedelta(days=365)
    beg = start_date.isoformat().replace('-', '')
    
    stock_code = stock_code.strip()
    stock_name = get_stock_name_bs(stock_code)
    
    # 1. 获取作图数据
    df_chart = get_chart_data(stock_code, beg, end)
    stock_current_price = df_chart['close'].iloc[-1] if not df_chart.empty else 0
    
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.7, 0.3])
    if not df_chart.empty:
        fig.add_trace(go.Candlestick(
            x=df_chart['date'], open=df_chart['open'], high=df_chart['high'],
            low=df_chart['low'], close=df_chart['close'], name="K线"
        ), row=1, col=1)
        fig.add_trace(go.Bar(
            x=df_chart['date'], y=df_chart['volume'], name="成交量", marker_color='rgba(0,150,255,0.5)'
        ), row=2, col=1)
        
    fig.update_layout(title=f"{stock_name} ({stock_code})", template="plotly_dark", margin=dict(l=20, r=20, t=40, b=20), xaxis_rangeslider_visible=False)

    # 2. 爬取数据 & 构建 Prompt
    data_input_str = get_stock_data(stock_code=stock_code, beg=beg, end=end, current_date=current_date_str)
    
    # 使用安全线程调用 Playwright 新闻抓取
    news_titles = fetch_news_safely(stock_code, stock_name, current_date_str)
    
    last_30_days_str = df_chart.tail(30).to_string(index=False) if not df_chart.empty else "暂无K线数据"

    user_message = f"""基于获得的以下数据和新闻消息，做出你的交易决策。\n\n{data_input_str}\n\n最近三十个交易日数据如下：\n{last_30_days_str}\n\n相关新闻如下：\n{news_titles}\n\n当前该股持仓：{position} 股\n当前持仓成本: {cost} 元\n\n请记住，行动必须是买入、卖出、持有或观望。\n谨慎考虑交易决策：考虑当前股价是高位还是低位，在低位买入，高位卖出。\n考虑自己的持仓成本，在有足够浮盈的情况下考虑卖出收获现金实利。"""

    # 3. 结果保存到本地 input 文件夹 (同步 worker.py)
    input_dir = f"input/{current_date_str}"
    os.makedirs(input_dir, exist_ok=True)
    with open(f"{input_dir}/{stock_code}_{stock_name}_input_{current_date_str}.txt", 'w', encoding='utf-8') as f:
        f.write(user_message)

    # 4. 调用 LLM 模型
    with open('LLM system content.txt', 'r', encoding='utf-8') as file:
        system_content = file.read()
        
    need_pro = float(position) > 0
    result_text = ""
    
    if not need_pro:
        result_text = get_LLM_message(system_content=system_content, user_message=user_message, model_choice=model_choice, model_tier='flash')
        try:
            temp_text = result_text.replace("“", '"').replace("”", '"')
            s_idx = temp_text.find('{')
            e_idx = temp_text.rfind('}')
            if s_idx != -1 and e_idx != -1:
                temp_data = json_repair.loads(temp_text[s_idx : e_idx + 1])
                if temp_data.get('操作', '') in ['买入', '卖出', '持有']:
                    need_pro = True
        except:
             need_pro = True

    if need_pro and model_choice == 'gemini':
         result_text = get_LLM_message(system_content=system_content, user_message=user_message, model_choice=model_choice, model_tier='pro')

    # 5. 结果保存到本地 output 文件夹 (同步 worker.py)
    output_dir = f"output/{current_date_str}"
    os.makedirs(output_dir, exist_ok=True)
    with open(f"{output_dir}/{stock_code}_{stock_name}_output_{model_choice}_{current_date_str}.txt", 'w', encoding='utf-8') as f:
        f.write(result_text)

    # 6. 解析 JSON，画线 & 记录至表格
    action, pos_adv, price_range, confidence, reasoning = "解析失败", "N/A", "N/A", "N/A", result_text
    
    try:
        corrected_text = result_text.replace("“", '"').replace("”", '"')
        s_idx = corrected_text.find('{')
        e_idx = corrected_text.rfind('}')
        if s_idx != -1 and e_idx != -1:
            parsed_data = json_repair.loads(corrected_text[s_idx : e_idx + 1])
            action = parsed_data.get("操作", "N/A")
            pos_adv = f"{parsed_data.get('建议仓位', 0)}%"
            conf_val = parsed_data.get('置信度')
            confidence = f"{conf_val * 100:.0f}%" if conf_val is not None else "N/A"
            
            buy_p = parsed_data.get('建议买入价')
            sell_p = parsed_data.get('目标卖出价')
            stop_p = parsed_data.get('建议止损价')
            
            # === K 线图画标线 ===
            if buy_p and str(buy_p).replace('.', '', 1).isdigit():
                fig.add_hline(y=float(buy_p), line_dash="dash", line_color="lime", annotation_text="建议买入", row=1, col=1)
            if sell_p and str(sell_p).replace('.', '', 1).isdigit():
                fig.add_hline(y=float(sell_p), line_dash="dash", line_color="red", annotation_text="目标卖出", row=1, col=1)
            if stop_p and str(stop_p).replace('.', '', 1).isdigit():
                fig.add_hline(y=float(stop_p), line_dash="dash", line_color="magenta", annotation_text="建议止损", row=1, col=1)

            price_range = f"买: {buy_p} | 卖: {sell_p} | 损: {stop_p}"
            reasoning = f"【方向预期】{parsed_data.get('预期', 'N/A')}\n\n【深度逻辑】\n{parsed_data.get('原因', 'N/A')}"

            # === 计算风险回报比 & 写入 Daily Table ===
            reward_risk_ratio_str = 'N/A'
            try:
                if buy_p and sell_p and stop_p:
                    pot_reward = float(sell_p) - float(buy_p)
                    pot_risk = float(buy_p) - float(stop_p)
                    if pot_risk > 0:
                        reward_risk_ratio_str = f"{pot_reward / pot_risk:.2f}:1"
            except:
                pass

            final_data = {
                "股票代码": stock_code,
                "股票名称": stock_name,
                "当前价格": stock_current_price,
                "预期": parsed_data.get("预期", "N/A"),
                "操作": action,
                "建议仓位": pos_adv,      
                "置信度": confidence,
                "建议买入价": buy_p if buy_p is not None else "N/A",
                "目标卖出价": sell_p if sell_p is not None else "N/A",
                "建议止损价": stop_p if stop_p is not None else "N/A",
                "回报风险比": reward_risk_ratio_str
            }
            
            file_path = os.path.join(output_dir, f"Daily Table_{current_date_str}.csv")
            output_df = pd.DataFrame([final_data])
            
            if not os.path.exists(file_path):
                header = {"股票代码": [], "股票名称": [], "当前价格": [], "预期": [], "操作": [], "建议仓位": [], "置信度": [], "建议买入价": [], "目标卖出价": [], "建议止损价": [], "回报风险比": []}
                pd.DataFrame(header).to_csv(file_path, index=False, encoding='utf-8-sig')
            output_df.to_csv(file_path, index=False, header=False, mode='a', encoding='utf-8-sig')

    except Exception:
        reasoning = f"JSON解析失败，原始输出:\n\n{result_text}"

    # 返回刷新后的表格数据
    updated_table_data = load_all_daily_tables()
    return fig, action, pos_adv, price_range, confidence, reasoning, news_titles, updated_table_data

if __name__ == '__main__':
    # 注意：最新版 Dash 使用 run()
    app.run(debug=True, port=8050)