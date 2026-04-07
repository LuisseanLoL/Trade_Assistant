# -*- coding: utf-8 -*-
import os
import re
import numpy as np
import pandas as pd
import time
import json
from datetime import datetime, timedelta

import akshare as ak
from mootdx.quotes import Quotes
from mootdx.contrib.adjust import get_adjust_year

# =========================================================================
# 第一部分：辅助与技术指标计算模块
# =========================================================================

def get_etf_market_prefix(symbol: str) -> str:
    """根据 ETF 代码生成新浪接口需要的带市场前缀的代码"""
    if symbol.startswith('5'): return f"sh{symbol}"
    elif symbol.startswith('1'): return f"sz{symbol}"
    return symbol

def safe_float(val, default=0.0):
    """安全地将字符串转换为浮点数"""
    try:
        if val == "" or pd.isna(val) or val is None: return default
        if isinstance(val, str): val = val.replace('—', '-').replace('%', '').replace(',', '')
        return float(val)
    except:
        return default

def format_large_number(num):
    try:
        num = float(num)
        if pd.isna(num) or num == 0.0: return "0.0"
        if abs(num) >= 1e8: return f"{num / 1e8:.2f}亿"
        elif abs(num) >= 1e4: return f"{num / 1e4:.2f}万"
        else: return f"{num:.2f}"
    except:
        return str(num)

def calculate_hurst(series):
    """计算赫斯特指数 (Hurst Exponent) 以判断时间序列的平稳性/趋势性"""
    try:
        series = series.dropna()
        if len(series) < 30: return np.nan
        lags = range(2, min(20, len(series) // 4))
        tau = [np.sqrt(np.std(np.subtract(series.values[lag:], series.values[:-lag]))) for lag in lags]
        poly = np.polyfit(np.log(lags), np.log(tau), 1)
        return poly[0] * 2.0
    except:
        return np.nan

def calculate_advanced_indicators(df):
    """计算高级技术指标与量化信号."""
    df = df.sort_values('日期').reset_index(drop=True)
    df['daily_return'] = df['收盘'].pct_change()
    
    # 极值与分位点
    df['high_4w'] = df['最高'].rolling(20).max()
    df['low_4w'] = df['最低'].rolling(20).min()
    df['high_13w'] = df['最高'].rolling(65).max()
    df['low_13w'] = df['最低'].rolling(65).min()
    df['high_52w'] = df['最高'].rolling(250).max()
    df['low_52w'] = df['最低'].rolling(250).min()

    # 动量
    df['momentum_1m'] = df['收盘'].pct_change(periods=20)
    df['momentum_3m'] = df['收盘'].pct_change(periods=60)
    df['momentum_6m'] = df['收盘'].pct_change(periods=120)
    df['volume_ma5'] = df['成交量'].rolling(5).mean()
    df['volume_ma20'] = df['成交量'].rolling(20).mean()
    df['volume_momentum'] = df['volume_ma5'] / df['volume_ma20'].replace(0, np.nan)
    
    # 移动平均
    df['MA5'] = df['收盘'].rolling(window=5).mean()
    df['MA10'] = df['收盘'].rolling(window=10).mean()
    df['MA20'] = df['收盘'].rolling(window=20).mean()
    df['MA60'] = df['收盘'].rolling(window=60).mean()
    df['MA120'] = df['收盘'].rolling(window=120).mean()
    df['MA200'] = df['收盘'].rolling(window=200).mean()

    # MACD
    df['EMA12'] = df['收盘'].ewm(span=12, adjust=False).mean()
    df['EMA26'] = df['收盘'].ewm(span=26, adjust=False).mean()
    df['DIF'] = df['EMA12'] - df['EMA26']
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD'] = 2 * (df['DIF'] - df['DEA'])

    # 趋势跟随
    df['tr0'] = abs(df['最高'] - df['最低'])
    df['tr1'] = abs(df['最高'] - df['收盘'].shift())
    df['tr2'] = abs(df['最低'] - df['收盘'].shift())
    df['tr'] = df[['tr0', 'tr1', 'tr2']].max(axis=1)
    df['up_move'] = df['最高'] - df['最高'].shift()
    df['down_move'] = df['最低'].shift() - df['最低']
    df['+dm'] = np.where((df['up_move'] > df['down_move']) & (df['up_move'] > 0), df['up_move'], 0)
    df['-dm'] = np.where((df['down_move'] > df['up_move']) & (df['down_move'] > 0), df['down_move'], 0)
    df['atr14'] = df['tr'].rolling(14).mean()
    df['+di'] = 100 * (df['+dm'].rolling(14).mean() / df['atr14'].replace(0, np.nan))
    df['-di'] = 100 * (df['-dm'].rolling(14).mean() / df['atr14'].replace(0, np.nan))
    df['dx'] = 100 * abs(df['+di'] - df['-di']) / (df['+di'] + df['-di']).replace(0, np.nan)
    df['adx'] = df['dx'].rolling(14).mean()

    # 均值回归 (Bollinger & RSI)
    df['std20'] = df['收盘'].rolling(20).std()
    df['upper_bb'] = df['MA20'] + 2 * df['std20']
    df['lower_bb'] = df['MA20'] - 2 * df['std20']
    df['z_score'] = (df['收盘'] - df['MA20']) / df['std20'].replace(0, np.nan)
    
    # 布林极限 %b 与 带宽 Bandwidth
    df['bb_pct_b'] = (df['收盘'] - df['lower_bb']) / (df['upper_bb'] - df['lower_bb']).replace(0, np.nan)
    df['bb_width'] = (df['upper_bb'] - df['lower_bb']) / df['MA20'].replace(0, np.nan)
    
    delta = df['收盘'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs14 = gain / loss.replace(0, np.nan)
    df['rsi_14'] = 100 - (100 / (1 + rs14))

    gain28 = (delta.where(delta > 0, 0)).rolling(window=28).mean()
    loss28 = (-delta.where(delta < 0, 0)).rolling(window=28).mean()
    rs28 = gain28 / loss28.replace(0, np.nan)
    df['rsi_28'] = 100 - (100 / (1 + rs28))

    # 波动率与套利
    df['historical_volatility'] = df['daily_return'].rolling(20).std() * np.sqrt(252)
    vol_120 = df['daily_return'].rolling(120).std() * np.sqrt(252)
    vol_120_min = vol_120.rolling(120).min()
    vol_120_max = vol_120.rolling(120).max()
    df['volatility_regime'] = (df['historical_volatility'] - vol_120_min) / (vol_120_max - vol_120_min).replace(0, np.nan)
    df['volatility_z_score'] = (df['historical_volatility'] - vol_120.rolling(120).mean()) / vol_120.rolling(120).std().replace(0, np.nan)
    df['atr_ratio'] = df['atr14'] / df['收盘']
    df['skewness'] = df['daily_return'].rolling(20).skew()
    df['kurtosis'] = df['daily_return'].rolling(20).kurt()
    df['hurst_exponent'] = df['收盘'].rolling(120).apply(calculate_hurst, raw=False)
    
    return df

def analyze_bb_status(df, period_name="日线"):
    """辅助函数：分析指定周期数据框中的布林带状态"""
    if df.empty or len(df) < 21:
        return f"{period_name}: 数据不足"
    
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    
    close = safe_float(latest['收盘'])
    upper = safe_float(latest.get('upper_bb', 0))
    lower = safe_float(latest.get('lower_bb', 0))
    mid = safe_float(latest.get('MA20', 0))
    width = safe_float(latest.get('bb_width', 0))
    prev_width = safe_float(prev.get('bb_width', 0))
    prev_mid = safe_float(prev.get('MA20', 0))
    
    if mid == 0: return f"{period_name}: 数据异常"

    if close > upper: pos = "突破上轨"
    elif close < lower: pos = "跌破下轨"
    elif close >= mid: 
        if upper - close < close - mid: pos = "逼近上轨"
        else: pos = "中轨之上"
    else:
        if close - lower < mid - close: pos = "逼近下轨"
        else: pos = "中轨之下"

    if width > prev_width * 1.05: state = "开口显著放大"
    elif width > prev_width * 1.01: state = "开口缓慢放大"
    elif width < prev_width * 0.95: state = "开口显著收缩"
    elif width < prev_width * 0.99: state = "开口缓慢收缩"
    else: state = "带宽平稳"

    if mid > prev_mid * 1.002: direction = "通道向上"
    elif mid < prev_mid * 0.998: direction = "通道向下"
    else: direction = "横向震荡"

    return f"{period_name}: {pos} | {state} | {direction}"

# =========================================================================
# 第二部分：宏观环境与 Mootdx F10 解析模块
# =========================================================================

def get_macro_market_context(current_date: str) -> str:
    """获取大盘(上证指数)数据并进行量化分析，感知系统性环境"""
    cache_dir = f"log/index_data"
    os.makedirs(cache_dir, exist_ok=True)
    
    macro_text_cache_file = os.path.join(cache_dir, f"macro_context_text_{current_date}.txt")
    
    if os.path.exists(macro_text_cache_file):
        print("\n🌍 📦 检测到宏观大盘环境文本缓存已存在，直接读取跳过计算...")
        with open(macro_text_cache_file, 'r', encoding='utf-8') as f:
            return f.read()

    print("\n🌍 正在获取并分析宏观大盘(上证指数)与期指宏观数据...")
    cache_file = os.path.join(cache_dir, f"sh000001_daily_{current_date}.csv")
    
    if os.path.exists(cache_file):
        df_index = pd.read_csv(cache_file)
        df_index['date'] = pd.to_datetime(df_index['date'])
    else:
        try:
            df_index = ak.stock_zh_index_daily(symbol="sh000001")
            df_index['date'] = pd.to_datetime(df_index['date'])
            df_index.to_csv(cache_file, index=False)
        except Exception as e:
            return "大盘数据获取失败，宏观环境未知。"

    df_calc = df_index.rename(columns={'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'volume': '成交量'})
    for col in ['开盘', '最高', '最低', '收盘', '成交量']: 
        df_calc[col] = pd.to_numeric(df_calc[col], errors='coerce')
        
    processed_index = calculate_advanced_indicators(df_calc)
    latest_idx = processed_index.iloc[-1]
    
    close_val = safe_float(latest_idx['收盘'])
    ma20_val = safe_float(latest_idx.get('MA20', 0))
    ma60_val = safe_float(latest_idx.get('MA60', 0))
    ma200_val = safe_float(latest_idx.get('MA200', 0))
    pct_chg = safe_float(latest_idx.get('daily_return', 0)) * 100
    
    if close_val > ma20_val and ma20_val > ma60_val: trend = "多头排列 (站上短中期均线)"
    elif close_val < ma20_val and ma20_val < ma60_val: trend = "空头排列 (跌破短中期均线)"
    elif close_val > ma20_val: trend = "震荡偏多 (短线站上MA20)"
    else: trend = "震荡偏空 (短线跌破MA20)"

    long_trend = "长线位于牛熊分界线之上" if close_val > ma200_val else "长线位于防守期"

    pe_str = "A股整体估值获取失败"
    try:
        pe_df = ak.stock_a_ttm_lyr()
        if not pe_df.empty:
            pe_df['date'] = pd.to_datetime(pe_df['date'])
            latest_pe = pe_df.sort_values('date').iloc[-1]
            mid_pe = float(latest_pe['middlePETTM'])
            pe_rank = float(latest_pe['quantileInRecent10YearsMiddlePeTtm']) * 100
            
            if pe_rank <= 20: pe_status = "极度低估(左侧击球区)"
            elif pe_rank >= 80: pe_status = "极度高估(泡沫警戒区)"
            else: pe_status = "估值中枢(中性区)"
            pe_str = f"A股中位数PE(TTM): {mid_pe:.2f} (处于近10年 {pe_rank:.2f}% 分位, {pe_status})"
    except: pass

    bond_str = "国债收益率获取失败"
    try:
        bond_df = ak.bond_gb_zh_sina(symbol="中国10年期国债")
        if not bond_df.empty:
            yield_val = float(bond_df.iloc[-1]['close'])
            if yield_val < 2.5: liq_status = "极度宽松 (利好权益资产Beta)"
            elif yield_val > 3.2: liq_status = "紧缩承压"
            else: liq_status = "中性稳定"
            bond_str = f"十年期国债收益率: {yield_val:.3f}% ({liq_status})"
    except: pass

    futures_text = "\n【期指与衍生品前瞻】\n暂无数据 (获取或计算失败)"
    try:
        raw_cffex = ak.match_main_contract(symbol="cffex")
        clean_cffex = ",".join([s for s in raw_cffex.split(',') if '无主力' not in s])
        df_spot_ff = ak.futures_zh_spot(symbol=clean_cffex, market="FF", adjust='0')
        if_row = df_spot_ff[df_spot_ff['symbol'].str.contains('沪深300')].iloc[0]
        if_current_price = float(if_row['current_price'])
        
        spot_df_all = ak.stock_zh_index_spot_sina()
        hs300_row = spot_df_all[spot_df_all['代码'] == 'sh000300']
        if hs300_row.empty: hs300_row = spot_df_all[spot_df_all['代码'] == 'sz399300']
        if not hs300_row.empty: 
            hs300_spot = float(hs300_row.iloc[0]['最新价'])
            if_basis_today = if_current_price - hs300_spot
            if_basis_str = f"升水 {if_basis_today:.1f}点" if if_basis_today > 0 else f"贴水 {abs(if_basis_today):.1f}点"
            futures_text = f"\n【期指与衍生品前瞻】\nIF主力基差: {if_basis_str} (沪深300现货报 {hs300_spot}, 期指报 {if_current_price})"
    except: pass

    macro_context = (
        f"1. 基础行情: 上证指数 {close_val:.2f} (日涨跌幅: {pct_chg:.2f}%)\n"
        f"2. 趋势判定: 短期{trend}；{long_trend}。\n"
        f"3. 估值与钟摆: {pe_str}\n"
        f"4. 宏观与流动性: {bond_str}\n{futures_text}"
    )
    
    try:
        with open(macro_text_cache_file, 'w', encoding='utf-8') as f: 
            f.write(macro_context)
    except: pass

    return macro_context

def parse_ascii_table(text: str) -> pd.DataFrame:
    """解析通达信 F10 中的 ASCII 表格为 Pandas DataFrame"""
    if not text: return pd.DataFrame()
    lines = text.strip().split('\n')
    data = []
    for line in lines:
        line = line.strip()
        if not line.startswith('│'): continue
        row = [cell.strip() for cell in line.split('│')[1:-1]]
        if row:
            if len(data) > 0 and row[0] == '' and any(c != '' for c in row):
                for i in range(min(len(row), len(data[-1]))): data[-1][i] += row[i]
            else:
                data.append(row)
    if not data or len(data) < 2: return pd.DataFrame()
    return pd.DataFrame(data[1:], columns=data[0])

def extract_latest_table(text_block: str) -> str:
    match = re.search(r'(┌.*?└[─┴]+┘)', text_block, re.S)
    return match.group(1) if match else ""

def get_section_text(text: str, section_name: str) -> str:
    pattern = r'(?:^|\r?\n)【' + re.escape(section_name) + r'】(.*?)(?=\r?\n【|\r?\n〖|$)'
    match = re.search(pattern, text, re.S)
    return match.group(1) if match else ""

def get_etf_f10_dataframes(etf_code: str) -> dict:
    """获取 ETF 的核心数据，并解析为结构化的 DataFrame 和 Dict"""
    print(f"📡 正在获取并解析 {etf_code} 的 Mootdx F10 数据...")
    client = Quotes.factory(market='std')
    f10_data = client.F10(symbol=etf_code, name='最新提示')
    result = {}
    try:
        if not isinstance(f10_data, dict): return result
        if '最新动态' in f10_data:
            sec_text = get_section_text(f10_data['最新动态'], '1.基金简况')
            if sec_text:
                profile_dict = {}
                for line in sec_text.strip().split('\n'):
                    if ':' in line or '：' in line:
                        parts = line.replace('：', ':').split(':', 1)
                        if len(parts) == 2: profile_dict[parts[0].strip()] = parts[1].strip()
                result['基金简况'] = profile_dict
        if '基金概况' in f10_data:
            match = re.search(r'│\s*基金名称\s*│([^│]+)│', f10_data['基金概况'])
            if match: result['基金名称'] = match.group(1).strip()
        if '基金份额' in f10_data:
            text = f10_data['基金份额']
            result['场内份额变动'] = parse_ascii_table(extract_latest_table(get_section_text(text, '2.场内份额变动')))
        if '行业分析' in f10_data:
            result['行业分析'] = parse_ascii_table(extract_latest_table(f10_data['行业分析']))
        if '持股情况' in f10_data:
            result['持股明细'] = parse_ascii_table(extract_latest_table(get_section_text(f10_data['持股情况'], '1.持股明细')))
    except Exception as e:
        print(f"❌ F10 解析过程中出现异常: {e}")
    return result

# =========================================================================
# 第三部分：数据组装，对外提供核心 Context 接口
# =========================================================================

def get_etf_data_context(etf_code: str, beg: str, end: str, current_date: str) -> str:
    """聚合 ETF 所有相关数据，生成供 LLM 使用的高质量上下文文本"""
    now = datetime.now()
    is_weekend = now.weekday() >= 5
    data_dir = f"log/etf_data/{current_date}"
    os.makedirs(data_dir, exist_ok=True)
    
    # 周末免打扰缓存机制
    cache_file_path = os.path.join(data_dir, f"{etf_code}_full_context_cache.txt")
    if is_weekend and os.path.exists(cache_file_path):
        print(f"📦 周末免更新机制触发：检测到 {etf_code} 缓存已存在，直接读取...")
        with open(cache_file_path, 'r', encoding='utf-8') as f: return f.read()
    
    # 1. 优先拉取大盘宏观环境
    macro_str = get_macro_market_context(current_date)

    # 2. 获取 F10 数据 (字典及DataFrame集合)
    f10_dfs = get_etf_f10_dataframes(etf_code)
    
    # 3. 获取并计算历史行情 (严格使用 mootdx 获取前复权历史日线数据)
    print(f"📈 正在通过 mootdx 获取 {etf_code} 的历史行情并计算指标...")
    daily_kline_str, monthly_kline_str = "暂无数据", "暂无数据"
    latest_row = pd.Series(dtype=float) 
    
    try:
        try: end_dt = datetime.strptime(end, "%Y%m%d")
        except: end_dt = pd.to_datetime(end)
        
        end_year = end_dt.year
        start_year = end_year - 3 

        dfs = []
        for y in range(start_year, end_year + 1):
            try:
                temp_df = get_adjust_year(symbol=etf_code, year=str(y), factor='01')
                if temp_df is not None and not temp_df.empty:
                    if 'date' not in temp_df.columns and temp_df.index.name == 'date': temp_df = temp_df.reset_index()
                    elif 'date' not in temp_df.columns: temp_df = temp_df.reset_index(names='date')
                    dfs.append(temp_df)
                time.sleep(0.2) 
            except: pass

        df_hist = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        if df_hist.empty: raise ValueError(f"未能获取到 {etf_code} 的复权数据")

        # 强制转换数据类型
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df_hist.columns: df_hist[col] = pd.to_numeric(df_hist[col], errors='coerce')

        if 'date' in df_hist.columns: df_hist['date'] = pd.to_datetime(df_hist['date'])
        df_hist = df_hist.rename(columns={'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'volume': '成交量'})
        df_hist['日期'] = df_hist['日期'].dt.strftime('%Y-%m-%d')
        # 去重、排序
        df_hist = df_hist.sort_values('日期').drop_duplicates(subset=['日期']).reset_index(drop=True)
        
        # 计算量化指标矩阵
        processed_data = calculate_advanced_indicators(df_hist)
        
        # ================== 多周期布林带测算 ==================
        df_resample = df_hist.copy()
        df_resample['日期'] = pd.to_datetime(df_resample['日期'])
        df_resample.set_index('日期', inplace=True)
        
        df_weekly = df_resample.resample('W-FRI').agg({'开盘': 'first', '最高': 'max', '最低': 'min', '收盘': 'last', '成交量': 'sum'}).dropna().reset_index()
        df_weekly['MA20'] = df_weekly['收盘'].rolling(20).mean()
        df_weekly['std20'] = df_weekly['收盘'].rolling(20).std()
        df_weekly['upper_bb'] = df_weekly['MA20'] + 2 * df_weekly['std20']
        df_weekly['lower_bb'] = df_weekly['MA20'] - 2 * df_weekly['std20']
        df_weekly['bb_width'] = (df_weekly['upper_bb'] - df_weekly['lower_bb']) / df_weekly['MA20'].replace(0, np.nan)
        
        df_monthly = df_resample.resample('ME').agg({'开盘': 'first', '最高': 'max', '最低': 'min', '收盘': 'last', '成交量': 'sum'}).dropna().reset_index()
        df_monthly['MA20'] = df_monthly['收盘'].rolling(20).mean()
        df_monthly['std20'] = df_monthly['收盘'].rolling(20).std()
        df_monthly['upper_bb'] = df_monthly['MA20'] + 2 * df_monthly['std20']
        df_monthly['lower_bb'] = df_monthly['MA20'] - 2 * df_monthly['std20']
        df_monthly['bb_width'] = (df_monthly['upper_bb'] - df_monthly['lower_bb']) / df_monthly['MA20'].replace(0, np.nan)

        daily_bb_str = analyze_bb_status(processed_data, "日线布林带")
        weekly_bb_str = analyze_bb_status(df_weekly, "周线布林带")
        monthly_bb_str = analyze_bb_status(df_monthly, "月线布林带")
        # =======================================================
        
        # 保存 CSV (供绘图工具使用)
        req_beg_date = f"{beg[:4]}-{beg[4:6]}-{beg[6:]}"
        save_data = processed_data[processed_data['日期'] >= req_beg_date].copy()
        filepath_data = os.path.join(data_dir, f"{etf_code}_indicators_{current_date}.csv")
        save_data.to_csv(filepath_data, index=False)
        
        # 截取发送给 LLM 的切片
        recent_20 = processed_data.tail(20)[['日期', '开盘', '最高', '最低', '收盘', '成交量', 'daily_return', 'MA20', 'MACD', 'rsi_14']].copy()
        recent_20['日期'] = recent_20['日期'].astype(str)
        daily_kline_str = recent_20.round(4).to_markdown(index=False)

        df_monthly_tmp = processed_data.copy()
        df_monthly_tmp['日期'] = pd.to_datetime(df_monthly_tmp['日期'])
        df_monthly_tmp['year_month'] = df_monthly_tmp['日期'].dt.to_period('M')
        df_monthly_k = df_monthly_tmp.groupby('year_month').agg({
            '开盘': 'first', '最高': 'max', '最低': 'min', '收盘': 'last', '成交量': 'sum',   
            'daily_return': lambda x: (1 + x).prod() - 1, 'MA20': 'last', 'MACD': 'last', 'rsi_14': 'last'   
        }).reset_index()
        df_monthly_k.rename(columns={'year_month': '日期', 'daily_return': 'monthly_return'}, inplace=True)
        df_monthly_k['日期'] = df_monthly_k['日期'].astype(str)
        monthly_kline_str = df_monthly_k.tail(20).round(4).to_markdown(index=False)

        latest_row = processed_data.iloc[-1]
        
    except Exception as e:
        print(f"❌ 行情数据处理失败: {e}")

    # ================= 4. 组装策略信号矩阵 (JSON) =================
    strategy_signals = {}
    if not latest_row.empty:
        adx_val = safe_float(latest_row.get('adx'))
        p_di = safe_float(latest_row.get('+di'))
        m_di = safe_float(latest_row.get('-di'))
        trend_sig = "bullish" if (p_di > m_di and adx_val > 25) else ("bearish" if (m_di > p_di and adx_val > 25) else "neutral")
        trend_conf = min(int(adx_val * 2.5), 99) if not np.isnan(adx_val) else 0

        z_sc = safe_float(latest_row.get('z_score'))
        rsi14 = safe_float(latest_row.get('rsi_14'))
        bb_pct_b = safe_float(latest_row.get('bb_pct_b', 0.5))
        bb_width = safe_float(latest_row.get('bb_width', 0))
        
        mr_sig = "bullish" if (z_sc < -2 or rsi14 < 30 or bb_pct_b < 0) else ("bearish" if (z_sc > 2 or rsi14 > 70 or bb_pct_b > 1) else "neutral")
        mr_conf = min(int(abs(z_sc) * 35), 99) if not np.isnan(z_sc) else 0

        mom3 = safe_float(latest_row.get('momentum_3m'))
        mom_sig = "bullish" if mom3 > 0.05 else ("bearish" if mom3 < -0.05 else "neutral")
        mom_conf = min(int(abs(mom3) * 500), 99) if not np.isnan(mom3) else 0

        hurst = safe_float(latest_row.get('hurst_exponent'))
        
        sig_map = {"bullish": "看多", "bearish": "看空", "neutral": "中性"}

        strategy_signals = {
            "趋势跟随": {
                "信号": sig_map.get(trend_sig, "中性"), "置信度": f"{trend_conf}%",
                "具体指标": {"ADX(平均趋向指数)": round(adx_val, 4), "趋势强度": f"{adx_val:.2f}%"}
            },
            "均值回归": {
                "信号": sig_map.get(mr_sig, "中性"), "置信度": f"{mr_conf}%",
                "具体指标": {
                    "Z-score": round(z_sc, 4), "布林极限(%b)": round(bb_pct_b, 4),
                    "布林带宽(Bandwidth)": f"{bb_width * 100:.2f}%", "RSI(14)": round(rsi14, 4)
                }
            },
            "动量效应": {
                "信号": sig_map.get(mom_sig, "中性"), "置信度": f"{mom_conf}%",
                "具体指标": {
                    "1个月动量": f"{safe_float(latest_row.get('momentum_1m')) * 100:.2f}%",
                    "3个月动量": f"{mom3 * 100:.2f}%"
                }
            },
            "统计套利": {
                "信号": "中性", "置信度": "50%",
                "具体指标": {"赫斯特指数(Hurst)": f"{hurst:.2f}"}
            }
        }

    # ================= 5. 构建最终发给 LLM 的 Context =================
    context_blocks = []
    
    context_blocks.append(f"======\n【宏观大盘环境】\n{macro_str}\n======")
    context_blocks.append(f"\n====== 【1. {etf_code} 基金概况】 ======")
    context_blocks.append(f"基金名称: {f10_dfs.get('基金名称', '未知')}")
    profile = f10_dfs.get('基金简况', {})
    if profile:
        for k, v in profile.items(): context_blocks.append(f"{k}: {v}")

    df_change = f10_dfs.get('场内份额变动')
    if isinstance(df_change, pd.DataFrame) and not df_change.empty:
        context_blocks.append("\n====== 【2. 近期场内份额变动 (前10天)】 ======")
        context_blocks.append(df_change.head(10).to_markdown(index=False))
        
    df_industry = f10_dfs.get('行业分析')
    if isinstance(df_industry, pd.DataFrame) and not df_industry.empty:
        context_blocks.append("\n====== 【3. 行业分布配置】 ======")
        context_blocks.append(df_industry.head(10).to_markdown(index=False))

    df_holdings = f10_dfs.get('持股明细')
    if isinstance(df_holdings, pd.DataFrame) and not df_holdings.empty:
        context_blocks.append("\n====== 【4. 最新重仓股明细 (前15名)】 ======")
        context_blocks.append(df_holdings.head(15).to_markdown(index=False))
        
    context_blocks.append("\n====== 【5. 核心量价数据 (日K与月K)】 ======")
    context_blocks.append("--- 近 20 个交易日数据 ---")
    context_blocks.append(daily_kline_str)
    context_blocks.append("\n--- 近 20 个月K线数据 ---")
    context_blocks.append(monthly_kline_str)
    
    if not latest_row.empty:
        context_blocks.append("\n====== 【6. 最新交易日量化信号】 ======")
        context_blocks.append("### 【多周期布林带共振状态】")
        context_blocks.append(f"- {daily_bb_str}")
        context_blocks.append(f"- {weekly_bb_str}")
        context_blocks.append(f"- {monthly_bb_str}")
        context_blocks.append("\n### 【量化策略信号矩阵】")
        context_blocks.append(json.dumps(strategy_signals, indent=2, ensure_ascii=False))

    final_text = "\n".join(context_blocks)
    
    if is_weekend:
        with open(cache_file_path, 'w', encoding='utf-8') as f:
            f.write(final_text)

    return final_text

# ================= 测试运行 =================
if __name__ == "__main__":
    pd.options.display.float_format = '{:.2f}'.format
    
    test_code = '510300'
    today = datetime.now()
    end_date = today.strftime("%Y%m%d")
    beg_date = (today - timedelta(days=60)).strftime("%Y%m%d")
    
    print(">>> 启动 ETF 数据抓取测试...")
    result_text = get_etf_data_context(
        etf_code=test_code, 
        beg=beg_date, 
        end=end_date, 
        current_date=today.strftime("%Y-%m-%d")
    )
    
    print("\n\n" + "="*50)
    print("最终生成的供 LLM 使用的上下文内容预览：\n")
    print(result_text)
    print("="*50)