# -*- coding: utf-8 -*-
import baostock as bs
import akshare as ak
import pandas as pd
import numpy as np
import os
import re
import json
from datetime import datetime, timedelta
import time
import random
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# 引入自定义的新浪实时行情接口
from src.sina_realtime import SinaRealtimeFetcher

# --- 辅助函数 ---
def get_bs_code(symbol: str) -> str:
    """将股票代码转换为 baostock 需要的格式"""
    if symbol.startswith('6'): return f"sh.{symbol}"
    elif symbol.startswith('0') or symbol.startswith('3'): return f"sz.{symbol}"
    elif symbol.startswith('8') or symbol.startswith('4'): return f"bj.{symbol}"
    return symbol

def get_stock_name_bs(stock_code):
    """获取股票名称 (支持在外部统一控制连接，兼容独立调用)"""
    bs_code = get_bs_code(stock_code)
    rs_basic = bs.query_stock_basic(code=bs_code)
    
    # 兼容前端单独调用的智能检测
    need_logout = False
    if rs_basic.error_code != '0' and "login" in str(rs_basic.error_msg).lower():
        bs.login()
        rs_basic = bs.query_stock_basic(code=bs_code)
        need_logout = True
        
    stock_name = "未知名称"
    if rs_basic.error_code == '0' and rs_basic.next():
        stock_name = rs_basic.get_row_data()[1]
        
    if need_logout:
        bs.logout()
        
    return stock_name

def get_chart_data(stock_code, beg, end):
    """获取日 K 线数据 (支持在外部统一控制连接，兼容独立调用)"""
    bs_code = get_bs_code(stock_code)
    bs_start = f"{beg[:4]}-{beg[4:6]}-{beg[6:]}"
    bs_end = f"{end[:4]}-{end[4:6]}-{end[6:]}"
    
    fields = "date,open,high,low,close,volume"
    rs = bs.query_history_k_data_plus(bs_code, fields, start_date=bs_start, end_date=bs_end, frequency="d", adjustflag="2")
    
    need_logout = False
    if rs.error_code != '0' and "login" in str(rs.error_msg).lower():
        bs.login()
        rs = bs.query_history_k_data_plus(bs_code, fields, start_date=bs_start, end_date=bs_end, frequency="d", adjustflag="2")
        need_logout = True

    data_list = []
    while (rs.error_code == '0') & rs.next(): 
        data_list.append(rs.get_row_data())
        
    df = pd.DataFrame(data_list, columns=rs.fields)
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns: 
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    if need_logout:
        bs.logout()
        
    return df

def get_30m_chart_data(stock_code, beg, end):
    """获取 30 分钟 K 线数据"""
    bs_code = get_bs_code(stock_code)
    bs_start = f"{beg[:4]}-{beg[4:6]}-{beg[6:]}"
    bs_end = f"{end[:4]}-{end[4:6]}-{end[6:]}"
    
    fields = "date,time,code,open,high,low,close,volume,amount,adjustflag"
    rs = bs.query_history_k_data_plus(bs_code, fields, start_date=bs_start, end_date=bs_end, frequency="30", adjustflag="2")
    
    need_logout = False
    if rs.error_code != '0' and "login" in str(rs.error_msg).lower():
        bs.login()
        rs = bs.query_history_k_data_plus(bs_code, fields, start_date=bs_start, end_date=bs_end, frequency="30", adjustflag="2")
        need_logout = True
    
    data_list = []
    while (rs.error_code == '0') & rs.next(): 
        data_list.append(rs.get_row_data())
        
    df = pd.DataFrame(data_list, columns=rs.fields)
    for col in ["open", "high", "low", "close", "volume", "amount"]:
        if col in df.columns: 
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    if need_logout:
        bs.logout()
            
    return df

def get_xq_symbol(symbol: str) -> str:
    if symbol.startswith('6'): return f"SH{symbol}"
    elif symbol.startswith('0') or symbol.startswith('3'): return f"SZ{symbol}"
    elif symbol.startswith('8') or symbol.startswith('4'): return f"BJ{symbol}"
    return symbol

def get_xueqiu_dividend_yield(symbol: str) -> str:
    """使用 Playwright 从雪球网页抓取 股息率(TTM)"""
    xq_symbol = get_xq_symbol(symbol)
    url = f"https://xueqiu.com/S/{xq_symbol}"
    dividend_yield = "N/A"

    try:
        print(f"🚀 正在通过 Playwright 获取雪球数据: {xq_symbol}...")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080}
            )
            page = context.new_page()
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            time.sleep(random.uniform(1.5, 3.5))
            page.goto(url, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_selector('body', timeout=15000)
            time.sleep(random.uniform(1.0, 2.0)) 
            
            html = page.content()
            soup = BeautifulSoup(html, 'html.parser')
            text_content = soup.get_text(separator=' ', strip=True)
            
            match = re.search(r'股息率\(TTM\)[\s:：]*([0-9\.]+%|--)', text_content)
            if match:
                dividend_yield = match.group(1)
                print(f"✅ 成功获取 {symbol} 股息率(TTM): {dividend_yield}")
            else:
                print(f"⚠️ 未能在雪球页面找到 {symbol} 的股息率数据。")
                
            browser.close()
    except Exception as e:
        print(f"❌ 获取雪球股息率失败: {e}")
        
    return dividend_yield

def get_ths_fund_flow(stock_code: str) -> pd.DataFrame:
    """使用 Playwright 从同花顺网页抓取历史资金流向数据"""
    url = f"https://stockpage.10jqka.com.cn/{stock_code}/funds/"
    fund_data = []
    
    try:
        print(f"🚀 正在通过 Playwright 获取同花顺历史资金流数据: {stock_code}...")
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080}
            )
            page = context.new_page()
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            time.sleep(random.uniform(1.0, 3.0))
            page.goto(url, wait_until="domcontentloaded", timeout=45000)
            page.wait_for_selector('table', timeout=15000)
            time.sleep(random.uniform(1.5, 2.5)) 
            
            html = page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            target_table = None
            for table in soup.find_all('table'):
                if '5日主力净额' in table.get_text() and '大单(主力)' in table.get_text():
                    target_table = table
                    break
                    
            if target_table:
                tbody = target_table.find('tbody')
                if tbody:
                    for tr in tbody.find_all('tr'):
                        tds = tr.find_all('td')
                        if len(tds) >= 11:
                            row = [td.get_text(strip=True) for td in tds]
                            fund_data.append(row)
                            
            browser.close()
    except Exception as e:
        print(f"❌ 获取同花顺资金数据失败: {e}")
        
    if fund_data:
        columns = ['日期', '收盘价', '涨跌幅', '资金净流入', '5日主力净额', '大单净额', '大单净占比', '中单净额', '中单净占比', '小单净额', '小单净占比']
        df = pd.DataFrame(fund_data, columns=columns)
        df['date'] = df['日期'].apply(lambda x: f"{x[:4]}-{x[4:6]}-{x[6:8]}" if len(x) == 8 else x)
        
        cols_to_keep = ['date', '资金净流入', '5日主力净额', '大单净额', '大单净占比', '中单净额', '中单净占比', '小单净额', '小单净占比']
        df = df[cols_to_keep].copy()
        
        for col in cols_to_keep[1:]:
            df[col] = df[col].str.replace('%', '').str.replace(',', '').apply(safe_float)
            
        return df
    return pd.DataFrame()

def safe_float(val, default=0.0):
    try:
        if val == "" or pd.isna(val) or val is None: return default
        if isinstance(val, str): val = val.replace('—', '-').replace('%', '').replace(',', '')
        return float(val)
    except:
        return default

def parse_chinese_number(val):
    if pd.isna(val) or val == '-' or val == '': return np.nan
    val = str(val).replace(',', '')
    multiplier = 1
    if '亿' in val:
        multiplier = 1e8
        val = val.replace('亿', '')
    elif '万' in val:
        multiplier = 1e4
        val = val.replace('万', '')
    try:
        return float(val) * multiplier
    except:
        return np.nan
    
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

    if 'peTTM' in df.columns:
        df['ep'] = np.where(df['peTTM'] == 0, np.nan, 1 / df['peTTM'])
        df['pe_rank'] = (-df['ep']).rank(pct=True) * 100

    if 'pbMRQ' in df.columns:
        df['bp'] = np.where(df['pbMRQ'] == 0, np.nan, 1 / df['pbMRQ'])
        df['pb_rank'] = (-df['bp']).rank(pct=True) * 100

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
    
    # 【核心新增】布林极限 %b 与 带宽 Bandwidth
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

    # 波动率测算
    df['historical_volatility'] = df['daily_return'].rolling(20).std() * np.sqrt(252)
    vol_120 = df['daily_return'].rolling(120).std() * np.sqrt(252)
    vol_120_min = vol_120.rolling(120).min()
    vol_120_max = vol_120.rolling(120).max()
    df['volatility_regime'] = (df['historical_volatility'] - vol_120_min) / (vol_120_max - vol_120_min).replace(0, np.nan)
    df['volatility_z_score'] = (df['historical_volatility'] - vol_120.rolling(120).mean()) / vol_120.rolling(120).std().replace(0, np.nan)
    df['atr_ratio'] = df['atr14'] / df['收盘']

    # 统计套利
    df['skewness'] = df['daily_return'].rolling(20).skew()
    df['kurtosis'] = df['daily_return'].rolling(20).kurt()
    df['hurst_exponent'] = df['收盘'].rolling(120).apply(calculate_hurst, raw=False)
    
    return df

def get_intraday_volume_ratio(traded_mins: int) -> float:
    """基于 A 股经典 U 型成交量分布的经验累积权重"""
    if traded_mins <= 0: return 0.01
    elif traded_mins <= 30: return 0.28 * (traded_mins / 30)
    elif traded_mins <= 60: return 0.28 + 0.14 * ((traded_mins - 30) / 30)
    elif traded_mins <= 120: return 0.42 + 0.16 * ((traded_mins - 60) / 60)
    elif traded_mins <= 180: return 0.58 + 0.17 * ((traded_mins - 120) / 60)
    elif traded_mins <= 240: return 0.75 + 0.25 * ((traded_mins - 180) / 60)
    return 1.0

def get_macro_market_context(current_date: str) -> str:
    """获取大盘(上证指数)数据并进行量化分析 (含估值、流动性与盘中非线性量能预估及盘后缓存)"""
    cache_dir = f"log/index_data"
    os.makedirs(cache_dir, exist_ok=True)
    
    macro_text_cache_file = os.path.join(cache_dir, f"macro_context_text_{current_date}.txt")
    
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    is_weekday = now.weekday() < 5
    current_time_str = now.strftime("%H:%M")
    is_trading_time = "09:30" <= current_time_str <= "15:05"

    # 如果当前不在交易时间，且今天的宏观文本缓存已存在，直接读取返回
    if not is_trading_time and os.path.exists(macro_text_cache_file):
        print("\n🌍 📦 检测到宏观大盘环境文本缓存已存在，直接读取跳过计算...")
        with open(macro_text_cache_file, 'r', encoding='utf-8') as f:
            return f.read()

    print("\n🌍 正在获取并分析宏观大盘(上证指数)与全市场宏观数据...")
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
            print(f"❌ 获取大盘历史数据失败: {e}")
            return "大盘数据获取失败，宏观环境未知。"
            
    latest_date = df_index['date'].iloc[-1].strftime("%Y-%m-%d") if not df_index.empty else ""
    is_estimating = False 
    
    # 盘中实时行情追加与修正
    if is_weekday and current_time_str >= "09:30":
        if is_trading_time or today_str != latest_date:
            try:
                spot_df = ak.stock_zh_index_spot_sina()
                sh_spot = spot_df[spot_df['代码'] == 'sh000001'].iloc[0]
                latest_close = safe_float(sh_spot['最新价'])
                spot_volume = safe_float(sh_spot['成交量'])
                
                # 成交量单位对齐 (手转股)
                if not df_index.empty:
                    prev_vol = df_index.iloc[-1]['volume']
                    if spot_volume > 0 and spot_volume < (prev_vol / 10):
                        spot_volume = spot_volume * 100
                
                # 盘中非线性量能预估
                if is_trading_time:
                    traded_minutes = 0
                    if "09:30" <= current_time_str <= "11:30":
                        h, m = map(int, current_time_str.split(':'))
                        traded_minutes = (h - 9) * 60 + m - 30
                    elif "13:00" <= current_time_str <= "15:05":
                        h, m = map(int, current_time_str.split(':'))
                        traded_minutes = 120 + (h - 13) * 60 + m
                        
                    if 0 < traded_minutes < 240:
                        current_ratio = get_intraday_volume_ratio(traded_minutes)
                        spot_volume = spot_volume / current_ratio  
                        is_estimating = True  
                
                if latest_close > 0 and spot_volume > 0:
                    new_row = {
                        'date': pd.to_datetime(today_str),
                        'open': safe_float(sh_spot['今开']),
                        'high': safe_float(sh_spot['最高']),
                        'low': safe_float(sh_spot['最低']),
                        'close': latest_close,
                        'volume': spot_volume
                    }
                    
                    if latest_date == today_str:
                        if df_index.iloc[-1]['close'] != latest_close:
                            for k, v in new_row.items():
                                df_index.iloc[-1, df_index.columns.get_loc(k)] = v
                    else:
                        df_index = pd.concat([df_index, pd.DataFrame([new_row])], ignore_index=True)
            except Exception as e:
                print(f"⚠️ 获取大盘实时行情失败(将使用最新历史数据): {e}")

    # 计算大盘技术指标
    df_calc = df_index.copy()
    df_calc = df_calc.rename(columns={'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'volume': '成交量'})
    for col in ['开盘', '最高', '最低', '收盘', '成交量']:
        df_calc[col] = pd.to_numeric(df_calc[col], errors='coerce')
        
    processed_index = calculate_advanced_indicators(df_calc)
    latest_idx = processed_index.iloc[-1]
    
    actual_latest_date = latest_idx['日期'].strftime("%Y-%m-%d")
    close_val = safe_float(latest_idx['收盘'])
    ma20_val = safe_float(latest_idx.get('MA20', 0))
    ma60_val = safe_float(latest_idx.get('MA60', 0))
    ma120_val = safe_float(latest_idx.get('MA120', 0))
    ma200_val = safe_float(latest_idx.get('MA200', 0))
    pct_chg = safe_float(latest_idx.get('daily_return', 0)) * 100
    
    # 趋势判定
    if close_val > ma20_val and ma20_val > ma60_val: trend = "多头排列 (站上短中期均线)"
    elif close_val < ma20_val and ma20_val < ma60_val: trend = "空头排列 (跌破短中期均线)"
    elif close_val > ma20_val: trend = "震荡偏多 (短线站上MA20)"
    else: trend = "震荡偏空 (短线跌破MA20)"

    long_trend = "长线位于牛熊分界线(MA200)之上" if close_val > ma200_val else "长线位于牛熊分界线(MA200)之下，处于战略防守期"

    # 获取 A 股等权重与中位数市盈率 
    pe_str = "A股整体估值获取失败"
    try:
        pe_df = ak.stock_a_ttm_lyr()
        if not pe_df.empty:
            pe_df['date'] = pd.to_datetime(pe_df['date'])
            pe_df = pe_df.sort_values('date')
            latest_pe = pe_df.iloc[-1]
            
            mid_pe = float(latest_pe['middlePETTM'])
            pe_rank = float(latest_pe['quantileInRecent10YearsMiddlePeTtm']) * 100
            
            if pe_rank <= 20: pe_status = "极度低估(左侧击球区)"
            elif pe_rank <= 40: pe_status = "偏低估(安全区)"
            elif pe_rank >= 90: pe_status = "极度高估(泡沫警戒区)"
            elif pe_rank >= 60: pe_status = "偏高估(风险区)"
            else: pe_status = "估值中枢(中性区)"
            
            pe_str = f"A股中位数PE(TTM): {mid_pe:.2f} (处于近10年 {pe_rank:.2f}% 分位, {pe_status})"
    except Exception as e:
        print(f"⚠️ A股PE获取失败: {e}")

    # 获取十年期国债收益率 
    bond_str = "国债收益率获取失败"
    try:
        bond_df = ak.bond_gb_zh_sina(symbol="中国10年期国债")
        if not bond_df.empty:
            latest_bond = bond_df.iloc[-1]
            yield_val = float(latest_bond['close'])
            
            if yield_val < 2.5: liq_status = "极度宽松 (利好权益资产Beta)"
            elif yield_val < 2.8: liq_status = "宽松偏多"
            elif yield_val > 3.2: liq_status = "紧缩承压 (利空权益资产估值)"
            else: liq_status = "中性稳定"
            
            bond_str = f"十年期国债收益率: {yield_val:.3f}% ({liq_status})"
    except Exception as e:
        print(f"⚠️ 国债收益率获取失败: {e}")

    # 量能放大对比测算 
    vol_current = safe_float(latest_idx.get('成交量', 0))
    vol_ma5 = safe_float(latest_idx.get('volume_ma5', 0))
    prefix_text = "预估全天" if is_estimating else "实际"
    
    if vol_ma5 > 0:
        vol_ratio = vol_current / vol_ma5
        if vol_ratio >= 1.2: vol_status = f"显著放量 ({prefix_text}较5日均量增量 +{(vol_ratio-1)*100:.1f}%)"
        elif vol_ratio <= 0.8: vol_status = f"显著缩量 ({prefix_text}较5日均量缩量 -{(1-vol_ratio)*100:.1f}%)"
        else: vol_status = f"平量震荡 ({prefix_text}约为5日均量的 {vol_ratio*100:.1f}%)"
    else:
        vol_status = "量能数据缺失"

    # 组合输出 (新增布林极值与带宽)
    rsi14 = safe_float(latest_idx.get('rsi_14', 50))
    z_score = safe_float(latest_idx.get('z_score', 0))
    bb_pct_b = safe_float(latest_idx.get('bb_pct_b', 0.5))
    bb_width = safe_float(latest_idx.get('bb_width', 0))
    
    rsi_status = "超买极值" if rsi14 > 70 else ("超卖极值" if rsi14 < 30 else "中性区间")
    
    if bb_pct_b >= 1.0: bb_status = "突破上轨 (超买/极强动量)"
    elif bb_pct_b <= 0.0: bb_status = "跌破下轨 (超卖/极度恐慌)"
    elif bb_pct_b >= 0.8: bb_status = "逼近上轨 (压力区)"
    elif bb_pct_b <= 0.2: bb_status = "逼近下轨 (支撑区)"
    else: bb_status = "通道内震荡"

    if bb_width < 0.05: width_status = "喇叭口极度收敛 (面临重大变盘)"
    elif bb_width > 0.10: width_status = "喇叭口敞开 (高波动趋势发散)"
    else: width_status = "带宽正常"

    macro_context = (
        f"1. 基础行情: 上证指数 {close_val:.2f} (数据日期: {actual_latest_date}, 日涨跌幅: {pct_chg:.2f}%)\n"
        f"2. 趋势与量能: 短期{trend}；大盘量能呈现{vol_status}。\n"
        f"3. 核心均线: MA20={ma20_val:.2f}, MA60={ma60_val:.2f}, MA120={ma120_val:.2f}, MA200={ma200_val:.2f} ({long_trend})\n"
        f"4. 估值与钟摆: {pe_str}\n"
        f"5. 宏观与流动性: {bond_str}\n"
        f"6. 情绪与通道: RSI14={rsi14:.2f} ({rsi_status}); 布林极限%b={bb_pct_b:.2f} ({bb_status}), 布林带宽={bb_width*100:.2f}% ({width_status})"
    )
    
    # 盘后写入缓存
    if not is_estimating:
        try:
            with open(macro_text_cache_file, 'w', encoding='utf-8') as f:
                f.write(macro_context)
        except Exception as e:
            print(f"⚠️ 宏观文本缓存写入失败: {e}")

    return macro_context

def get_stock_data(stock_code:str, beg:str, end:str, current_date:str):
    now = datetime.now()
    is_weekend = now.weekday() >= 5
    data_dir = f"log/stock_data/{current_date}"
    os.makedirs(data_dir, exist_ok=True)
    
    # 周末免拉取缓存机制
    cache_file_path = os.path.join(data_dir, f"{stock_code}_full_metrics_cache.txt")
    if is_weekend and os.path.exists(cache_file_path):
        print(f"📦 周末免更新机制触发：检测到 {stock_code} 行情与财务数据已存在，直接读取...")
        with open(cache_file_path, 'r', encoding='utf-8') as f:
            return f.read()

    # 优先拉取大盘宏观环境
    macro_str = get_macro_market_context(current_date)
    
    bs_code = get_bs_code(stock_code)

    try:
        end_dt = datetime.strptime(end, "%Y%m%d")
    except:
        end_dt = datetime.now()
        
    start_dt = end_dt - timedelta(days=1095)
    long_beg_date = start_dt.strftime("%Y-%m-%d")
    end_date_str = end_dt.strftime("%Y-%m-%d")

    # 1. 获取基本信息
    rs_basic = bs.query_stock_basic(code=bs_code)
    stock_name = "未知名称"
    if rs_basic.error_code == '0' and rs_basic.next():
        stock_name = rs_basic.get_row_data()[1]
        
    rs_ind = bs.query_stock_industry(code=bs_code)
    industry = "未知行业"
    if rs_ind.error_code == '0' and rs_ind.next():
        industry = rs_ind.get_row_data()[3]

    # 2. 获取行情
    fields = "date,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,psTTM"
    rs_k = bs.query_history_k_data_plus(bs_code, fields, start_date=long_beg_date, end_date=end_date_str, frequency="d", adjustflag="2")
    
    k_data_list = []
    while (rs_k.error_code == '0') & rs_k.next():
        k_data_list.append(rs_k.get_row_data())
        
    k_df = pd.DataFrame(k_data_list, columns=fields.split(','))
    
    today_str = now.strftime("%Y-%m-%d")
    current_time_str = now.strftime("%H:%M")
    latest_bs_date = k_df['date'].iloc[-1] if not k_df.empty else ""

    if not is_weekend and "09:30" <= current_time_str <= "18:00" and latest_bs_date != today_str:
        try:
            print(f"⚡ 正在调用 SinaRealtime 接口获取 {stock_code} 今日行情...")
            fetcher = SinaRealtimeFetcher()
            spot_df = fetcher.fetch_snapshot([stock_code])

            if not spot_df.empty:
                spot_data = spot_df.iloc[0]
                latest_close = safe_float(spot_data['close'])
                spot_volume = safe_float(spot_data['vol'])
                prev_close = safe_float(spot_data['prev_close'])
                
                pct_chg = 0.0
                if prev_close > 0: pct_chg = (latest_close - prev_close) / prev_close * 100.0

                if latest_close > 0 and spot_volume > 0:
                    new_row = {
                        'date': today_str,
                        'open': safe_float(spot_data['open']),
                        'high': safe_float(spot_data['high']),
                        'low': safe_float(spot_data['low']),
                        'close': latest_close,
                        'volume': spot_volume,
                        'amount': safe_float(spot_data['amount']),
                        'turn': k_df['turn'].iloc[-1] if not k_df.empty else 0.0, 
                        'pctChg': pct_chg,
                        'peTTM': k_df['peTTM'].iloc[-1] if not k_df.empty else 0, 
                        'pbMRQ': k_df['pbMRQ'].iloc[-1] if not k_df.empty else 0,
                        'psTTM': k_df['psTTM'].iloc[-1] if not k_df.empty else 0
                    }
                    k_df = pd.concat([k_df, pd.DataFrame([new_row])], ignore_index=True)
                    print(f"✅ 个股今日实时行情修补成功！当前计算最新价: {latest_close}")
        except Exception as e:
            print(f"⚠️ 个股实时行情修补失败(将使用最新历史数据): {e}")

    k_df_calc = k_df.copy()
    k_df_calc = k_df_calc.rename(columns={'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'volume': '成交量'})
    for col in ['开盘', '最高', '最低', '收盘', '成交量', 'peTTM', 'pbMRQ']:
        k_df_calc[col] = pd.to_numeric(k_df_calc[col], errors='coerce')

    processed_data = calculate_advanced_indicators(k_df_calc)

    req_beg_date = f"{beg[:4]}-{beg[4:6]}-{beg[6:]}"
    save_data = processed_data[processed_data['日期'] >= req_beg_date]

    safe_stock_name = re.sub(r'[\\/*?:"<>|]', '', stock_name)
    filename_data = f"{stock_code}_{safe_stock_name}_data_{current_date}.csv"
    filepath_data = os.path.join(data_dir, filename_data)
    save_data.to_csv(filepath_data, index=False)
    print(f"行情数据文件已保存至: {filepath_data}")

    latest_row = processed_data.iloc[-1]
    latest_close = safe_float(latest_row['收盘'])
    turnover_rate = safe_float(k_df.iloc[-1]['turn'])
    pct_change = safe_float(k_df.iloc[-1]['pctChg'])
    volume_shares = safe_float(latest_row['成交量'])
    pe_ttm = safe_float(latest_row['peTTM'])
    pb_mrq = safe_float(latest_row['pbMRQ'])
    ps_ttm = safe_float(k_df.iloc[-1]['psTTM'])

    market_cap = (volume_shares / (turnover_rate / 100)) * latest_close if turnover_rate > 0 else 0.0

    # 3. 抓取财务预测等
    try:
        fin_df = ak.stock_financial_abstract(symbol=stock_code)
        latest_report_date = fin_df.columns[2]
        def get_fin_metric(indicator_name):
            try: return safe_float(fin_df.loc[fin_df['指标'] == indicator_name, latest_report_date].values[0])
            except: return 0.0
    except:
        latest_report_date = "无数据"
        def get_fin_metric(indicator_name): return 0.0

    forecast_metrics = {}
    try:
        forecast_df = ak.stock_profit_forecast_ths(symbol=stock_code, indicator="业绩预测详表-机构")
        if not forecast_df.empty:
            forecast_df['报告日期'] = pd.to_datetime(forecast_df['报告日期'])
            latest_date = forecast_df['报告日期'].max()
            recent_forecasts = forecast_df[
                (forecast_df['报告日期'].dt.year == latest_date.year) & 
                (forecast_df['报告日期'].dt.quarter == latest_date.quarter)
            ].copy()

            eps_cols = [col for col in recent_forecasts.columns if '预测年报每股收益' in col]
            profit_cols = [col for col in recent_forecasts.columns if '预测年报净利润' in col]
            
            for col in profit_cols: recent_forecasts[col] = recent_forecasts[col].apply(parse_chinese_number)
            for col in eps_cols: recent_forecasts[col] = pd.to_numeric(recent_forecasts[col], errors='coerce')
                
            avg_forecasts = recent_forecasts[eps_cols + profit_cols].mean()
            forecast_metrics['预测机构数量(最新一季)'] = f"{len(recent_forecasts)}家"
            forecast_metrics['最新机构预测期'] = f"{latest_date.year}年Q{latest_date.quarter}"
            
            for col in eps_cols: forecast_metrics[f"平均{col}(元)"] = f"{avg_forecasts[col]:.2f}" if not pd.isna(avg_forecasts[col]) else "暂无"
            for col in profit_cols: forecast_metrics[f"平均{col}"] = f"{avg_forecasts[col] / 1e8:.2f}亿" if not pd.isna(avg_forecasts[col]) else "暂无"
        else:
            forecast_metrics['机构预测'] = '暂无机构给出预测'
    except:
        forecast_metrics['机构预测'] = '接口获取失败'

    dividend_yield_ttm = get_xueqiu_dividend_yield(stock_code)

    # ---------------- 动态生成策略信号 ----------------
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
    sa_sig = "bullish" if (hurst < 0.45 and z_sc < 0) else ("bearish" if (hurst < 0.45 and z_sc > 0) else "neutral")
    
    sig_map = {"bullish": "看多", "bearish": "看空", "neutral": "中性"}

    strategy_signals = {
        "趋势跟随": {
            "信号": sig_map.get(trend_sig, "中性"), "置信度": f"{trend_conf}%",
            "具体指标": {"ADX(平均趋向指数)": round(adx_val, 4), "趋势强度": f"{adx_val:.2f}%"}
        },
        "均值回归": {
            "信号": sig_map.get(mr_sig, "中性"), "置信度": f"{mr_conf}%",
            "具体指标": {
                "Z-score": round(z_sc, 4),
                "布林极限(%b)": round(bb_pct_b, 4),
                "布林带宽(Bandwidth)": f"{bb_width * 100:.2f}%",
                "RSI(14)": round(rsi14, 4), 
                "RSI(28)": round(safe_float(latest_row.get('rsi_28')), 4)
            }
        },
        "动量效应": {
            "信号": sig_map.get(mom_sig, "中性"), "置信度": f"{mom_conf}%",
            "具体指标": {
                "1个月动量": f"{safe_float(latest_row.get('momentum_1m')) * 100:.2f}%",
                "3个月动量": f"{mom3 * 100:.2f}%",
                "6个月动量": f"{safe_float(latest_row.get('momentum_6m')) * 100:.2f}%",
                "成交量动量": round(safe_float(latest_row.get('volume_momentum')), 4)
            }
        },
        "波动率测算": {
            "信号": "中性", "置信度": "50%",
            "具体指标": {
                "历史波动率(年化)": f"{safe_float(latest_row.get('historical_volatility')) * 100:.2f}%",
                "波动率历史分位": f"{safe_float(latest_row.get('volatility_regime')) * 100:.2f}%",
                "波动率Z-score": f"{safe_float(latest_row.get('volatility_z_score')) * 100:.2f}%",
                "ATR(真实波动)比率": f"{safe_float(latest_row.get('atr_ratio')) * 100:.2f}%"
            }
        },
        "统计套利": {
            "信号": sig_map.get(sa_sig, "中性"), "置信度": "75%",
            "具体指标": {
                "赫斯特指数(Hurst)": f"{hurst:.2f}",
                "收益率偏度(Skewness)": round(safe_float(latest_row.get('skewness')), 4),
                "收益率峰度(Kurtosis)": round(safe_float(latest_row.get('kurtosis')), 4)
            }
        }
    }

    # 4. 构建输出数据
    all_metrics = {
        "宏观大盘环境": macro_str,
        "股票代码": stock_code,
        "股票名称": stock_name,
        "所处行业": industry,
        "最新财务报告期": latest_report_date,
        "最新价": latest_close,
        "涨跌幅": f"{pct_change:.2f}%",
        "换手率": f"{turnover_rate:.2f}%",
        "总市值": format_large_number(market_cap),
        "MA5": round(safe_float(latest_row.get('MA5')), 2),
        "MA10": round(safe_float(latest_row.get('MA10')), 2),
        "MA20": round(safe_float(latest_row.get('MA20')), 2),
        "MA60": round(safe_float(latest_row.get('MA60')), 2),
        "MA120": round(safe_float(latest_row.get('MA120')), 2),
        "MA200": round(safe_float(latest_row.get('MA200')), 2),
        "MACD": round(safe_float(latest_row.get('MACD')), 2),
        "近4周最高价": round(safe_float(latest_row.get('high_4w')), 2),
        "近4周最低价": round(safe_float(latest_row.get('low_4w')), 2),
        "近13周最高价": round(safe_float(latest_row.get('high_13w')), 2),
        "近13周最低价": round(safe_float(latest_row.get('low_13w')), 2),
        "近52周最高价": round(safe_float(latest_row.get('high_52w')), 2),
        "近52周最低价": round(safe_float(latest_row.get('low_52w')), 2),
        "strategy_signals": json.dumps(strategy_signals, indent=2, ensure_ascii=False),
        "滚动市盈率 P/E(TTM)": f"{pe_ttm:.2f}",
        "市盈率(PE)历史分位": f"{safe_float(latest_row.get('pe_rank')):.2f}%",
        "市净率 P/B": f"{pb_mrq:.2f}",
        "市净率(PB)历史分位": f"{safe_float(latest_row.get('pb_rank')):.2f}%",
        "市销率 P/S": f"{ps_ttm:.2f}",
        "股息率(TTM)": dividend_yield_ttm,
        "营业总收入": format_large_number(get_fin_metric("营业总收入")),
        "净利润": format_large_number(get_fin_metric("净利润")),
        "扣非净利润": format_large_number(get_fin_metric("扣非净利润")),
        "毛利率": f"{get_fin_metric('毛利率'):.2f}%",
        "营业利润率": f"{get_fin_metric('营业利润率'):.2f}%",
        "销售净利率": f"{get_fin_metric('销售净利率'):.2f}%",
        "净资产收益率(ROE)": f"{get_fin_metric('净资产收益率(ROE)'):.2f}%",
        "营业总收入增长率": f"{get_fin_metric('营业总收入增长率'):.2f}%",
        "净利润增长率": f"{get_fin_metric('归属母公司净利润增长率'):.2f}%",
        "资产负债率": f"{get_fin_metric('资产负债率'):.2f}%",
        "流动比率": f"{get_fin_metric('流动比率'):.2f}",
        "速动比率": f"{get_fin_metric('速动比率'):.2f}",
        "基本每股收益(元)": f"{get_fin_metric('基本每股收益'):.2f}",
        "每股净资产(元)": f"{get_fin_metric('每股净资产'):.2f}",
        "每股经营现金流(元)": f"{get_fin_metric('每股经营现金流'):.2f}",
        "应收账款周转天数": f"{get_fin_metric('应收账款周转天数'):.1f}天",
        "存货周转天数": f"{get_fin_metric('存货周转天数'):.1f}天",
        "总资产周转率": f"{get_fin_metric('总资产周转率'):.3f}",
        **forecast_metrics,
    }

    print("\n获取到的完整指标数据：")
    for key, value in all_metrics.items():
        if key in ["strategy_signals", "宏观大盘环境"]: continue
        print(f"{key}: {value}")
    
    print(f"\n【宏观大盘环境】\n{all_metrics['宏观大盘环境']}")
    print(f"\n【量化策略信号矩阵】\n{all_metrics['strategy_signals']}")

    result = []
    result.append(f"======\n【宏观大盘环境】\n{macro_str}\n======")
    result.append("\n### 【基础与行情数据】")
    for key in ["股票代码", "股票名称", "所处行业", "最新财务报告期", "最新价", "涨跌幅", "换手率", "总市值", "近52周最高价", "近52周最低价"]:
        result.append(f"{key}: {all_metrics.get(key, 'N/A')}")
        
    result.append("\n### 【量化策略信号矩阵】")
    result.append(json.dumps(strategy_signals, indent=2, ensure_ascii=False))
    
    result.append("\n### 【核心财务指标】")
    result.append("--- 估值指标 ---")
    for key in ["滚动市盈率 P/E(TTM)", "市盈率(PE)历史分位", "市净率 P/B", "市净率(PB)历史分位", "市销率 P/S", "股息率(TTM)"]:
        result.append(f"{key}: {all_metrics.get(key, 'N/A')}")
        
    result.append("\n--- 盈利与成长能力 ---")
    for key in ["营业总收入", "净利润", "扣非净利润", "毛利率", "营业利润率", "销售净利率", "净资产收益率(ROE)", "营业总收入增长率", "净利润增长率"]:
        result.append(f"{key}: {all_metrics.get(key, 'N/A')}")
        
    result.append("\n--- 资产负债与营运能力 ---")
    for key in ["资产负债率", "流动比率", "速动比率", "应收账款周转天数", "存货周转天数", "总资产周转率"]:
        result.append(f"{key}: {all_metrics.get(key, 'N/A')}")
        
    result.append("\n--- 每股指标 ---")
    for key in ["基本每股收益", "每股净资产", "每股经营现金流"]:
        result.append(f"{key}: {all_metrics.get(key, 'N/A')}")

    result.append("\n### 【机构业绩预测】")
    for key, value in forecast_metrics.items():
        result.append(f"{key}: {value}")

    text_input = "\n".join(result)
    
    if is_weekend:
        with open(cache_file_path, 'w', encoding='utf-8') as f:
            f.write(text_input)
            
    return text_input