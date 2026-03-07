# -*- coding: utf-8 -*-
import baostock as bs
import akshare as ak
import pandas as pd
import numpy as np
import os
import re
import json
from datetime import datetime, timedelta

# 引入自定义的新浪实时行情接口
from src.sina_realtime import SinaRealtimeFetcher

# --- 辅助函数 ---
def get_bs_code(symbol: str) -> str:
    """将股票代码转换为 baostock 需要的格式"""
    if symbol.startswith('6'): return f"sh.{symbol}"
    elif symbol.startswith('0') or symbol.startswith('3'): return f"sz.{symbol}"
    elif symbol.startswith('8') or symbol.startswith('4'): return f"bj.{symbol}"
    return symbol

# ========== 新增拆分出来的函数 ==========
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
    
    rs = bs.query_history_k_data_plus(bs_code, "date,open,high,low,close,volume", start_date=bs_start, end_date=bs_end, frequency="d", adjustflag="2")
    data_list = []
    while (rs.error_code == '0') & rs.next(): data_list.append(rs.get_row_data())
        
    df = pd.DataFrame(data_list, columns=rs.fields)
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
    bs.logout()
    return df
# ========================================

def safe_float(val, default=0.0):
    """安全地将字符串转换为浮点数"""
    try:
        if val == "" or pd.isna(val) or val is None:
            return default
        if isinstance(val, str):
            val = val.replace('—', '-').replace('%', '').replace(',', '')
        return float(val)
    except:
        return default

def parse_chinese_number(val):
    """解析带有 '亿', '万' 等中文单位的数字字符串"""
    if pd.isna(val) or val == '-' or val == '':
        return np.nan
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
    
    # 基础收益率
    df['daily_return'] = df['收盘'].pct_change()
    
    # ---------------- 极值与分位点计算 ----------------
    df['high_4w'] = df['最高'].rolling(20).max()
    df['low_4w'] = df['最低'].rolling(20).min()
    df['high_13w'] = df['最高'].rolling(65).max()
    df['low_13w'] = df['最低'].rolling(65).min()
    df['high_52w'] = df['最高'].rolling(250).max()
    df['low_52w'] = df['最低'].rolling(250).min()

    # 估值历史分位计算优化：解决PE跨越0时的数学不连续性陷阱
    if 'peTTM' in df.columns:
        # 将PE转换为EP (Earnings Yield)
        # 注意处理 PE 为 0 的极端异常情况，防止除以0
        df['ep'] = np.where(df['peTTM'] == 0, np.nan, 1 / df['peTTM'])
        
        # EP越大代表越便宜/基本面越好。
        # 为了符合大家看PE分位“数值越小越便宜”的习惯，我们对 -EP 进行升序排列
        df['pe_rank'] = (-df['ep']).rank(pct=True) * 100

    if 'pbMRQ' in df.columns:
        # 净资产(PB)很少为负，即使为负(资不抵债)，倒数法(BP = 1/PB)同样适用
        df['bp'] = np.where(df['pbMRQ'] == 0, np.nan, 1 / df['pbMRQ'])
        df['pb_rank'] = (-df['bp']).rank(pct=True) * 100

    # ---------------- 动量计算 (Momentum) ----------------
    df['momentum_1m'] = df['收盘'].pct_change(periods=20)
    df['momentum_3m'] = df['收盘'].pct_change(periods=60)
    df['momentum_6m'] = df['收盘'].pct_change(periods=120)
    df['volume_ma5'] = df['成交量'].rolling(5).mean()
    df['volume_ma20'] = df['成交量'].rolling(20).mean()
    df['volume_momentum'] = df['volume_ma5'] / df['volume_ma20'].replace(0, np.nan)
    
    # ---------------- 基础移动平均 ----------------
    df['MA5'] = df['收盘'].rolling(window=5).mean()
    df['MA10'] = df['收盘'].rolling(window=10).mean()
    df['MA20'] = df['收盘'].rolling(window=20).mean()
    df['MA60'] = df['收盘'].rolling(window=60).mean()
    df['MA120'] = df['收盘'].rolling(window=120).mean()
    df['MA200'] = df['收盘'].rolling(window=200).mean()

    # ---------------- MACD ----------------
    df['EMA12'] = df['收盘'].ewm(span=12, adjust=False).mean()
    df['EMA26'] = df['收盘'].ewm(span=26, adjust=False).mean()
    df['DIF'] = df['EMA12'] - df['EMA26']
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD'] = 2 * (df['DIF'] - df['DEA'])

    # ---------------- 趋势跟随 (Trend Following - ADX) ----------------
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

    # ---------------- 均值回归 (Mean Reversion - Bollinger & RSI) ----------------
    df['std20'] = df['收盘'].rolling(20).std()
    df['upper_bb'] = df['MA20'] + 2 * df['std20']
    df['lower_bb'] = df['MA20'] - 2 * df['std20']
    df['z_score'] = (df['收盘'] - df['MA20']) / df['std20'].replace(0, np.nan)
    df['price_vs_bb'] = ((df['收盘'] - df['lower_bb']) / (df['upper_bb'] - df['lower_bb']).replace(0, np.nan)) - 0.5
    
    delta = df['收盘'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs14 = gain / loss.replace(0, np.nan)
    df['rsi_14'] = 100 - (100 / (1 + rs14))

    gain28 = (delta.where(delta > 0, 0)).rolling(window=28).mean()
    loss28 = (-delta.where(delta < 0, 0)).rolling(window=28).mean()
    rs28 = gain28 / loss28.replace(0, np.nan)
    df['rsi_28'] = 100 - (100 / (1 + rs28))

    # ---------------- 波动率测算 (Volatility) ----------------
    df['historical_volatility'] = df['daily_return'].rolling(20).std() * np.sqrt(252)
    vol_120 = df['daily_return'].rolling(120).std() * np.sqrt(252)
    vol_120_min = vol_120.rolling(120).min()
    vol_120_max = vol_120.rolling(120).max()
    df['volatility_regime'] = (df['historical_volatility'] - vol_120_min) / (vol_120_max - vol_120_min).replace(0, np.nan)
    df['volatility_z_score'] = (df['historical_volatility'] - vol_120.rolling(120).mean()) / vol_120.rolling(120).std().replace(0, np.nan)
    df['atr_ratio'] = df['atr14'] / df['收盘']

    # ---------------- 统计套利 (Statistical Arbitrage) ----------------
    df['skewness'] = df['daily_return'].rolling(20).skew()
    df['kurtosis'] = df['daily_return'].rolling(20).kurt()
    df['hurst_exponent'] = df['收盘'].rolling(120).apply(calculate_hurst, raw=False)
    
    return df

def get_macro_market_context(current_date: str) -> str:
    """获取大盘(上证指数)数据并进行量化分析"""
    print("\n🌍 正在获取并分析宏观大盘(上证指数)数据...")
    cache_dir = f"log/index_data"
    os.makedirs(cache_dir, exist_ok=True)
    cache_file = os.path.join(cache_dir, f"sh000001_daily_{current_date}.csv")
    
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    
    # 1. 加载历史数据并缓存 (每天只全量拉一次历史)
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

    # 2. 判断是否需要拉取实时数据 (工作日 9:30 - 15:05)
    is_weekday = now.weekday() < 5
    current_time_str = now.strftime("%H:%M")
    is_trading_time = "09:30" <= current_time_str <= "15:05"
    
    latest_date = df_index['date'].iloc[-1].strftime("%Y-%m-%d")
    
    # 【核心修复】：必须是工作日的 09:30 之后，才允许追加/覆盖当天的实时行情
    if is_weekday and current_time_str >= "09:30":
        # 处于盘中，或时间已过 15:05 但历史接口尚未更新出今天的数据时
        if is_trading_time or today_str != latest_date:
            try:
                spot_df = ak.stock_zh_index_spot_sina()
                sh_spot = spot_df[spot_df['代码'] == 'sh000001'].iloc[0]
                latest_close = safe_float(sh_spot['最新价'])
                spot_volume = safe_float(sh_spot['成交量'])
                
                # 【核心修复】：必须验证成交量大于0，防止把节假日或开盘前的无效现货数据追加进去
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
                        # 最新价如果不一致，更新今天的K线数据
                        if df_index.iloc[-1]['close'] != latest_close:
                            for k, v in new_row.items():
                                df_index.iloc[-1, df_index.columns.get_loc(k)] = v
                    else:
                        # 插入今日真实的实时数据作为最新的一条K线
                        df_index = pd.concat([df_index, pd.DataFrame([new_row])], ignore_index=True)
            except Exception as e:
                print(f"⚠️ 获取大盘实时行情失败(将使用最新历史数据): {e}")

    # 3. 计算大盘技术指标
    df_calc = df_index.copy()
    df_calc = df_calc.rename(columns={'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'volume': '成交量'})
    for col in ['开盘', '最高', '最低', '收盘', '成交量']:
        df_calc[col] = pd.to_numeric(df_calc[col], errors='coerce')
        
    processed_index = calculate_advanced_indicators(df_calc)
    latest_idx = processed_index.iloc[-1]
    
    # 4. 构建宏观上下文文本
    actual_latest_date = latest_idx['日期'].strftime("%Y-%m-%d")  # 动态获取生效的最后一天日期
    close_val = safe_float(latest_idx['收盘'])
    ma20_val = safe_float(latest_idx['MA20'])
    ma60_val = safe_float(latest_idx['MA60'])
    
    # 判定大盘趋势状态
    if close_val > ma20_val and ma20_val > ma60_val:
        trend = "多头排列 (站上MA20且均线向上)"
    elif close_val < ma20_val and ma20_val < ma60_val:
        trend = "空头排列 (跌破MA20且均线向下)"
    elif close_val > ma20_val:
        trend = "震荡偏多 (站上MA20)"
    else:
        trend = "震荡偏空 (跌破MA20)"
        
    pct_chg = safe_float(latest_idx.get('daily_return', 0)) * 100
    
    macro_context = (
        f"上证指数: {close_val:.2f} (数据日期: {actual_latest_date}, 日涨跌幅: {pct_chg:.2f}%)\n"
        f"大盘趋势: {trend}\n"
        f"核心均线: MA20={ma20_val:.2f}, MA60={ma60_val:.2f}\n"
        f"情绪指标(RSI14): {safe_float(latest_idx.get('rsi_14')):.2f} (通常>70超买，<30超卖)\n"
        f"偏离度(Z-Score): {safe_float(latest_idx.get('z_score')):.2f} (偏离20日均线的标准差，绝对值>2极度偏离)"
    )
    return macro_context


def get_stock_data(stock_code:str, beg:str, end:str, current_date:str):
    """
    获取股票历史数据, 计算技术指标, 并保存为CSV文件 
    (行情基于 Baostock，财务基于新浪，机构预测基于同花顺)
    """
    now = datetime.now()
    is_weekend = now.weekday() >= 5
    data_dir = f"log/stock_data/{current_date}"
    os.makedirs(data_dir, exist_ok=True)
    
    # =========================================================================
    # 🌟 核心新增：周末/已有数据缓存免拉取机制
    # =========================================================================
    # 如果是周末且财务及历史数据文本已生成，直接读取缓存跳过所有接口调用
    cache_file_path = os.path.join(data_dir, f"{stock_code}_full_metrics_cache.txt")
    if is_weekend and os.path.exists(cache_file_path):
        print(f"📦 周末免更新机制触发：检测到 {stock_code} 行情与财务数据已存在，直接读取...")
        with open(cache_file_path, 'r', encoding='utf-8') as f:
            return f.read()

    # 优先拉取大盘宏观环境
    macro_str = get_macro_market_context(current_date)
    
    bs.login()
    bs_code = get_bs_code(stock_code)

    try:
        end_dt = datetime.strptime(end, "%Y%m%d")
    except:
        end_dt = datetime.now()
        
    start_dt = end_dt - timedelta(days=1095) # 前推3年
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

    # 2. 获取 K线 行情数据与估值指标切片 (Baostock 前复权，长周期)
    fields = "date,open,high,low,close,volume,amount,turn,pctChg,peTTM,pbMRQ,psTTM"
    rs_k = bs.query_history_k_data_plus(bs_code, fields, start_date=long_beg_date, end_date=end_date_str, frequency="d", adjustflag="2")
    
    k_data_list = []
    while (rs_k.error_code == '0') & rs_k.next():
        k_data_list.append(rs_k.get_row_data())
        
    k_df = pd.DataFrame(k_data_list, columns=fields.split(','))
    
    # =========================================================================
    # 🚀 核心新增：工作日盘中实时行情修补验证 (防止重复数据)
    # =========================================================================
    today_str = now.strftime("%Y-%m-%d")
    current_time_str = now.strftime("%H:%M")
    latest_bs_date = k_df['date'].iloc[-1] if not k_df.empty else ""

    # 仅工作日上午9:30后、下午18:00前，且历史数据中尚未包含今天数据时调用
    if not is_weekend and "09:30" <= current_time_str <= "18:00" and latest_bs_date != today_str:
        try:
            print(f"⚡ 正在调用 SinaRealtime 接口获取 {stock_code} 今日行情...")
            fetcher = SinaRealtimeFetcher()
            spot_df = fetcher.fetch_snapshot([stock_code])

            if not spot_df.empty:
                spot_data = spot_df.iloc[0]
                latest_close = safe_float(spot_data['close'])
                spot_volume = safe_float(spot_data['vol']) # 直接取股数
                prev_close = safe_float(spot_data['prev_close'])
                
                # 手动计算涨跌幅
                pct_chg = 0.0
                if prev_close > 0:
                    pct_chg = (latest_close - prev_close) / prev_close * 100.0

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
    # =========================================================================

    # 格式化兼容计算函数
    k_df_calc = k_df.copy()
    k_df_calc = k_df_calc.rename(columns={'date': '日期', 'open': '开盘', 'high': '最高', 'low': '最低', 'close': '收盘', 'volume': '成交量'})
    for col in ['开盘', '最高', '最低', '收盘', '成交量', 'peTTM', 'pbMRQ']:
        k_df_calc[col] = pd.to_numeric(k_df_calc[col], errors='coerce')

    # 执行高级指标计算
    processed_data = calculate_advanced_indicators(k_df_calc)

    # 截取用户指定周期以内的数据进行保存
    req_beg_date = f"{beg[:4]}-{beg[4:6]}-{beg[6:]}"
    save_data = processed_data[processed_data['日期'] >= req_beg_date]

    # 保存文件
    safe_stock_name = re.sub(r'[\\/*?:"<>|]', '', stock_name)
    filename_data = f"{stock_code}_{safe_stock_name}_data_{current_date}.csv"
    filepath_data = os.path.join(data_dir, filename_data)
    save_data.to_csv(filepath_data, index=False)
    print(f"行情数据文件已保存至: {filepath_data}")

    # 获取最新一天的行情切片
    latest_row = processed_data.iloc[-1]
    
    latest_close = safe_float(latest_row['收盘'])
    turnover_rate = safe_float(k_df.iloc[-1]['turn'])
    pct_change = safe_float(k_df.iloc[-1]['pctChg'])
    volume_shares = safe_float(latest_row['成交量'])
    pe_ttm = safe_float(latest_row['peTTM'])
    pb_mrq = safe_float(latest_row['pbMRQ'])
    ps_ttm = safe_float(k_df.iloc[-1]['psTTM'])

    if turnover_rate > 0:
        market_cap = (volume_shares / (turnover_rate / 100)) * latest_close
    else:
        market_cap = 0.0

    # 3. 抓取新浪财务数据 & 同花顺机构预测
    try:
        fin_df = ak.stock_financial_abstract(symbol=stock_code)
        latest_report_date = fin_df.columns[2]
        def get_fin_metric(indicator_name):
            try:
                val = fin_df.loc[fin_df['指标'] == indicator_name, latest_report_date].values[0]
                return safe_float(val)
            except:
                return 0.0
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

    # ---------------- 动态生成策略信号 ----------------
    adx_val = safe_float(latest_row.get('adx'))
    p_di = safe_float(latest_row.get('+di'))
    m_di = safe_float(latest_row.get('-di'))
    trend_sig = "bullish" if (p_di > m_di and adx_val > 25) else ("bearish" if (m_di > p_di and adx_val > 25) else "neutral")
    trend_conf = min(int(adx_val * 2.5), 99) if not np.isnan(adx_val) else 0

    z_sc = safe_float(latest_row.get('z_score'))
    rsi14 = safe_float(latest_row.get('rsi_14'))
    mr_sig = "bullish" if (z_sc < -2 or rsi14 < 30) else ("bearish" if (z_sc > 2 or rsi14 > 70) else "neutral")
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
            "具体指标": {
                "ADX(平均趋向指数)": round(adx_val, 4), 
                "趋势强度": f"{adx_val:.2f}%"
            }
        },
        "均值回归": {
            "信号": sig_map.get(mr_sig, "中性"), "置信度": f"{mr_conf}%",
            "具体指标": {
                "Z-score": round(z_sc, 4),
                "布林带偏离度": f"{safe_float(latest_row.get('price_vs_bb')) * 100:.2f}%",
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
        # ---------------- 宏观大盘环境 ----------------
        "宏观大盘环境": macro_str,
        
        # ---------------- 基础与行情数据 ----------------
        "股票代码": stock_code,
        "股票名称": stock_name,
        "所处行业": industry,
        "最新财务报告期": latest_report_date,
        
        "最新价": latest_close,
        "涨跌幅": f"{pct_change:.2f}%",
        "换手率": f"{turnover_rate:.2f}%",
        "总市值": market_cap,

        # ---------------- 均线与趋势指标 ----------------
        "MA5": round(safe_float(latest_row.get('MA5')), 2),
        "MA10": round(safe_float(latest_row.get('MA10')), 2),
        "MA20": round(safe_float(latest_row.get('MA20')), 2),
        "MA60": round(safe_float(latest_row.get('MA60')), 2),
        "MA120": round(safe_float(latest_row.get('MA120')), 2),
        "MA200": round(safe_float(latest_row.get('MA200')), 2),
        "MACD": round(safe_float(latest_row.get('MACD')), 2),

        # ---------------- 周期极值 ----------------
        "近4周最高价": round(safe_float(latest_row.get('high_4w')), 2),
        "近4周最低价": round(safe_float(latest_row.get('low_4w')), 2),
        "近13周最高价": round(safe_float(latest_row.get('high_13w')), 2),
        "近13周最低价": round(safe_float(latest_row.get('low_13w')), 2),
        "近52周最高价": round(safe_float(latest_row.get('high_52w')), 2),
        "近52周最低价": round(safe_float(latest_row.get('low_52w')), 2),
        
        # ---------------- 高阶策略信号矩阵 ----------------
        "strategy_signals": json.dumps(strategy_signals, indent=2, ensure_ascii=False),

        # ---------------- 估值指标及历史分位 ----------------
        "滚动市盈率 P/E(TTM)": f"{pe_ttm:.2f}",
        "市盈率(PE)历史分位": f"{safe_float(latest_row.get('pe_rank')):.2f}%",
        "市净率 P/B": f"{pb_mrq:.2f}",
        "市净率(PB)历史分位": f"{safe_float(latest_row.get('pb_rank')):.2f}%",
        "市销率 P/S": f"{ps_ttm:.2f}",

        # ---------------- 盈利与收益质量 (新浪) ----------------
        "营业总收入(元)": get_fin_metric("营业总收入"),
        "净利润(元)": get_fin_metric("净利润"),
        "扣非净利润(元)": get_fin_metric("扣非净利润"),
        "毛利率": f"{get_fin_metric('毛利率'):.2f}%",
        "营业利润率": f"{get_fin_metric('营业利润率'):.2f}%",
        "销售净利率": f"{get_fin_metric('销售净利率'):.2f}%",
        "净资产收益率(ROE)": f"{get_fin_metric('净资产收益率(ROE)'):.2f}%",

        # ---------------- 成长能力 (新浪) ----------------
        "营业总收入增长率": f"{get_fin_metric('营业总收入增长率'):.2f}%",
        "净利润增长率": f"{get_fin_metric('归属母公司净利润增长率'):.2f}%",

        # ---------------- 财务风险与每股指标 (新浪) ----------------
        "资产负债率": f"{get_fin_metric('资产负债率'):.2f}%",
        "流动比率": f"{get_fin_metric('流动比率'):.2f}",
        "速动比率": f"{get_fin_metric('速动比率'):.2f}",
        "基本每股收益(元)": f"{get_fin_metric('基本每股收益'):.2f}",
        "每股净资产(元)": f"{get_fin_metric('每股净资产'):.2f}",
        "每股经营现金流(元)": f"{get_fin_metric('每股经营现金流'):.2f}",

        # ---------------- 营运能力 (新浪) ----------------
        "应收账款周转天数": f"{get_fin_metric('应收账款周转天数'):.1f}天",
        "存货周转天数": f"{get_fin_metric('存货周转天数'):.1f}天",
        "总资产周转率": f"{get_fin_metric('总资产周转率'):.3f}",
        
        # ---------------- 机构业绩预测 (同花顺) ----------------
        **forecast_metrics,
    }

    print("\n获取到的完整指标数据：")
    for key, value in all_metrics.items():
        if key in ["strategy_signals", "宏观大盘环境"]: continue
        print(f"{key}: {value}")
    
    print(f"\n【宏观大盘环境】\n{all_metrics['宏观大盘环境']}")
    print(f"\n【量化策略信号矩阵】\n{all_metrics['strategy_signals']}")

    # 生成传给大模型的最终 Prompt 文本
    result = []
    for key, value in all_metrics.items():
        if key == "strategy_signals":
            result.append(f"量化策略信号矩阵:\n{value}")
        elif key == "宏观大盘环境":
            result.append(f"======\n【宏观大盘环境】\n{value}\n======")
        else:
            if isinstance(value, np.float64): value = value.item()
            result.append(f"{key}: {value}")

    text_input = "\n".join(result)
    
    bs.logout()
    
    # 周末保存文本缓存，避免同日多次执行或不同脚本调用时重复走 API
    if is_weekend:
        with open(cache_file_path, 'w', encoding='utf-8') as f:
            f.write(text_input)
            
    return text_input