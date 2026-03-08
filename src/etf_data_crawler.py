# -*- coding: utf-8 -*-
import os
import re
import numpy as np
import pandas as pd
import time
from datetime import datetime, timedelta

import akshare as ak
from mootdx.quotes import Quotes
from mootdx.contrib.adjust import get_adjust_year

# =========================================================================
# 第一部分：辅助与技术指标计算模块
# =========================================================================

def get_etf_market_prefix(symbol: str) -> str:
    """根据 ETF 代码生成新浪接口需要的带市场前缀的代码"""
    if symbol.startswith('5'):
        return f"sh{symbol}"
    elif symbol.startswith('1'):
        return f"sz{symbol}"
    return symbol

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
    """计算高级技术指标与量化信号"""
    df = df.sort_values('日期').reset_index(drop=True)
    
    # 基础收益率
    df['daily_return'] = df['收盘'].pct_change()
    
    # 极值与分位点计算
    df['high_4w'] = df['最高'].rolling(20).max()
    df['low_4w'] = df['最低'].rolling(20).min()
    df['high_52w'] = df['最高'].rolling(250).max()
    df['low_52w'] = df['最低'].rolling(250).min()

    # 动量计算
    df['momentum_1m'] = df['收盘'].pct_change(periods=20)
    df['momentum_3m'] = df['收盘'].pct_change(periods=60)
    df['volume_ma5'] = df['成交量'].rolling(5).mean()
    df['volume_ma20'] = df['成交量'].rolling(20).mean()
    df['volume_momentum'] = df['volume_ma5'] / df['volume_ma20'].replace(0, np.nan)
    
    # 基础移动平均
    df['MA5'] = df['收盘'].rolling(window=5).mean()
    df['MA20'] = df['收盘'].rolling(window=20).mean()
    df['MA60'] = df['收盘'].rolling(window=60).mean()

    # MACD
    df['EMA12'] = df['收盘'].ewm(span=12, adjust=False).mean()
    df['EMA26'] = df['收盘'].ewm(span=26, adjust=False).mean()
    df['DIF'] = df['EMA12'] - df['EMA26']
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD'] = 2 * (df['DIF'] - df['DEA'])

    # 趋势跟随 (Trend Following - ADX)
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

    # 均值回归 (Mean Reversion - Bollinger & RSI)
    df['std20'] = df['收盘'].rolling(20).std()
    df['upper_bb'] = df['MA20'] + 2 * df['std20']
    df['lower_bb'] = df['MA20'] - 2 * df['std20']
    df['z_score'] = (df['收盘'] - df['MA20']) / df['std20'].replace(0, np.nan)
    
    delta = df['收盘'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs14 = gain / loss.replace(0, np.nan)
    df['rsi_14'] = 100 - (100 / (1 + rs14))

    # 波动率与统计套利
    df['historical_volatility'] = df['daily_return'].rolling(20).std() * np.sqrt(252)
    df['hurst_exponent'] = df['收盘'].rolling(120).apply(calculate_hurst, raw=False)
    
    return df

# =========================================================================
# 第二部分：Mootdx F10 文本与表格精细化解析模块
# =========================================================================

def parse_ascii_table(text: str) -> pd.DataFrame:
    """解析通达信 F10 中的 ASCII 表格为 Pandas DataFrame，并自动修复折行问题"""
    if not text:
        return pd.DataFrame()
        
    lines = text.strip().split('\n')
    data = []
    
    for line in lines:
        line = line.strip()
        if not line.startswith('│'):
            continue
            
        row = [cell.strip() for cell in line.split('│')[1:-1]]
        if row:
            # 修复列宽不够导致的自动换行问题
            if len(data) > 0 and row[0] == '' and any(c != '' for c in row):
                for i in range(min(len(row), len(data[-1]))):
                    data[-1][i] += row[i]
            else:
                data.append(row)
                
    if not data or len(data) < 2:
        return pd.DataFrame()
        
    columns = data[0]
    df = pd.DataFrame(data[1:], columns=columns)
    return df

def extract_latest_table(text_block: str) -> str:
    """使用正则提取文本块中的第一个完整表格（即最新一期的表格）"""
    match = re.search(r'(┌.*?└[─┴]+┘)', text_block, re.S)
    if match:
        return match.group(1)
    return ""

def get_section_text(text: str, section_name: str) -> str:
    """精准抽取章节文本，防穿透防目录干扰"""
    pattern = r'(?:^|\r?\n)【' + re.escape(section_name) + r'】(.*?)(?=\r?\n【|\r?\n〖|$)'
    match = re.search(pattern, text, re.S)
    if match:
        return match.group(1)
    return ""

def get_etf_f10_dataframes(etf_code: str) -> dict:
    """获取 ETF 的核心数据，并直接解析为结构化的 DataFrame 和 Dict"""
    print(f"📡 正在获取并解析 {etf_code} 的 Mootdx F10 数据...")
    client = Quotes.factory(market='std')
    f10_data = client.F10(symbol=etf_code, name='最新提示')
    
    result = {}
    try:
        if not isinstance(f10_data, dict):
            return result

        # 1. 提取【基金简况】
        if '最新动态' in f10_data:
            sec_text = get_section_text(f10_data['最新动态'], '1.基金简况')
            if sec_text:
                profile_dict = {}
                for line in sec_text.strip().split('\n'):
                    if ':' in line or '：' in line:
                        line = line.replace('：', ':')
                        parts = line.split(':', 1)
                        if len(parts) == 2:
                            profile_dict[parts[0].strip()] = parts[1].strip()
                result['基金简况'] = profile_dict

        # 2. 提取【基金名称】
        if '基金概况' in f10_data:
            match = re.search(r'│\s*基金名称\s*│([^│]+)│', f10_data['基金概况'])
            if match:
                result['基金名称'] = match.group(1).strip()

        # 3. 提取【份额与持有人相关】
        if '基金份额' in f10_data:
            text = f10_data['基金份额']
            result['基金份额'] = parse_ascii_table(extract_latest_table(get_section_text(text, '1.基金份额')))
            result['场内份额变动'] = parse_ascii_table(extract_latest_table(get_section_text(text, '2.场内份额变动')))
            result['持有人结构'] = parse_ascii_table(extract_latest_table(get_section_text(text, '3.持有人户数及结构')))
            result['十大持有人'] = parse_ascii_table(extract_latest_table(get_section_text(text, '4.基金十大持有人')))

        # 4. 提取【资产配置】
        if '投资组合' in f10_data:
            result['资产配置'] = parse_ascii_table(extract_latest_table(get_section_text(f10_data['投资组合'], '1.资产配置')))

        # 5. 提取【最新持股明细】
        if '持股情况' in f10_data:
            result['持股明细'] = parse_ascii_table(extract_latest_table(get_section_text(f10_data['持股情况'], '1.持股明细')))

        # 6. 提取【最新行业分析】
        if '行业分析' in f10_data:
            result['行业分析'] = parse_ascii_table(extract_latest_table(f10_data['行业分析']))

    except Exception as e:
        print(f"❌ F10 解析过程中出现异常: {e}")
        
    return result

# =========================================================================
# 第三部分：数据组装，对外提供核心 Context 接口
# =========================================================================

def get_etf_data_context(etf_code: str, beg: str, end: str, current_date: str) -> str:
    """
    聚合 ETF 所有相关数据，生成供 LLM 使用的高质量上下文文本
    """
    data_dir = f"log/etf_data/{current_date}"
    os.makedirs(data_dir, exist_ok=True)
    
    # 1. 获取 F10 数据 (字典及DataFrame集合)
    f10_dfs = get_etf_f10_dataframes(etf_code)
    
    # 2. 获取并计算历史行情
    print(f"📈 正在获取 {etf_code} 的历史行情并计算指标...")
    
    # 提前赋默认值占位，彻底杜绝 UnboundLocalError 报错崩溃
    daily_kline_str = "行情数据获取失败，暂无日线数据"
    monthly_kline_str = "行情数据获取失败，暂无月线数据"
    latest_row = pd.Series(dtype=float) 
    
    try:
        # 自动推算年份跨度
        try:
            end_dt = datetime.strptime(end, "%Y%m%d")
        except:
            end_dt = pd.to_datetime(end)
            
        end_year = end_dt.year
        start_year = end_year - 3 # 前推三年保证长周期指标（如250日均线）有足够数据

        dfs = []
        # 按年份循环获取前复权数据，采用健壮的 index 处理与防封延时
        for y in range(start_year, end_year + 1):
            try:
                temp_df = get_adjust_year(symbol=etf_code, year=str(y), factor='01')
                if temp_df is not None and not temp_df.empty:
                    if 'date' not in temp_df.columns and temp_df.index.name == 'date': 
                        temp_df = temp_df.reset_index()
                    elif 'date' not in temp_df.columns: 
                        temp_df = temp_df.reset_index(names='date')
                    dfs.append(temp_df)
                time.sleep(0.3)  # 增加延时，防止连续高频请求被断连
            except Exception as e:
                print(f"⚠️ 获取 {etf_code} {y}年复权数据失败: {e}")

        df_hist = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        if df_hist.empty:
            raise ValueError(f"未能从 mootdx 获取到 {etf_code} 的复权行情数据")

        # ==========================================================
        # 🌟 核心修复：强制转换数据类型
        # mootdx 默认返回的开高低收是字符串(object)类型，必须转为 float
        # ==========================================================
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'adjust']
        for col in numeric_cols:
            if col in df_hist.columns:
                df_hist[col] = pd.to_numeric(df_hist[col], errors='coerce')

        # 统一转为 datetime 进行后续处理
        if 'date' in df_hist.columns:
            df_hist['date'] = pd.to_datetime(df_hist['date'])

        # 规范化列名以对接算法引擎
        df_hist = df_hist.rename(columns={
            'date': '日期', 'open': '开盘', 'high': '最高', 
            'low': '最低', 'close': '收盘', 'volume': '成交量'
        })
        
        # 强制格式化日期并去重排序，确保首尾相接的年份数据没有毛刺
        df_hist['日期'] = df_hist['日期'].dt.strftime('%Y-%m-%d')
        df_hist = df_hist.sort_values('日期').drop_duplicates(subset=['日期']).reset_index(drop=True)
        
        # 执行高级指标计算 (现在传入的是纯净的 Float 数字，不会再崩溃了)
        processed_data = calculate_advanced_indicators(df_hist)
        
        # 截取所需时间段保存为 CSV 供复盘使用
        req_beg_date = f"{beg[:4]}-{beg[4:6]}-{beg[6:]}"
        save_data = processed_data[processed_data['日期'] >= req_beg_date].copy()
        filepath_data = os.path.join(data_dir, f"{etf_code}_indicators_{current_date}.csv")
        save_data.to_csv(filepath_data, index=False)
        print(f"✅ ETF 前复权指标数据已保存至: {filepath_data}")
        
        # 提取近 20 个交易日供 Prompt 使用
        recent_20 = processed_data.tail(20)[['日期', '开盘', '最高', '最低', '收盘', '成交量', 'daily_return', 'MA20', 'MACD', 'rsi_14']].copy()
        recent_20['日期'] = recent_20['日期'].astype(str)
        recent_20 = recent_20.round(4)
        daily_kline_str = recent_20.to_markdown(index=False)

        # 合成近 20 个月 K 线供 Prompt 使用
        df_monthly_tmp = processed_data.copy()
        df_monthly_tmp['日期'] = pd.to_datetime(df_monthly_tmp['日期'])
        df_monthly_tmp['year_month'] = df_monthly_tmp['日期'].dt.to_period('M')
        
        # 按月分组聚合
        df_monthly = df_monthly_tmp.groupby('year_month').agg({
            '开盘': 'first',   
            '最高': 'max',     
            '最低': 'min',     
            '收盘': 'last',    
            '成交量': 'sum',   
            'daily_return': lambda x: (1 + x).prod() - 1, 
            'MA20': 'last',    
            'MACD': 'last',    
            'rsi_14': 'last'   
        }).reset_index()
        
        df_monthly.rename(columns={'year_month': '日期', 'daily_return': 'monthly_return'}, inplace=True)
        df_monthly['日期'] = df_monthly['日期'].astype(str)
        
        recent_20_monthly = df_monthly.tail(20).round(4)
        monthly_kline_str = recent_20_monthly.to_markdown(index=False)

        latest_row = processed_data.iloc[-1]
        
    except Exception as e:
        print(f"❌ 行情数据处理失败: {e}")

    # ================= 3. 构建最终发给 LLM 的 Context =================
    context_blocks = []
    
    # [模块 A] 基础概况
    context_blocks.append(f"====== 【1. {etf_code} 基金概况】 ======")
    context_blocks.append(f"基金名称: {f10_dfs.get('基金名称', '未知')}")
    profile = f10_dfs.get('基金简况', {})
    if profile:
        for k, v in profile.items():
            context_blocks.append(f"{k}: {v}")

    # [模块 B] 份额与动向
    df_change = f10_dfs.get('场内份额变动')
    if isinstance(df_change, pd.DataFrame) and not df_change.empty:
        context_blocks.append("\n====== 【2. 近期场内份额变动 (前10天)】 ======")
        context_blocks.append(df_change.head(10).to_markdown(index=False))
        
    # [模块 C] 重仓与行业
    df_industry = f10_dfs.get('行业分析')
    if isinstance(df_industry, pd.DataFrame) and not df_industry.empty:
        context_blocks.append("\n====== 【3. 行业分布配置】 ======")
        context_blocks.append(df_industry.head(10).to_markdown(index=False))

    df_holdings = f10_dfs.get('持股明细')
    if isinstance(df_holdings, pd.DataFrame) and not df_holdings.empty:
        context_blocks.append("\n====== 【4. 最新重仓股明细 (前15名)】 ======")
        context_blocks.append(df_holdings.head(15).to_markdown(index=False))
        
    # [模块 D] 核心量价数据
    context_blocks.append("\n====== 【5. 核心量价数据 (日K与月K)】 ======")
    context_blocks.append("--- 近 20 个交易日数据 ---")
    context_blocks.append(daily_kline_str)
    context_blocks.append("\n--- 近 20 个月K线数据 ---")
    context_blocks.append(monthly_kline_str)
    
    # [模块 E] 策略信号切片
    if not latest_row.empty:
        context_blocks.append("\n====== 【6. 最新交易日量化信号】 ======")
        context_blocks.append(f"趋势强度(ADX): {safe_float(latest_row.get('adx')):.2f} (大于25代表存在明确趋势)")
        context_blocks.append(f"情绪指标(RSI14): {safe_float(latest_row.get('rsi_14')):.2f} (通常>70超买，<30超卖)")
        context_blocks.append(f"均线偏离(Z-Score): {safe_float(latest_row.get('z_score')):.2f} (偏离20日均线的标准差，绝对值>2极度偏离)")
        context_blocks.append(f"赫斯特指数: {safe_float(latest_row.get('hurst_exponent')):.2f} (<0.5有均值回归倾向，>0.5有趋势延续倾向)")

    return "\n".join(context_blocks)

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