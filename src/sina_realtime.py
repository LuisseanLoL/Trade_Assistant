# -*- coding: utf-8 -*-
"""
新浪实时行情接口
对应原脚本: sina_realtimequote_api.py
功能: 获取个股实时快照 (Snapshot)，包含买一卖一、最新价、成交量等
"""

import requests
import pandas as pd
import datetime
import logging
import sys
from typing import List
from pathlib import Path

# 🚑 路径补丁 (方便单独运行测试)
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SinaRealtimeFetcher:
    def __init__(self, timeout: int = 3):
        self.base_url = "http://hq.sinajs.cn/list="
        self.headers = {'Referer': 'http://finance.sina.com.cn'}
        self.timeout = timeout
        self.max_chunk_size = 80  # URL长度有限制，分批请求

    def _add_exchange_prefix(self, code: str) -> str:
        """
        内部工具: 为代码添加交易所前缀 (sh/sz/bj)
        逻辑严谨复刻原脚本，适配 A 股及北交所
        """
        code = str(code).strip()
        # 如果已经有前缀，先去掉再判断 (防止 shsh600000)
        if code.startswith(('sh', 'sz', 'bj')):
            return code
            
        if code.startswith('6'):
            return f"sh{code}"
        elif code.startswith(('8', '4')):
            return f"bj{code}"
        else:
            return f"sz{code}"

    def fetch_snapshot(self, code_list: List[str]) -> pd.DataFrame:
        """
        获取多只股票的实时快照
        :param code_list: 股票代码列表，如 ['600519', '000001']
        :return: DataFrame
        """
        if not code_list:
            return pd.DataFrame()

        # 1. 代码预处理
        sina_codes = [self._add_exchange_prefix(c) for c in code_list]
        total_count = len(sina_codes)
        
        logger.info(f"正在请求 {total_count} 只股票实时行情...")
        
        all_data = []
        
        # 2. 分批请求 (Chunking)
        for i in range(0, total_count, self.max_chunk_size):
            chunk = sina_codes[i : i + self.max_chunk_size]
            query_url = self.base_url + ",".join(chunk)
            
            try:
                resp = requests.get(query_url, headers=self.headers, timeout=self.timeout)
                # 新浪接口必须使用 GBK 解码
                content = resp.content.decode('gbk', errors='ignore').strip()
                lines = content.split('\n')
                
                for line in lines:
                    if '=""' in line or not line: 
                        continue 
                    
                    # 解析格式: var hq_str_sh600519="贵州茅台,..."
                    eq_idx = line.find('=')
                    if eq_idx == -1: 
                        continue
                    
                    # 提取代码: var hq_str_sh600519 -> sh600519
                    # line[0:eq_idx] 是 var hq_str_sh600519
                    # 我们可以安全地取 eq_idx 之前的部分，并去掉 "var hq_str_" (长度11)
                    # 剩下的就是 sh600519
                    full_code_str = line[11:eq_idx] 
                    stock_code = full_code_str[2:]  # 去掉 sh/sz/bj，保留纯数字代码 600519
                    
                    # 提取数据内容
                    data_str = line[eq_idx+2 : -2] # 去掉 =" 和 ";
                    fields = data_str.split(',')
                    
                    # 校验字段长度 (标准长度通常为 32 或 33)
                    if len(fields) < 30: 
                        continue 

                    # 3. 字段解析
                    # fields[1]: open, [2]: prev_close, [3]: close, [4]: high, [5]: low
                    open_price = float(fields[1])
                    current_price = float(fields[3])
                    
                    # 过滤: 停牌或未开盘的无效数据 (根据原脚本逻辑，开盘价<=0 则跳过)
                    if open_price <= 0: 
                        continue 

                    stock_info = {
                        'code': stock_code,
                        'name': fields[0],
                        'open': open_price,
                        'prev_close': float(fields[2]),
                        'close': current_price,
                        'high': float(fields[4]),
                        'low': float(fields[5]),
                        'buy1': float(fields[6]),
                        'sell1': float(fields[7]),
                        'vol': float(fields[8]),    # 成交量 (股)
                        'amount': float(fields[9]), # 成交额 (元)
                        'date': fields[30],         # API 返回的日期 (YYYY-MM-DD)
                        'time': fields[31]          # API 返回的时间 (HH:MM:SS)
                    }
                    all_data.append(stock_info)
                    
            except Exception as e:
                logger.error(f"⚠️ 请求分片 {i} 失败: {e}")
                continue
    
        # 4. 构建 DataFrame
        if not all_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(all_data)
        
        # 类型转换
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
        # 确保列顺序符合规范
        cols = ['code', 'name', 'date', 'time', 'open', 'high', 'low', 'close', 'prev_close', 'vol', 'amount', 'buy1', 'sell1']
        final_cols = [c for c in cols if c in df.columns]
        
        return df[final_cols]

# ==========================================
# 测试代码
# ==========================================
if __name__ == "__main__":
    fetcher = SinaRealtimeFetcher()
    
    # 测试代码: 茅台(沪), 平安(深), 北交所测试
    test_codes = ['600519', '000001', '838275']
    print(f"正在获取 {test_codes} 的实时行情...")
    
    df = fetcher.fetch_snapshot(test_codes)
    
    if not df.empty:
        print("\n✅ 抓取成功:")
        # 打印时防止列名对不齐，转为 string
        print(df.to_string())
        print("\n数据类型:")
        print(df.dtypes)
    else:
        print("❌ 未获取到数据 (可能是非交易时间或网络问题)")