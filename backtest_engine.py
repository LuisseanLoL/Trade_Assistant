# -*- coding: utf-8 -*-
import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import baostock as bs

class AITradeBacktester:
    def __init__(self, output_dir="output", result_dir="log/backtest_results"):
        self.output_dir = output_dir
        self.result_dir = result_dir
        os.makedirs(self.result_dir, exist_ok=True)
        self.predictions_df = pd.DataFrame()
        self.market_data = {}

    def _format_bs_code(self, symbol):
        """转换股票代码为 baostock 格式"""
        symbol = str(symbol).strip()
        if symbol.startswith('6'): return f"sh.{symbol}"
        elif symbol.startswith(('0', '3')): return f"sz.{symbol}"
        elif symbol.startswith(('8', '4')): return f"bj.{symbol}"
        return symbol

    def load_historical_predictions(self, days_ago=None):
        """加载历史预测数据"""
        print(f"📂 正在扫描 {self.output_dir} 目录下的历史策略表...")
        all_files = glob.glob(os.path.join(self.output_dir, "*", "Daily Table_*.csv"))
        
        df_list = []
        for file in all_files:
            try:
                # 从路径提取预测日期
                date_str = os.path.basename(os.path.dirname(file))
                pred_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                
                # 如果设置了天数过滤
                if days_ago is not None:
                    if (datetime.now().date() - pred_date).days > days_ago:
                        continue
                
                df = pd.read_csv(file, dtype={'股票代码': str})
                df['预测日期'] = pred_date
                df_list.append(df)
            except Exception as e:
                print(f"⚠️ 读取 {file} 失败: {e}")

        if not df_list:
            print("❌ 未找到任何历史预测数据。")
            return False

        self.predictions_df = pd.concat(df_list, ignore_index=True)
        
        # 数据清洗：处理置信度、价格等字段
        self.predictions_df['置信度(%)'] = self.predictions_df['置信度'].str.replace('%', '').astype(float).fillna(0)
        for col in ['当前价格', '建议买入价', '目标卖出价', '建议止损价']:
            self.predictions_df[col] = pd.to_numeric(self.predictions_df[col].replace('-', np.nan), errors='coerce')
        
        print(f"✅ 成功加载 {len(self.predictions_df)} 条历史预测记录。")
        return True

    def fetch_market_data(self):
        """批量获取回测所需的真实市场行情"""
        
        # 🚀 核心优化：只筛选出大模型给出了“买入”指令的记录
        buy_predictions = self.predictions_df[self.predictions_df['操作'] == '买入']
        
        unique_stocks = buy_predictions['股票代码'].dropna().unique()
        total_stocks = len(unique_stocks)

        if total_stocks == 0:
            print("ℹ️ 历史记录中没有找到任何【买入】指令，无需拉取行情。")
            return

        # 日期也只根据有买入指令的记录来计算，进一步节省数据量
        min_date = buy_predictions['预测日期'].min()
        start_date = min_date.strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")

        print(f"📈 正在向 Baostock 请求 {total_stocks} 只触发了【买入】指令的股票真实行情 ({start_date} 至今)...")
        bs.login()
        
        for i, code in enumerate(unique_stocks, 1):
            # 动态打印进度，不刷屏
            print(f"\r⏳ 获取进度: [{i}/{total_stocks}] ({(i/total_stocks)*100:.1f}%) - 当前代码: {code}", end="", flush=True)
            
            bs_code = self._format_bs_code(code)
            rs = bs.query_history_k_data_plus(
                bs_code, "date,open,high,low,close", 
                start_date=start_date, end_date=end_date, frequency="d", adjustflag="2"
            )
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                data_list.append(rs.get_row_data())
            
            if data_list:
                df = pd.DataFrame(data_list, columns=rs.fields)
                for c in ['open', 'high', 'low', 'close']:
                    df[c] = df[c].astype(float)
                df['date'] = pd.to_datetime(df['date']).dt.date
                self.market_data[str(code).strip()] = df.sort_values('date').reset_index(drop=True)
                
        bs.logout()
        print("\n✅ 行情数据获取完成。")

    def run_simulation(self):
        """执行回测模拟与归因分析"""
        print("⚙️ 正在执行买卖点触碰模拟与收益归因...")
        results = []

        for _, row in self.predictions_df.iterrows():
            code = str(row['股票代码']).strip()
            pred_date = row['预测日期']
            
            # 如果是观望或持有，跳过严格回测，只记录实际涨跌
            if row['操作'] not in ['买入', '卖出']:
                continue

            if code not in self.market_data:
                continue
                
            df_k = self.market_data[code]
            # 获取预测日之后的未来行情
            future_k = df_k[df_k['date'] > pred_date].copy()
            
            if future_k.empty:
                continue # 没有未来数据（可能是今天刚预测的）

            buy_p = row['建议买入价']
            sell_p = row['目标卖出价']
            stop_p = row['建议止损价']
            
            # 如果没有建议买入价，假设次日开盘无脑市价买入
            if pd.isna(buy_p): buy_p = future_k.iloc[0]['open']

            # 模拟状态机
            is_bought = False
            buy_date = None
            actual_buy_price = 0.0
            
            is_sold = False
            sell_date = None
            sell_reason = ""
            actual_sell_price = 0.0
            
            highest_after_buy = 0.0
            lowest_after_buy = float('inf')

            for _, bar in future_k.iterrows():
                # 1. 寻找买点
                if not is_bought:
                    if bar['low'] <= buy_p: # 触碰买点
                        is_bought = True
                        buy_date = bar['date']
                        # 假设滑点，以设定的买入价或开盘价中较低者成交
                        actual_buy_price = min(buy_p, bar['open']) 
                        highest_after_buy = actual_buy_price
                        lowest_after_buy = actual_buy_price
                
                # 2. 持仓中寻找卖点或止损点
                else:
                    highest_after_buy = max(highest_after_buy, bar['high'])
                    lowest_after_buy = min(lowest_after_buy, bar['low'])
                    
                    # 优先检查止损 (防御优先原则)
                    if pd.notna(stop_p) and bar['low'] <= stop_p:
                        is_sold = True
                        sell_date = bar['date']
                        actual_sell_price = min(stop_p, bar['open'])
                        sell_reason = "止损离场"
                        break
                        
                    # 检查止盈
                    elif pd.notna(sell_p) and bar['high'] >= sell_p:
                        is_sold = True
                        sell_date = bar['date']
                        actual_sell_price = max(sell_p, bar['open'])
                        sell_reason = "止盈达标"
                        break

            # 如果持有到当前仍未卖出，以最后一天收盘价计算浮盈
            if is_bought and not is_sold:
                actual_sell_price = future_k.iloc[-1]['close']
                sell_reason = "持有至今浮盈/亏"

            # 计算收益率
            pnl_pct = 0.0
            if is_bought:
                pnl_pct = (actual_sell_price - actual_buy_price) / actual_buy_price * 100

            # 评估 Case 质量
            case_tag = "Normal"
            if row['操作'] == '买入':
                if is_sold and sell_reason == "止盈达标":
                    case_tag = "Good Case (精准命中)"
                elif pnl_pct > 5.0:
                    case_tag = "Good Case (超额收益)"
                elif sell_reason == "止损离场" or pnl_pct < -5.0:
                    case_tag = "Bad Case (踩雷/破位)"
                elif not is_bought:
                    case_tag = "Missed (未到买点)"

            results.append({
                '预测日期': pred_date,
                '股票代码': code,
                '股票名称': row['股票名称'],
                '模型': row['决策模型'],
                '预期': row['预期'],
                '置信度(%)': row['置信度(%)'],
                '是否买入': is_bought,
                '买入日期': buy_date,
                '买入价': round(actual_buy_price, 2) if is_bought else np.nan,
                '最高冲至': round(highest_after_buy, 2) if is_bought else np.nan,
                '卖出/最新日期': sell_date if is_sold else (future_k.iloc[-1]['date'] if is_bought else None),
                '离场价格': round(actual_sell_price, 2) if is_bought else np.nan,
                '离场原因': sell_reason,
                '实际收益率(%)': round(pnl_pct, 2) if is_bought else 0.0,
                'AI原判原因': row.get('原因', '无记录'),
                'Case判定': case_tag
            })

        self.results_df = pd.DataFrame(results)
        print("✅ 模拟回测完成！")

    def generate_report(self):
        """生成归因统计报告"""
        if self.results_df.empty:
            print("无有效回测结果可生成报告。")
            return

        # 导出详细明细
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        detail_path = os.path.join(self.result_dir, f"backtest_details_{timestamp}.csv")
        self.results_df.to_csv(detail_path, index=False, encoding='utf-8-sig')

        # 计算统计学指标
        total_trades = self.results_df['是否买入'].sum()
        win_trades = len(self.results_df[self.results_df['实际收益率(%)'] > 0])
        win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0
        
        avg_profit = self.results_df[self.results_df['实际收益率(%)'] > 0]['实际收益率(%)'].mean()
        avg_loss = self.results_df[self.results_df['实际收益率(%)'] < 0]['实际收益率(%)'].mean()
        
        bad_cases = self.results_df[self.results_df['Case判定'].str.contains('Bad Case')]

        print("\n==================================================")
        print("🏆 AI Trade Assistant 自动化归因与回测报告")
        print("==================================================")
        print(f"参与回测的买入建议总数: {len(self.results_df)}")
        print(f"实际触发买入的交易总数: {total_trades}")
        print(f"整体胜率 (Win Rate): {win_rate:.2f}%")
        print(f"平均盈利幅度: +{avg_profit:.2f}% | 平均亏损幅度: {avg_loss:.2f}%")
        if avg_loss != 0 and not pd.isna(avg_loss):
            print(f"盈亏比 (P/L Ratio): {abs(avg_profit / avg_loss):.2f}")
        print("--------------------------------------------------")
        
        # 按照模型/流派进行胜率切片统计
        if '模型' in self.results_df.columns:
            print("🤖 各决策模型表现分布:")
            model_stats = self.results_df[self.results_df['是否买入']].groupby('模型').agg(
                交易次数=('实际收益率(%)', 'count'),
                平均收益=('实际收益率(%)', 'mean'),
                胜率=('实际收益率(%)', lambda x: sum(x > 0) / len(x) * 100)
            ).round(2)
            print(model_stats)
            print("--------------------------------------------------")

        print(f"🚨 发现 {len(bad_cases)} 个 Bad Case (止损或深度亏损)！")
        print(f"已将详细比对数据保存至: {detail_path}")
        print("💡 建议：打开生成的 csv，提取 Bad Case 的【AI原判原因】，作为提示词丢给 AI 裁判，让其总结系统性盲区。")
        print("==================================================")


if __name__ == "__main__":
    tester = AITradeBacktester()
    
    # 1. 加载最近 30 天的预测记录 (你可以改为 None 加载全部)
    if tester.load_historical_predictions(days_ago=30):
        # 2. 抓取所需股票的 K 线
        tester.fetch_market_data()
        # 3. 时间序列仿真
        tester.run_simulation()
        # 4. 生成报告
        tester.generate_report()