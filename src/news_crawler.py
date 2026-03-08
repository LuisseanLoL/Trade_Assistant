import os
import re
from bs4 import BeautifulSoup
import akshare as ak
from datetime import datetime, timedelta

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("⚠️ 缺少 Playwright 库，请先运行: pip install playwright && playwright install chromium")


def get_macro_news(current_date_str):
    """获取宏观财经早餐摘要，每日缓存一次，周末自动使用周五数据"""
    macro_dir = "log/macro_news"
    os.makedirs(macro_dir, exist_ok=True)
    
    # 1. 智能日期处理（自动处理周末回溯）
    try:
        date_str = str(current_date_str)
        # 兼容 YYYY-MM-DD 和 YYYYMMDD 格式
        if '-' in date_str:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        else:
            dt = datetime.strptime(date_str, "%Y%m%d")
            
        if dt.weekday() == 5: # 周六，回溯到周五
            effective_dt = dt - timedelta(days=1)
        elif dt.weekday() == 6: # 周日，回溯到周五
            effective_dt = dt - timedelta(days=2)
        else:
            effective_dt = dt
            
        effective_date_str = effective_dt.strftime("%Y-%m-%d")
    except:
        effective_date_str = str(current_date_str)
        
    cache_file = os.path.join(macro_dir, f"macro_news_{effective_date_str}.txt")
    
    # 2. 检查缓存，避免重复拉取
    if os.path.exists(cache_file):
        with open(cache_file, 'r', encoding='utf-8') as f:
            return f.read()
            
    # 3. 缓存不存在，调用接口拉取
    try:
        print(f"🌍 正在获取 {effective_date_str} 宏观财经早餐...")
        df = ak.stock_info_cjzc_em()
        if not df.empty:
            # 提取近 5 条摘要
            summaries = df['摘要'].head(5).tolist()
            macro_text = "【宏观财经早餐】\n" + "\n".join(summaries)
            
            # 保存至缓存文件
            with open(cache_file, 'w', encoding='utf-8') as f:
                f.write(macro_text)
            return macro_text
    except Exception as e:
        print(f"⚠️ 获取宏观财经早餐失败: {e}")
        
    return ""


def get_news_titles(symbol="600000", stock_name='浦发银行', max_news=20, save_txt=True, current_date='20250214'):
    """
    获取并处理新闻标题 (使用 Playwright 无头浏览器绕过反爬)
    参数:
        symbol: 股票代码/指数名称
        stock_name: 股票名称
        max_news: 最大新闻数量
        save_txt: 是否保存为文本文件
        current_date: 当前日期，用于日志路径
    返回:
        包含标题和宏观摘要的字符串
    """
    url = f"https://so.eastmoney.com/news/s?keyword={symbol}&type=content&sort=time"
    titles = []

    try:
        print(f'🚀 开始获取 {symbol} ({stock_name}) 的新闻标题...')
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080}
            )
            page = context.new_page()
            
            # 隐藏 webdriver 特征
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=45000)
            except Exception as e:
                print(f"页面加载超时提示(可忽略): {e}")
            
            while len(titles) < max_news:
                try:
                    page.wait_for_selector('.news_item', timeout=15000)
                except Exception:
                    print("未找到新闻列表(.news_item)，可能无数据或被高级反爬拦截。")
                    break
                    
                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                items = soup.select('.news_item')
                
                if not items:
                    break
                    
                new_titles_found = False
                for item in items:
                    if len(titles) >= max_news:
                        break
                        
                    a_tag = item.select_one('.news_item_t a') or item.find('a')
                    if not a_tag:
                        continue
                        
                    title = a_tag.get_text(strip=True)
                    
                    # 🔴 新增：获取日期
                    # 获取该新闻块的所有文本，并用空格分隔
                    item_text = item.get_text(separator=' ', strip=True)
                    # 使用正则匹配类似 "2026-03-06 18:25:35" 的时间戳
                    date_match = re.search(r'\d{4}-\d{2}-\d{2}', item_text)
                    date_str = date_match.group(0) if date_match else "未知日期"
                    
                    if len(title) > 4:
                        # 将日期和标题拼接，例如："[2026-03-06 18:25:35] A股史上第一个..."
                        formatted_item = f"[{date_str}] {title}"
                        
                        # 检查排重（这里只用标题排重比较好，防止同一条新闻更新了时间戳）
                        if not any(title in existing_item for existing_item in titles):
                            titles.append(formatted_item)
                            new_titles_found = True
                        
                if len(titles) >= max_news:
                    break
                
                if not new_titles_found:
                    break
                    
                next_btn_1 = page.locator("a:text-is('>')").first
                next_btn_2 = page.locator("a:text-is('下一页')").first
                
                if next_btn_1.is_visible():
                    next_btn_1.click()
                elif next_btn_2.is_visible():
                    next_btn_2.click()
                else:
                    print("没有更多新闻页面了。")
                    break
                    
                page.wait_for_timeout(2000)

            browser.close()

    except Exception as e:
        print(f"抓取操作失败: {e}")

    # 截取所需数量并生成个股新闻文本
    text_content = '\n'.join(titles[:max_news]) if titles else ""
    
    if not text_content:
        print(f"未获取到 {symbol} 的个股新闻数据")

    # --- 核心新增：追加宏观财经早餐 ---
    macro_news_text = get_macro_news(current_date)
    if macro_news_text:
        if text_content:
            text_content = text_content + "\n\n" + macro_news_text
        else:
            text_content = macro_news_text

    # 创建存储目录
    news_dir = f"log/stock_news/{current_date}"
    os.makedirs(news_dir, exist_ok=True)
    
    # 保存文件
    if save_txt and text_content:
        filename = f"{symbol}_{stock_name}_News_{current_date}.txt"
        filepath = os.path.join(news_dir, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(text_content)
            print(f"✅ 新闻与宏观摘要已保存至: {filepath}")
        except Exception as e:
            print(f"保存文件失败: {e}")
        
    return text_content


# 测试代码
if __name__ == "__main__":
    # 使用今日日期测试
    today_str = datetime.now().strftime("%Y-%m-%d")
    result = get_news_titles(symbol="002170", stock_name='芭田股份', max_news=20, current_date=today_str)
    print("\n最终抓取结果展示：")
    print(result)