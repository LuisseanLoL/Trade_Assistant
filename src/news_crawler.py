import os
import re
from bs4 import BeautifulSoup

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("⚠️ 缺少 Playwright 库，请先运行: pip install playwright && playwright install chromium")


def get_news_titles(symbol="600325", stock_name='华发股份', max_news=20, save_txt=True, current_date='20250214'):
    """
    获取并处理新闻标题 (使用 Playwright 无头浏览器绕过反爬)
    参数:
        symbol: 股票代码/指数名称
        stock_name: 股票名称
        max_news: 最大新闻数量
        save_txt: 是否保存为文本文件
        current_date: 当前日期，用于日志路径
    返回:
        包含标题的字符串(每行一个标题)
    """
    url = f"https://so.eastmoney.com/news/s?keyword={symbol}"
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
                # 只要基础 HTML 加载完就算成功，不用等图片和广告
                page.goto(url, wait_until="domcontentloaded", timeout=45000)
            except Exception as e:
                print(f"页面加载超时提示(可忽略，继续尝试提取): {e}")
            
            while len(titles) < max_news:
                try:
                    # 显式等待包含新闻的 div 出现
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
                    
                    # 过滤无效标题并去重
                    if len(title) > 4 and title not in titles:
                        titles.append(title)
                        new_titles_found = True
                        
                if len(titles) >= max_news:
                    break
                
                # 如果当前页没有找到任何新的有效标题，防止死循环
                if not new_titles_found:
                    break
                    
                # 尝试翻页
                next_btn_1 = page.locator("a:text-is('>')").first
                next_btn_2 = page.locator("a:text-is('下一页')").first
                
                if next_btn_1.is_visible():
                    next_btn_1.click()
                elif next_btn_2.is_visible():
                    next_btn_2.click()
                else:
                    print("没有更多新闻页面了。")
                    break
                    
                # 翻页后强行等 2 秒，给渲染留出时间
                page.wait_for_timeout(2000)

            browser.close()

    except Exception as e:
        print(f"抓取操作失败: {e}")

    # 如果没有任何数据，返回空字符串
    if not titles:
        print(f"未获取到 {symbol} 的新闻数据")
        return ""

    # 截取所需数量并生成文本内容
    text_content = '\n'.join(titles[:max_news])

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
            print(f"✅ 标题文件已保存至: {filepath}")
        except Exception as e:
            print(f"保存文件失败: {e}")
        
    return text_content


# 测试代码
if __name__ == "__main__":
    result = get_news_titles(symbol="600325", stock_name='华发股份', max_news=20)
    print("\n抓取结果：")
    print(result)