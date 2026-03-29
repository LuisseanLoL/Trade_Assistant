import os
import re
import time
import random
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
            
            # 【新增处理逻辑】：遍历摘要，将“【东方财富财经早餐 ”替换为“【”
            cleaned_summaries = [s.replace("【东方财富财经早餐 ", "【") for s in summaries]
            
            macro_text = "【宏观财经早餐】\n" + "\n".join(cleaned_summaries)
            
            # 保存至缓存文件
            with open(cache_file, 'w', encoding='utf-8') as f:
                f.write(macro_text)
            return macro_text
    except Exception as e:
        print(f"⚠️ 获取宏观财经早餐失败: {e}")
        
    return ""

def get_latest_flash_news(limit=20):
    """【新增】获取最新新浪全球快讯 (实时获取，不缓存)"""
    try:
        print("🌍 正在实时获取最新全球快讯...")
        df = ak.stock_info_global_sina()
        if not df.empty:
            # 提取前 limit 条（默认20条，可按需修改）
            df_head = df.head(limit)
            flash_texts = [f"[{t}] {c}" for t, c in zip(df_head['时间'], df_head['内容'])]
            return "【最新全球快讯】\n" + "\n".join(flash_texts)
    except Exception as e:
        print(f"⚠️ 获取最新全球快讯失败: {e}")
    return ""


def get_news_titles(symbol="600000", stock_name='浦发银行', max_news=20, save_txt=True, current_date='20250214'):
    """
    获取并处理新闻标题 (包含内容与标题双搜索并集，并增加随机休眠防反爬)
    """
    # ==========================================
    # 🌟 核心新增：在最开头检查是否存在今日缓存
    # ==========================================
    news_dir = f"log/stock_news/{current_date}"
    os.makedirs(news_dir, exist_ok=True)
    filename = f"{symbol}_{stock_name}_News_{current_date}.txt"
    filepath = os.path.join(news_dir, filename)
    
    if os.path.exists(filepath):
        print(f"📦 [新闻缓存命中] 今日新闻已抓取过，直接读取: {filepath}")
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()

    # 如果没有缓存，则继续执行原有的抓取逻辑
    urls = [
        f"https://so.eastmoney.com/news/s?keyword={stock_name}&type=title&sort=time",
        f"https://so.eastmoney.com/news/s?keyword={stock_name}&type=content&sort=time",
    ]
    
    all_news_data = []
    seen_titles = set()

    try:
        print(f'🚀 开始获取 {symbol} ({stock_name}) 的新闻标题...')
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080}
            )
            page = context.new_page()
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            for i, url in enumerate(urls):
                # 🛑 新增：在抓取第二个 URL 之前，随机休眠 2 到 4 秒
                if i > 0:
                    sleep_time = random.uniform(2.0, 4.0)
                    print(f"⏳ 切换搜索类型，随机休眠 {sleep_time:.2f} 秒...")
                    time.sleep(sleep_time)

                print(f"👉 正在抓取节点: {url.split('type=')[1].split('&')[0]}")
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=45000)
                except Exception as e:
                    print(f"页面加载超时提示(可忽略): {e}")
                
                collected_count = 0 
                
                while collected_count < max_news:
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
                        if collected_count >= max_news:
                            break
                            
                        a_tag = item.select_one('.news_item_t a') or item.find('a')
                        if not a_tag:
                            continue
                            
                        title = a_tag.get_text(strip=True)
                        
                        item_text = item.get_text(separator=' ', strip=True)
                        date_match = re.search(r'\d{4}-\d{2}-\d{2}', item_text)
                        date_str = date_match.group(0) if date_match else "未知日期"
                        
                        if len(title) > 4:
                            formatted_item = f"[{date_str}] {title}"
                            
                            if title not in seen_titles:
                                seen_titles.add(title)
                                all_news_data.append((date_str, title, formatted_item))
                                collected_count += 1
                                new_titles_found = True
                                
                    if collected_count >= max_news:
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
                        
                    # 🛑 修改：将固定的 page.wait_for_timeout 改为随机 time.sleep
                    page_sleep = random.uniform(1.5, 3.5)
                    time.sleep(page_sleep)

            browser.close()

    except Exception as e:
        print(f"抓取操作失败: {e}")

    # 并集排序与截取
    all_news_data.sort(key=lambda x: x[0] if x[0] != "未知日期" else "0000-00-00", reverse=True)
    final_titles = [item[2] for item in all_news_data[:max_news]]
    text_content = '\n'.join(final_titles) if final_titles else ""
    
    if not text_content:
        print(f"未获取到 {symbol} 的个股新闻数据")

    # 1. 拼接宏观财经早餐 (带缓存逻辑)
    try:
        macro_news_text = get_macro_news(current_date)
        if macro_news_text:
            text_content = (text_content + "\n\n" + macro_news_text) if text_content else macro_news_text
    except Exception as e:
        print(f"处理宏观新闻时跳过: {e}")

    # 2. 【新增】拼接最新全球快讯 (不缓存)
    try:
        flash_news_text = get_latest_flash_news(limit=10) # 默认抓取最新10条，可以自己调整参数
        if flash_news_text:
            text_content = (text_content + "\n\n" + flash_news_text) if text_content else flash_news_text
    except Exception as e:
        print(f"处理快讯时跳过: {e}")

    # 保存文件
    news_dir = f"log/stock_news/{current_date}"
    os.makedirs(news_dir, exist_ok=True)
    
    if save_txt and text_content:
        filename = f"{symbol}_{stock_name}_News_{current_date}.txt"
        filepath = os.path.join(news_dir, filename)
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(text_content)
            print(f"✅ 新闻摘要(个股+宏观+快讯)已保存至: {filepath}")
        except Exception as e:
            print(f"保存文件失败: {e}")
        
    return text_content

# 测试代码
if __name__ == "__main__":
    # 使用今日日期测试
    today_str = datetime.now().strftime("%Y-%m-%d")
    result = get_news_titles(symbol="600000", stock_name='浦发银行', max_news=20, current_date=today_str)
    print("\n最终抓取结果展示：")
    print(result)