# -*- coding: utf-8 -*-
# 文件路径: src/financial_analyzer.py

import os
import time
import requests
import fitz  # PyMuPDF
import re
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 导入你的 LLM 接口
try:
    from src.LLM_chat import get_LLM_message, get_model_config
except ImportError:
    from LLM_chat import get_LLM_message, get_model_config

def download_specific_report_api(stock_code: str, year: str, category: str, download_dir: str):
    """
    巨潮网接口直链下载器 (引入正则模糊匹配应对不同公司命名习惯)
    :param year: 例如 "2025"
    :param category: 只能是 "年报", "半年报", "一季报", "三季报" (这需要和巨潮网的按钮文字严格对应)
    """
    os.makedirs(download_dir, exist_ok=True)
    # 规范化本地保存的文件名
    file_name = f"{stock_code}_{year}年{category}.pdf"
    file_path = os.path.join(download_dir, file_name)
    
    if os.path.exists(file_path):
        print(f"   📦 [PDF 缓存命中] 跳过下载: {file_path}")
        return file_path

    print(f"   🌐 [网络请求] 正在前往巨潮网下载: {year}年 {category}...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True) 
        context = browser.new_context()
        page = context.new_page()

        try:
            page.goto("https://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&lastPage=index", timeout=30000)
            page.wait_for_load_state("networkidle")

            search_input = page.get_by_placeholder("代码/简称/拼音").last
            search_input.fill(stock_code)
            page.locator(".el-autocomplete-suggestion li").first.click()
            
            # 点击对应的分类按钮 (刚好和我们的 category 变量一致)
            page.get_by_text(category, exact=True).click()

            try:
                page.locator(".el-date-editor--daterange").first.click()
                page.get_by_text("近3年", exact=True).click()
                page.wait_for_timeout(500)
            except Exception:
                pass 

            with page.expect_response(lambda response: "query" in response.url and response.status == 200, timeout=15000) as response_info:
                page.get_by_role("button", name="查询").click()
                
            data = response_info.value.json()
            if not data.get("announcements"): return None

            target_report = None
            
            # ==========================================
            # 🌟 核心修复：基于正则的动态匹配引擎
            # ==========================================
            for item in data["announcements"]:
                title = item["announcementTitle"]
                
                # 1. 排除干扰项
                if any(keyword in title for keyword in ["摘要", "英文", "取消", "更正"]):
                    continue
                
                # 2. 根据不同类型构建兼容性极强的正则表达式
                if category == "年报":
                    # 匹配: 2025年年度报告, 2025年报
                    pattern = f"{year}年?年度报告|{year}年报"
                elif category == "半年报":
                    # 匹配: 2025年半年度报告, 2025年半年报
                    pattern = f"{year}年?半年度报告|{year}半年报"
                elif category == "一季报":
                    # 匹配: 2025年第一季度报告, 2025年一季度报告, 2025年第1季度报告
                    pattern = f"{year}年?第?[一1]季度报告|{year}年?一季报"
                elif category == "三季报":
                    # 匹配: 2025年第三季度报告, 2025年三季度报告, 2025年第3季度报告
                    pattern = f"{year}年?第?[三3]季度报告|{year}年?三季报"
                else:
                    pattern = f"{year}.*{category}"
                    
                # 3. 执行正则匹配
                if re.search(pattern, title):
                    target_report = item
                    break

            if not target_report: return None

            pdf_download_url = f"https://static.cninfo.com.cn/{target_report['adjunctUrl']}"
            pdf_response = requests.get(pdf_download_url, timeout=30)
            
            with open(file_path, "wb") as f:
                f.write(pdf_response.content)
            return file_path

        except Exception as e:
            print(f"   ❌ 下载财报失败 {year}{category}: {e}")
            return None
        finally:
            browser.close()

def slice_financial_report_pdf(pdf_path: str, is_annual: bool) -> str:
    """定向切片 PDF"""
    if not pdf_path or not os.path.exists(pdf_path): return ""
    
    try:
        doc = fitz.open(pdf_path)
        extracted_text = []
        
        if is_annual:
            start_extracting = False
            for page_num in range(min(150, len(doc))):
                text = doc[page_num].get_text()
                if "管理层讨论与分析" in text or "重要事项" in text: start_extracting = True
                if "财务报告" in text and "审计报告" in text and page_num > 30: break
                if start_extracting: extracted_text.append(text)
        else:
            for page_num in range(min(25, len(doc))):
                extracted_text.append(doc[page_num].get_text())
                
        doc.close()
        full_text = "\n".join(extracted_text)
        return re.sub(r'\n+', '\n', full_text)[:80000] 
    except Exception as e:
        return ""

def generate_report_summary_with_llm(current_text: str, prev_text: str, report_type: str, stock_name: str) -> str:
    """调用大模型进行财报分析并打分"""
    if not current_text: return "未提取到有效财报文本。"
    
    sys_prompt = f"""你是一个顶级的 A股基本面风控分析师与量化研究员。你的任务是从财报文本中提取关键信息，严格按照 JSON 格式输出，核心结论必须包含原文引用（Grounded Extraction）。
当前分析对象：{stock_name} 的 {report_type}。
"""
    if prev_text:
        sys_prompt += "\n【特别任务：战略执行与业绩兑现一致性校验】\n我将同时提供上一年度的财报文本片段。你需要提取上期报告中管理层作出的核心前瞻性承诺，并与本期财报的实际经营成果交叉比对。客观评估其战略执行力，若有偏差请提取其归因解释，并进行综合信誉度研判。"

    sys_prompt += """
必须输出且仅输出合法的 JSON 字符串，格式如下：
{
  "业绩爆点与风控雷区": {
    "核心结论": "...",
    "原文摘录": "..."
  },
  "主营业务及财务异动": {
    "核心结论": "...",
    "原文摘录": "..."
  },
  "战略及业绩兑现一致性评估": {
    "上期前瞻性指引与承诺": "...",
    "当期实际经营成果与兑现": "...",
    "业绩偏差归因(若未达标)": "...",
    "管理层信誉度与执行力评分(1-10分)": "...",
    "综合研判结论": "..."
  }
}
"""
    user_prompt = f"【当前财报提取文本】:\n{current_text[:40000]}\n"
    if prev_text: user_prompt += f"\n【上一年度财报提取文本(用于校验)】:\n{prev_text[:20000]}\n"

    try:
        # ⚠️ 注意：这里默认使用你之前测试成功的 kimi_k25，或者你可以动态传入
        model_name_to_use = "kimi_k25"  
        return get_LLM_message(system_content=sys_prompt, user_message=user_prompt, model_id=model_name_to_use, schema=None) # type: ignore
    except Exception as e:
        return f"财报 LLM 解析失败: {e}"

def process_pipeline(stock_code: str, stock_name: str, report_date_raw: str) -> str:
    """财报主控流 (带双重缓存机制与智能报告期解析)"""
    if not report_date_raw or "无数据" in report_date_raw: return ""
    
    cache_dir = "log/financial_summaries"
    os.makedirs(cache_dir, exist_ok=True)
    json_cache_file = os.path.join(cache_dir, f"{stock_code}_{report_date_raw}_summary.json")
    
    if os.path.exists(json_cache_file):
        print(f"   📦 [大模型分析缓存命中] 读取已保存的财报解析结果: {json_cache_file}")
        with open(json_cache_file, 'r', encoding='utf-8') as f:
            return f.read()

    print(f"   🔍 未命中大模型分析缓存，启动深度财报研读引擎...")
    
    # 解析年份和分类
    try:
        year = report_date_raw[:4]
        mmdd = report_date_raw[4:8]
    except:
        return ""
        
    prev_year = str(int(year) - 1)
    
    if mmdd == "1231":
        curr_cat = "年报"
        prev_cat = "年报"
        is_annual = True
    elif mmdd == "0630":
        curr_cat = "半年报"
        prev_cat = None
        is_annual = True
    elif mmdd == "0331":
        curr_cat = "一季报"
        prev_cat = None
        is_annual = False
    elif mmdd == "0930":
        curr_cat = "三季报" 
        prev_cat = None
        is_annual = False
    else:
        return ""

    pdf_dir = "log/financial_pdfs"
    
    # 传递 year 和 category 给下载器
    curr_pdf_path = download_specific_report_api(stock_code, year, curr_cat, pdf_dir)
    curr_text = slice_financial_report_pdf(curr_pdf_path, is_annual)
    
    prev_text = ""
    if prev_cat:
        prev_pdf_path = download_specific_report_api(stock_code, prev_year, prev_cat, pdf_dir)
        prev_text = slice_financial_report_pdf(prev_pdf_path, True)

    print(f"   🤖 正在呼叫财报分析专员处理文本...")
    
    # 构造用于 Prompt 显示的名字，比如 "2025年年报"
    display_title = f"{year}年{curr_cat}" 
    summary_result = generate_report_summary_with_llm(curr_text, prev_text, display_title, stock_name)
    
    if summary_result and "解析失败" not in summary_result:
        with open(json_cache_file, 'w', encoding='utf-8') as f:
            f.write(summary_result)
            
    return summary_result