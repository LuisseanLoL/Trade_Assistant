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
            
            for item in data["announcements"]:
                title = item["announcementTitle"]
                
                # 排除干扰项
                if any(keyword in title for keyword in ["摘要", "英文", "取消", "更正"]):
                    continue
                
                # 正则动态匹配引擎
                if category == "年报":
                    pattern = f"{year}年?年度报告|{year}年报"
                elif category == "半年报":
                    pattern = f"{year}年?半年度报告|{year}半年报"
                elif category == "一季报":
                    pattern = f"{year}年?第?[一1]季度报告|{year}年?一季报"
                elif category == "三季报":
                    pattern = f"{year}年?第?[三3]季度报告|{year}年?三季报"
                else:
                    pattern = f"{year}.*{category}"
                    
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
    """
    V3.0 广域提炼+智能降噪切片引擎：
    扩大提取范围（含财务指标总结、重大事项、股东等），并用强力正则抹除无意义表格数据。
    """
    if not pdf_path or not os.path.exists(pdf_path): return ""
    
    print(f"   ✂️ 正在通过[广域降噪模式]切片解析 PDF: {os.path.basename(pdf_path)}")
    try:
        doc = fitz.open(pdf_path)
        extracted_text = []
        
        # 匹配无意义表格残骸：纯数字、纯金额、纯百分比、日期、短横线、空括号
        table_noise_pattern = re.compile(r'^[\s\d\.\,\%\-\(\)（）/]+$')
        
        # 重点关注的章节标题锚点
        core_keywords = ["主要财务数据", "管理层讨论与分析", "重要事项", "股份变动", "股东情况", "利润分配"]

        found_stop_sign = False
        max_pages = min(120, len(doc)) if is_annual else min(30, len(doc))

        for page_num in range(max_pages):
            if found_stop_sign:
                break

            blocks = doc[page_num].get_text("blocks")

            for block in blocks:
                if block[6] != 0: continue # 排除图片

                text = block[4].strip()
                if not text: continue

                text = re.sub(r'\s*\n\s*', '', text)
                
                # --- 1. 终点判定 ---
                if ("第十节" in text and "财务报告" in text) or ("审计报告" in text and page_num > 40):
                    found_stop_sign = True
                    extracted_text.append("\n【系统提示：后续为财务明细附注，已终止提取】\n")
                    break

                # --- 2. 章节高亮 ---
                if len(text) < 25 and any(k in text for k in core_keywords):
                    extracted_text.append(f"\n===== 【{text}】 =====\n")
                    continue

                # --- 3. 暴力降噪 ---
                if len(text) < 4: continue 
                if table_noise_pattern.match(text): continue 

                extracted_text.append(text)

        doc.close()
        
        full_text = "\n".join(extracted_text)
        full_text = re.sub(r'\n{2,}', '\n', full_text) 
        
        # 将文本严格控制在 50000 字符内，保障本地显存安全
        final_text = full_text[:50000]
        
        print(f"   ✅ 切片与降噪完成，高密度文本有效字符数: {len(final_text)}")
        return final_text 
    except Exception as e:
        print(f"   ❌ PDF解析失败: {e}")
        return ""

def generate_report_summary_with_llm(current_text: str, prev_text: str, report_type: str, stock_name: str) -> str:
    """调用大模型进行财报分析并打分"""
    if not current_text: return "未提取到有效财报文本。"
    
    sys_prompt = f"""你是一个顶级的 A股基本面风控分析师与量化研究员。你的任务是从财报文本中提取关键信息，严格按照 JSON 格式输出，所有核心结论必须包含原文引用以确保数据真实性（Grounded Extraction）。
当前分析对象：{stock_name} 的 {report_type}。
"""
    if prev_text:
        # 🌟 优化点：提示模型绕过免责声明，直击核心承诺
        sys_prompt += "\n【特别任务：战略执行与业绩兑现一致性校验】\n我将同时提供上一年度的财报文本片段。你需要提取上期报告中管理层作出的核心量化/定性承诺（注意：请跳过免责声明，直接提取实质性的经营目标或项目计划），并与本期财报披露的实际经营成果进行严格交叉比对。请客观评估其战略执行力与承诺兑现程度。若有偏差请提取其归因解释，并进行综合信誉度研判。"

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
  "重大事项及股东变动": {
    "核心结论": "请重点提取涉诉、资产重组以及前十大股东的显著增减持情况...",
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
    user_prompt = f"【当前财报提取文本】:\n{current_text}\n"
    if prev_text: user_prompt += f"\n【上一年度财报提取文本(用于校验)】:\n{prev_text}\n"

    try:
        print(f"   🤖 正在调用 LLM 进行财报深度阅读与战略一致性校验...")
        
        # 使用你本地最强的模型 (例如 Qwen 9B 或者其他的模型名称)
        # 你可以将其替换为 `os.getenv("FINANCIAL_MODEL", "qwen_9b")` 动态读取
        model_name_to_use = "kimi_k25"
        
        # 🌟 核心点：强制 schema=None，解除交易主程序的格式约束！
        return get_LLM_message(system_content=sys_prompt, user_message=user_prompt, model_id=model_name_to_use, schema=None)
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
    
    curr_pdf_path = download_specific_report_api(stock_code, year, curr_cat, pdf_dir)
    curr_text = slice_financial_report_pdf(curr_pdf_path, is_annual)
    
    prev_text = ""
    if prev_cat:
        prev_pdf_path = download_specific_report_api(stock_code, prev_year, prev_cat, pdf_dir)
        prev_text = slice_financial_report_pdf(prev_pdf_path, True)
    
    display_title = f"{year}年{curr_cat}" 
    summary_result = generate_report_summary_with_llm(curr_text, prev_text, display_title, stock_name)
    
    if summary_result and "解析失败" not in summary_result:
        with open(json_cache_file, 'w', encoding='utf-8') as f:
            f.write(summary_result)
            
    return summary_result