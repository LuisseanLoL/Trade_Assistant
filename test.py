# -*- coding: utf-8 -*-
import os
import time
import requests
import fitz  # PyMuPDF
import re
from playwright.sync_api import sync_playwright

# ==========================================
# 🌟 修复点 1：加载 .env 环境变量
# ==========================================
from dotenv import load_dotenv
load_dotenv() # 自动寻找项目根目录的 .env 文件并加载到系统环境变量中

try:
    from src.LLM_chat import get_LLM_message
except ImportError:
    print("❌ 无法导入 LLM_chat.py，请确保该文件在同一目录下。")
    exit(1)

def download_specific_report_api(stock_code: str, report_title_keyword: str, download_dir: str):
    """
    巨潮网接口直链下载器 (带严格的 PDF 缓存机制，并扩大了搜索时间范围)
    """
    os.makedirs(download_dir, exist_ok=True)
    file_name = f"{stock_code}_{report_title_keyword}.pdf".replace("*", "").replace("/", "")
    file_path = os.path.join(download_dir, file_name)
    
    # PDF 缓存判断
    if os.path.exists(file_path):
        print(f"📦 [PDF 缓存命中] 发现本地文件，跳过下载: {file_path}")
        return file_path

    print(f"🌐 [网络请求] 本地无缓存，正在前往巨潮网下载: {report_title_keyword}...")
    category = "年报" if "年度报告" in report_title_keyword else ("半年报" if "半年度" in report_title_keyword else "季报")

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

            # ==========================================
            # 🌟 修复点 2：自动点击展开日期，并选择“近3年”
            # ==========================================
            try:
                # 定位到那个包含了日期的框并点击
                page.locator(".el-date-editor--daterange").first.click()
                # 你的截图里左侧有个“近3年”，我们直接点击它
                page.get_by_text("近3年", exact=True).click()
                # 稍微等半秒钟，让前端页面把参数写进去
                page.wait_for_timeout(500)
                print("   ⏱️ 已自动将搜索时间范围扩大为: 近3年")
            except Exception as e:
                print(f"   ⚠️ 修改日期范围时遇到问题 (可能UI已变更)，将使用默认时间重试: {e}")

            with page.expect_response(lambda response: "query" in response.url and response.status == 200, timeout=15000) as response_info:
                page.get_by_role("button", name="查询").click()
                
            data = response_info.value.json()
            if not data.get("announcements"):
                print("❌ 未查询到相关公告。")
                return None

            target_report = None
            for item in data["announcements"]:
                title = item["announcementTitle"]
                if report_title_keyword in title and "摘要" not in title and "英文" not in title and "取消" not in title:
                    target_report = item
                    break

            if not target_report:
                print(f"❌ 未在结果集中找到标题包含 '{report_title_keyword}' 的正文报告。")
                return None

            pdf_download_url = f"https://static.cninfo.com.cn/{target_report['adjunctUrl']}"
            print(f"⬇️ 成功获取下载直链，正在下载 PDF...")
            pdf_response = requests.get(pdf_download_url, timeout=30)
            
            with open(file_path, "wb") as f:
                f.write(pdf_response.content)
            print(f"✅ 下载并保存成功: {file_path}")
            return file_path

        except Exception as e:
            print(f"❌ 下载财报失败 {report_title_keyword}: {e}")
            return None
        finally:
            browser.close()

def slice_financial_report_pdf(pdf_path: str, is_annual: bool) -> str:
    """定向切片 PDF，提取核心文本以节省 Token"""
    if not pdf_path or not os.path.exists(pdf_path): return ""
    
    print(f"✂️ 正在切片解析 PDF: {os.path.basename(pdf_path)}")
    try:
        doc = fitz.open(pdf_path)
        extracted_text = []
        
        if is_annual:
            start_extracting = False
            for page_num in range(min(150, len(doc))):
                text = doc[page_num].get_text()
                if "管理层讨论与分析" in text or "重要事项" in text:
                    start_extracting = True
                if "财务报告" in text and "审计报告" in text and page_num > 30:
                    break
                if start_extracting:
                    extracted_text.append(text)
        else:
            for page_num in range(min(25, len(doc))):
                extracted_text.append(doc[page_num].get_text())
                
        doc.close()
        full_text = "\n".join(extracted_text)
        full_text = re.sub(r'\n+', '\n', full_text)
        
        text_length = len(full_text)
        print(f"✅ 切片完成，提取有效字符数: {text_length}")
        return full_text[:80000] 
    except Exception as e:
        print(f"❌ PDF解析失败: {e}")
        return ""

def generate_report_summary_with_llm(current_text: str, prev_text: str, report_type: str, stock_name: str) -> str:
    """构建 Prompt 并调用大模型进行结构化信息抽取和一致性校验"""
    if not current_text: return "未提取到有效财报文本。"
    
    sys_prompt = f"""你是一个顶级的 A股基本面风控分析师与量化研究员。你的任务是从财报文本中提取关键信息，并严格按照 JSON 格式输出，所有核心结论必须包含原文引用以确保数据真实性（Grounded Extraction）。
当前分析对象：{stock_name} 的 {report_type}。
"""
    if prev_text:
        sys_prompt += "\n【特别任务：战略执行与业绩兑现一致性校验】\n我将同时提供上一年度的财报文本片段（包含前瞻性指引与经营计划）。你需要提取上期报告中管理层作出的核心量化/定性承诺（如产能扩张、营收目标、重点研发进度等），并与本期财报披露的实际经营成果进行严格交叉比对。请客观评估其战略执行力与承诺兑现程度。若存在未达预期的重大偏差，请提取管理层对该偏差的归因解释，并对其信誉度进行综合研判。"

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
    "管理层信誉度与执行力评分(1-10分)": 8,
    "综合研判结论": "..."
  }
}
"""
    user_prompt = f"【当前财报提取文本】:\n{current_text[:40000]}\n"
    if prev_text:
        user_prompt += f"\n【上一年度财报提取文本(用于校验)】:\n{prev_text[:20000]}\n"

    try:
        print("🤖 正在调用 LLM 进行财报深度阅读与战略一致性校验...")
        # ⚠️ 注意这里：请确保 "kimi_k25" 是你在 .env 文件里配置过的 ACTIVE_MODELS 之一！
        model_name_to_use = "kimi_k25"  
        
        result = get_LLM_message(
            system_content=sys_prompt, 
            user_message=user_prompt, 
            model_id=model_name_to_use
        )
        return result
    except Exception as e:
        return f"LLM 解析失败: {e}"

def process_pipeline(stock_code: str, stock_name: str, latest_report_date: str) -> str:
    """主控调度流"""
    year = latest_report_date[:4]
    mmdd = latest_report_date[4:]
    
    if mmdd == "1231":
        current_title = f"{year}年年度报告"
        prev_title = f"{int(year)-1}年年度报告"
        is_annual = True
    else:
        current_title = f"{year}年三季度报告" 
        prev_title = None
        is_annual = False

    pdf_dir = "log/financial_pdfs"
    
    # 1. 搞定当期财报
    curr_pdf_path = download_specific_report_api(stock_code, current_title, pdf_dir)
    curr_text = slice_financial_report_pdf(curr_pdf_path, is_annual)
    
    # 2. 搞定上期财报用于打脸测试
    prev_text = ""
    if prev_title:
        print(f"🔍 触发一致性校验，准备处理上期财报: {prev_title}")
        prev_pdf_path = download_specific_report_api(stock_code, prev_title, pdf_dir)
        prev_text = slice_financial_report_pdf(prev_pdf_path, True)

    # 3. 喂给 LLM
    summary_result = generate_report_summary_with_llm(curr_text, prev_text, current_title, stock_name)
    
    return summary_result


if __name__ == "__main__":
    stock_code = "000001"
    stock_name = "平安银行"
    latest_report_date = "20251231" 
    
    os.makedirs("log/financial_pdfs", exist_ok=True)

    print("\n" + "="*50)
    print("🛠️ 【第一轮测试】：模拟新财报发布，执行全量下载与分析")
    print("="*50)
    t1_start = time.time()
    result_1 = process_pipeline(stock_code, stock_name, latest_report_date)
    t1_end = time.time()
    print(f"\n✅ 第一轮完成，总耗时: {t1_end - t1_start:.2f} 秒\n")

    print("\n" + "="*50)
    print("⚡ 【第二轮测试】：验证 PDF 缓存机制！(应该瞬间跳过网络下载)")
    print("="*50)
    t2_start = time.time()
    result_2 = process_pipeline(stock_code, stock_name, latest_report_date)
    t2_end = time.time()
    
    print(f"\n✅ 第二轮完成，总耗时: {t2_end - t2_start:.2f} 秒")
    print("\n大模型返回的 JSON 分析结果如下：\n")
    print(result_2)