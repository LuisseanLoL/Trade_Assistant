# -*- coding: utf-8 -*-
import os
import time
import requests
import fitz  # PyMuPDF
import re
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

# 自动寻找项目根目录的 .env 文件并加载到系统环境变量中
load_dotenv() 

try:
    from src.LLM_chat import get_LLM_message
except ImportError:
    print("❌ 无法导入 LLM_chat.py，请确保该文件在 src 目录下。")
    exit(1)

def download_specific_report_api(stock_code: str, report_title_keyword: str, download_dir: str):
    """
    巨潮网接口直链下载器 (带严格的 PDF 缓存机制，并扩大了搜索时间范围)
    """
    os.makedirs(download_dir, exist_ok=True)
    file_name = f"{stock_code}_{report_title_keyword}.pdf".replace("*", "").replace("/", "")
    file_path = os.path.join(download_dir, file_name)
    
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

            try:
                page.locator(".el-date-editor--daterange").first.click()
                page.get_by_text("近3年", exact=True).click()
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
    """
    V3.0 广域提炼+智能降噪切片引擎：
    扩大提取范围（含财务指标总结、重大事项、股东等），并用强力正则抹除无意义表格数据以保护本地显存。
    """
    if not pdf_path or not os.path.exists(pdf_path): return ""
    
    print(f"✂️ 正在通过[广域降噪模式]切片解析 PDF: {os.path.basename(pdf_path)}")
    try:
        doc = fitz.open(pdf_path)
        extracted_text = []
        
        # 匹配无意义表格残骸：纯数字、纯金额、纯百分比、日期、短横线、空括号
        table_noise_pattern = re.compile(r'^[\s\d\.\,\%\-\(\)（）/]+$')
        
        # 重点关注的章节标题锚点（用于在大模型阅读时给予提示）
        core_keywords = ["主要财务数据", "管理层讨论与分析", "重要事项", "股份变动", "股东情况", "利润分配"]

        found_stop_sign = False
        # 财报真正有价值的文字通常在前100页内，后面的全是一望无际的财务报表附注
        max_pages = min(120, len(doc)) if is_annual else min(30, len(doc))

        for page_num in range(max_pages):
            if found_stop_sign:
                break

            # 使用 blocks 模式，能更好地识别段落边界，而不是生硬地按行切断
            blocks = doc[page_num].get_text("blocks")

            for block in blocks:
                # 排除图片等非文本 block
                if block[6] != 0: continue

                text = block[4].strip()
                if not text: continue

                # 清除段落内部不必要的换行符，合并为完整句子
                text = re.sub(r'\s*\n\s*', '', text)
                
                # --- 1. 终点判定 ---
                # 遇到“第十节 财务报告”或者“审计报告”全文时，说明高价值的文字叙述部分已经结束
                if ("第十节" in text and "财务报告" in text) or ("审计报告" in text and page_num > 40):
                    found_stop_sign = True
                    extracted_text.append("\n【系统提示：后续为财务明细附注，已终止提取】\n")
                    break

                # --- 2. 章节高亮 ---
                if len(text) < 25 and any(k in text for k in core_keywords):
                    extracted_text.append(f"\n===== 【{text}】 =====\n")
                    continue

                # --- 3. 暴力降噪 ---
                if len(text) < 4: continue # 过滤页码、字母等极短无用文本
                if table_noise_pattern.match(text): continue # 过滤纯数字/财务表格噪音

                # 通过重重筛选的，都是含有自然语言价值的长段落，保留！
                extracted_text.append(text)

        doc.close()
        
        # 组装最终文本并做最后的防爆显存安全截断
        full_text = "\n".join(extracted_text)
        # 将连续空行压缩为单行
        full_text = re.sub(r'\n{2,}', '\n', full_text) 
        
        # 放宽限制：保留前 50000 字符。配合降噪，这 5万字的信息密度极高。
        final_text = full_text[:50000]
        
        print(f"✅ 切片与降噪完成，高密度文本有效字符数: {len(final_text)}")
        return final_text 
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
  "重大事项及股东变动": {
    "核心结论": "请重点提取涉诉、资产重组以及前十大股东的显著增减持情况...",
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
    # 结合降噪切片，此处放入大模型的文本几乎全是干货
    user_prompt = f"【当前财报提取文本】:\n{current_text}\n"
    if prev_text:
        user_prompt += f"\n【上一年度财报提取文本(用于校验)】:\n{prev_text}\n"

    try:
        print("🤖 正在调用 LLM 进行财报深度阅读与战略一致性校验...")
        model_name_to_use = "qwen_9b"  
        
        # 🌟 核心修复：传入 schema=None 避免被主交易逻辑的 JSON 约束覆盖导致解析报错！
        result = get_LLM_message(
            system_content=sys_prompt, 
            user_message=user_prompt, 
            model_id=model_name_to_use,
            schema=None
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
    stock_code = "600600"
    stock_name = "青岛啤酒"
    latest_report_date = "20251231"
    
    os.makedirs("log/financial_pdfs", exist_ok=True)

    print("\n" + "="*50)
    print("🛠️ 【全景财报排雷测试】：模拟新财报发布，执行广域下载与分析")
    print("="*50)
    t1_start = time.time()
    result_1 = process_pipeline(stock_code, stock_name, latest_report_date)
    t1_end = time.time()
    print(f"\n✅ 测试完成，总耗时: {t1_end - t1_start:.2f} 秒\n")

    print("\n大模型返回的 JSON 分析结果如下：\n")
    print(result_1)