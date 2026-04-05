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
    V4.0 智能切片引擎：状态机模型 + 动态配额控制 + 密度降噪
    解决银行股财报过长截断、小盘股提取过多无用信息的问题。
    """
    if not pdf_path or not os.path.exists(pdf_path): return ""
    
    print(f"   ✂️ 启动[智能状态机模式]精准提取 PDF: {os.path.basename(pdf_path)}")
    try:
        doc = fitz.open(pdf_path)
        
        # --- 1. 核心节区位定义 ---
        # 我们只在进入这些“黄金章节”时才开启录制
        target_keywords = ["主要财务数据", "管理层讨论与分析", "重要事项", "股份变动", "股东情况", "利润分配"]
        
        # 遇到这些章节直接闭麦（尤其是冗长的公司治理和最后的财务明细）
        stop_keywords = ["财务报告", "审计报告", "公司治理", "环境和社会责任", "备查文件目录"]
        
        # 按板块分类收集文本，避免某一个板块把 50000 字符额度吃光
        section_buffers = {
            "财务数据与管理层讨论": [],
            "重要事项与股东情况": [],
            "其他核心信息": []
        }
        
        current_bucket = "其他核心信息"
        is_recording = False  # 状态机开关
        found_financial_report = False
        
        # 避免把目录(TOC)里的标题误认为正文标题，跳过前 3 页的标题判定
        skip_toc_pages = 3 

        max_pages = min(250, len(doc)) # 放宽最大页数，靠状态机自己停

        for page_num in range(max_pages):
            if found_financial_report:
                break # 彻底结束

            blocks = doc[page_num].get_text("blocks")

            for block in blocks:
                if block[6] != 0: continue # 排除图片

                text = block[4].strip()
                if not text: continue
                text = re.sub(r'\s*\n\s*', '', text)
                
                # --- 2. 状态机切换逻辑 (检测短文本标题) ---
                if len(text) < 35 and page_num > skip_toc_pages:
                    # 碰到终止章节 -> 关机
                    if any(k in text for k in stop_keywords) and ("第" in text or "十节" in text or "十一节" in text):
                        is_recording = False
                        if "财务报告" in text or "审计报告" in text:
                            found_financial_report = True
                            break
                        continue

                    # 碰到目标章节 -> 开机并分类
                    for k in target_keywords:
                        if k in text:
                            is_recording = True
                            if k in ["主要财务数据", "管理层讨论与分析"]:
                                current_bucket = "财务数据与管理层讨论"
                            elif k in ["重要事项", "股份变动", "股东情况"]:
                                current_bucket = "重要事项与股东情况"
                            else:
                                current_bucket = "其他核心信息"
                            
                            section_buffers[current_bucket].append(f"\n\n===== 【{text}】 =====\n")
                            break
                            
                # 如果没在录制状态，直接跳过这段文本
                if not is_recording:
                    continue

                # --- 3. 增强型降噪 (对抗银行股的恐怖表格) ---
                if len(text) < 4: continue 
                
                # 过滤纯符号/数字行
                if re.match(r'^[\s\d\.\,\%\-\(\)（）/]+$', text): 
                    continue 
                
                # 【新增】数字密度检测：如果一段话里全是数字和金额，文字极少，大概率是无意义的表格残骸
                numbers_len = sum(c.isdigit() or c in '.,%' for c in text)
                if len(text) > 20 and (numbers_len / len(text)) > 0.6:
                    continue 

                section_buffers[current_bucket].append(text)

        doc.close()
        
        # --- 4. 动态配额组装 (Dynamic Budgeting) ---
        # 强制给不同的板块分配额度，防止某个板块（如银行的管理层讨论）吃干抹净
        final_text_parts = []
        
        # 分配额度：总长最多控制在 40000 字（留给 Prompt 空间）
        budgets = {
            "财务数据与管理层讨论": 20000,
            "重要事项与股东情况": 15000,
            "其他核心信息": 5000
        }
        
        for bucket, text_list in section_buffers.items():
            bucket_text = "\n".join(text_list)
            bucket_text = re.sub(r'\n{2,}', '\n', bucket_text) # 清理多余换行
            
            # 如果超额，只截取该板块的前 N 个字
            if len(bucket_text) > budgets[bucket]:
                bucket_text = bucket_text[:budgets[bucket]] + f"\n...[系统提示：该章节过长，已被安全截断 (字数上限 {budgets[bucket]})]...\n"
                
            final_text_parts.append(bucket_text)
            
        final_text = "\n".join(final_text_parts).strip()
        
        if not final_text:
            # 兜底逻辑：如果状态机因为格式奇葩没抓到任何东西，退化为抓取前40页
            print("   ⚠️ 状态机未匹配到标准章节，触发兜底提取逻辑...")
            return slice_financial_report_pdf_fallback(pdf_path, is_annual)

        print(f"   ✅ 智能切片完成，有效信息提取: {len(final_text)} 字符 (大盘股/银行股防截断机制已生效)")
        return final_text 

    except Exception as e:
        print(f"   ❌ PDF解析失败: {e}")
        return ""

def slice_financial_report_pdf_fallback(pdf_path: str, is_annual: bool) -> str:
    """极其不规范的财报兜底提取函数（原 V3.0 的浓缩版）"""
    # 这里的逻辑就是在极端情况下，直接提取前30页文字，防崩溃
    doc = fitz.open(pdf_path)
    text_blocks = []
    max_pages = 40 if is_annual else 20
    for page_num in range(min(max_pages, len(doc))):
        for block in doc[page_num].get_text("blocks"):
            if block[6] == 0:
                t = block[4].strip()
                if len(t) > 5 and not re.match(r'^[\s\d\.\,\%\-\(\)（）/]+$', t):
                    text_blocks.append(t)
    doc.close()
    return "\n".join(text_blocks)[:25000]

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
    
    # 🌟 修改点 1：将上一年度对比目标强制定死为“年报”
    prev_cat = "年报"
    
    if mmdd == "1231":
        curr_cat = "年报"
        is_annual = True
    elif mmdd == "0630":
        curr_cat = "半年报"
        # 保留 is_annual = True 是为了让半年报提取更多的页数（半年报通常也很长）
        is_annual = True 
    elif mmdd == "0331":
        curr_cat = "一季报"
        is_annual = False
    elif mmdd == "0930":
        curr_cat = "三季报" 
        is_annual = False
    else:
        return ""

    pdf_dir = "log/financial_pdfs"
    
    curr_pdf_path = download_specific_report_api(stock_code, year, curr_cat, pdf_dir)
    curr_text = slice_financial_report_pdf(curr_pdf_path, is_annual)
    
    prev_text = ""
    # 🌟 修改点 2：这里现在一定会触发，去下载并切片上一年度的年报
    if prev_cat:
        prev_pdf_path = download_specific_report_api(stock_code, prev_year, prev_cat, pdf_dir)
        # 年报切片必须强制传入 is_annual=True，以保证提取足够的页数（120页）
        prev_text = slice_financial_report_pdf(prev_pdf_path, True)
    
    display_title = f"{year}年{curr_cat}" 
    summary_result = generate_report_summary_with_llm(curr_text, prev_text, display_title, stock_name)
    
    if summary_result and "解析失败" not in summary_result:
        with open(json_cache_file, 'w', encoding='utf-8') as f:
            f.write(summary_result)
            
    return summary_result