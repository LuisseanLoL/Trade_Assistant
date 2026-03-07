@echo off
:: 设置控制台为 UTF-8 编码，防止中文乱码
chcp 65001 >nul
title AI Trade Assistant - 启动程序

:: 1. 自动切换到当前 .bat 文件所在的目录 (完美解决项目路径变化)
cd /d "%~dp0"
echo ========================================
echo 正在启动 AI Trade Assistant...
echo 当前运行目录: %cd%
echo ========================================

:: 2. 尝试激活特定的 Conda 环境
:: (如果目标电脑没有 conda 或者环境不叫 agent，这句会静默跳过，不会报错)
call conda activate agent 2>nul

:: 3. 检查 Python 是否可用并运行程序
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 未检测到 Python 环境！
    echo 请确认这台电脑已安装 Python 或 Conda，并已配置到系统环境变量中。
    pause
    exit /b
)

:: 启动 Dash 网页应用
python app.py

:: 运行结束或崩溃时暂停，防止黑色窗口一闪而过看不到报错信息
pause