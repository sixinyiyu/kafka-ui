# =============================================================================
#  Kafka UI — Windows 构建 & 打包脚本
# -----------------------------------------------------------------------------
#  在 Windows 机器上（PowerShell）运行：
#     powershell -ExecutionPolicy Bypass -File .\scripts\build-windows.ps1
#
#  产物：
#     target\release\kafka-ui.exe                       # 单文件可执行
#     dist\kafka-ui-<version>-setup.exe                 # NSIS 安装包（若已装 makensis）
#     dist\kafka-ui-<version>.zip                       # 免安装压缩包
#
#  前置条件：
#     1) 安装 Rust（MSVC 工具链）：https://rustup.rs
#        - 需要「Visual Studio Build Tools」的 C++ 桌面开发组件（提供 link.exe）
#     2) （可选，生成安装包）安装 NSIS：https://nsis.sourceforge.io  （或 choco install nsis）
#     3) 首次构建会从 GitHub 拉取 gpui（zed v0.195.5），需能访问 GitHub。
# =============================================================================
param(
    [switch]$SkipInstaller,   # 只构建可执行与 zip，不生成 NSIS 安装包
    [string]$Target = "x86_64-pc-windows-msvc"
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

function Fail($msg) { Write-Host "ERROR: $msg" -ForegroundColor Red; exit 1 }
function Info($msg) { Write-Host "==> $msg" -ForegroundColor Cyan }

# ---- 0. 校验工具链 ----------------------------------------------------------
if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    Fail "未找到 cargo，请先安装 Rust：https://rustup.rs"
}
Info ("rustc: " + (rustc --version))

# 确保目标三元组可用
rustup target add $Target 2>$null | Out-Null

# ---- 1. 读取版本号 ----------------------------------------------------------
$cargoToml = Get-Content "Cargo.toml" -Raw
if ($cargoToml -match '(?m)^\s*version\s*=\s*"([^"]+)"') {
    $Version = $Matches[1]
} else {
    $Version = "0.1.0"
}
Info "版本号: $Version"

# ---- 2. 编译 release --------------------------------------------------------
Info "开始编译（release，$Target）…"
cargo build --release --target $Target
if ($LASTEXITCODE -ne 0) { Fail "cargo build 失败" }

$ExePath = Join-Path "target" (Join-Path $Target "release\kafka-ui.exe")
if (-not (Test-Path $ExePath)) { Fail "未找到编译产物：$ExePath" }
$ExeSize = "{0:N1} MB" -f ((Get-Item $ExePath).Length / 1MB)
Info "编译完成：$ExePath ($ExeSize)"

# ---- 3. 准备 dist 目录 ------------------------------------------------------
$Dist = "dist"
if (Test-Path $Dist) { Remove-Item $Dist -Recurse -Force }
New-Item -ItemType Directory -Path $Dist | Out-Null

Copy-Item $ExePath (Join-Path $Dist "kafka-ui.exe")
Copy-Item "README.md" (Join-Path $Dist "README.md") -ErrorAction SilentlyContinue

# 使用说明（随包附带）
@"
Kafka UI $Version

运行：双击 kafka-ui.exe
配置：连接信息保存在 %APPDATA%\kafka-ui\clusters.yaml
      可用环境变量 KAFKA_UI_CONFIG 指定配置文件路径。
日志：设置环境变量 KAFKA_UI_LOG=debug 可查看详细日志（release 版无控制台，
      如需日志请用 debug 构建或在设置中查看）。
"@ | Out-File -Encoding UTF8 (Join-Path $Dist "使用说明.txt")

# ---- 4. 免安装 zip ----------------------------------------------------------
$ZipPath = Join-Path $Dist "kafka-ui-$Version.zip"
Info "生成免安装压缩包：$ZipPath"
Compress-Archive -Path (Join-Path $Dist "kafka-ui.exe"), (Join-Path $Dist "README.md"), (Join-Path $Dist "使用说明.txt") -DestinationPath $ZipPath -Force

# ---- 5. NSIS 安装包 ---------------------------------------------------------
if ($SkipInstaller) {
    Info "已跳过 NSIS 安装包（-SkipInstaller）"
} else {
    $Makensis = Get-Command makensis -ErrorAction SilentlyContinue
    if (-not $Makensis) {
        # 常见安装位置探测
        $candidates = @(
            "$env:ProgramFiles\NSIS\makensis.exe",
            "${env:ProgramFiles(x86)}\NSIS\makensis.exe"
        )
        foreach ($c in $candidates) { if (Test-Path $c) { $Makensis = $c; break } }
    }
    if (-not $Makensis) {
        Write-Host "WARN: 未找到 makensis（NSIS），跳过安装包生成。" -ForegroundColor Yellow
        Write-Host "      安装 NSIS 后重跑即可：choco install nsis  或 https://nsis.sourceforge.io" -ForegroundColor Yellow
    } else {
        Info "调用 NSIS 生成安装包…"
        $OutFile = (Join-Path (Resolve-Path $Dist) "kafka-ui-$Version-setup.exe")
        & "$Makensis" /DVERSION=$Version /DOUTDIR="$(Resolve-Path $Dist)" /DEXEDIR="$(Resolve-Path $Dist)" "packaging\kafka-ui.nsi"
        if ($LASTEXITCODE -ne 0) { Fail "NSIS 打包失败" }
        Info "安装包：$OutFile"
    }
}

# ---- 6. 汇总 ---------------------------------------------------------------
Write-Host ""
Write-Host "================ 构建完成 ================" -ForegroundColor Green
Get-ChildItem $Dist | ForEach-Object {
    "  {0,-40} {1,10:N1} KB" -f $_.Name, ($_.Length / 1KB)
}
Write-Host "产物目录：$((Resolve-Path $Dist).Path)"
