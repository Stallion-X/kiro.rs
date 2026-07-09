#Requires -Version 5.1
<#
.SYNOPSIS
    kiro-rs 启动脚本 (Windows / PowerShell)

.DESCRIPTION
    默认前台运行 (Ctrl+C 停止)。启动横幅出现后即为"就绪"状态 —— 服务器会常驻
    监听端口, 不会自己退出, 这是正常现象, 不是卡住。

.PARAMETER Background
    后台运行, 日志写入 logs\ 目录 (终端不被占用)

.PARAMETER Stop
    停止正在运行的 kiro-rs 进程

.PARAMETER LogLevel
    RUST_LOG 日志级别 (默认 info; 可选 debug / trace / warn / error)

.PARAMETER Config
    config.json 路径 (默认 .\config.json)

.PARAMETER Credentials
    credentials.json 路径 (默认 .\credentials.json)

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File start.ps1
    # 前台运行

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File start.ps1 -Background
    # 后台运行

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File start.ps1 -Stop
    # 停止服务
#>
[CmdletBinding()]
param(
    [switch]$Background,
    [switch]$Stop,
    [string]$LogLevel = 'info',
    [string]$Config,
    [string]$Credentials
)

$ErrorActionPreference = 'Continue'
Set-Location -LiteralPath $PSScriptRoot

function Info([string]$m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Good([string]$m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn([string]$m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Die ([string]$m){ Write-Host "[FAIL] $m" -ForegroundColor Red; exit 1 }

# ---------- 停止模式 ----------
if ($Stop){
    $procs = Get-Process kiro-rs -ErrorAction SilentlyContinue
    if ($procs){
        $procs | Stop-Process -Force
        Good ("已停止 kiro-rs (PID: {0})" -f ($procs.Id -join ', '))
    } else {
        Info "没有正在运行的 kiro-rs 进程。"
    }
    return
}

# ---------- 定位可执行文件 (优先 release) ----------
$exe = $null
foreach ($p in 'target\release\kiro-rs.exe','target\debug\kiro-rs.exe'){
    if (Test-Path -LiteralPath $p){ $exe = (Resolve-Path -LiteralPath $p).Path; break }
}
if (-not $exe){ Die "未找到 kiro-rs.exe, 请先运行:  powershell -ExecutionPolicy Bypass -File build.ps1" }

# ---------- 配置文件检查 ----------
$cfgPath = if ($Config){ $Config } else { 'config.json' }
if (-not (Test-Path -LiteralPath $cfgPath)){ Die "配置文件不存在: $cfgPath (可运行 build.ps1 生成)" }

# ---------- 端口占用提示 ----------
$port = 8990
try {
    $cfgObj = Get-Content -Raw -Encoding UTF8 -LiteralPath $cfgPath | ConvertFrom-Json
    if ($cfgObj.port){ $port = [int]$cfgObj.port }
} catch { Warn "解析 $cfgPath 失败, 端口占用检查跳过。" }
if (Get-Command Get-NetTCPConnection -ErrorAction SilentlyContinue){
    $inUse = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    if ($inUse){ Warn "端口 $port 已被占用, 服务可能启动失败 (可先 -Stop 或改端口)。" }
}

# ---------- 组装参数 / 环境 ----------
$argList = @()
if ($Config){      $argList += @('-c', $Config) }
if ($Credentials){ $argList += @('--credentials', $Credentials) }
$env:RUST_LOG = $LogLevel

Info "可执行文件: $exe"
Info "RUST_LOG=$LogLevel  端口=$port"

# ---------- 启动 ----------
if ($Background){
    New-Item -ItemType Directory -Force -Path 'logs' | Out-Null
    $out = 'logs\kiro-rs.out.log'
    $err = 'logs\kiro-rs.err.log'
    # 注意: -ArgumentList 不接受空数组, 故用 splat 按需附加
    $spArgs = @{
        FilePath               = $exe
        WorkingDirectory       = $PSScriptRoot
        WindowStyle            = 'Hidden'
        PassThru               = $true
        RedirectStandardOutput = $out
        RedirectStandardError  = $err
    }
    if ($argList.Count -gt 0){ $spArgs['ArgumentList'] = $argList }
    $p = Start-Process @spArgs
    Start-Sleep -Seconds 1
    if (-not $p -or $p.HasExited){
        Warn "进程启动失败或立即退出, 最近日志:"
        Get-Content -LiteralPath $err -Tail 20 -ErrorAction SilentlyContinue
        Die "后台启动失败。"
    }
    Good "已后台启动, PID=$($p.Id)"
    Write-Host "  管理页: http://127.0.0.1:$port/admin"
    Write-Host "  日志:   $out"
    Write-Host "  停止:   powershell -ExecutionPolicy Bypass -File start.ps1 -Stop"
} else {
    Good "前台启动 (Ctrl+C 停止)。看到启动横幅即为就绪, 常驻运行属正常现象。"
    Write-Host "  管理页: http://127.0.0.1:$port/admin"
    Write-Host ("-" * 60)
    & $exe @argList
}
