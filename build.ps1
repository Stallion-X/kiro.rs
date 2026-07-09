#Requires -Version 5.1
<#
.SYNOPSIS
    kiro-rs 一键编译 + 环境配置脚本 (Windows / PowerShell)

.DESCRIPTION
    执行步骤:
      1) 检查工具链 (cargo / node / pnpm)
      2) 配置 crates.io 国内镜像 (.cargo/config.toml, 仅在缺失时创建)
      3) 构建前端 admin-ui -> admin-ui/dist  (自动兼容 pnpm 9 / 10)
      4) 编译后端 -> target/release/kiro-rs.exe
      5) 生成 config.json / credentials.json (仅在缺失时, 不覆盖已有文件)

.PARAMETER NoMirror
    跳过 crates.io 镜像配置 (海外网络或已有全局镜像时使用)

.PARAMETER DebugBuild
    使用 debug 构建 (编译更快, 产物在 target/debug/kiro-rs.exe)

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File build.ps1

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File build.ps1 -NoMirror -DebugBuild
#>
[CmdletBinding()]
param(
    [switch]$NoMirror,
    [switch]$DebugBuild
)

# 原生命令(cargo/pnpm)会往 stderr 打印进度, 不应因此中断脚本; 统一手动检查 $LASTEXITCODE
$ErrorActionPreference = 'Continue'
Set-Location -LiteralPath $PSScriptRoot

function Info([string]$m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Good([string]$m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn([string]$m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Die ([string]$m){ Write-Host "[FAIL] $m" -ForegroundColor Red; exit 1 }

# 写 UTF-8 无 BOM 文件 (serde_json / toml 不接受 BOM)
function Write-Utf8NoBom([string]$Path, [string]$Content){
    $enc  = New-Object System.Text.UTF8Encoding($false)
    $full = [System.IO.Path]::GetFullPath((Join-Path (Get-Location) $Path))
    [System.IO.File]::WriteAllText($full, $Content, $enc)
}

$sw = [System.Diagnostics.Stopwatch]::StartNew()
Write-Host "==== kiro-rs 一键编译 + 配置 ====" -ForegroundColor Magenta

# ---------- 1) 工具链检查 ----------
Info "检查工具链 (cargo / node / pnpm) ..."
foreach ($t in 'cargo','node','pnpm'){
    if (-not (Get-Command $t -ErrorAction SilentlyContinue)){
        Die "$t 未安装或不在 PATH 中, 请先安装后重试。"
    }
}
$pnpmVer = "$(pnpm --version 2>$null)".Trim()
$pnpmMajor = 0
if ($pnpmVer -match '^(\d+)\.'){ $pnpmMajor = [int]$Matches[1] }
Good "cargo=$("$(cargo --version 2>$null)".Trim())  node=$("$(node --version 2>$null)".Trim())  pnpm=$pnpmVer"

# ---------- 2) crates.io 镜像 ----------
if ($NoMirror){
    Info "已指定 -NoMirror, 跳过镜像配置。"
} else {
    $cargoCfg = '.cargo\config.toml'
    if (Test-Path -LiteralPath $cargoCfg){
        Info ".cargo/config.toml 已存在, 跳过 (如需改回官方源可删除它)。"
    } else {
        New-Item -ItemType Directory -Force -Path '.cargo' | Out-Null
        $mirror = @"
# crates.io 国内镜像 (rsproxy.cn) - 由 build.ps1 生成
# 仅对本项目生效; 删除本文件即可恢复默认 crates.io。
[source.crates-io]
replace-with = "rsproxy-sparse"

[source.rsproxy]
registry = "https://rsproxy.cn/crates.io-index"

[source.rsproxy-sparse]
registry = "sparse+https://rsproxy.cn/index/"

[registries.rsproxy]
index = "https://rsproxy.cn/crates.io-index"

[net]
git-fetch-with-cli = true
"@
        Write-Utf8NoBom $cargoCfg $mirror
        Good "已创建 .cargo/config.toml (rsproxy.cn 镜像)"
    }
}

# ---------- 3) 前端 admin-ui ----------
Info "构建前端 admin-ui ..."
Push-Location -LiteralPath 'admin-ui'
try {
    $env:COREPACK_ENABLE_AUTO_PIN = '0'   # 防止 corepack 往 package.json 写 packageManager
    $ws  = 'pnpm-workspace.yaml'
    $bak = 'pnpm-workspace.yaml.disabled'

    # 自愈: 上次异常中断可能把 workspace 文件留在 .disabled
    if ((-not (Test-Path -LiteralPath $ws)) -and (Test-Path -LiteralPath $bak)){
        Rename-Item -LiteralPath $bak -NewName $ws -Force
        Warn "检测到上次残留的 $bak, 已自动还原为 $ws。"
    }

    # pnpm 9 无法解析 pnpm10 风格的 pnpm-workspace.yaml(allowBuilds) -> 临时移开
    # (构建脚本审批已由 .npmrc 的 approve-builds 与 package.json 的 onlyBuiltDependencies 覆盖)
    $moved = $false
    if ($pnpmMajor -gt 0 -and $pnpmMajor -lt 10 -and (Test-Path -LiteralPath $ws)){
        Rename-Item -LiteralPath $ws -NewName $bak -Force
        $moved = $true
        Warn "pnpm $pnpmVer (<10): 已临时移开 $ws (构建后自动还原)。"
    }

    try {
        Info "pnpm install --frozen-lockfile ..."
        pnpm install --frozen-lockfile
        if ($LASTEXITCODE -ne 0){
            Warn "--frozen-lockfile 失败 (exit $LASTEXITCODE), 改用普通 install ..."
            pnpm install
            if ($LASTEXITCODE -ne 0){ Die "pnpm install 失败 (exit $LASTEXITCODE)" }
        }
        Info "pnpm build ..."
        pnpm build
        if ($LASTEXITCODE -ne 0){ Die "pnpm build 失败 (exit $LASTEXITCODE)" }
    } finally {
        if ($moved -and (Test-Path -LiteralPath $bak)){
            Rename-Item -LiteralPath $bak -NewName $ws -Force
            Info "已还原 $ws"
        }
    }

    if (-not (Test-Path -LiteralPath 'dist\index.html')){ Die "admin-ui/dist/index.html 未生成, 前端构建失败。" }
    Good "前端构建完成 -> admin-ui/dist"
} finally {
    Pop-Location
}

# ---------- 4) 后端编译 ----------
$outDir = if ($DebugBuild){ 'debug' } else { 'release' }
Info "编译后端 cargo build $(if(-not $DebugBuild){'--release'}) ... (首次编译较慢, 后续增量很快)"
if ($DebugBuild){ cargo build } else { cargo build --release }
if ($LASTEXITCODE -ne 0){ Die "cargo build 失败 (exit $LASTEXITCODE)" }
$exe = "target\$outDir\kiro-rs.exe"
if (-not (Test-Path -LiteralPath $exe)){ Die "$exe 未生成。" }
Good "后端编译完成 -> $exe"

# ---------- 5) 配置文件 (仅缺失时生成, 不覆盖) ----------
if (-not (Test-Path -LiteralPath 'config.json')){
    $cfg = @"
{
  "host": "127.0.0.1",
  "port": 8990,
  "apiKey": "sk-kiro-rs-qazWSXedcRFV123456",
  "region": "us-east-1",
  "tlsBackend": "rustls",
  "adminApiKey": "sk-admin-your-secret-key",
  "defaultEndpoint": "ide"
}
"@
    Write-Utf8NoBom 'config.json' $cfg
    Good "已生成 config.json (示例密钥! 对外使用前务必修改 apiKey / adminApiKey)"
} else {
    Info "config.json 已存在, 跳过 (不覆盖)。"
}
if (-not (Test-Path -LiteralPath 'credentials.json')){
    Write-Utf8NoBom 'credentials.json' "[]`r`n"
    Good "已生成 credentials.json (空数组; 启动后可在 /admin 添加 Kiro 凭据)"
} else {
    Info "credentials.json 已存在, 跳过 (不覆盖)。"
}

$sw.Stop()
Write-Host ""
Good ("全部完成! 用时 {0:N1} 秒。" -f $sw.Elapsed.TotalSeconds)
Write-Host "下一步:" -ForegroundColor Magenta
Write-Host "  1) 启动服务:   powershell -ExecutionPolicy Bypass -File start.ps1"
Write-Host "  2) 管理页面:   http://127.0.0.1:8990/admin  (用 adminApiKey 登录后添加 Kiro 凭据)"
Write-Host "  3) 或手动编辑 credentials.json 填入 refreshToken 等凭据"
