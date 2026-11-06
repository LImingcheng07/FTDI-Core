param(
  [switch]$InitializeGit,
  [switch]$SkipTests
)
$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Push-Location $Root
try {
  Write-Host '[FTDI] Development root:' $Root
  if (Get-Command git -ErrorAction SilentlyContinue) {
    Write-Host '[FTDI] Git version:'
    & git --version
  } else {
    Write-Warning 'Git is not installed. Install Git for Windows when you want local version control.'
  }
  if (Get-Command claude -ErrorAction SilentlyContinue) {
    Write-Host '[FTDI] Claude Code version:'
    & claude --version
  } else {
    Write-Warning 'Claude Code was not found. Install it using the official Claude Code instructions and restart PowerShell.'
  }
  if ($InitializeGit) {
    if (-not (Get-Command git -ErrorAction SilentlyContinue)) { throw 'Git not installed' }
    if (-not (Test-Path -LiteralPath (Join-Path $Root '.git'))) {
      & git init -b main
      if ($LASTEXITCODE -ne 0) { throw 'git init failed' }
      Write-Host '[FTDI] Initialized a LOCAL Git repository; no remote configured.'
    } else {
      Write-Host '[FTDI] Existing Git repository left unchanged.'
    }
  }
  if (-not $SkipTests) {
    & (Join-Path $PSScriptRoot 'Verify-FTDI.ps1')
    if (-not $?) { throw 'Verification script failed' }
  }
  Write-Host '[FTDI] Setup complete. Run scripts/Start-Claude.ps1 to enter Claude Code.' -ForegroundColor Green
} finally { Pop-Location }
