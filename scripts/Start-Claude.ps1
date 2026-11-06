$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
if (-not (Get-Command claude -ErrorAction SilentlyContinue)) {
  throw 'Claude Code is not on PATH. See https://code.claude.com/docs/en/overview and reopen PowerShell.'
}
Push-Location $Root
try {
  Write-Host '[FTDI] Opening Claude Code from:' $Root
  Write-Host '[FTDI] CLAUDE.md will provide the project constraints. No auto-approval flags are used.'
  & claude
} finally { Pop-Location }
