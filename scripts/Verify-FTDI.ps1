param([switch]$SkipCompatibility)
$ErrorActionPreference = 'Stop'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path

$PythonExe = 'python'
$PythonArgs = @()
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  if (Get-Command py -ErrorAction SilentlyContinue) {
    $PythonExe = 'py'
    $PythonArgs = @('-3')
  } else {
    throw 'Python 3.10+ is required. Install it before running FTDI.'
  }
}
Push-Location $Root
try {
  Write-Host '[FTDI] Python version:'
  & $PythonExe @PythonArgs --version
  & $PythonExe @PythonArgs -c 'import sys; assert sys.version_info >= (3,10), "Python 3.10+ required"'
  if ($LASTEXITCODE -ne 0) { throw 'Unsupported Python version' }
  Write-Host '[FTDI] Core baseline suite:'
  & $PythonExe @PythonArgs -W error::ResourceWarning -m unittest discover -s tests -q
  if ($LASTEXITCODE -ne 0) { throw 'Core tests failed' }
  if (-not $SkipCompatibility) {
    Push-Location (Join-Path $Root 'reference_s07_0041')
    try {
      Write-Host '[FTDI] Frozen S07 compatibility suite:'
      & $PythonExe @PythonArgs -W error::ResourceWarning -m unittest discover -s tests -q
      if ($LASTEXITCODE -ne 0) { throw 'S07 compatibility tests failed' }
    } finally { Pop-Location }
  }
  Write-Host '[FTDI] CLI smoke test:'
  & $PythonExe @PythonArgs -m ftdi_core --help | Out-Null
  if ($LASTEXITCODE -ne 0) { throw 'FTDI CLI smoke failed' }
  Write-Host '[FTDI] PASS: tests and CLI completed.' -ForegroundColor Green
} finally {
  Pop-Location
}
