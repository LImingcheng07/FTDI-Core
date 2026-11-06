$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '..')
try {
  python -W error::ResourceWarning -m unittest discover -s tests -q
  if ($LASTEXITCODE -ne 0) { throw 'Core tests failed' }
  Push-Location reference_s07_0041
  try {
    python -W error::ResourceWarning -m unittest discover -s tests -q
    if ($LASTEXITCODE -ne 0) { throw 'S07 compatibility tests failed' }
  } finally { Pop-Location }
  Write-Host 'FTDI-Core v0.1 local suites PASS'
} finally { Pop-Location }
