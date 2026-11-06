param(
  [Parameter(Mandatory=$true)][string]$Archive,
  [Parameter(Mandatory=$true)][string]$Checksum
)
$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '..')
try {
  python -W error::ResourceWarning -m unittest discover -s tests -q
  if ($LASTEXITCODE -ne 0) { throw 'Core tests failed' }
  Push-Location reference_s07_0041
  try {
    python -W error::ResourceWarning -m unittest discover -s tests -q
    if ($LASTEXITCODE -ne 0) { throw 'Compatibility tests failed' }
  } finally { Pop-Location }
  python -m ftdi_core prepare --archive $Archive --checksum $Checksum --outdir data\btc_50k --limit 50000
  if ($LASTEXITCODE -ne 0) { throw 'Prepare failed; existing outputs are never overwritten' }
  python -m ftdi_core replay --lane 'BTC=data\btc_50k\BTCUSDT-aggTrades-50000-core-v1.jsonl' --outdir runs\btc_50k
  if ($LASTEXITCODE -ne 0) { throw 'Replay incomplete or blocked' }
  python -m ftdi_core audit --outdir runs\btc_50k
  if ($LASTEXITCODE -ne 0) { throw 'Audit failed' }
  Write-Host 'FTDI-Core local replay and integrity audit PASS'
} finally { Pop-Location }
