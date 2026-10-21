<#
.SYNOPSIS
  Run installed worker.exe (WebUI on 127.0.0.1:8788). Config in %APPDATA%\pi-host-ops.
#>
[CmdletBinding()]
param([string]$InstallDir = "")
$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($InstallDir)) {
  $InstallDir = Join-Path $env:LOCALAPPDATA "pi-host-ops\worker"
}
$exe = Join-Path $InstallDir "worker.exe"
if (-not (Test-Path $exe)) {
  throw "worker.exe not found in $InstallDir — run scripts\install-worker.ps1 first (GitHub Releases)."
}
Write-Host "Starting $exe"
Write-Host "WebUI http://127.0.0.1:8788/  config $env:APPDATA\pi-host-ops\config.json"
Set-Location $InstallDir
& $exe
