<#
.SYNOPSIS
  Download host-ops worker from GitHub Releases into %LOCALAPPDATA% (no cargo).
  Open http://127.0.0.1:8788/ — manage directories & models in the WebUI.
  Config: %APPDATA%\pi-host-ops\config.json (never inside project folders).
#>
[CmdletBinding()]
param(
  [string]$Repo = "ModerRAS/pi-host-ops-server",
  [string]$Version = "latest",
  [string]$InstallDir = "$env:LOCALAPPDATA\pi-host-ops\worker",
  [switch]$Run
)
$ErrorActionPreference = "Stop"
function Die([string]$m){ Write-Host "ERROR: $m" -ForegroundColor Red; exit 1 }

$assetName = "worker-windows-x64.exe"
$api = "https://api.github.com/repos/$Repo"
$releaseUrl = if ($Version -eq "latest") { "$api/releases/latest" } else {
  $tag = if ($Version.StartsWith("v")) { $Version } else { "v$Version" }
  "$api/releases/tags/$tag"
}
Write-Host "Resolving $releaseUrl ..."
try {
  $release = Invoke-RestMethod -Uri $releaseUrl -Headers @{ Accept="application/vnd.github+json"; "User-Agent"="pi-host-ops-install" }
} catch { Die "Failed to fetch release (create a v* tag / run Release workflow first). $_" }

$asset = $release.assets | Where-Object { $_.name -eq $assetName } | Select-Object -First 1
if (-not $asset) {
  $names = ($release.assets | ForEach-Object { $_.name }) -join ", "
  Die "Asset '$assetName' not in $($release.tag_name). Available: $names"
}

New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
$dest = Join-Path $InstallDir "worker.exe"
$tmp = "$dest.download"
Write-Host "Downloading $($asset.browser_download_url)"
Invoke-WebRequest -Uri $asset.browser_download_url -OutFile $tmp -UseBasicParsing
Move-Item -Force $tmp $dest

$run = Join-Path $InstallDir "run-worker.ps1"
@"
`$ErrorActionPreference = 'Stop'
Set-Location '$InstallDir'
Write-Host 'WebUI: http://127.0.0.1:8788/  config:' ([IO.Path]::Combine(`$env:APPDATA, 'pi-host-ops', 'config.json'))
& '$dest'
"@ | Set-Content -Path $run -Encoding UTF8

# Also copy repo run-worker.ps1 helper if present next to this script
$repoRun = Join-Path $PSScriptRoot "run-worker.ps1"
if (Test-Path $repoRun) { Copy-Item -Force $repoRun (Join-Path $InstallDir "run-worker.ps1") }

Write-Host "Installed $($release.tag_name) -> $dest"
Write-Host "Config: $env:APPDATA\pi-host-ops\config.json"
if ($Run) { & $dest } else { Write-Host "Start: powershell -File `"$run`"" }
