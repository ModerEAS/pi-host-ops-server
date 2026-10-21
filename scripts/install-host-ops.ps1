<#
.SYNOPSIS
  Download host-ops dispatcher CLI from GitHub Releases into %LOCALAPPDATA% (no cargo).
  Config: %APPDATA%\pi-host-ops\dispatcher.json (never inside project folders).
#>
[CmdletBinding()]
param(
  [string]$Repo = "ModerRAS/pi-host-ops-server",
  [string]$Version = "latest",
  [string]$InstallDir = "$env:LOCALAPPDATA\pi-host-ops\bin"
)
$ErrorActionPreference = "Stop"
function Die([string]$m){ Write-Host "ERROR: $m" -ForegroundColor Red; exit 1 }

$assetName = "host-ops-windows-x64.exe"
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
$dest = Join-Path $InstallDir "host-ops.exe"
$tmp = "$dest.download"
Write-Host "Downloading $($asset.browser_download_url)"
Invoke-WebRequest -Uri $asset.browser_download_url -OutFile $tmp -UseBasicParsing
Move-Item -Force $tmp $dest

Write-Host "Installed $($release.tag_name) -> $dest"
Write-Host "Config: $env:APPDATA\pi-host-ops\dispatcher.json"
Write-Host "Add to PATH: $InstallDir"
Write-Host "Then: host-ops config set --url http://127.0.0.1:8787 --token <DISPATCHER_TOKEN>"
