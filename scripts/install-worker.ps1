<#
.SYNOPSIS
  Download host-ops worker from GitHub Releases into %LOCALAPPDATA% (no cargo).
  Open http://127.0.0.1:8788/ — manage directories & models in the WebUI.
  Config: %APPDATA%\pi-host-ops\config.json (never inside project folders).

  With -DispatcherToken (+ -HostOpsUrl) the script also enrolls multiplexed
  mode: creates the worker server-side, saves worker_token to the user
  config (UTF-8 without BOM — the Rust worker rejects BOM), and binds
  already-registered directory agents. Re-run after adding directories to
  bind the new ones. Without a token it stays binary-only (legacy
  per-directory heartbeat, flaps offline past a few dirs).
#>
[CmdletBinding()]
param(
  [string]$Repo = "ModerRAS/pi-host-ops-server",
  [string]$Version = "latest",
  [string]$InstallDir = "$env:LOCALAPPDATA\pi-host-ops\worker",
  [string]$DispatcherToken = "",
  [string]$HostOpsUrl = "",
  [string]$WorkerLabel = "",
  [string]$WorkerId = "",
  [switch]$ForceDownload,
  [switch]$Run
)
$ErrorActionPreference = "Stop"
function Die([string]$m){ Write-Host "ERROR: $m" -ForegroundColor Red; exit 1 }

function Sanitize-WorkerId([string]$s) {
  $t = ($s.ToLowerInvariant() -replace '[^a-z0-9]+','-').Trim('-')
  if ([string]::IsNullOrWhiteSpace($t)) { $t = 'worker-' + ([Guid]::NewGuid().ToString('N').Substring(0,8)) }
  return $t
}

function Merge-WorkerConfig([string]$hostOps, [string]$dispToken, [string]$workerId, [string]$workerToken) {
  # NOTE: UTF-8 *without* BOM — the Rust worker (serde_json) fails to parse BOM.
  $cfgDir = Join-Path $env:APPDATA 'pi-host-ops'
  New-Item -ItemType Directory -Force -Path $cfgDir | Out-Null
  $cfg = Join-Path $cfgDir 'config.json'
  $obj = $null
  if (Test-Path $cfg) {
    try {
      $raw = [System.IO.File]::ReadAllText($cfg)
      if (-not [string]::IsNullOrWhiteSpace($raw)) { $obj = $raw | ConvertFrom-Json }
    } catch { Die "Existing config unreadable: $cfg ($($_.Exception.Message))" }
  }
  if ($null -eq $obj) { $obj = [pscustomobject]@{ directories = @() } }
  $obj | Add-Member -NotePropertyName 'host_ops_url' -NotePropertyValue $hostOps -Force
  $obj | Add-Member -NotePropertyName 'dispatcher_token' -NotePropertyValue $dispToken -Force
  $obj | Add-Member -NotePropertyName 'worker_token' -NotePropertyValue $workerToken -Force
  $obj | Add-Member -NotePropertyName 'worker_id' -NotePropertyValue $workerId -Force
  if (-not ($obj.PSObject.Properties.Name -contains 'directories')) { $obj | Add-Member -NotePropertyName 'directories' -NotePropertyValue @() -Force }
  [System.IO.File]::WriteAllText($cfg, ($obj | ConvertTo-Json -Depth 10), (New-Object System.Text.UTF8Encoding $false))
  return $cfg
}

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
if ((Test-Path $dest) -and (-not $ForceDownload)) {
  Write-Host "Binary already present, skipping download (pass -ForceDownload to refresh; stop the worker first)."
} else {
  Write-Host "Downloading $($asset.browser_download_url)"
  Invoke-WebRequest -Uri $asset.browser_download_url -OutFile $tmp -UseBasicParsing
  try { Move-Item -Force $tmp $dest }
  catch {
    Remove-Item $tmp -Force -ErrorAction SilentlyContinue
    Die "Cannot replace $dest (is the worker running? stop it first, then re-run)."
  }
}

$runScript = Join-Path $InstallDir "run-worker.ps1"
@"
`$ErrorActionPreference = 'Stop'
Set-Location '$InstallDir'
Write-Host 'WebUI: http://127.0.0.1:8788/  config:' ([IO.Path]::Combine(`$env:APPDATA, 'pi-host-ops', 'config.json'))
& '$dest'
"@ | Set-Content -Path $runScript -Encoding UTF8

# Also copy repo run-worker.ps1 helper if present next to this script
$repoRun = Join-Path $PSScriptRoot "run-worker.ps1"
if (Test-Path $repoRun) { Copy-Item -Force $repoRun (Join-Path $InstallDir "run-worker.ps1") }

Write-Host "Installed $($release.tag_name) -> $dest"
Write-Host "Config: $env:APPDATA\pi-host-ops\config.json"

# --- multiplexed-mode enroll (needs dispatcher token; tokens never printed) ---
$dispToken = if ([string]::IsNullOrWhiteSpace($DispatcherToken)) { $env:DISPATCHER_TOKEN } else { $DispatcherToken }
$hostOps = if ([string]::IsNullOrWhiteSpace($HostOpsUrl)) { $env:HOST_OPS_URL } else { $HostOpsUrl }
if ([string]::IsNullOrWhiteSpace($dispToken)) {
  Write-Host "No dispatcher token: binary only. Re-run with -DispatcherToken (+ -HostOpsUrl) to auto-enroll multiplexed mode."
} else {
  if ([string]::IsNullOrWhiteSpace($hostOps)) { Die "Enroll needs -HostOpsUrl (or `$env:HOST_OPS_URL)." }
  $hostOps = $hostOps.TrimEnd('/')
  $cfgPath = Join-Path $env:APPDATA 'pi-host-ops\config.json'
  $savedToken = ""; $savedId = ""
  if (Test-Path $cfgPath) {
    try { $c = [System.IO.File]::ReadAllText($cfgPath) | ConvertFrom-Json; $savedToken = [string]$c.worker_token; $savedId = [string]$c.worker_id } catch {}
  }
  if ([string]::IsNullOrWhiteSpace($savedToken)) {
    $label = if ([string]::IsNullOrWhiteSpace($WorkerLabel)) { $env:COMPUTERNAME } else { $WorkerLabel }
    if ([string]::IsNullOrWhiteSpace($label)) { $label = 'worker' }
    $wid = if ([string]::IsNullOrWhiteSpace($WorkerId)) { Sanitize-WorkerId $label } else { Sanitize-WorkerId $WorkerId }
    try {
      $w = Invoke-RestMethod -Uri "$hostOps/v1/workers" -Method Post -Headers @{ Authorization = "Bearer $dispToken" } -ContentType 'application/json' -Body (@{ id = $wid; label = $label } | ConvertTo-Json -Compress) -TimeoutSec 15 -UseBasicParsing
    } catch {
      $detail = ''
      try { $detail = ($_ | Select-Object -ExpandProperty ErrorDetails -ErrorAction Stop).Message } catch {}
      if ($detail -match 'already exists') { Die "Worker id '$wid' already exists server-side. Pass a fresh -WorkerId." }
      Die "Create worker failed: $($_.Exception.Message) $detail"
    }
    $wid = [string]$w.id
    $savedToken = [string]$w.token
    if ([string]::IsNullOrWhiteSpace($savedToken)) { Die "Server did not return a worker token." }
    Write-Host "Worker enrolled: id=$wid (token saved to config, not shown)"
  } else {
    $wid = [string]$savedId
    if ([string]::IsNullOrWhiteSpace($wid)) {
      if ([string]::IsNullOrWhiteSpace($WorkerId)) { Die "Config has worker_token but no worker_id. Re-run with -WorkerId <id>." }
      $wid = Sanitize-WorkerId $WorkerId
    }
    Write-Host "Reusing saved worker_token for worker id=$wid"
  }
  $saved = Merge-WorkerConfig $hostOps $dispToken $wid $savedToken
  Write-Host "Config updated: $saved"
  try {
    $dirs = Invoke-RestMethod -Uri 'http://127.0.0.1:8788/api/directories' -TimeoutSec 5 -UseBasicParsing
    $ok = 0; $skip = 0
    foreach ($d in @($dirs)) {
      if ([string]::IsNullOrWhiteSpace([string]$d.agent_id)) { $skip++; continue }
      try {
        Invoke-RestMethod -Uri "$hostOps/v1/workers/$wid/agents" -Method Post -Headers @{ Authorization = "Bearer $dispToken" } -ContentType 'application/json' -Body (@{ agent_id = [string]$d.agent_id } | ConvertTo-Json -Compress) -TimeoutSec 10 -UseBasicParsing | Out-Null
        $ok++
      } catch {
        $m = ''
        try { $m = ($_ | Select-Object -ExpandProperty ErrorDetails -ErrorAction Stop).Message } catch {}
        if ($m -match 'already bound') { $ok++ } else { Write-Host "  bind failed for $($d.label): $m" }
      }
    }
    Write-Host "Agents bound: $ok (not yet registered: $skip). Dirs added later via WebUI need one re-run to bind."
  } catch {
    Write-Host "Worker WebUI not reachable: start it (-Run), add directories, then re-run this script to bind."
  }
}

if ($Run) { & $dest } else { Write-Host "Start: powershell -File `"$runScript`"" }
