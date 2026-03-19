param(
    [string]$InputDir = ".\csv",
    [string]$Pattern = "*.csv",
    [int]$BatchSize = 1000,
    [int]$BatchNo = 0,
    [string]$WorkDir = ".\data\scheduler\member_sync",
    [string]$ManifestFile = "",
    [string]$StateFile = "",
    [string]$Username = $env:ICSP_USERNAME,
    [string]$Password = $env:ICSP_PASSWORD,
    [switch]$ForceRefreshManifest,
    [switch]$PrepareManifestOnly,
    [switch]$ForceRerun
)

$ErrorActionPreference = "Stop"
[Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [Console]::OutputEncoding
chcp 65001 | Out-Null
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:PYTHONUNBUFFERED = "1"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoDir = (Resolve-Path (Join-Path $scriptDir "..")).Path
Set-Location $repoDir

$pythonArgs = @(
    "scripts/run_member_sync_batch.py",
    "--input-dir", $InputDir,
    "--pattern", $Pattern,
    "--batch-size", $BatchSize,
    "--work-dir", $WorkDir
)

if ($BatchNo -gt 0) {
    $pythonArgs += @("--batch-no", $BatchNo)
}
if ($ManifestFile) {
    $pythonArgs += @("--manifest-file", $ManifestFile)
}
if ($StateFile) {
    $pythonArgs += @("--state-file", $StateFile)
}
if ($Username) {
    $pythonArgs += @("--username", $Username)
}
if ($Password) {
    $pythonArgs += @("--password", $Password)
}
if ($ForceRefreshManifest) {
    $pythonArgs += "--force-refresh-manifest"
}
if ($PrepareManifestOnly) {
    $pythonArgs += "--prepare-manifest-only"
}
if ($ForceRerun) {
    $pythonArgs += "--force-rerun"
}

python @pythonArgs
exit $LASTEXITCODE
