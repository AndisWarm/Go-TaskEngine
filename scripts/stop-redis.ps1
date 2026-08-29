[CmdletBinding()]
param(
    [string]$RedisCli = $(if ($env:GTE_REDIS_CLI) { $env:GTE_REDIS_CLI } else { "redis-cli" }),
    [int]$Port = 6379,
    [string]$PidFile = "$PSScriptRoot\redis-$Port.pid",
    [string]$DataDir = "$env:TEMP\go-taskengine-redis-$Port"
)

$ErrorActionPreference = "Stop"
if (Test-Path $PidFile) {
    $processId = [int](Get-Content -Raw $PidFile)
    try {
        & $RedisCli -p $Port shutdown nosave 2>$null
    } catch {
        # Redis may already be stopped.
    }
    Start-Sleep -Milliseconds 100
    if (Get-Process -Id $processId -ErrorAction SilentlyContinue) {
        Stop-Process -Id $processId -Force
    }
    Remove-Item $PidFile -Force
    if (Test-Path $DataDir) {
        Remove-Item $DataDir -Recurse -Force
    }
    Write-Output "Redis stopped on port $Port"
} else {
    Write-Output "No Redis pid file found for port $Port"
}
