[CmdletBinding()]
param(
    [string]$RedisServer = $(if ($env:GTE_REDIS_SERVER) { $env:GTE_REDIS_SERVER } else { "redis-server" }),
    [string]$RedisCli = $(if ($env:GTE_REDIS_CLI) { $env:GTE_REDIS_CLI } else { "redis-cli" }),
    [int]$Port = 6379,
    [string]$PidFile = "$PSScriptRoot\redis-$Port.pid",
    [string]$LogFile = "$PSScriptRoot\redis-$Port.log",
    [string]$DataDir = "$env:TEMP\go-taskengine-redis-$Port"
)

$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
$process = Start-Process -FilePath $RedisServer -ArgumentList @(
    "--port", $Port,
    "--dir", $DataDir,
    "--save", "",
    "--appendonly", "no",
    "--loglevel", "notice",
    "--logfile", $LogFile
) -PassThru
Set-Content -Path $PidFile -Value $process.Id

$deadline = (Get-Date).AddSeconds(5)
while ((Get-Date) -lt $deadline) {
    try {
        $pong = & $RedisCli -p $Port ping 2>$null
        if ($pong -eq "PONG") {
            Write-Output "Redis started on 127.0.0.1:$Port (pid=$($process.Id), data_dir=$DataDir)"
            exit 0
        }
    } catch {
        # Redis is still starting or redis-cli is not ready.
    }
    Start-Sleep -Milliseconds 100
}

if (!$process.HasExited) {
    Stop-Process -Id $process.Id -Force
}
throw "Redis did not start on port $Port; see $LogFile"
