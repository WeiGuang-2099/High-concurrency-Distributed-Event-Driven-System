<#
  Imports the shared-*.yml configs into Nacos so the microservices can boot.

  docker-compose starts Nacos but does NOT auto-publish these configs. Each
  service's bootstrap.yml pulls shared-db/redis/kafka/jwt from Nacos on
  startup, so the configs must exist there first or the service fails to start.

  Run this once after `docker-compose up -d nacos` reports healthy:
    powershell -ExecutionPolicy Bypass -File deploy/nacos/import-configs.ps1
#>
param(
    [string]$NacosAddr = "http://localhost:8848",
    [string]$Group     = "DEFAULT_GROUP"
)

$ErrorActionPreference = "Stop"
$files = Get-ChildItem -Path $PSScriptRoot -Filter "shared-*.yml" | Sort-Object Name

if (-not $files) {
    Write-Error "No shared-*.yml found next to this script ($PSScriptRoot)"
    exit 1
}

$failed = 0
foreach ($f in $files) {
    $content = Get-Content -LiteralPath $f.FullName -Raw -Encoding UTF8
    $body = @{
        dataId  = $f.Name
        group   = $Group
        content = $content
        type    = "yaml"
    }
    $params = @{
        Uri         = "$NacosAddr/nacos/v1/cs/configs"
        Method      = "Post"
        Body        = $body
        ContentType = "application/x-www-form-urlencoded;charset=UTF-8"
    }
    try {
        $resp = Invoke-RestMethod @params
        if ("$resp".Trim() -eq "true") {
            Write-Host "[OK]   $($f.Name)  (group=$Group)"
        } else {
            Write-Host "[WARN] $($f.Name) -> $resp"
            $failed++
        }
    } catch {
        Write-Host "[FAIL] $($f.Name): $($_.Exception.Message)"
        $failed++
    }
}

Write-Host ""
if ($failed -eq 0) {
    Write-Host "All configs published. Services can now start."
} else {
    Write-Host "$failed config(s) failed. Is Nacos up? Check $NacosAddr/nacos"
    exit 1
}
