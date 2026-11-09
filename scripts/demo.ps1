# End-to-end demo against a running control plane and host.
#
#   Terminal 1:  dotnet run --project src/AlgoPlatform.ControlPlane -- --Url=http://127.0.0.1:5100
#   Terminal 2:  dotnet run --project src/AlgoPlatform.Host -- --HostId=host-1 --Url=http://127.0.0.1:5101 --ControlPlane=http://127.0.0.1:5100
#   Terminal 3:  ./scripts/demo.ps1
#
# Packages the sample market maker, uploads it, deploys and starts it, shows metrics and the journal,
# hits the kill switch, stops it, and replays the recorded session to prove it is deterministic.
param(
    [string]$ControlPlane = "http://127.0.0.1:5100",
    [string]$Configuration = "Release"
)
$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
$name = "mm-demo"

Write-Host "== build sample strategy"
dotnet build "$root/src/AlgoPlatform.Strategies.Samples" -c $Configuration -v q | Out-Null
$dll = Join-Path $root "src/AlgoPlatform.Strategies.Samples/bin/$Configuration/net8.0/AlgoPlatform.Strategies.Samples.dll"

Write-Host "== package (strategy.json + assembly in a zip)"
$stage = Join-Path ([IO.Path]::GetTempPath()) "algo-demo-pkg"
Remove-Item $stage -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory $stage | Out-Null
Copy-Item "$root/samples/market-maker/strategy.json", $dll $stage
$zip = Join-Path ([IO.Path]::GetTempPath()) "algo-demo.zip"
Remove-Item $zip -ErrorAction SilentlyContinue
Compress-Archive -Path "$stage/*" -DestinationPath $zip

Write-Host "== upload"
Invoke-RestMethod "$ControlPlane/packages" -Method Post -InFile $zip -ContentType "application/zip" | Format-List

Write-Host "== deploy + start"
(Invoke-RestMethod "$ControlPlane/strategies/$name/deploy" -Method Post).deployment | Format-List
Invoke-RestMethod "$ControlPlane/strategies/$name/start" -Method Post | Format-List
Start-Sleep -Seconds 3

Write-Host "== strategies"
(Invoke-RestMethod "$ControlPlane/strategies") | ForEach-Object { $_.strategy } | Format-Table name, status, ordersSent, ordersRejected, fills, liveOrders

Write-Host "== a few metrics"
(Invoke-RestMethod "$ControlPlane/metrics") -split "`n" | Where-Object { $_ -match "^algo_strategy_(orders_sent|fills|tick_to_order_seconds\{.*quantile=`"0.99`")" }

Write-Host "== kill switch"
Invoke-RestMethod "$ControlPlane/strategies/$name/kill" -Method Post | Format-List status, liveOrders
Start-Sleep -Seconds 1

Write-Host "== journal around the kill"
$j = Invoke-RestMethod "$ControlPlane/strategies/$name/journal"
$k = [array]::FindIndex([object[]]$j, [Predicate[object]] { param($e) $e.type -eq "In.Control" -and $e.detail -eq "Kill" })
$j[[Math]::Max(0, $k - 2)..[Math]::Min($j.Count - 1, $k + 6)] | Format-Table seq, type, detail -AutoSize

Write-Host "== stop"
Invoke-RestMethod "$ControlPlane/strategies/$name/stop" -Method Post | Format-List status

Write-Host "== replay the recorded session"
Invoke-RestMethod "$ControlPlane/replays" -Method Post -ContentType "application/json" -Body (@{ strategy = $name } | ConvertTo-Json) | Format-List
