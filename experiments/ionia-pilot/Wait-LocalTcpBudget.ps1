#requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$EvidencePath,
    [ValidateRange(30,1800)][int]$WaitSeconds=600,
    [ValidateRange(1,30)][int]$PollSeconds=5
)
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
if([Environment]::OSVersion.Platform -ne [PlatformID]::Win32NT){throw 'Windows TCP gate only'}
$samples=[Collections.Generic.List[object]]::new()
$until=[DateTime]::UtcNow.AddSeconds($WaitSeconds)
$record=[ordered]@{status='waiting';maxObservedPortFraction=0.10;samples=$samples}
try {
    while($true){
        $range=@(& netsh int ipv4 show dynamicport tcp)
        if($LASTEXITCODE -ne 0){throw 'Cannot inspect dynamic TCP range'}
        $numbers=@($range | ForEach-Object {if($_ -match ':\s*(\d+)\s*$'){[int]$Matches[1]}})
        if($numbers.Count -ne 2 -or $numbers[1] -lt 1){throw 'Cannot parse dynamic TCP range'}
        $ip=[Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties()
        $ports=[Collections.Generic.HashSet[int]]::new();$timeWait=0
        foreach($c in $ip.GetActiveTcpConnections()){
            if($c.State -eq [Net.NetworkInformation.TcpState]::TimeWait){$timeWait++}
            $port=$c.LocalEndPoint.Port
            if($port -ge $numbers[0] -and $port -lt ($numbers[0]+$numbers[1])){[void]$ports.Add($port)}
        }
        foreach($listener in $ip.GetActiveTcpListeners()){
            if($listener.Port -ge $numbers[0] -and $listener.Port -lt ($numbers[0]+$numbers[1])){[void]$ports.Add($listener.Port)}
        }
        $ready=$ports.Count -le [Math]::Floor($numbers[1]*0.10)
        $samples.Add([pscustomobject]@{atUtc=[DateTime]::UtcNow.ToString('o');start=$numbers[0];size=$numbers[1];observedUniqueDynamicPorts=$ports.Count;timeWaitConnections=$timeWait;ready=$ready})
        Write-Output ("V01 TCP group gate: dynamic ports {0}/{1}, TIME_WAIT {2}" -f $ports.Count,$numbers[1],$timeWait)
        if($ready){$record.status='passed';break}
        if([DateTime]::UtcNow -ge $until){throw 'TCP group port budget did not recover; tests must not be skipped or passed'}
        Start-Sleep -Seconds $PollSeconds
    }
} catch {$record.status='failed';$record.failure=$_.Exception.Message;throw}
finally {$record | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $EvidencePath -Encoding UTF8}
