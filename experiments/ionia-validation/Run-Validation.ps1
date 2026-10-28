[CmdletBinding()]
param(
    [ValidateSet('Full','V02aTcp')][string]$Mode='Full',
    [ValidateRange(1,20)][int]$Repeat=1,
    [ValidateRange(30,1800)][int]$PortWaitSeconds=600,
    [ValidateRange(1,30)][int]$PollSeconds=5
)
$ErrorActionPreference='Stop'
$repoValidation=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
if(-not $env:JAVA_HOME){throw 'Set JAVA_HOME to a Java 21 installation.'}
$onWindowsValidation=[Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT
if(-not $onWindowsValidation){throw 'This helper requires Windows. On Linux/WSL run the Gradle commands directly.'}
$outputValidation=Join-Path $repoValidation ('build/ionia-validation/'+(Get-Date -Format 'yyyyMMdd-HHmmss')+'-'+[Guid]::NewGuid().ToString('N').Substring(0,8))
New-Item -ItemType Directory -Path $outputValidation -Force | Out-Null
$portSamplesValidation=[Collections.Generic.List[object]]::new()
function Get-PortUseValidation {
    # Parse numeric output without depending on the OS display language.
    $portOutputValidation=@(& netsh int ipv4 show dynamicport tcp)
    if($LASTEXITCODE -ne 0){throw 'Cannot read the Windows TCP dynamic port range.'}
    $numbersValidation=@($portOutputValidation | ForEach-Object {if($_ -match ':\s*(\d+)\s*$'){[int]$Matches[1]}})
    if($numbersValidation.Count -ne 2 -or $numbersValidation[1] -lt 1){throw 'Cannot parse the Windows TCP dynamic port range.'}
    $startValidation=$numbersValidation[0];$sizeValidation=$numbersValidation[1];$endValidation=$startValidation+$sizeValidation
    $ipValidation=[Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties()
    $allValidation=@($ipValidation.GetActiveTcpConnections())
    $portsValidation=[Collections.Generic.HashSet[int]]::new()
    $timeWaitValidation=0
    foreach($itemValidation in $allValidation){
        if($itemValidation.State -eq [Net.NetworkInformation.TcpState]::TimeWait){$timeWaitValidation++}
        $portValidation=$itemValidation.LocalEndPoint.Port
        if($portValidation -ge $startValidation -and $portValidation -lt $endValidation){[void]$portsValidation.Add($portValidation)}
    }
    foreach($endpointValidation in $ipValidation.GetActiveTcpListeners()){
        if($endpointValidation.Port -ge $startValidation -and $endpointValidation.Port -lt $endValidation){[void]$portsValidation.Add($endpointValidation.Port)}
    }
    return [pscustomobject]@{atUtc=[DateTime]::UtcNow.ToString('o');start=$startValidation;size=$sizeValidation;observedUniqueDynamicPorts=$portsValidation.Count;timeWaitConnections=$timeWaitValidation;ready=($portsValidation.Count -le [Math]::Floor($sizeValidation*0.10))}
}
function Wait-PortBudgetValidation([int]$iterationValidation){
    if(-not $onWindowsValidation){return}
    $untilValidation=[DateTime]::UtcNow.AddSeconds($PortWaitSeconds)
    while($true){
        $sampleValidation=Get-PortUseValidation
        $portSamplesValidation.Add($sampleValidation)
        $portSamplesValidation | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $outputValidation 'port-samples.json') -Encoding UTF8
        Write-Host ('TCP preflight {0}/{1}: dynamic ports observed {2}/{3}, TIME_WAIT {4}' -f $iterationValidation,$Repeat,$sampleValidation.observedUniqueDynamicPorts,$sampleValidation.size,$sampleValidation.timeWaitConnections)
        if($sampleValidation.ready){return}
        if([DateTime]::UtcNow -ge $untilValidation){throw 'TCP port budget did not recover before the wait limit; tests were not skipped or marked as passed. Close heavy TCP workloads and retry later.'}
        Start-Sleep -Seconds $PollSeconds
    }
}
$argsValidation=@('build',':cache-server:installDist','--rerun-tasks','--console=plain')
if($Mode -eq 'V02aTcp'){$argsValidation=@(':cache-server:test','--tests','*IoniaThirdTcpConcurrencyTest','--tests','*IoniaReadTransportFailureTest','--rerun-tasks','--console=plain')}
$manifestValidation=[ordered]@{mode=$Mode;repeat=$Repeat;javaHome=$env:JAVA_HOME;windowsPortGate=$onWindowsValidation;maxObservedPortFraction=0.10;gradleArgs=$argsValidation;status='running';completedRuns=0}
try{
    Push-Location $repoValidation
    for($runValidation=1;$runValidation -le $Repeat;$runValidation++){
        Wait-PortBudgetValidation $runValidation
        $manifestValidation | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $outputValidation 'manifest.json') -Encoding UTF8
        & (Join-Path $repoValidation 'gradlew.bat') @argsValidation 2>&1 | Tee-Object -FilePath (Join-Path $outputValidation ('run-'+$runValidation+'.log')) | Out-Host
        $exitValidation=$LASTEXITCODE
        $modulesValidation=if($Mode -eq 'Full'){@('cache-core','cache-server','experiment-runner')}else{@('cache-server')}
        foreach($moduleValidation in $modulesValidation){
            $xmlValidation=Join-Path $repoValidation ($moduleValidation+'/build/test-results/test')
            if(Test-Path -LiteralPath $xmlValidation){
                $destinationValidation=Join-Path $outputValidation ('run-'+$runValidation+'/'+$moduleValidation)
                New-Item -ItemType Directory -Path $destinationValidation -Force | Out-Null
                Get-ChildItem -LiteralPath $xmlValidation -Filter 'TEST-*.xml' | Copy-Item -Destination $destinationValidation
            }
        }
        if($exitValidation -ne 0){throw ('Gradle failed, exit code '+$exitValidation+'; see '+$outputValidation)}
        $manifestValidation.completedRuns=$runValidation
    }
    $manifestValidation.status='passed'
}catch{
    $manifestValidation.status='failed';$manifestValidation.failure=$_.Exception.Message
    throw
}finally{
    Pop-Location
    $manifestValidation | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $outputValidation 'manifest.json') -Encoding UTF8
    Write-Host ('Validation evidence: '+$outputValidation)
}
