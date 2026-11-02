param(
    [Parameter(Mandatory=$true)][string]$Bundle,
    [Parameter(Mandatory=$true)][string]$NewOutput,
    [string]$Java = 'java'
)
$ErrorActionPreference = 'Stop'
$bundlePath = (Resolve-Path -LiteralPath $Bundle).Path
$outputPath = [IO.Path]::GetFullPath($NewOutput)
if (Test-Path -LiteralPath $outputPath) { throw 'NewOutput must not exist; old results are retained' }
[IO.Directory]::CreateDirectory($outputPath) | Out-Null
$javaPath = (Get-Command $Java -ErrorAction Stop).Source
$mainClass = 'io.openkache.server.benchmark.pilot.PilotMain'
function Invoke-Pilot([string[]]$CommandArgs) {
    & $javaPath '-cp' "$bundlePath/lib/*" $mainClass @CommandArgs
    if ($LASTEXITCODE -ne 0) { throw "Pilot command failed: $CommandArgs" }
}
# Reserve three distinct ports, release only immediately before starting the replicas.
$listeners = @(); $ownedProcesses = @(); $runPath = Join-Path $outputPath 'run'
try {
    $ports = @(1..3 | ForEach-Object {
        $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 0)
        $listener.Start(); $script:listeners += $listener
        $listener.LocalEndpoint.Port
    })
    $topology = "schema=ionia-pilot-topology-v1`nleader.id=1`nreplicas=1@127.0.0.1:$($ports[0]),2@127.0.0.1:$($ports[1]),3@127.0.0.1:$($ports[2])`n"
    [IO.File]::WriteAllText((Join-Path $outputPath 'topology.properties'),$topology,[Text.UTF8Encoding]::new($false))
    Invoke-Pilot @('dataset',"$bundlePath/tools/smoke.properties",(Join-Path $outputPath 'dataset'))
    Invoke-Pilot @('prepare',(Join-Path $outputPath 'topology.properties'),(Join-Path $outputPath 'dataset'),$runPath)
    Invoke-Pilot @('verify',$bundlePath,$runPath)
    foreach ($listener in $listeners) { $listener.Stop() }; $listeners = @()
    foreach ($id in 1..3) {
        $argsLine = '-cp "' + $bundlePath + '/lib/*" ' + $mainClass + ' node "' + $bundlePath + '" "' + $runPath + '" ' + $id
        $process = Start-Process -FilePath $javaPath -ArgumentList $argsLine -WindowStyle Hidden -PassThru -RedirectStandardOutput "$runPath/state/node-$id.stdout.log" -RedirectStandardError "$runPath/state/node-$id.stderr.log"
        $ownedProcesses += [pscustomobject]@{ Id=$id; Process=$process; Started=$process.StartTime }
    }
    $until = [DateTime]::UtcNow.AddSeconds(25)
    foreach ($entry in $ownedProcesses) {
        $ready = "$runPath/state/node-$($entry.Id).ready.properties"
        while (!(Test-Path -LiteralPath $ready)) {
            if ($entry.Process.HasExited) { throw "Node $($entry.Id) exited; inspect its stderr" }
            if ([DateTime]::UtcNow -gt $until) { throw 'Node startup timed out' }
            Start-Sleep -Milliseconds 100
        }
        Copy-Item -LiteralPath $ready -Destination "$runPath/ready/node-$($entry.Id).ready.properties"
    }
    Invoke-Pilot @('wait',$bundlePath,$runPath)
    Invoke-Pilot @('run',$bundlePath,$runPath)
} finally {
    foreach ($listener in $listeners) { $listener.Stop() }
    foreach ($entry in $ownedProcesses) {
        $stop = "$runPath/state/node-$($entry.Id).stop"
        if (!(Test-Path -LiteralPath $stop)) { [IO.File]::WriteAllText($stop,'stop') }
    }
    $stopErrors = @()
    foreach ($entry in $ownedProcesses) {
        $process=$entry.Process
        if (!$process.WaitForExit(20000)) {
            # Only terminate a process returned by this exact launch, after identity revalidation.
            $actual=Get-Process -Id $process.Id -ErrorAction SilentlyContinue
            $details=Get-CimInstance Win32_Process -Filter "ProcessId=$($process.Id)"
            if ($null -ne $actual -and $actual.StartTime -eq $entry.Started -and $details.CommandLine.Contains($runPath) -and $details.CommandLine.Contains($mainClass)) {
                Stop-Process -Id $process.Id
            }
            $stopErrors += "Node $($entry.Id) did not stop gracefully"
        } elseif ($process.ExitCode -ne 0 -or !(Test-Path -LiteralPath "$runPath/state/node-$($entry.Id).stopped")) {
            $stopErrors += "Node $($entry.Id) close was not confirmed"
        }
    }
    if ($stopErrors.Count -gt 0) { throw ($stopErrors -join '; ') }
}
Write-Host "PILOT_REHEARSAL_PASS=$runPath"
