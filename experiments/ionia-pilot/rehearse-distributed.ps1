param(
 [Parameter(Mandatory=$true)][string]$Bundle,
 [Parameter(Mandatory=$true)][string]$NewOutput,
 [ValidateSet(3,5)][int]$Replicas=3,
 [string]$Java='java',
 [string]$Dataset,
 [string]$Profile,
 [ValidateSet('None','StopLoadOne','MissingLoadOne')][string]$Fault='None'
)
$ErrorActionPreference='Stop'
$bundlePath=(Resolve-Path -LiteralPath $Bundle).Path
$outputPath=[IO.Path]::GetFullPath($NewOutput)
if(Test-Path -LiteralPath $outputPath){throw 'NewOutput must not exist'}
[IO.Directory]::CreateDirectory($outputPath)|Out-Null
& (Join-Path $PSScriptRoot 'Wait-LocalTcpBudget.ps1') -EvidencePath (Join-Path $outputPath 'tcp-preflight.json') -WaitSeconds 60 -PollSeconds 5
$javaPath=(Get-Command $Java -ErrorAction Stop).Source
$single='io.openkache.server.benchmark.pilot.PilotMain'
$distributed='io.openkache.server.benchmark.pilot.DistributedPilotMain'
$runPath=Join-Path $outputPath 'run'
$owned=[Collections.Generic.List[object]]::new()
$listeners=[Collections.Generic.List[Net.Sockets.TcpListener]]::new()
function Invoke-Java([string]$Class,[string[]]$CommandArgs){
 & $javaPath '-cp' "$bundlePath/lib/*" $Class @CommandArgs
 if($LASTEXITCODE -ne 0){throw "Pilot command failed: $Class $($CommandArgs[0])"}
}
function Start-Owned([string]$Role,[string]$Class,[string[]]$CommandArgs){
 $quoted=@($CommandArgs|ForEach-Object{'"'+$_+'"'})
 $arguments='-cp "'+$bundlePath+'/lib/*" '+$Class+' '+($quoted -join ' ')
 $process=Start-Process -FilePath $javaPath -ArgumentList $arguments -WindowStyle Hidden -PassThru -RedirectStandardOutput "$runPath/state/$Role.stdout.log" -RedirectStandardError "$runPath/state/$Role.stderr.log"
 $entry=[pscustomobject]@{Role=$Role;Class=$Class;Process=$process;Started=$process.StartTime;Injected=$false}
 $owned.Add($entry);return $entry
}
function Stop-Owned([object]$Entry){
 $actual=Get-Process -Id $Entry.Process.Id -ErrorAction SilentlyContinue
 if($null -eq $actual){return}
 $detail=Get-CimInstance Win32_Process -Filter "ProcessId=$($actual.Id)"
 if($actual.StartTime -ne $Entry.Started -or $null -eq $detail -or !$detail.CommandLine.Contains($runPath) -or !$detail.CommandLine.Contains($Entry.Class)){throw 'Owned process identity mismatch; refusing to stop'}
 Stop-Process -Id $actual.Id
}
function Wait-File([string]$File,[object]$Entry,[datetime]$Until){
 while(!(Test-Path -LiteralPath $File)){
  if($Entry.Process.HasExited){throw "$($Entry.Role) exited: inspect state logs"}
  if([DateTime]::UtcNow -gt $Until){throw "Timed out waiting for $File"}
  Start-Sleep -Milliseconds 50
 }
}
try{
 $ports=[Collections.Generic.List[int]]::new()
 for($i=0;$i -lt $Replicas+1;$i++){$listener=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0);$listener.Start();$listeners.Add($listener);$ports.Add($listener.LocalEndpoint.Port)}
 $members=@(1..$Replicas|ForEach-Object{"$_@127.0.0.1:$($ports[$_-1])"}) -join ','
 $topology=Join-Path $outputPath 'topology.properties'
 [IO.File]::WriteAllText($topology,"schema=ionia-pilot-topology-v1`nleader.id=1`nreplicas=$members`n",[Text.UTF8Encoding]::new($false))
 if($Dataset){$datasetPath=(Resolve-Path -LiteralPath $Dataset).Path}else{
  $datasetPath=Join-Path $outputPath 'dataset';if(!$Profile){$Profile=Join-Path $bundlePath 'tools/smoke.properties'}
  Invoke-Java $single @('dataset',$Profile,$datasetPath)
 }
 Invoke-Java $distributed @('prepare',$topology,$datasetPath,$runPath,"127.0.0.1:$($ports[$Replicas])")
 Invoke-Java $distributed @('verify',$bundlePath,$runPath)
 foreach($listener in $listeners){$listener.Stop()};$listeners.Clear()
 $nodeProcesses=@()
 foreach($id in 1..$Replicas){$nodeProcesses+=Start-Owned "node-$id" $single @('node',$bundlePath,$runPath,"$id")}
 $startupUntil=[DateTime]::UtcNow.AddSeconds(25)
 foreach($entry in $nodeProcesses){$ready="$runPath/state/$($entry.Role).ready.properties";Wait-File $ready $entry $startupUntil;Copy-Item -LiteralPath $ready -Destination "$runPath/ready/$($entry.Role).ready.properties"}
 $controller=Start-Owned 'controller' $distributed @('controller',$bundlePath,$runPath)
 Wait-File "$runPath/state/controller.ready.properties" $controller ([DateTime]::UtcNow.AddSeconds(20))
 $load0=Start-Owned 'load-0' $distributed @('load',$bundlePath,$runPath,'0')
 $load1=$null
 if($Fault -ne 'MissingLoadOne'){$load1=Start-Owned 'load-1' $distributed @('load',$bundlePath,$runPath,'1')}
 if($Fault -eq 'StopLoadOne'){
  Wait-File "$runPath/state/phase-load.started" $controller ([DateTime]::UtcNow.AddSeconds(20))
  if($load1.Process.HasExited){throw 'Fault target already exited before injection'}
  Stop-Owned $load1;$load1.Injected=$true
  [IO.File]::WriteAllText("$runPath/state/fault-injected.txt",'owned load-1 terminated after LOAD dispatch')
 }
 $workMs=600000
 foreach($line in Get-Content -LiteralPath "$runPath/profile.properties"){if($line -match '^work\.timeout\.ms=(\d+)$'){$workMs=[int]$Matches[1]}}
 if(!$controller.Process.WaitForExit($workMs+15000)){throw 'Controller exceeded work/cleanup bound'}
 if(!$load0.Process.WaitForExit(10000) -or ($null -ne $load1 -and !$load1.Process.WaitForExit(10000))){throw 'Load process failed to exit after controller'}
 if($Fault -eq 'None'){
  foreach($entry in @($controller,$load0,$load1)){if($entry.Process.ExitCode -ne 0){throw "$($entry.Role) failed; inspect stderr"}}
  foreach($file in @('DISTRIBUTED_COMPLETE','LOAD_0_COMPLETE','LOAD_1_COMPLETE')){if(!(Test-Path -LiteralPath "$runPath/$file")){throw "Missing $file"}}
 }else{
  if($controller.Process.ExitCode -eq 0 -or !(Test-Path -LiteralPath "$runPath/DISTRIBUTED_FAILED.json") -or (Test-Path -LiteralPath "$runPath/DISTRIBUTED_COMPLETE")){throw 'Peer failure was not rejected'}
  if($load0.Process.ExitCode -eq 0 -or !(Test-Path -LiteralPath "$runPath/LOAD_0_FAILED.json")){throw 'Surviving load did not abort'}
 }
}finally{
 foreach($listener in $listeners){$listener.Stop()}
 if(Test-Path -LiteralPath "$runPath/state"){
  if(!(Test-Path -LiteralPath "$runPath/state/distributed.abort")){[IO.File]::WriteAllText("$runPath/state/distributed.abort",'local rehearsal cleanup')}
 }
 $stopErrors=@()
 foreach($entry in $owned|Where-Object{$_.Role -notlike 'node-*'}){
  if(!$entry.Process.WaitForExit(10000)){Stop-Owned $entry;$stopErrors+="$($entry.Role) required forced cleanup"}
 }
 foreach($entry in $owned|Where-Object{$_.Role -like 'node-*'}){
  $stop="$runPath/state/$($entry.Role).stop";if(!(Test-Path -LiteralPath $stop)){[IO.File]::WriteAllText($stop,'stop')}
 }
 foreach($entry in $owned|Where-Object{$_.Role -like 'node-*'}){
  if(!$entry.Process.WaitForExit(15000)){Stop-Owned $entry;$stopErrors+="$($entry.Role) required forced cleanup"}
  elseif($entry.Process.ExitCode -ne 0 -or !(Test-Path -LiteralPath "$runPath/state/$($entry.Role).stopped")){$stopErrors+="$($entry.Role) graceful close not confirmed"}
 }
 if($stopErrors.Count){throw ($stopErrors -join '; ')}
}
Write-Host "DISTRIBUTED_REHEARSAL_PASS replicas=$Replicas fault=$Fault output=$runPath"
