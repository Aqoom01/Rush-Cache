param([Parameter(Mandatory=$true)][ValidateSet('get','set','delete')][string]$Operation,
      [Parameter(Mandatory=$true)][string]$Key,[string]$Value)
$ErrorActionPreference='Stop'
$repositoryRoot=(Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$runtimeLibraries=Join-Path $repositoryRoot 'cache-server\build\install\openkache-server\lib\*'
$arguments=@('-cp',$runtimeLibraries,'io.openkache.server.ionia.IoniaClientMain',(Join-Path $PSScriptRoot 'node-1.properties'),$Operation,$Key)
if($Operation -eq 'set') { $arguments+= $Value }
Push-Location -LiteralPath $repositoryRoot
try { & java @arguments }
finally { Pop-Location }
