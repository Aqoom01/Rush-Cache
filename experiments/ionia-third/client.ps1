param([Parameter(Mandatory=$true)][ValidateSet('get','set','delete')][string]$Operation,
      [Parameter(Mandatory=$true)][string]$Key,[string]$Value)
$ErrorActionPreference='Stop'
if($Operation -eq 'set' -and -not $PSBoundParameters.ContainsKey('Value')) { throw 'SET에는 -Value가 필요합니다.' }
$repositoryRoot=(Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$runtimeLibraries=Join-Path $repositoryRoot 'cache-server\build\install\openkache-server\lib\*'
$arguments=@('-cp',$runtimeLibraries,'io.openkache.server.ionia_third.IoniaThirdClientMain',(Join-Path $PSScriptRoot 'node-1.properties'),$Operation,$Key)
if($Operation -eq 'set') { $arguments+= $Value }
Push-Location -LiteralPath $repositoryRoot
try { & java @arguments; if($LASTEXITCODE -ne 0) { throw "V02a client failed: exit code $LASTEXITCODE" } }
finally { Pop-Location }
