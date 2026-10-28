param([Parameter(Mandatory=$true)][ValidateRange(1,3)][int]$Node)
$ErrorActionPreference='Stop'
$repositoryRoot=(Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$runtimeLibraries=Join-Path $repositoryRoot 'cache-server\build\install\openkache-server\lib\*'
$nodeConfiguration=Join-Path $PSScriptRoot "node-$Node.properties"
Push-Location -LiteralPath $repositoryRoot
try { & java -cp $runtimeLibraries io.openkache.server.ionia.IoniaMain $nodeConfiguration; if($LASTEXITCODE -ne 0) { throw "V01 node failed: exit code $LASTEXITCODE" } }
finally { Pop-Location }
