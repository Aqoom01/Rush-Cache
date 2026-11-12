#requires -Version 7.0
[CmdletBinding()]
param([Parameter(Mandatory)][string]$RunDirectory)
$ErrorActionPreference='Stop'
$runRoot=[IO.Path]::GetFullPath($RunDirectory)
function Assert-Run([bool]$Condition,[string]$Reason) { if(-not $Condition){throw "Invalid run artifact: $Reason"} }
function Read-RunJson([string]$Name) { Get-Content -LiteralPath (Join-Path $runRoot $Name) -Raw -Encoding UTF8 | ConvertFrom-Json -Depth 100 }
function Assert-RunHash($Entry) {
    Assert-Run ($Entry.path -in @('manifest.json','environment.json','execution.json','histograms.jsonl','events.jsonl','counters.csv','result.json')) 'unexpected artifact path'
    $actual=(Get-FileHash -LiteralPath (Join-Path $runRoot $Entry.path) -Algorithm SHA256).Hash
    Assert-Run ($actual -eq $Entry.sha256) "hash mismatch: $($Entry.path)"
}
Assert-Run (Test-Path -LiteralPath (Join-Path $runRoot 'COMPLETE') -PathType Leaf) 'COMPLETE missing; partial result'
$integrity=Read-RunJson 'integrity.json'
Assert-Run ($integrity.schema_version -eq 'benchmark-integrity-v1' -and $integrity.complete) 'integrity format'
$integrityHash=(Get-FileHash -LiteralPath (Join-Path $runRoot 'integrity.json') -Algorithm SHA256).Hash
$marker=(Get-Content -LiteralPath (Join-Path $runRoot 'COMPLETE') -Raw).Trim()
Assert-Run ($marker -eq "integrity_sha256=$integrityHash") 'integrity marker hash'
Assert-Run (@($integrity.files).Count -eq 7 -and @($integrity.files.path | Sort-Object -Unique).Count -eq 7) 'integrity file population'
foreach($entry in $integrity.files){Assert-RunHash $entry}
$resultPath=Join-Path $runRoot 'result.json'
Assert-Run (Test-Json -LiteralPath $resultPath -SchemaFile (Join-Path $PSScriptRoot 'result.schema.json')) 'result schema'
$result=Read-RunJson 'result.json'
foreach($entry in $result.artifacts){Assert-RunHash $entry}
Assert-Run (@($result.artifacts).Count -eq 6 -and @($result.artifacts.path | Sort-Object -Unique).Count -eq 6) 'result artifact population'
$manifest=Read-RunJson 'manifest.json'
$environment=Read-RunJson 'environment.json'
Assert-Run ($manifest.schema_version -eq 'benchmark-local-manifest-v1') 'local manifest format'
Assert-Run ($manifest.run_id -eq $result.run_id) 'run identity'
Assert-Run ($result.manifest_sha256 -eq (Get-FileHash -LiteralPath (Join-Path $runRoot 'manifest.json') -Algorithm SHA256).Hash) 'manifest hash'
if($null -ne $result.cohort){
    $c=$result.cohort
    Assert-Run ($c.admitted -eq $c.success+$c.error_response+$c.no_response+$c.pending) 'measure cohort conservation'
}
Assert-Run ($result.validity -ne 'invalid' -or @($result.invalid_reasons).Count -gt 0) 'missing invalid reason'
$catalog=(Get-Content -LiteralPath (Join-Path $PSScriptRoot 'metric-catalog.json') -Raw -Encoding UTF8 | ConvertFrom-Json).metrics
Assert-Run (@($result.metrics).Count -eq 36 -and @($result.metrics.metric_id | Sort-Object -Unique).Count -eq 36) 'metric population'
foreach($metric in $result.metrics){
    $definition=$catalog | Where-Object id -eq $metric.metric_id
    Assert-Run ($null -ne $definition -and $metric.unit -eq $definition.unit) "metric unit: $($metric.metric_id)"
}
foreach($required in $manifest.metadata.requiredMetrics){
    $record=$result.metrics | Where-Object metric_id -eq $required
    Assert-Run ($result.validity -ne 'valid' -or $record.status -eq 'measured') "required metric: $required"
}
$histograms=@(Get-Content -LiteralPath (Join-Path $runRoot 'histograms.jsonl') -Encoding UTF8 | Where-Object {$_} | ForEach-Object {ConvertFrom-Json -InputObject $_ -Depth 100})
$keys=[Collections.Generic.HashSet[string]]::new()
foreach($h in $histograms){
    $p=$h.population
    $key="$($p.phase)/$($p.operation)/$($p.outcome)/$($p.reason)/$($p.responseStatus)/$($p.censored)/$($p.kind)"
    Assert-Run ($keys.Add($key)) 'duplicate histogram population'
    Assert-Run ($h.bin_version -eq 'ns-ceil-1pct-v1' -and $h.clock_id -eq $environment.clock_id) 'histogram bin/clock'
    Assert-Run ($p.censored -eq ($p.outcome -eq 'NO_RESPONSE')) 'censor label'
    $boundaries=[Collections.Generic.HashSet[long]]::new(); [void]$boundaries.Add(0)
    [long]$upper=1
    while($upper -le $h.highest_finite_ns){
        [void]$boundaries.Add($upper)
        [long]$step=[decimal]::Truncate(([decimal]$upper)/100)
        if($upper % 100 -ne 0){$step++}
        if([long]::MaxValue-$upper -lt $step){break}
        $upper+=$step
    }
    Assert-Run ($boundaries.Contains([long]$h.highest_finite_ns)) 'finite range boundary'
    [long]$population=$h.overflow_count; [long]$previous=-1
    foreach($bucket in $h.buckets){
        Assert-Run ($bucket.count -gt 0 -and $bucket.upper_bound_ns -gt $previous -and $boundaries.Contains([long]$bucket.upper_bound_ns)) 'sparse bucket order/range/count'
        $population+=$bucket.count; $previous=$bucket.upper_bound_ns
    }
    Assert-Run ($population -eq $h.count) 'histogram count conservation'
    foreach($q in @(@('p50',50,100),@('p95',95,100),@('p99',99,100),@('p99_9',999,1000))){
        $quantile=$h.($q[0]); $expected=$null
        [decimal]$rank=[Math]::Ceiling(([decimal]$h.count*$q[1])/$q[2]); [long]$seen=0
        foreach($bucket in $h.buckets){$seen+=$bucket.count; if($seen -ge $rank){$expected=$bucket.upper_bound_ns; break}}
        Assert-Run ($quantile.upperBoundNanos -eq $expected) "nearest-rank percentile: $($q[0])"
        if($q[0] -eq 'p99_9' -and $h.count -lt 100000){Assert-Run (-not $quantile.comparisonEligible) 'p99.9 sample policy'}
    }
}
$rows=@(Import-Csv -LiteralPath (Join-Path $runRoot 'counters.csv'))
foreach($row in $rows){
    Assert-Run ([long]$row.considered -eq [long]$row.success+[long]$row.error_response+[long]$row.no_response+[long]$row.not_admitted+[long]$row.unresolved) 'counter conservation'
    # INVALID 계측 누락도 완전한 실패 증거로 저장할 수 있다. 일치 여부를 성공으로 바꾸지 않는다.
    if($integrity.collector_reconciled){
        foreach($kind in @('SERVICE','SCHEDULED','DISPATCH_LAG')){
            foreach($category in @(@('SUCCESS','success'),@('ERROR_RESPONSE','error_response'),@('NO_RESPONSE','no_response'))){
                $sum=($histograms | Where-Object {$_.population.phase -eq $row.phase -and $_.population.operation -eq $row.operation -and $_.population.kind -eq $kind -and $_.population.outcome -eq $category[0]} | Measure-Object count -Sum).Sum
                Assert-Run ([long]$sum -eq [long]$row.($category[1])) 'histogram/counter population'
            }
        }
    }
}
foreach($line in Get-Content -LiteralPath (Join-Path $runRoot 'events.jsonl') -Encoding UTF8){
    if($line){$event=ConvertFrom-Json -InputObject $line -Depth 100; Assert-Run ($event.clock_id -eq $environment.clock_id -and $event.run_id -eq $result.run_id) 'event identity/clock'}
}
[pscustomobject]@{directory=$runRoot;artifact_check='passed';result_validity=$result.validity;histogram_populations=$histograms.Count;counter_rows=$rows.Count;performance_qualified=$false}
