#Requires -Version 7.0
[CmdletBinding()]
param([string]$Root = $PSScriptRoot)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
# Local document/JSON validation only. No network, Java, AWS or process control.
$checks = [System.Collections.Generic.List[object]]::new()
function Read-Json([string]$Name) {
    Get-Content -LiteralPath (Join-Path $Root $Name) -Raw | ConvertFrom-Json -AsHashtable
}
function Clone-Object($Object) {
    $Object | ConvertTo-Json -Depth 80 | ConvertFrom-Json -AsHashtable
}
function Test-Schema($Object, [string]$Schema) {
    try {
        [bool](($Object | ConvertTo-Json -Depth 80) | Test-Json -SchemaFile (Join-Path $Root $Schema) -ErrorAction SilentlyContinue -WarningAction SilentlyContinue)
    } catch { $false }
}
function Test-PlanSemantics($Plan) {
    if (($Plan.workload.mix.get + $Plan.workload.mix.set + $Plan.workload.mix.delete) -ne 100) { return $false }
    $case = @($script:cases | Where-Object id -EQ $Plan.scenario_id)
    if ($case.Count -ne 1) { return $false }
    foreach ($metric in $case[0].metrics) {
        if ($metric -notin $Plan.required_metrics) { return $false }
    }
    if ($Plan.scenario_id -match '^(PERF|SENS|LONG|VAR)-') {
        if ('M35' -notin $Plan.required_metrics -or 'M36' -notin $Plan.required_metrics) { return $false }
    }
    $hosts = @($Plan.environment.replica_hosts) + @($Plan.environment.load_hosts)
    if ($null -ne $Plan.environment.control_host) { $hosts += @($Plan.environment.control_host) }
    if (@($hosts | ForEach-Object { $_.slot } | Sort-Object -Unique).Count -ne $hosts.Count) { return $false }
    if (@($hosts | ForEach-Object { $_.address.Trim().ToLowerInvariant() } | Sort-Object -Unique).Count -ne $hosts.Count) { return $false }
    if ($Plan.readiness -eq 'ready') {
        if ($Plan.dataset.cache_regime -eq 'unknown') { return $false }
        if ($Plan.workload.distribution -eq 'zipf' -and $null -eq $Plan.workload.zipf_theta) { return $false }
        if ($Plan.workload.distribution -eq 'fixed_hot' -and $null -eq $Plan.workload.hot_probability) { return $false }
        if ($Plan.workload.load_mode -eq 'closed_loop' -and $null -ne $Plan.workload.offered_ops_per_second) { return $false }
        if ($Plan.workload.load_mode -eq 'open_loop' -and $null -ne $Plan.workload.concurrency_total) { return $false }
        if ($Plan.timing.drain_limit_seconds * 1000 -lt $Plan.timing.request_deadline_ms) { return $false }
        if ($Plan.timing.run_hard_limit_seconds -le ($Plan.timing.warmup_minimum_seconds + $Plan.timing.minimum_measure_seconds + $Plan.timing.drain_limit_seconds)) { return $false }
        if ($Plan.timing.termination -eq 'fixed_duration') {
            if ($Plan.timing.measure_seconds -lt $Plan.timing.minimum_measure_seconds) { return $false }
            if ($Plan.timing.run_hard_limit_seconds -le ($Plan.timing.warmup_minimum_seconds + $Plan.timing.measure_seconds + $Plan.timing.drain_limit_seconds)) { return $false }
        }
        if ($null -ne $Plan.fault) {
            if ($Plan.fault.at_seconds -lt 0) { return $false }
            if ($Plan.timing.termination -eq 'fixed_duration' -and $Plan.fault.at_seconds -ge $Plan.timing.measure_seconds) { return $false }
        }
    }
    return $true
}
function Test-Plan($Object) {
    (Test-Schema $Object 'manifest.schema.json') -and (Test-PlanSemantics $Object)
}
function Test-Result($Object) {
    if (-not (Test-Schema $Object 'result.schema.json')) { return $false }
    if ($null -ne $Object.cohort) {
        $c = $Object.cohort
        if ($c.admitted -ne ($c.success + $c.error_response + $c.no_response + $c.pending)) { return $false }
    }
    if ($Object.validity -eq 'invalid' -and $Object.invalid_reasons.Count -eq 0) { return $false }
    foreach ($m in $Object.metrics) {
        if ($m.status -eq 'insufficient_samples' -and [string]::IsNullOrWhiteSpace($m.reason)) { return $false }
        if ($null -ne $m.denominator -and $m.denominator -lt 0) { return $false }
    }
    return $true
}
function Check([string]$Name, [bool]$Passed) {
    $checks.Add([ordered]@{ name=$Name; passed=$Passed })
}
$catalog = Read-Json 'scenario-catalog.json'
$cases = @($catalog.scenarios)
$metrics = @((Read-Json 'metric-catalog.json').metrics)
$plan = Read-Json 'plan.template.json'
$result = Read-Json 'result-shape.example.json'
$schema = Read-Json 'manifest.schema.json'
$resultSchema = Read-Json 'result.schema.json'
$scenarioDoc = Get-Content -LiteralPath (Join-Path $Root 'SCENARIOS.md') -Raw
$metricDoc = Get-Content -LiteralPath (Join-Path $Root 'METRICS.md') -Raw
Check '27 unique scenario IDs' ($cases.Count -eq 27 -and @($cases.id | Sort-Object -Unique).Count -eq 27)
Check '36 unique metric IDs' ($metrics.Count -eq 36 -and @($metrics.id | Sort-Object -Unique).Count -eq 36)
Check 'All cases are NOT_RUN' (@($cases | Where-Object execution_status -NE 'not_run').Count -eq 0)
Check 'Scenario schema/catalog IDs agree' ((Compare-Object @($cases.id) @($schema.properties.scenario_id.enum) | Measure-Object).Count -eq 0)
Check 'Manifest metric schema/catalog IDs agree' ((Compare-Object @($metrics.id) @($schema.properties.required_metrics.items.enum) | Measure-Object).Count -eq 0)
Check 'Result metric schema/catalog IDs agree' ((Compare-Object @($metrics.id) @($resultSchema.properties.metrics.items.properties.metric_id.enum) | Measure-Object).Count -eq 0)
$refsValid = $true
foreach ($case in $cases) { foreach ($id in $case.metrics) { if ($id -notin $metrics.id) { $refsValid=$false } } }
Check 'Every case metric reference exists' $refsValid
Check 'Scenario documentation covers every card' (@($cases | Where-Object { $scenarioDoc -notmatch ('### ' + [regex]::Escape($_.id) + ' —') }).Count -eq 0)
Check 'Metric documentation covers every card' (@($metrics | Where-Object { $metricDoc -notmatch ('### ' + [regex]::Escape($_.id) + ' —') }).Count -eq 0)
Check 'Draft template passes schema and semantic checks' (Test-Plan $plan)
Check 'Template remains draft and disabled' ($plan.readiness -eq 'draft' -and -not $plan.execution_enabled)
Check 'NOT_RUN result example contains no measurements' (Test-Result $result)

# Synthetic fixture: .invalid addresses, fake hashes/evidence; never exported as a run plan.
$ready = Clone-Object $plan
$ready.readiness='ready'; $ready.execution_enabled=$true; $ready.unresolved=@()
$hash='0' * 64
$ready.source=@{commit='0000000';dirty=$false;source_hash=$hash;adapter_id='synthetic-only';capability_evidence='synthetic schema fixture, no actual qualification'}
function Fixture-Host([string]$Slot) {
    @{slot=$Slot;address=("$Slot.invalid");instance_type='synthetic';az='synthetic';vcpu=1;ram_bytes=1000000000;storage_kind='ebs';data_path='/synthetic-only'}
}
$ready.environment.replica_hosts=@((Fixture-Host 'r1'),(Fixture-Host 'r2'),(Fixture-Host 'r3'))
$ready.environment.load_hosts=@((Fixture-Host 'l1'),(Fixture-Host 'l2'))
$ready.environment.control_host=Fixture-Host 'c1'
$ready.environment.inventory_id='synthetic-only'
$ready.environment.config_hash=$hash; $ready.environment.resource_budget_hash=$hash
$ready.dataset.memory_budget_bytes=1000000000
$ready.dataset.cache_regime='memory_resident'
$ready.dataset.load_trace_sha256=$hash; $ready.dataset.run_trace_sha256=$hash
$ready.workload.concurrency_total=8
$ready.timing.request_deadline_ms=1000; $ready.timing.drain_limit_seconds=10; $ready.timing.run_hard_limit_seconds=1800
$ready.persistence.kv_force=$false; $ready.persistence.stable_view_force=$true
$ready.persistence.failure_model='independent_replica_process_failures'
$ready.qualification=@{pilot_qualified=$true;history_checker_qualified=$true;instrumentation_qualified=$true;driver_capabilities=@('synthetic-only')}
$ready.metric_policy.clock_method='offset_bounded'
Check 'Synthetic IONIA ready fixture accepted (no execution)' (Test-Plan $ready)
$bad=Clone-Object $plan; $bad.execution_enabled=$true
Check 'Reject draft enabled' (-not (Test-Plan $bad))
$bad=Clone-Object $ready; $bad.source.source_hash=$null
Check 'Reject ready without source evidence hash' (-not (Test-Plan $bad))
$bad=Clone-Object $ready; $bad.node_count=5
Check 'Reject replica count mismatch' (-not (Test-Plan $bad))
$bad=Clone-Object $ready; $bad.required_metrics+=@('M99')
Check 'Reject unknown metric ID' (-not (Test-Plan $bad))
$bad=Clone-Object $ready; $bad.required_metrics=@('M01')
Check 'Reject missing case metrics' (-not (Test-Plan $bad))
$bad=Clone-Object $ready; $bad.workload.mix.get=96
Check 'Reject mix sum 101' (-not (Test-Plan $bad))
$bad=Clone-Object $ready; $bad.environment.load_hosts[0].address='r1.invalid'
Check 'Reject role host overlap' (-not (Test-Plan $bad))
$bad=Clone-Object $ready; $bad.workload.load_mode='open_loop'; $bad.workload.concurrency_total=$null
Check 'Reject open loop missing rate and bound' (-not (Test-Plan $bad))
$bad=Clone-Object $ready; $bad.persistence.kv_force=$null
Check 'Reject unqualified KV force' (-not (Test-Plan $bad))
$bad=Clone-Object $ready; $bad.variant_id='HERMES'
Check 'Reject reserved Hermes ready profile' (-not (Test-Plan $bad))
$bad=Clone-Object $ready; $bad.variant_id='UNREPLICATED'
Check 'Reject reserved unreplicated ready profile' (-not (Test-Plan $bad))
$bad=Clone-Object $ready; $bad.timing.drain_limit_seconds=0
Check 'Reject nonpositive drain limit' (-not (Test-Plan $bad))
$bad=Clone-Object $result; $bad.metrics[0].value=123
Check 'Reject invented NOT_RUN measurement' (-not (Test-Result $bad))
$complete=Clone-Object $result
$complete.execution_status='completed'; $complete.validity='valid'; $complete.manifest_sha256=$hash
$complete.cohort=@{admitted=100;success=90;error_response=5;no_response=5;pending=0}
$complete.elapsed_ns=1000000000
$complete.metrics[0].status='measured'; $complete.metrics[0].value=90; $complete.metrics[0].reason=$null
$complete.metrics[0].numerator=90; $complete.metrics[0].denominator=1
$complete.artifacts=@(@{path='synthetic-only/history.jsonl';sha256=$hash})
Check 'Synthetic completed result shape accepted' (Test-Result $complete)
$bad=Clone-Object $complete; $bad.cohort.success=91
Check 'Reject broken cohort conservation' (-not (Test-Result $bad))
$bad=Clone-Object $complete; $bad.artifacts=@()
Check 'Reject completed valid result without artifact references' (-not (Test-Result $bad))
$bad=Clone-Object $complete; $bad.metrics[0].status='unsupported'
Check 'Reject unsupported metric carrying a value' (-not (Test-Result $bad))
$bad=Clone-Object $complete; $bad.validity='invalid'
Check 'Reject invalid result without a reason' (-not (Test-Result $bad))
$report=[ordered]@{
    schema_version='replication-spec-validation-v1'
    validated_at_utc=[DateTime]::UtcNow.ToString('o')
    scope='specification_only'
    checks=$checks
    checks_total=$checks.Count
    passed=@($checks | Where-Object passed).Count
    failed=@($checks | Where-Object { -not $_.passed }).Count
    scenario_count=$cases.Count
    metric_family_count=$metrics.Count
    template_readiness=$plan.readiness
    example_execution_status=$result.execution_status
    remote_actions=0
    benchmark_runs=0
    source_code_tests_run=0
    limitations=@('Schema fixtures are synthetic and never executed.','No environment, driver, correctness or performance qualification.','Evidence strings/hashes do not prove truth or readiness.')
}
$report | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $Root 'validation.json') -Encoding utf8
[pscustomobject]$report | Select-Object scope,checks_total,passed,failed,scenario_count,metric_family_count,remote_actions,benchmark_runs | ConvertTo-Json
if ($report.failed -ne 0) { throw "Specification validation failed; inspect validation.json." }

