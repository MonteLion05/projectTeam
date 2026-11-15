param(
    [ValidateSet('Verify','Quality','Clean','Analyze','EnrichmentSelect','EnrichmentFinish')]
    [string]$Step = 'Verify',
    [ValidateSet('q1','q2','q3','q4','q5','q6','q7','q8','open','all')]
    [string]$Question = 'all',
    [string]$DataPath = ''
)
$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)
if (-not $DataPath) { $DataPath = Join-Path (Get-Location) 'data' }
function Check-Exit {
    if ($LASTEXITCODE -ne 0) { throw "Command failed (exit $LASTEXITCODE). No later stage was started." }
}
function Invoke-Spark([string]$Script, [string[]]$ScriptArgs = @()) {
    & docker compose exec -T -w /home/jovyan -e PYTHONPATH=/home/jovyan pyspark-notebook spark-submit --driver-memory 4g --conf spark.local.dir=/spark-tmp $Script @ScriptArgs
    Check-Exit
}
if ($Step -eq 'Verify') {
    & py -3.13 scripts/download_imdb.py --data $DataPath --verify-only
    Check-Exit
    return
}
& docker compose config --quiet
Check-Exit
& docker compose up -d --no-deps pyspark-notebook
Check-Exit
# Fail before a Spark job if Docker cannot see the USB source mount.
& docker compose exec -T pyspark-notebook test -f /home/jovyan/scripts/clean_values.py
Check-Exit
& docker compose exec -T pyspark-notebook test -s /home/jovyan/data/raw/title.basics.tsv.gz
Check-Exit
switch ($Step) {
    'Quality' {
        foreach ($script in @('profile_data','check_casts','check_duplicates','check_values','check_relations')) {
            Invoke-Spark "scripts/$script.py"
        }
    }
    'Clean' { Invoke-Spark 'scripts/clean_values.py' }
    'Analyze' { Invoke-Spark 'jobs/analyze.py' @('--question', $Question) }
    'EnrichmentSelect' { Invoke-Spark 'jobs/select_enrichment.py' }
    'EnrichmentFinish' { Invoke-Spark 'jobs/finish_enrichment.py' }
}
