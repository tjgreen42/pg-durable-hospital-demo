# Configure PGHOST, PGDATABASE, PGUSER, PGSSLMODE and a libpq password file before running.
[CmdletBinding()]
param(
    [ValidateSet('chunks','models')][string]$Mode='chunks',
    [string]$EmbeddingModel='default-embedding',
    [string]$ChatModel='default-chat',
    [switch]$ProbeMissingModels
)
$ErrorActionPreference='Stop'
if (-not $env:PGHOST -or -not $env:PGDATABASE) { throw 'Set PGHOST and PGDATABASE explicitly to the test database.' }
$agent='demo-'+[guid]::NewGuid().ToString('N').Substring(0,12)
function Query([string]$Sql,[hashtable]$Vars=@{}) {
    $p=@('-X','-w','-qAt','-v','ON_ERROR_STOP=1','-v',"agent=$agent",
         '-v',"em=$EmbeddingModel",'-v',"cm=$ChatModel")
    foreach($key in $Vars.Keys) { $p+=@('-v',"$key=$($Vars[$key])") }
    $lines=$Sql | & psql @p
    if($LASTEXITCODE -ne 0){throw "psql failed ($LASTEXITCODE)"}
    return (($lines -join "`n").Trim())
}
function Check([string]$Sql,[string]$Description,[hashtable]$Vars=@{}) {
    if((Query $Sql $Vars) -cne 't'){throw "Failed: $Description"}
    Write-Output $Description
}
function Candidate([int]$Amount,[string]$BuildMode,[switch]$Incomplete) {
    $pages=@(@{id=101;url='https://hospital.example/benefits';is_plan=$true;content="The synthetic Cedar plan annual deductible is $Amount US dollars. Benefits are described on this page. Contact member services for administrative questions."})
    if(-not $Incomplete) {
        $pages+=@{id=907;url='https://hospital.example/visiting';is_plan=$false;content=('Visiting hours are nine to five. Use the main entrance for scheduled visits. '+('The information desk has directions to the visitor lobby. '*12)).Trim()}
    }
    $data=ConvertTo-Json -InputObject $pages -Depth 3 -Compress
    return [long](Query "select hospital_refresh_lab.stage(:'agent', :'pages'::jsonb, :'mode', :'em', :'cm');" @{pages=$data;mode=$BuildMode})
}
function Start-Candidate([long]$Id) {
    return Query "select hospital_refresh_lab.submit(:id);" @{id=$Id}
}
function Observe([long]$Id,[string]$Expected) {
    $until=[datetime]::UtcNow.AddMinutes(6)
    do {
        $row=Query "select state||'|'||coalesce(decision,'')||'|'||df.status(coordinator) from hospital_refresh_lab.releases where id=:id;" @{id=$Id}
        if($row -match '\|(failed|cancelled|canceled)$'){throw "Coordinator failed for release $Id : $row"}
        if($row.StartsWith("$Expected|")){Write-Output "Release $Id : $row";return}
        if($row -notmatch '^building\|'){throw "Unexpected release $Id : $row"}
        Start-Sleep -Seconds 1
    } while([datetime]::UtcNow -lt $until)
    throw "Observation timed out; inspect release $Id. Did not alter its state."
}
& psql -X -w -q -v ON_ERROR_STOP=1 -f (Join-Path $PSScriptRoot 'refresh.sql')
if($LASTEXITCODE -ne 0){throw 'Install failed'}
if($Mode -eq 'models'){
    Check "select exists(select from model_registry.model_list_all() where alias=:'em') and exists(select from model_registry.model_list_all() where alias=:'cm');" 'Requested embedding and chat aliases are registered'
}
[void](Query "insert into hospital_refresh_lab.agents(agent) values(:'agent'); insert into hospital_refresh_lab.required_pages values(:'agent','https://hospital.example/benefits'),(:'agent','https://hospital.example/visiting');")
Write-Output "Agent $agent; mode=$Mode; input is synthetic."
$first=Candidate 1200 $Mode
Write-Output "Submitted v1: $(Start-Candidate $first); submitting connection has closed."
Observe $first 'ready'
$bad=Candidate 9999 $Mode -Incomplete
Write-Output "Submitted incomplete scrape: $(Start-Candidate $bad)"
Observe $bad 'rejected'
Check "select active_id=:id from hospital_refresh_lab.agents where agent=:'agent';" 'Bad refresh did not replace the last good version' @{id=$first}
$second=Candidate 1800 $Mode
$late=Candidate 9900 'chunks'
$coordinator=Start-Candidate $second
Write-Output "Submitted v2: $coordinator; no client-side finalizer."
Observe $second 'ready'
Check "select count(*)=1 and min(content) like '%1800%' from hospital_refresh_lab.search_chunks(:'agent','deductible');" 'Search returns the new deductible with a source passage'
Check "select count(*)=1 and min(url)='https://hospital.example/benefits' from hospital_refresh_lab.search_chunks(:'agent','deductible');" 'Citation points to the benefits page'
Check "select hospital_refresh_lab.submit(:id)=:'coord';" 'Duplicate submission returned the original coordinator' @{id=$second;coord=$coordinator}
if($Mode -eq 'models'){
    Check "select exists(select from hospital_refresh_lab.search_vectors(:'agent','What is my deductible?')) and exists(select from hospital_refresh_lab.live_benefits where agent=:'agent' and (fields->>'deductible_usd')::numeric=1800);" 'Semantic lookup and extracted deductible both available'
}
[void](Query "select hospital_refresh_lab.switch_version(:'agent',:prev,:next);" @{prev=$second;next=$first})
Check "select count(*)=1 and min(content) like '%1200%' from hospital_refresh_lab.search_chunks(:'agent','deductible');" 'Rollback restored the old answer without rebuilding'
Write-Output "Submitted stale candidate after rollback: $(Start-Candidate $late)"
Observe $late 'rejected'
Check "select decision='Active version changed; refusing stale promotion' from hospital_refresh_lab.releases where id=:id;" 'Old in-flight work cannot undo an operator rollback' @{id=$late}
if($ProbeMissingModels){
    if ($Mode -ne 'chunks') {throw 'Use the missing-model drill in chunks mode only'}
    $EmbeddingModel='hospital_demo_intentionally_absent_embedding'
    $ChatModel='hospital_demo_intentionally_absent_chat'
    Check "select not exists(select from model_registry.model_list_all() where alias in (:'em',:'cm'));" 'Fault-injection aliases absent; drill cannot charge a configured model'
    $noModels=Candidate 2600 'models'
    Write-Output "Missing-model drill: $(Start-Candidate $noModels)"
    Observe $noModels 'rejected'
    Check "select active_id=:id from hospital_refresh_lab.agents where agent=:'agent';" 'Model failure left the previous release live' @{id=$first}
}
Query "select s.id,j.kind,j.instance,df.status(j.instance),coalesce(pr.rows_processed::text,'-') from hospital_refresh_lab.jobs j join hospital_refresh_lab.releases s on s.id=j.release_id left join ai.pipeline_runs pr on pr.instance_id=j.instance where s.agent=:'agent' order by s.id,j.kind;"
