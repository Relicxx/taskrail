param([string]$BaseUrl='http://localhost:8080',[string]$ApiKey=$env:API_KEY)
$ErrorActionPreference='Stop'
if (!$ApiKey) { throw 'Set API_KEY to the demo client key' }
$headers=@{'X-API-Key'=$ApiKey;'Idempotency-Key'=[guid]::NewGuid().ToString()}
$task=Invoke-RestMethod -TimeoutSec 5 -Method Post -Uri "$BaseUrl/v1/tasks" -Headers $headers -ContentType 'application/json' -Body '{"type":"demo.wait","payload":{"duration_ms":10000}}'
$deadline=[datetime]::UtcNow.AddSeconds(90)
try {
  do {
    if ([datetime]::UtcNow -gt $deadline) { throw 'Timed out waiting for running task' }
    $attempts=Invoke-RestMethod -TimeoutSec 5 -Uri "$BaseUrl/v1/tasks/$($task.id)/attempts" -Headers $headers
    $running=@($attempts.items | Where-Object outcome -eq 'running')
    if (!$running.Count) { Start-Sleep -Milliseconds 200 }
  } while (!$running.Count)
  $service=$running[-1].worker_id
  if ($service -notin @('worker-a','worker-b')) { throw 'Unexpected worker service' }
  docker compose kill -s SIGKILL $service
  if ($LASTEXITCODE -ne 0) { throw 'Unable to kill worker' }
  do {
    if ([datetime]::UtcNow -gt $deadline) { throw 'Task did not recover within deadline' }
    Start-Sleep -Milliseconds 300
    $task=Invoke-RestMethod -TimeoutSec 5 -Uri "$BaseUrl/v1/tasks/$($task.id)" -Headers $headers
    if ($task.state -in @('failed','canceled')) { throw "Unexpected terminal state: $($task.state)" }
  } while ($task.state -ne 'succeeded')
  $attempts=Invoke-RestMethod -TimeoutSec 5 -Uri "$BaseUrl/v1/tasks/$($task.id)/attempts" -Headers $headers
  if ($attempts.items.Count -lt 2 -or !@($attempts.items | Where-Object outcome -eq 'lease_expired').Count) { throw 'Missing lease recovery history' }
  Write-Output "Recovery verified: task $($task.id), attempts $($attempts.items.Count)"
} finally {
  if ($service -in @('worker-a','worker-b')) { docker compose start $service | Out-Null }
}
