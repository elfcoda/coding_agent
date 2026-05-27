# 给我一份可复制粘贴的 PowerShell 一键验收脚本（含变量保存、断言失败即停止、最后输出 PASS/FAIL）。

# 我会新建一个独立的最小版 ps1，保留 6 个核心断言并复用同样的 PASS/FAIL 与报告输出，默认 60 秒内能跑完。随后给你直接运行命令。

# Created acceptance_demo_min.ps1

# 最小版脚本已经落地为独立文件，包含 6 项核心断言和 PASS/FAIL 报告输出。下一步你可以直接运行，我也可以按你环境再压缩成“只复用已启动网关”的超快版。

# 已写好最小版脚本文件：
# acceptance_demo_min.ps1

# 它只保留 6 项核心断言，通常 1 分钟左右跑完：

# health 可用
# snapshot 结构完整
# 直接命令创建 work item 成功
# 直接命令提交 decision 成功
# 项目属性命令路径可写
# metrics 结构完整
# 直接执行：
# Set-Location D:\code\FastCode
# .\scripts\acceptance_demo_min.ps1
# 可选参数：
# .\scripts\acceptance_demo_min.ps1 -RepoRoot 'D:\code\FastCode' -ConfigPath 'D:\code\FastCode\nanobot_config.json' -BindHost '127.0.0.1' -Port 18790
# 输出文件：

# logs/acceptance/acceptance_min_state.json
# logs/acceptance/acceptance_min_report.json
# logs/acceptance/gateway_acceptance_min.stdout.log
# logs/acceptance/gateway_acceptance_min.stderr.log

param(
    [string]$RepoRoot = 'D:\code\FastCode',
    [string]$ConfigPath = 'D:\code\FastCode\nanobot_config.json',
    [string]$BindHost = '127.0.0.1',
    [int]$Port = 18790
)

$ErrorActionPreference = 'Stop'

$BaseUrl = 'http://' + $BindHost + ':' + $Port
$NanobotCwd = Join-Path $RepoRoot 'nanobot'
$PythonExe = Join-Path $RepoRoot '.venv\Scripts\python.exe'
$OutDir = Join-Path $RepoRoot 'logs\acceptance'
$StatePath = Join-Path $OutDir 'acceptance_min_state.json'
$ReportPath = Join-Path $OutDir 'acceptance_min_report.json'
$GatewayStdoutLogPath = Join-Path $OutDir 'gateway_acceptance_min.stdout.log'
$GatewayStderrLogPath = Join-Path $OutDir 'gateway_acceptance_min.stderr.log'

New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

$Script:Checks = New-Object System.Collections.Generic.List[object]
$Script:State = [ordered]@{
    started_at = (Get-Date).ToString('o')
    base_url = $BaseUrl
    ids = [ordered]@{}
    gateway = [ordered]@{
        started_by_script = $false
        pid = $null
    }
}

function Save-State {
    $Script:State | ConvertTo-Json -Depth 30 | Set-Content -Path $StatePath -Encoding UTF8
}

function Add-Check {
    param([string]$Name,[bool]$Passed,[string]$Detail)
    $Script:Checks.Add([ordered]@{name=$Name;passed=$Passed;detail=$Detail;ts=(Get-Date).ToString('o')}) | Out-Null
}

function Assert-True {
    param([bool]$Condition,[string]$Message)
    if (-not $Condition) { throw 'ASSERT FAILED: ' + $Message }
}

function Invoke-Api {
    param([string]$Method,[string]$Path,[object]$Body = $null)
    $uri = $BaseUrl + $Path
    if ($null -eq $Body) {
        return Invoke-RestMethod -Method $Method -Uri $uri -TimeoutSec 12
    }
    $json = $Body | ConvertTo-Json -Depth 20 -Compress
    return Invoke-RestMethod -Method $Method -Uri $uri -ContentType 'application/json' -Body $json -TimeoutSec 12
}

function Test-Health {
    try {
        $h = Invoke-Api -Method 'GET' -Path '/api/control/health'
        return ($h.status -eq 'ok')
    }
    catch {
        return $false
    }
}

$gatewayProc = $null

try {
    Save-State

    if (-not (Test-Health)) {
        Assert-True (Test-Path $PythonExe) ('Python not found: ' + $PythonExe)
        Assert-True (Test-Path $ConfigPath) ('Config not found: ' + $ConfigPath)

        if (Test-Path $GatewayStdoutLogPath) { Remove-Item $GatewayStdoutLogPath -Force }
        if (Test-Path $GatewayStderrLogPath) { Remove-Item $GatewayStderrLogPath -Force }

        $args = @('-m','nanobot','gateway','--host',$BindHost,'--port',[string]$Port,'--config-path',$ConfigPath)
        $gatewayProc = Start-Process -FilePath $PythonExe -ArgumentList $args -WorkingDirectory $NanobotCwd -RedirectStandardOutput $GatewayStdoutLogPath -RedirectStandardError $GatewayStderrLogPath -PassThru
        $Script:State.gateway.started_by_script = $true
        $Script:State.gateway.pid = $gatewayProc.Id
        Save-State

        $up = $false
        foreach ($i in 1..20) {
            Start-Sleep -Seconds 1
            if (Test-Health) { $up = $true; break }
        }
        Assert-True $up 'gateway not healthy within 20s'
    }

    # 1) health
    $health = Invoke-Api -Method 'GET' -Path '/api/control/health'
    Assert-True ($health.status -eq 'ok') 'health.status != ok'
    Add-Check -Name '1_health' -Passed $true -Detail 'health ok'

    # 2) snapshot structure
    $snapshot = Invoke-Api -Method 'GET' -Path '/api/control/snapshot?limit=20'
    Assert-True ($null -ne $snapshot.work_items -and $null -ne $snapshot.contracts -and $null -ne $snapshot.decisions) 'snapshot missing core arrays'
    Add-Check -Name '2_snapshot' -Passed $true -Detail 'snapshot structure ok'

    # 3) direct command create work item
    $wi = Invoke-Api -Method 'POST' -Path '/api/control/commands/work-items/create' -Body @{
        module = 'fastcode'
        goal = 'min acceptance check'
        status = 'proposed'
        priority = 3
    }
    $workItemId = [string]$wi.work_item.id
    Assert-True (-not [string]::IsNullOrWhiteSpace($workItemId)) 'create work-item returned empty id'
    $Script:State.ids.work_item_id = $workItemId
    Add-Check -Name '3_create_work_item' -Passed $true -Detail ('id=' + $workItemId)

    # 4) direct command submit decision
    $decision = Invoke-Api -Method 'POST' -Path '/api/control/commands/decisions/submit' -Body @{
        work_item_id = $workItemId
        decision_type = 'min_check'
        status = 'approved'
        chosen_option = 'approve'
        decider = 'human'
        rationale = 'minimal acceptance'
    }
    $decisionId = [string]$decision.decision.id
    Assert-True (-not [string]::IsNullOrWhiteSpace($decisionId)) 'submit decision returned empty id'
    Assert-True ([string]$decision.decision.status -eq 'approved') 'decision status != approved'
    $Script:State.ids.decision_id = $decisionId
    Add-Check -Name '4_submit_decision' -Passed $true -Detail ('id=' + $decisionId)

    # 5) project attribute command path
    $setAttr = Invoke-Api -Method 'PUT' -Path '/api/control/commands/projects/fastcode/attributes' -Body @{
        attributes = @{ scheduler = @{ priority_bias = 2; dispatch_enabled = $true } }
    }
    Assert-True ($setAttr.ok -eq $true) 'set project attributes failed'
    Add-Check -Name '5_project_attributes' -Passed $true -Detail 'project attributes set via command path'

    # 6) metrics endpoint shape
    $metrics = Invoke-Api -Method 'GET' -Path '/api/control/metrics?limit=50'
    Assert-True ($metrics.ok -eq $true) 'metrics.ok != true'
    Assert-True ($null -ne $metrics.agents -and $null -ne $metrics.contracts -and $null -ne $metrics.decisions) 'metrics missing core sections'
    Add-Check -Name '6_metrics' -Passed $true -Detail 'metrics shape ok'

    $Script:State.finished_at = (Get-Date).ToString('o')
    $Script:State.result = 'PASS'
    Save-State

    $report = [ordered]@{
        result = 'PASS'
        started_at = $Script:State.started_at
        finished_at = $Script:State.finished_at
        base_url = $BaseUrl
        ids = $Script:State.ids
        checks = $Script:Checks
        state_file = $StatePath
        gateway_stdout_log = $GatewayStdoutLogPath
        gateway_stderr_log = $GatewayStderrLogPath
    }
    $report | ConvertTo-Json -Depth 30 | Set-Content -Path $ReportPath -Encoding UTF8

    Write-Host ''
    Write-Host 'PASS: minimal acceptance checks passed.' -ForegroundColor Green
    Write-Host ('State saved to: ' + $StatePath)
    Write-Host ('Report saved to: ' + $ReportPath)
}
catch {
    $Script:State.finished_at = (Get-Date).ToString('o')
    $Script:State.result = 'FAIL'
    $Script:State.error = $_.Exception.Message
    Save-State

    Add-Check -Name 'fatal' -Passed $false -Detail $_.Exception.Message

    $report = [ordered]@{
        result = 'FAIL'
        started_at = $Script:State.started_at
        finished_at = $Script:State.finished_at
        base_url = $BaseUrl
        ids = $Script:State.ids
        checks = $Script:Checks
        error = $_.Exception.Message
        state_file = $StatePath
        gateway_stdout_log = $GatewayStdoutLogPath
        gateway_stderr_log = $GatewayStderrLogPath
    }
    $report | ConvertTo-Json -Depth 30 | Set-Content -Path $ReportPath -Encoding UTF8

    Write-Host ''
    Write-Host ('FAIL: ' + $_.Exception.Message) -ForegroundColor Red
    Write-Host ('State saved to: ' + $StatePath)
    Write-Host ('Report saved to: ' + $ReportPath)
    exit 1
}
finally {
    if ($null -ne $gatewayProc -and $Script:State.gateway.started_by_script -eq $true) {
        try {
            if (-not $gatewayProc.HasExited) {
                Stop-Process -Id $gatewayProc.Id -Force
            }
        }
        catch {
        }
    }
}
