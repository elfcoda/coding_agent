### 从 API 创建任务到 contract 合并、decision、revalidation、metrics 拉取的一次性回归。

# Set-Location D:\code\FastCode
# .\scripts\acceptance_demo_oneclick.ps1

# .\scripts\acceptance_demo_oneclick.ps1 -RepoRoot 'D:\code\FastCode' -ConfigPath 'D:\code\FastCode\nanobot_config.json' -Host '127.0.0.1' -Port 18790

# 下面给你一份可直接执行的 10 分钟后端验收清单。
# 目标是一次走完你最初定义的核心能力：人控命令面、并行调度、contract 去重与链式 revalidation、decision 流、可观测性、配置可复现。

# 建议开两个终端：

# 终端 A：跑网关
# 终端 B：发 API 请求
# 第 0-1 分钟：启动网关（显式配置路径，确保可复现）
# 调用顺序
# 终端 A 执行
# python.exe -m nanobot gateway --host 127.0.0.1 --port 18790 --config-path nanobot_config.json --verbose
# 预期观测结果
# 启动日志里看到 gateway 和 control plane 已监听 127.0.0.1:18790
# 没有启动即退出的报错
# 第 1-2 分钟：健康检查与全量快照
# 调用顺序
# 终端 B 执行
# $base = "http://127.0.0.1:18790"
# Invoke-RestMethod "$base/api/control/health"
# Invoke-RestMethod "$base/api/control/snapshot?limit=50"
# 预期观测结果
# health 返回 status = ok
# snapshot 返回 work_items/contracts/dependency_edges/decisions 数组结构完整
# 第 2-3 分钟：设置 project 运行时调度属性（人控入口）
# 调用顺序
# Invoke-RestMethod -Method Put -Uri "$base/api/control/commands/projects/fastcode/attributes" -ContentType "application/json" -Body '{"attributes":{"scheduler":{"dispatch_enabled":true,"priority_bias":5,"concurrency_cap":2,"min_priority":0}}}'
# Invoke-RestMethod -Method Put -Uri "$base/api/control/commands/projects/nanobot/attributes" -ContentType "application/json" -Body '{"attributes":{"scheduler":{"dispatch_enabled":true,"priority_bias":1,"concurrency_cap":1,"min_priority":0}}}'
# Invoke-RestMethod "$base/api/control/projects/fastcode/attributes"
# 预期观测结果
# 返回里能看到 scheduler 四项属性生效
# 证明“前端改属性可直达后端调度器”
# 第 3-4 分钟：创建两条工作项（创建任务命令验收）
# 调用顺序
# $wiConsumer = Invoke-RestMethod -Method Post -Uri "$base/api/control/commands/work-items/create" -ContentType "application/json" -Body '{"module":"fastcode","goal":"consumer uses metrics contract","status":"proposed","priority":8}'
# $wiProvider = Invoke-RestMethod -Method Post -Uri "$base/api/control/commands/work-items/create" -ContentType "application/json" -Body '{"module":"nanobot","goal":"provider implements metrics contract","status":"proposed","priority":5}'
# $consumerId = $wiConsumer.work_item.id
# $providerId = $wiProvider.work_item.id
# 预期观测结果
# 返回包含两个可用 work_item id
# status 初始为 proposed
# 第 4-5 分钟：通过通用 workflow 接口创建 contract，并做重复请求去重
# 调用顺序
# 第一次创建
# $c1 = Invoke-RestMethod -Method Post -Uri "$base/api/control/workflow/manage" -ContentType "application/json" -Body (@{
# entity="contract"; action="create"; fields=@{
# provider_module="nanobot"; consumer_module="fastcode"; interface_name="metrics_api";
# version=1; status="requested"; work_item_id=$consumerId;
# consumer_work_item_id=$consumerId; provider_work_item_id=$providerId
# }
# } | ConvertTo-Json -Depth 10)
# 第二次重复创建（同语义）
# $c2 = Invoke-RestMethod -Method Post -Uri "$base/api/control/workflow/manage" -ContentType "application/json" -Body (@{
# entity="contract"; action="create"; fields=@{
# provider_module="nanobot"; consumer_module="fastcode"; interface_name="metrics_api";
# version=1; status="requested"; work_item_id=$consumerId;
# consumer_work_item_id=$consumerId; provider_work_item_id=$providerId
# }
# } | ConvertTo-Json -Depth 10)
# 查询合并审计
# $audit = Invoke-RestMethod "$base/api/control/contracts/merge-audit?merged_only=true&limit=20"
# 预期观测结果
# 去重后应形成单一 contract（同一 id 或 audit 中 merged_request_count >= 2）
# merge audit 里能看到 merged_request_count、合并条目与相关 work_item 轨迹
# 第 5-6 分钟：触发 contract 版本变化，验证链式 revalidation
# 调用顺序
# 从审计或 snapshot 拿 contract id
# $contractId = $audit.items[0].contract_id
# 更新版本
# Invoke-RestMethod -Method Post -Uri "$base/api/control/workflow/manage" -ContentType "application/json" -Body (@{
# entity="contract"; action="update"; record_id=$contractId; fields=@{version=2}
# } | ConvertTo-Json -Depth 10)
# 查询 snapshot
# $snap = Invoke-RestMethod "$base/api/control/snapshot?limit=200"
# 预期观测结果
# 相关 dependency edge/work item 的 metadata 出现 revalidation_required 或 required_version 类字段
# 证明“任一版本变化触发下游复核链”已经生效
# 第 6-7 分钟：提交 decision（创建/更新决策命令验收）
# 调用顺序
# 创建 pending 决策
# $d1 = Invoke-RestMethod -Method Post -Uri "$base/api/control/commands/decisions/submit" -ContentType "application/json" -Body (@{
# work_item_id=$consumerId; decision_type="api_contract_review"; status="pending";
# chosen_option=""; decider="human"; rationale="need explicit approval"
# } | ConvertTo-Json -Depth 10)
# 立刻审批
# Invoke-RestMethod -Method Post -Uri "$base/api/control/commands/decisions/submit" -ContentType "application/json" -Body (@{
# decision_id=$d1.decision.id; work_item_id=$consumerId; decision_type="api_contract_review";
# status="approved"; chosen_option="approve"; decider="human"; rationale="approved for demo"
# } | ConvertTo-Json -Depth 10)
# 预期观测结果
# decision 从 pending 变为 approved
# decision turnaround 可进入后续 metrics 统计
# 第 7-8 分钟：更新工作项状态（第二类命令验收）
# 调用顺序
# Invoke-RestMethod -Method Post -Uri "$base/api/control/commands/work-items/$consumerId/status" -ContentType "application/json" -Body '{"status":"ready","metadata_patch":{"demo_tag":"acceptance_10m"}}'
# Invoke-RestMethod -Method Post -Uri "$base/api/control/workflow/manage" -ContentType "application/json" -Body '{"entity":"scheduler","action":"tick"}'
# 预期观测结果
# work item 状态更新成功
# scheduler tick 返回 transitions 摘要（有调度计算发生）
# 第 8-9 分钟：拉取可观测性指标（总入口 + 分入口）
# 调用顺序
# $mAll = Invoke-RestMethod "$base/api/control/metrics?limit=500"
# $mAgents = Invoke-RestMethod "$base/api/control/metrics/agents?limit=500"
# $mContracts = Invoke-RestMethod "$base/api/control/metrics/contracts?limit=500"
# $mDecisions = Invoke-RestMethod "$base/api/control/metrics/decisions?limit=500"
# 预期观测结果
# agents 里有 queue_length、failure_rate、dispatch_latency_seconds_avg
# contracts 里有 lifecycle_seconds 与平均值
# decisions 里有 turnaround_seconds、pending_count、overdue_count
# 证明“前端观测面板所需指标已可直接消费”
# 第 9-10 分钟：验收结论判定（通过标准）
# 调用顺序
# 对照以下 7 条逐项打勾
# 预期观测结果（全部满足即后端通过）
# 能用命令接口创建任务、改任务状态、提交决策、改 agent 属性
# contract 重复请求被合并，merge audit 可查
# contract 版本变化后，下游出现 revalidation 标记
# scheduler tick 有结果，属性控制可读取
# metrics 总入口和分入口都可返回结构化数据
# health/snapshot 稳定可用
# 使用显式 config-path 启动时行为可复现
# 如果你要，我可以下一条直接给你一份可复制粘贴的 PowerShell 一键验收脚本（含变量保存、断言失败即停止、最后输出 PASS/FAIL）。

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
$StatePath = Join-Path $OutDir 'acceptance_state.json'
$ReportPath = Join-Path $OutDir 'acceptance_report.json'
$GatewayStdoutLogPath = Join-Path $OutDir 'gateway_acceptance.stdout.log'
$GatewayStderrLogPath = Join-Path $OutDir 'gateway_acceptance.stderr.log'

New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

$Script:Checks = New-Object System.Collections.Generic.List[object]
$Script:State = [ordered]@{
    started_at = (Get-Date).ToString('o')
    base_url = $BaseUrl
    repo_root = $RepoRoot
    config_path = $ConfigPath
    ids = [ordered]@{}
    responses = [ordered]@{}
    gateway = [ordered]@{
        started_by_script = $false
        pid = $null
    }
}

function Save-State {
    $Script:State | ConvertTo-Json -Depth 50 | Set-Content -Path $StatePath -Encoding UTF8
}

function Add-Check {
    param(
        [string]$Name,
        [bool]$Passed,
        [string]$Detail
    )
    $item = [ordered]@{
        name = $Name
        passed = $Passed
        detail = $Detail
        ts = (Get-Date).ToString('o')
    }
    $Script:Checks.Add($item) | Out-Null
}

function Assert-True {
    param(
        [bool]$Condition,
        [string]$Message
    )
    if (-not $Condition) {
        throw 'ASSERT FAILED: ' + $Message
    }
}

function Invoke-Api {
    param(
        [string]$Method,
        [string]$Path,
        [object]$Body = $null
    )
    $uri = $BaseUrl + $Path
    if ($null -eq $Body) {
        return Invoke-RestMethod -Method $Method -Uri $uri -TimeoutSec 20
    }
    $json = $Body | ConvertTo-Json -Depth 30 -Compress
    return Invoke-RestMethod -Method $Method -Uri $uri -ContentType 'application/json' -Body $json -TimeoutSec 20
}

function Test-Health {
    try {
        $h = Invoke-Api -Method 'GET' -Path '/api/control/health'
        return ($h.status -eq 'ok')
    } catch {
        return $false
    }
}

$gatewayProc = $null

try {
    Save-State

    if (-not (Test-Health)) {
        Assert-True (Test-Path $PythonExe) ('Python not found: ' + $PythonExe)
        Assert-True (Test-Path $ConfigPath) ('Config not found: ' + $ConfigPath)

        if (Test-Path $GatewayStdoutLogPath) {
            Remove-Item $GatewayStdoutLogPath -Force
        }
        if (Test-Path $GatewayStderrLogPath) {
            Remove-Item $GatewayStderrLogPath -Force
        }

        $args = @(
            '-m', 'nanobot', 'gateway',
            '--host', $BindHost,
            '--port', [string]$Port,
            '--config-path', $ConfigPath,
            '--verbose'
        )

        $gatewayProc = Start-Process -FilePath $PythonExe -ArgumentList $args -WorkingDirectory $NanobotCwd -RedirectStandardOutput $GatewayStdoutLogPath -RedirectStandardError $GatewayStderrLogPath -PassThru
        $Script:State.gateway.started_by_script = $true
        $Script:State.gateway.pid = $gatewayProc.Id
        Save-State

        $up = $false
        foreach ($i in 1..30) {
            Start-Sleep -Seconds 1
            if (Test-Health) {
                $up = $true
                break
            }
        }

        Assert-True $up 'Gateway failed to become healthy within 30s'
        Add-Check -Name 'gateway_started' -Passed $true -Detail ('Started by script, pid=' + $gatewayProc.Id)
    } else {
        Add-Check -Name 'gateway_reused' -Passed $true -Detail 'Existing gateway is healthy'
    }

    $health = Invoke-Api -Method 'GET' -Path '/api/control/health'
    $Script:State.responses.health = $health
    Assert-True ($health.status -eq 'ok') 'Health endpoint did not return status=ok'
    Add-Check -Name 'health' -Passed $true -Detail 'status=ok'
    Save-State

    $snapshot0 = Invoke-Api -Method 'GET' -Path '/api/control/snapshot?limit=100'
    $Script:State.responses.snapshot0 = $snapshot0
    Assert-True ($null -ne $snapshot0.work_items) 'snapshot missing work_items'
    Assert-True ($null -ne $snapshot0.contracts) 'snapshot missing contracts'
    Assert-True ($null -ne $snapshot0.decisions) 'snapshot missing decisions'
    Add-Check -Name 'snapshot' -Passed $true -Detail 'snapshot structure looks good'
    Save-State

    $setFastcode = Invoke-Api -Method 'PUT' -Path '/api/control/commands/projects/fastcode/attributes' -Body @{
        attributes = @{
            scheduler = @{
                dispatch_enabled = $true
                priority_bias = 5
                concurrency_cap = 2
                min_priority = 0
            }
        }
    }
    $setNanobot = Invoke-Api -Method 'PUT' -Path '/api/control/commands/projects/nanobot/attributes' -Body @{
        attributes = @{
            scheduler = @{
                dispatch_enabled = $true
                priority_bias = 1
                concurrency_cap = 1
                min_priority = 0
            }
        }
    }
    $getFastcode = Invoke-Api -Method 'GET' -Path '/api/control/projects/fastcode/attributes'
    $Script:State.responses.project_attributes_fastcode = $getFastcode
    Assert-True ($getFastcode.ok -eq $true) 'get project attributes failed'
    Assert-True ($getFastcode.attributes.scheduler.priority_bias -eq 5) 'project scheduler priority_bias not applied'
    Add-Check -Name 'project_attributes' -Passed $true -Detail 'project runtime scheduler attributes applied'
    Save-State

    $wiConsumer = Invoke-Api -Method 'POST' -Path '/api/control/commands/work-items/create' -Body @{
        module = 'fastcode'
        goal = 'consumer uses metrics contract'
        status = 'proposed'
        priority = 8
    }
    $wiProvider = Invoke-Api -Method 'POST' -Path '/api/control/commands/work-items/create' -Body @{
        module = 'nanobot'
        goal = 'provider implements metrics contract'
        status = 'proposed'
        priority = 5
    }

    $consumerId = [string]$wiConsumer.work_item.id
    $providerId = [string]$wiProvider.work_item.id

    Assert-True (-not [string]::IsNullOrWhiteSpace($consumerId)) 'consumer work item id missing'
    Assert-True (-not [string]::IsNullOrWhiteSpace($providerId)) 'provider work item id missing'

    $Script:State.ids.consumer_work_item_id = $consumerId
    $Script:State.ids.provider_work_item_id = $providerId
    $Script:State.responses.work_item_consumer = $wiConsumer
    $Script:State.responses.work_item_provider = $wiProvider
    Add-Check -Name 'work_item_create' -Passed $true -Detail 'created consumer/provider work items'
    Save-State

    $contractCreate1 = Invoke-Api -Method 'POST' -Path '/api/control/workflow/manage' -Body @{
        entity = 'contract'
        action = 'create'
        fields = @{
            provider_module = 'nanobot'
            consumer_module = 'fastcode'
            interface_name = 'metrics_api'
            version = 1
            status = 'requested'
            work_item_id = $consumerId
            consumer_work_item_id = $consumerId
            provider_work_item_id = $providerId
        }
    }

    $contractCreate2 = Invoke-Api -Method 'POST' -Path '/api/control/workflow/manage' -Body @{
        entity = 'contract'
        action = 'create'
        fields = @{
            provider_module = 'nanobot'
            consumer_module = 'fastcode'
            interface_name = 'metrics_api'
            version = 1
            status = 'requested'
            work_item_id = $consumerId
            consumer_work_item_id = $consumerId
            provider_work_item_id = $providerId
        }
    }

    $contractId1 = [string]$contractCreate1.result.id
    $contractId2 = [string]$contractCreate2.result.id
    Assert-True (-not [string]::IsNullOrWhiteSpace($contractId1)) 'first contract id missing'
    Assert-True ($contractId1 -eq $contractId2) 'contract dedupe failed: duplicate create returned different id'

    $Script:State.ids.contract_id = $contractId1
    $Script:State.responses.contract_create_1 = $contractCreate1
    $Script:State.responses.contract_create_2 = $contractCreate2

    $audit = Invoke-Api -Method 'GET' -Path '/api/control/contracts/merge-audit?merged_only=true&limit=100'
    $Script:State.responses.merge_audit = $audit

    $auditRecord = $null
    if ($audit.records) {
        $auditRecord = $audit.records | Where-Object { $_.contract_id -eq $contractId1 } | Select-Object -First 1
    }

    Assert-True ($null -ne $auditRecord) 'merge audit record not found for contract'
    Assert-True ([int]$auditRecord.merged_request_count -ge 2) 'merged_request_count < 2'
    Add-Check -Name 'contract_dedupe_audit' -Passed $true -Detail ('contract_id=' + $contractId1 + ', merged_request_count=' + $auditRecord.merged_request_count)
    Save-State

    $edgeCreate = Invoke-Api -Method 'POST' -Path '/api/control/workflow/manage' -Body @{
        entity = 'dependency_edge'
        action = 'create'
        fields = @{
            source_work_item_id = $consumerId
            target_work_item_id = $providerId
            edge_type = 'requires_contract'
            status = 'active'
            metadata = @{
                contract_id = $contractId1
                required_contract_version = 1
            }
        }
    }
    $edgeId = [string]$edgeCreate.result.id
    Assert-True (-not [string]::IsNullOrWhiteSpace($edgeId)) 'dependency edge id missing'
    $Script:State.ids.dependency_edge_id = $edgeId
    $Script:State.responses.dependency_edge_create = $edgeCreate
    Save-State

    $contractUpdate = Invoke-Api -Method 'POST' -Path '/api/control/workflow/manage' -Body @{
        entity = 'contract'
        action = 'update'
        record_id = $contractId1
        fields = @{
            version = 2
        }
    }
    $Script:State.responses.contract_update_version = $contractUpdate
    Assert-True ([int]$contractUpdate.result.version -eq 2) 'contract version update did not apply'

    $snapshot1 = Invoke-Api -Method 'GET' -Path '/api/control/snapshot?limit=500'
    $Script:State.responses.snapshot1 = $snapshot1

    $edgeNow = $snapshot1.dependency_edges | Where-Object { $_.id -eq $edgeId } | Select-Object -First 1
    Assert-True ($null -ne $edgeNow) 'updated snapshot missing dependency edge'
    $edgeRevalidation = $false
    if ($edgeNow.metadata -and $edgeNow.metadata.revalidation_required -eq $true) {
        $edgeRevalidation = $true
    }
    Assert-True $edgeRevalidation 'expected revalidation_required=true on dependency edge after contract version change'
    Add-Check -Name 'contract_revalidation' -Passed $true -Detail 'contract version change propagated revalidation marker'
    Save-State

    $decisionCreate = Invoke-Api -Method 'POST' -Path '/api/control/commands/decisions/submit' -Body @{
        work_item_id = $consumerId
        decision_type = 'api_contract_review'
        status = 'pending'
        chosen_option = ''
        decider = 'human'
        rationale = 'need explicit approval'
    }
    $decisionId = [string]$decisionCreate.decision.id
    Assert-True (-not [string]::IsNullOrWhiteSpace($decisionId)) 'decision id missing on create'

    $decisionApprove = Invoke-Api -Method 'POST' -Path '/api/control/commands/decisions/submit' -Body @{
        decision_id = $decisionId
        work_item_id = $consumerId
        decision_type = 'api_contract_review'
        status = 'approved'
        chosen_option = 'approve'
        decider = 'human'
        rationale = 'approved for acceptance demo'
    }

    Assert-True ([string]$decisionApprove.decision.status -eq 'approved') 'decision was not approved'
    $Script:State.ids.decision_id = $decisionId
    $Script:State.responses.decision_create = $decisionCreate
    $Script:State.responses.decision_approve = $decisionApprove
    Add-Check -Name 'decision_flow' -Passed $true -Detail 'pending -> approved'
    Save-State

    $workItemReady = Invoke-Api -Method 'POST' -Path ('/api/control/commands/work-items/' + $consumerId + '/status') -Body @{
        status = 'ready'
        metadata_patch = @{
            demo_tag = 'acceptance_10m'
        }
    }
    Assert-True ([string]$workItemReady.work_item.status -eq 'ready') 'work item status update to ready failed'
    $Script:State.responses.work_item_ready = $workItemReady

    $tick = Invoke-Api -Method 'POST' -Path '/api/control/workflow/manage' -Body @{
        entity = 'scheduler'
        action = 'tick'
    }
    Assert-True ($tick.ok -eq $true) 'scheduler tick failed'
    $Script:State.responses.scheduler_tick = $tick
    Add-Check -Name 'work_item_status_and_tick' -Passed $true -Detail 'status updated and scheduler tick executed'
    Save-State

    $metricsAll = Invoke-Api -Method 'GET' -Path '/api/control/metrics?limit=500'
    $metricsAgents = Invoke-Api -Method 'GET' -Path '/api/control/metrics/agents?limit=500'
    $metricsContracts = Invoke-Api -Method 'GET' -Path '/api/control/metrics/contracts?limit=500'
    $metricsDecisions = Invoke-Api -Method 'GET' -Path '/api/control/metrics/decisions?limit=500'

    Assert-True ($metricsAll.ok -eq $true) 'metrics all endpoint failed'
    Assert-True ($metricsAgents.ok -eq $true) 'metrics agents endpoint failed'
    Assert-True ($metricsContracts.ok -eq $true) 'metrics contracts endpoint failed'
    Assert-True ($metricsDecisions.ok -eq $true) 'metrics decisions endpoint failed'

    Assert-True ($null -ne $metricsAgents.agents) 'metrics agents missing agents list'
    Assert-True ($null -ne $metricsContracts.contracts) 'metrics contracts missing contracts object'
    Assert-True ($null -ne $metricsDecisions.decisions) 'metrics decisions missing decisions object'

    $agentHasKey = $false
    if ($metricsAgents.agents.Count -gt 0) {
        $firstAgent = $metricsAgents.agents[0]
        if ($null -ne $firstAgent.queue_length -and $null -ne $firstAgent.failure_rate -and $null -ne $firstAgent.dispatch_latency_seconds_avg) {
            $agentHasKey = $true
        }
    } else {
        $agentHasKey = $true
    }
    Assert-True $agentHasKey 'agent metrics missing queue_length/failure_rate/dispatch_latency_seconds_avg'

    $Script:State.responses.metrics_all = $metricsAll
    $Script:State.responses.metrics_agents = $metricsAgents
    $Script:State.responses.metrics_contracts = $metricsContracts
    $Script:State.responses.metrics_decisions = $metricsDecisions

    Add-Check -Name 'metrics' -Passed $true -Detail 'all metrics endpoints responded with expected shape'
    Save-State

    $Script:State.finished_at = (Get-Date).ToString('o')
    $Script:State.result = 'PASS'

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
    $report | ConvertTo-Json -Depth 50 | Set-Content -Path $ReportPath -Encoding UTF8

    Write-Host ''
    Write-Host 'PASS: acceptance demo checks all passed.' -ForegroundColor Green
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
    $report | ConvertTo-Json -Depth 50 | Set-Content -Path $ReportPath -Encoding UTF8

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
        } catch {
        }
    }
}
