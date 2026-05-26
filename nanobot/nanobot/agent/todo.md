





如果一个agent要请求另一个agent让他新增接口怎么办，以及能不能直接对用户发送消息。core做的是派发消息而不是project agent的传话筒。

1. 把它从对话式协作，收敛成“基于契约的异步任务编排”。如果不先把接口契约、依赖图和状态机做出来，所谓高并行最后通常会退化成高冲突

第二，subagent 的异步执行是“fire-and-forget + 文本结果通知”，不是“结构化依赖任务”。它能后台跑，但没有：

任务 ID 对应的依赖边
契约版本
子任务产物位置
哪些模块被谁阻塞
哪些下游因为接口变更而失效
现在的 subagent 结果只是通过消息总线回主 agent，总线本身只是两个 asyncio queue，没有调度器、优先级、锁、依赖解析或冲突仲裁，queue.py:20。

第三，当前“隔离”是目录隔离，不是模块契约隔离。project agent 的作用域是路径级，context.py:92。这对减少乱改代码有帮助，但它并不知道：

自己拥有哪些接口
哪些接口是外部依赖
哪些文件属于公开契约，哪些属于内部实现
修改接口后需要通知谁重新生成/验证

第四，状态管理现在只有“会话历史”，没有“工作流状态”。

第五，当前主循环整体仍然是串行消费消息的。AgentLoop.run() 是单个 inbound queue 上逐条 await _process_message，loop.py。所以你现在能做到的是“局部后台任务并发”，不是“系统级多模块调度并发”。


最稳妥的做法不是让 agent 之间自由对话，而是让它们围绕“接口工单”协作。也就是：

module agent 只拥有本模块目录的写权限。
agent 之间不直接共享自然语言上下文作为事实源，而是共享结构化契约。
任意跨模块请求都产出一个 artifact，比如：
InterfaceRequest
InterfaceSpec
StubGenerated
ImplementationReady
Verified
上游 agent 可以基于 InterfaceSpec + stub 继续开发，不必等待真实实现。
下游 agent 完成实现后，调度器触发依赖方重新验证。

数据平面可以是 agent 发请求
控制平面必须还是 core / orchestrator 记账和裁决

多 module 状态管理，建议用 DAG + 事件溯源，不要只靠 session
你提到的复杂状态，核心不是“聊天历史”，而是“任务依赖图”。最合适的是把每次跨模块请求建模成图上的节点和边。

我建议最少有这几个实体：

Module

针对你这个仓库，最缺的不是 agent，而是 6 个部件

模块注册表
需要明确每个目录对应哪个 module agent、拥有哪些文件、暴露哪些接口。现在只有目录 scope，没有 ownership registry。

契约仓库
需要一个统一位置存接口请求和接口定义，比如每个模块下放 contracts/，或全局 data/agent_graph/。现在 agent 之间只传文本，没有结构化 contract artifact。

异步批量委派
你在 todo.md:5 写的 delegate_projects_batch 是必须的，但不够。它还要返回 task handle，而不是立即等待所有结果拼完。

调度器
现在只有消息队列，没有“依赖解析 + ready queue + retry/backoff + cancellation + invalidation”。这才是你的并行系统的大脑。

状态存储
当前 session 只存对话，manager.py:70。你需要 workflow store，持久化 work item、contract、edge、artifact、version。

验证器
你的方案成立的前提，是 agent 先按接口继续写代码，后续真实实现补上还能自动发现偏差。所以每个模块必须有最小验证动作，比如类型检查、接口测试、编译检查。否则只是把串行错误并行化。


但它距离你的目标仍然差两层：

结构化模块协作协议
图级状态编排



建议的落地顺序


加一个 reconciler/scheduler，持续把 ready 任务派给对应 module agent。
最后再做版本失效传播和重验证。
