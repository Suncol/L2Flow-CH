# `mdl_ingestd` NUMA 绑核五分钟压力测试报告

测试日期：2026-08-06

> 历史报告说明：本报告记录的是当日已经被替换的独立
> `LateRecovery` 队列架构及其指标名称。当前实现使用统一
> `TickDispatch`、strict first-wins、精确 hole ledger 和 owner FIFO
> controls；以下旧术语与数字仅用于保留原始测试证据，不构成当前接口或
> 正确性契约。当前契约见
> [`mdl-ingestd-design.md`](mdl-ingestd-design.md)。
>
> 默认值变更说明：本报告所测 2026-08-06 版本的 FROM_OPEN/PARTIAL gap
> wait 默认值均为 500,000 ns。当前版本已将两者的默认值改为
> 20,000,000 ns；以下 500 us 描述均指当日被测版本，不应解释为当前默认值。

## 1. 结论

在本机 NUMA node 1 上显式绑定一个硬件线程/物理核，当前
`callback -> admission -> decoder -> Channel recovery -> canonical ->
instrument dispatch` 实现完成了六组 300 秒稳态测试：

| 到达模式 | 最高通过速率 | 单组计量消息 | p99（最高档） | p999（最高档） | 数据质量 |
|---|---:|---:|---:|---:|---|
| Channel 内有序，默认 500 us gap wait | 1,200,000 msg/s | 360,000,000 | 2.770 us | 7.130 us | 全部质量计数为 0 |
| Channel 内局部逆序，窗口 8，显式 20 ms 测试覆盖 | 1,200,000 msg/s | 360,000,000 | 11.230 us | 15.510 us | 全部质量计数为 0 |

因此，可以确认这套**合成单一 tick 类型的进程内热路径**至少具备
1.2M msg/s 的五分钟处理能力；不能据此声称真实 MDL 网络接入、全部
五类消息混合负载或 1 TiB 生产机已经验收。

局部逆序的 20 ms 是本次共享测试机为了隔离“管线吞吐能力”和
“500 us 缺口决策稳定性”而使用的显式测试参数，当日被测版本的默认值
没有修改，仍为 500,000 ns。该版本默认 500 us 的局部逆序长测在约
205.1 秒时遇到一次
约 3.029 ms 的生产者调度迟滞，正确地提交 gap 并把迟到报文送入
LateRecovery，因而不能满足“全部报文仍从实时 Instrument 支路输出”的
本组验收条件。显式绑核并不等价于 CPU isolation。

## 2. 测量边界与工作负载

| 项目 | 本次定义 |
|---|---|
| 起点 | `MdlMessageHandler::OnMessage` 函数内第一条单调时钟采样 |
| 终点 | 对应 Instrument owner 从 dispatch endpoint 成功取到 canonical tick 后采样 |
| 包含 | callback 活跃计数、`GetHead`/`GetBody`、23-byte header copy/parse、admission body copy、lane 传递、完整 decode/normalization、Channel reorder/recovery、lane x owner dispatch queue、owner poll |
| 不包含 | 网络收包、vendor SDK 在调用 callback 之前的工作、Event/KLine、共享内存、Kafka/Redpanda、ClickHouse |
| 合成消息 | `4.101.24` `NGTSTick`，70-byte body + 23-byte header；16 个上海 Channel、16 个 A 股样式 Instrument |
| 并行度 | 12 tick decoder lanes、1 个空闲 snapshot lane、16 instrument owners、1 producer/callback thread |
| 启动模式 | FROM_OPEN |
| 预热 | 5 秒，不计入吞吐和延迟结果 |
| 稳态窗口 | 每组 300 秒 |
| 有序模式 | 每个 Channel 的 native sequence 严格递增 |
| 局部乱序模式 | 每个 Channel 连续 8 条按 `8,7,...,1` 到达；窗口闭合后必须按 `1,2,...,8` dispatch |
| 百分位 | 每个 Instrument 每 67 条确定性采样一次；67 与窗口 8 互质，避免固定相位偏差 |
| 最大延迟 | `max_all` 遍历全部计量消息，不做抽样 |
| 通过条件 | 实际 producer/dispatch 均不低于目标的 99%；消息数完整；每 Instrument sequence 严格递增；所有 overflow/gap/LateRecovery/fault/error 计数为 0；engine healthy；线程实际 CPU 与配置一致 |

局部逆序的 8 条使用同一个计划发送时刻，表示一个小型到达突发；整体
块间隔仍受目标平均速率控制。延迟起点位于 callback 函数内部，因此不
包含进入虚函数之前的调用指令开销。

## 3. 测试机与 NUMA 布局

| 项目 | 实测值 |
|---|---|
| CPU | 2 x AMD EPYC 9354，32 cores/socket，SMT2；64 physical cores / 128 logical CPUs |
| NUMA | 2 nodes；node 0=`0-31,64-95`，node 1=`32-63,96-127`；local distance 10，remote distance 32 |
| 内存 | 503 GiB OS 可见，node 0=257,777 MB，node 1=257,932 MB；不是目标 1 TiB 生产机 |
| Kernel | Linux 6.8.0-124-generic，`PREEMPT_DYNAMIC` |
| CPU governor | 被测 CPU 为 `schedutil`，不是 `performance` |
| Isolation | `isolated` 为空，未配置 `nohz_full`；kernel cmdline 未配置 `isolcpus`/`rcu_nocbs` |
| 实时调度 | 当前环境不允许 `SCHED_FIFO`/`chrt -f` |
| 进程 NUMA 约束 | `numactl --physcpubind=32-63 --membind=1` |

线程映射只使用 node 1 每个物理核的第一条硬件线程；对应 SMT siblings
`96-127` 未分配给本进程，但本机没有 cpuset/isolation 阻止其他任务使用
这些 sibling。

| 角色 | logical CPUs | 数量 | 行为 |
|---|---|---:|---|
| Instrument owner consumers | `32-47` | 16 | pause-spin poll |
| Tick decoder/recovery | `48-59` | 12 | 显式绑定后 pause-spin |
| Snapshot decoder | `60` | 1 | 本 workload 无 snapshot，仍专核 pause-spin |
| 预留 | `61-62` | 2 | 未由 benchmark 线程使用 |
| Producer / SDK callback 模拟线程 | `63` | 1 | 按目标速率 busy-wait 调度 |

六组成功结果均报告 `observed_bindings_valid=true`。进程读取
`/proc/self/numa_maps` 时，匿名页在 node 0 的计数均为 0；这验证了本次
已触页的落点，不代表所有未来 1 TiB 容量配置均已验证。

## 4. 五分钟吞吐与延迟结果

所有延迟单位均为 microseconds（us），吞吐单位为 msg/s。

| 模式 | gap wait | 目标 | Producer 实测 | Dispatch 实测 | 计量消息 | Samples | p50 | p90 | p99 | p999 | max_all | 最大计划迟滞 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ordered | 500 us | 800,000 | 800,000.001 | 799,999.997 | 240,000,000 | 3,582,096 | 2.050 | 2.500 | 2.851 | 300.512 | 14,067.276 | 9,090.839 |
| local-reverse-8 | 20 ms | 800,000 | 800,000.008 | 799,999.997 | 240,000,000 | 3,582,096 | 6.380 | 10.150 | 11.980 | 323.913 | 29,909.995 | 22,441.434 |
| ordered | 500 us | 1,000,000 | 1,000,000.001 | 999,999.996 | 300,000,000 | 4,477,616 | 2.040 | 2.330 | 2.720 | 41.300 | 19,100.250 | 19,097.406 |
| local-reverse-8 | 20 ms | 1,000,000 | 1,000,000.005 | 999,999.992 | 300,000,000 | 4,477,616 | 6.240 | 10.100 | 11.110 | 15.140 | 79.481 | 73.684 |
| ordered | 500 us | 1,200,000 | 1,200,000.000 | 1,199,999.993 | 360,000,000 | 5,373,136 | 2.150 | 2.570 | 2.770 | 7.130 | 92.000 | 14,323.526 |
| local-reverse-8 | 20 ms | 1,200,000 | 1,200,000.002 | 1,199,999.988 | 360,000,000 | 5,373,136 | 6.300 | 10.110 | 11.230 | 15.510 | 78.460 | 23,496.442 |

`最大计划迟滞` 是 producer 实际开始 callback 相对合成发送 deadline 的
最坏迟到，不属于 callback-entry-to-dispatch 延迟。较大的计划迟滞可以
发生在两个 reverse block 之间；此时尚无打开的 Channel gap timer，故
不会自动形成 gap。反之，如果调度暂停落在尚未闭合的 reverse block
内部，即使总体吞吐足够，也可能超过缺口等待时间。

800k 和 1.0M 部分组的 p999/max 出现毫秒级调度长尾，而 p99 保持稳定。
这与本机共享 OS、`schedutil`、无 CPU/IRQ isolation 的条件一致；它说明
不能只报告百分位，也不能把这台机器上的单次 max 当作纯算法成本。

## 5. 完整性与 NUMA 质量计数

`Admitted/Consumed` 包含预热，`Measured dispatch` 不包含预热。1.0M
局部乱序组为了保持 `16 channels x 8 window` 的完整量子，将预热从
5,000,000 向上取整到 5,000,064。

| 模式/目标 | Admitted | Consumed | Measured dispatch | Ordering error | Lane full | Decode error | Dispatch overflow | Gap | LateRecovery | Channel fault | Anonymous pages N0/N1 | 状态 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| ordered / 800k | 244,000,000 | 244,000,000 | 240,000,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 / 20,917 | PASS |
| reverse-8 / 800k | 244,000,000 | 244,000,000 | 240,000,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 / 20,881 | PASS |
| ordered / 1.0M | 305,000,000 | 305,000,000 | 300,000,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 / 24,442 | PASS |
| reverse-8 / 1.0M | 305,000,064 | 305,000,064 | 300,000,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 / 24,202 | PASS |
| ordered / 1.2M | 366,000,000 | 366,000,000 | 360,000,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 / 27,699 | PASS |
| reverse-8 / 1.2M | 366,000,000 | 366,000,000 | 360,000,000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 / 27,695 | PASS |

成功组总计处理 1,830,000,064 条（包含预热），其中计量窗口合计
1,800,000,000 条；未发现丢失、重复导致的顺序偏移、reorder 输出倒序、
admission lane 饱和或 instrument dispatch 饱和。

## 6. 局部乱序成本

下表只比较稳定百分位；`p999` 和 `max_all` 更容易由共享 OS 的偶发暂停
主导，不适合用单次差值解释 reorder 算法成本。

| 目标速率 | Ordered p50 | Reverse-8 p50 | p50 增量 | Ordered p99 | Reverse-8 p99 | p99 增量 |
|---:|---:|---:|---:|---:|---:|---:|
| 800k | 2.050 | 6.380 | +4.330 | 2.851 | 11.980 | +9.129 |
| 1.0M | 2.040 | 6.240 | +4.200 | 2.720 | 11.110 | +8.390 |
| 1.2M | 2.150 | 6.300 | +4.150 | 2.770 | 11.230 | +8.460 |

该差值包含 reverse block 等待最低序号到达、连续 drain 以及消费者排队，
不是单个 reorder 查表操作的微基准。

## 7. 默认 500 us 的 FROM_OPEN 边界测试

为了遵守当时“不要默认等得更久”的测试约束，被测实现与正常有序测试保留
`from_open_gap_wait_ns=500,000`。另一次 800k、reverse-8、默认 500 us
的原定 300 秒测试得到以下非通过结果：

| 项目 | 结果 |
|---|---:|
| 完成时长 | 约 205.1 秒计量窗口后提前失败 |
| 已 admission（含 4M 预热） | 168,115,227 |
| 最大 producer 计划迟滞 | 3,028.945 us |
| 截止失败时 p50 / p90 / p99 / p999 | 6.470 / 10.400 / 12.550 / 151.541 us |
| 截止失败时 max_all | 4,006.016 us |
| Gap counter | 123 |
| LateRecovery bodies | 469 |
| Dispatch overflow counter（LateRecovery publish） | 2 |
| Engine | unhealthy，测试 FAIL |

以下因果顺序只解释 2026-08-06 当日的旧实现：共享 OS 在一个未闭合
reverse window 内暂停 producer 超过 500 us；recovery 记录 `ChannelGap`、
推进 frontier 并继续实时流；迟到的完整 canonical body 进入当时独立的
`LateRecovery` 队列。该 benchmark 没有运行旧队列的 consumer，队列耗尽
后按当时设计 fail-fast，而不是静默丢 body。

这个历史结果不是 reorder buffer 丢弃已收到报文，也不表示应把生产默认值
暗中改长；但它不能转化为给当前实现配置 `LateRecovery` consumer 的建议。
当前实现没有该队列：仍在精确 hole ledger 和在线窗口内的首个回补通过统一
`TickDispatch` 进入 owner repair，其他后到 occurrence 通过 disposition/raw
ACK join 结算并拒绝。现行验收应分别覆盖这两条路径及其容量边界。

20 ms 测试覆盖只回答“在缺口决策没有被非业务调度暂停意外触发时，
reorder/canonical/dispatch 管线能否承载目标速率”。它不构成把生产等待
参数调为 20 ms 的建议。

## 8. 复现命令

有序组（替换 `RATE` 为 `800000`、`1000000`、`1200000`）：

```bash
numactl --physcpubind=32-63 --membind=1 \
  ./build/benchmark_mdl_ingest \
  --rate RATE --seconds 300 --warmup-seconds 5 \
  --pattern ordered --sample-every 67 \
  --channels 16 --tick-lanes 12 --owners 16 \
  --producer-cpu 63 --first-consumer-cpu 32 \
  --first-decoder-cpu 48
```

局部逆序的纯管线容量组：

```bash
numactl --physcpubind=32-63 --membind=1 \
  ./build/benchmark_mdl_ingest \
  --rate RATE --seconds 300 --warmup-seconds 5 \
  --pattern local-reverse --reorder-window 8 \
  --sample-every 67 --gap-wait-ns 20000000 \
  --channels 16 --tick-lanes 12 --owners 16 \
  --producer-cpu 63 --first-consumer-cpu 32 \
  --first-decoder-cpu 48
```

在当日被测版本中，删除 `--gap-wait-ns 20000000` 即恢复 500 us 默认值；
当前版本省略该参数时使用 20 ms 默认值。

## 9. 生产验收建议

| 优先级 | 动作 | 验收目的 |
|---:|---|---|
| P0 | 使用 cpuset/cgroup 或 `isolcpus` 同时隔离被测物理核及其 SMT siblings；将 NIC/存储 IRQ 和 RCU 工作移出热核 | 使 500 us gap 决策不被普通调度暂停主导 |
| P0 | 固定 `performance` governor，并在授权和审计后评估 `SCHED_FIFO`；监控 starvation | 降低频率与调度唤醒抖动 |
| P0 | 真实 MDL SDK + NIC 上重复六组 5 分钟测试，保留默认 500 us，并单独注入 100/300/499/>500 us 回补 | 区分可闭合乱序、`GapOpen`、窗口内 hole fill 和过窗拒绝语义 |
| P0 | 压测 exact hole ledger、统一 `TickDispatch`/owner fence 与 disposition/raw-ACK join 的最坏突发 | 验证回补修订能力、拒绝结算和所有有界 ledger 的 fail-closed 行为 |
| P0 | 使用同时交织的 SZ `6.101.33`/`6.101.36` 捕获回放确认共享 `ApplSeqNum` 域 | 关闭当前 vendor 文档未明确声明共享计数器的事实边界 |
| P1 | 用五类 tuple 的真实比例、真实 body size、Channel/Instrument 基数和 burst profile 重测 | 本报告只有单一 `4.101.24` 合成负载 |
| P1 | 在目标 1 TiB 主机上执行 first-touch/NUMA 页面、huge page、内存锁定和 queue capacity 验证 | 本测试机仅约 503 GiB，不能代替目标内存拓扑验收 |
| P1 | 将 p50/p99/p999/max、schedule lag、gap epoch、hole fill/reject/expiry、ACK join 和 lane/dispatch occupancy 接入监控 | 在生产中分辨算法延迟、网络回补、拒绝结算和 OS 抖动 |

在完成上述 P0 之前，合理结论是“当前实现的合成进程内路径已证明
1.2M msg/s / 5 min 能力”，而不是“生产端到端 1.2M msg/s 已验收”。

## 10. 构建与动态检查

| 检查 | 配置 | 结果 |
|---|---|---|
| Release 严格编译 | `-Wall -Wextra -Wpedantic -Wconversion -Wshadow -Werror` | PASS |
| CTest | `mdl_ingest_core`、`mdl_ingest_default_config` | 2/2 PASS |
| ASan + UBSan | `detect_leaks=0:halt_on_error=1`、`UBSAN_OPTIONS=halt_on_error=1` | PASS |
| LeakSanitizer | 当前 Codex ptrace 环境关闭 leak detection | 未作为通过结果 |
| ThreadSanitizer | 可构建；当前 ptrace/container 环境在测试前因 unsupported memory mapping 退出 | 环境阻塞，未作为通过结果 |
