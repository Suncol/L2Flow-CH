# Event worker → ClickHouse 吞吐压测报告

测试日期：2026-08-08（Asia/Shanghai）
测试程序：`build-benchmark-clickhouse-release/benchmark_mdl_ingest`
测试对象：`synthetic MDL callback → EventRuntime/EventWorker → Event ClickHouse sink`

本次按用户要求测试区间两端（800k/s 与 1M/s）；没有把 800k–1M 之间的每个
中间速率点都扫一遍，因此报告不声称已建立完整的速率曲线。

> 历史结果边界：本页数值来自 2026-08-08 的旧内存 FactJournal 实现。当前
> `benchmark_mdl_ingest --event-enable` 已替换为每次运行创建唯一的文件型
> `CanonicalFactJournal`，并在结束时执行 `fdatasync`、账务核对和文件清理。
> 因此下表仍是当日旧版本的历史证据，不是当前文件型 journal 路径的性能结果，
> 也不能与 2026-08-09 的新 Event/FactJournal 长测直接比较。下面的命令模板已
> 增加当前版本必需的 journal 目录，但执行它会产生一组新的、不可套用本页数值
> 的结果。

## 结论先行

这组测试不能支持“当前实现已经能够持续承载 800k–1M callback/s”的结论。

在本机、单节点本地 `MergeTree`、一个 Event sink writer、仅 Shanghai Tick
source-event workload 下：

| 能力项 | 800k callback/s 窗口 | 1M callback/s 窗口 | 解释 |
| --- | ---: | ---: | --- |
| callback producer 实测速率 | 800,000.398 msg/s | 1,000,000.262 msg/s | 合成输入达到了目标速率 |
| owner dispatch + EventRuntime 处理速率 | 209,778.498 fact/s（目标的 26.222%） | 202,250.168 fact/s（目标的 20.225%） | Event worker 的端到端 owner 处理能力，明显低于目标 |
| Event revision rows 最终 ACK 有效速率 | 101,030.454 row/s（目标的 12.629%） | 106,706.782 row/s（目标的 10.671%） | 包含 Event sink drain 尾部；不是 ClickHouse 单条 SQL 的服务端耗时 |
| 完整 1 秒窗口的数据完整性 | 800,000/800,000 raw 与 Event rows ACK | 1,000,000/1,000,000 raw 与 Event rows ACK | 短窗口结束后最终全部落库，但需要较长 drain |
| 30 秒资格窗口 | 在 1,448,610 条已接收消息处 fail-closed | 在 1,548,618 条已接收消息处 fail-closed | dispatch queue overflow；未完成资格窗口 |

因此，800k/1M 的 1 秒结果是吞吐 probe，不是持续容量认证。当前实现的
瓶颈在 Event owner 处理和单 writer Event revision 写入路径；不能把短窗口的
“最终 ACK 完整”写成“可持续 800k–1M/s”。

## 1. 测试路径与统计口径

本次 Event 专项路径如下：

```text
synthetic MDL callback
  → admission / decode / normalize / sequence recovery
  → owner dispatch
  → EventRuntime / EventWorker（FactJournal、状态索引、projection）
  → raw_tick ClickHouse ACK gate
  → EventRevisionSink（event_revision_log）
  → materialized view（event）
  → event_recovery_run commit marker
```

Event worker 只有在对应的 raw occurrence 收到 raw ClickHouse ACK 后，才会把
revision batch 交给 Event sink。每个 calculation micro-batch 还会写一个
`event_recovery_run` marker，所以一个 Event batch 至少产生两次 HTTP INSERT：
一次 revision rows、一次 recovery marker。基线结果使用一个 writer thread；
当前代码另外支持 `writer_lanes=1/2/4/8`，但本报告中的 ClickHouse 数字没有
在 2/4/8 lane 下重测，不能直接外推。

报告中几个速率的定义不同，不能混用：

| 名称 | 定义 | 是否包含排队/收尾 |
| --- | --- | --- |
| `producer_msg_s` | measured callback 数 ÷ producer 测量窗口 | 不包含 warm-up；不代表下游已处理 |
| `event_worker_rate` | measured `facts_journaled` ÷ 从第一个 measured callback 到所有 owner 完成 dispatch 的时间 | 包含 owner dispatch、EventRuntime append/flush；不是只调用 `EventWorker::ApplyBatch` 的纯函数 microbenchmark |
| `event_revision_rows_ack_msg_s` | 最终 ACK 的 revision rows ÷ 从 schedule origin 到 Event sink stop 完成的 wall time | 包含 warm-up/排队/最终 drain；不是 ClickHouse server execution rate |
| query-log INSERT latency | `system.query_log.query_duration_ms` | 只表示 ClickHouse 服务端处理该 INSERT 的时间，不含客户端填批和客户端队列等待 |

benchmark 的 `PASS` 门槛是各启用路径至少达到目标的 99%，同时要求准确行数、
无 gap/overflow/retry/unknown outcome、所有 batch ACK/release 且 sink healthy。
因此短窗口的 `FAIL` 是吞吐门槛失败，不等于数据丢失。

## 2. 环境

| 项目 | 实测配置 |
| --- | --- |
| 主机 CPU | 2 × AMD EPYC 9534，128 physical cores / 256 logical CPUs，SMT2 |
| NUMA | node 0：`0-63,128-191`；node 1：`64-127,192-255` |
| 内存 | 约 1.0 TiB |
| OS / kernel | Linux 6.17.0-35-generic |
| 编译器 | GCC 13.3.0 |
| Apache Arrow | 25.0.0 |
| ClickHouse | 26.8.1.923（仓库内官方单二进制） |
| ClickHouse HTTP / Native | `127.0.0.1:8123` / `127.0.0.1:19000` |
| ClickHouse 引擎 | 本地单节点 `MergeTree` / `ReplacingMergeTree` |
| `insert_quorum` | `0`（没有复制集群 quorum 语义） |
| 传输 | 同机 loopback HTTP；Event/Raw 均为同步批量 INSERT |
| ClickHouse CPU | 固定在 NUMA node 0 |
| benchmark CPU | 固定在 NUMA node 1；producer CPU 127，owner consumers 64–79，tick decoder lanes 80–92 |
| 数据目录 | `clickhouse-test/data`；本轮结束约 5.9 GiB（包含本轮多个数据库及其他本地测试数据） |

ClickHouse 与 benchmark 使用不同 NUMA node 是为了减少明显的 CPU 争用；这
仍然是同机测试，不等价于生产网络、磁盘和复制拓扑。

## 3. 工作负载与压测配置

### 3.1 输入数据边界

| 项目 | 配置 |
| --- | --- |
| callback 来源 | synthetic in-process MDL callback，不是 live vendor SDK/network |
| stream tuple | Shanghai `4.101.24` Tick |
| 合成字段 | Tick type=`T`，flag=`B`；16 个 synthetic instrument/channel |
| 测试日期字段 | `20260806` |
| 到达顺序 | `ordered`，无人为 reverse/reorder |
| phase | 未设置为 `Continuous`；本次主要产生 source-event revision |
| revision 行数/事实 | `1.000`（本 workload 中每个 callback 产生 1 个 revision） |
| 未覆盖输入 | Shanghai Add/Cancel/Status、Shenzhen order/transaction、snapshot、late recovery/repair、order-chain 高关联场景 |

### 3.2 完整 1 秒窗口配置

| 参数 | 800k / 1M 窗口 |
| --- | ---: |
| channels / owners | 16 / 16 |
| tick decoder lanes | 12 |
| raw writer threads | 6 |
| raw batch rows | 16,384 |
| raw batch max delay | 500 ms |
| raw queue batches/lane | 64 |
| Event micro-batch rows | 65,536 |
| Event micro-batch max delay | 500 ms |
| Event INSERT chunk rows | 65,536 |
| Event revision queue batches | 4,096 |
| Event revision queue rows | 16,777,216 |
| Event raw-ACK backlog / owner | 1,048,576 |
| dispatch queue capacity | 262,144 |
| warm-up / measured window | 0 s / 1 s |

### 3.3 30 秒资格窗口配置

30 秒窗口将 dispatch queue capacity 设为 65,536，并使用 1 秒 warm-up、30
秒 measurement。其它核心 batch 参数与上表相同。这个窗口的目的，是验证在
更长时间内是否会积累下游 backlog；一旦有明确的 dispatch queue overflow，
引擎按设计 fail-closed，不静默丢弃输入。

### 3.4 可复现命令模板

下面是完整 1 秒窗口使用的等价命令模板；`<database>` 和 `<feed_epoch>` 应
使用每次运行唯一的值（本轮 800k/1M 分别使用 `8104`/`8105`）。30 秒资格
测试只需把 `--seconds` 改为 `30`、`--warmup-seconds` 改为 `1`，并把
`--dispatch-queue-capacity` 改为 `65536`。

```bash
./clickhouse-test/start.sh
numactl --physcpubind=64-127,192-255 --membind=1 \
  ./build-benchmark-clickhouse-release/benchmark_mdl_ingest \
  --rate 800000 --seconds 1 --warmup-seconds 0 \
  --channels 16 --tick-lanes 12 --owners 16 \
  --dispatch-queue-capacity 262144 \
  --producer-cpu 127 --first-consumer-cpu 64 --first-decoder-cpu 80 \
  --clickhouse-url http://127.0.0.1:8123 \
  --clickhouse-database <database> --clickhouse-feed-epoch <feed_epoch> \
  --clickhouse-writers 6 \
  --clickhouse-tick-batch-rows 16384 \
  --clickhouse-tick-batch-max-delay-ns 500000000 \
  --clickhouse-queue-batches-per-lane 64 \
  --event-enable \
  --event-micro-batch-rows 65536 \
  --event-micro-batch-max-delay-ns 500000000 \
  --event-insert-chunk-rows 65536 \
  --event-queue-revision-batches 4096 \
  --event-queue-revision-rows 16777216 \
  --event-maximum-raw-ack-backlog 1048576 \
  --event-journal-dir /tmp
```

命令中的 `--event` 参数通过 benchmark 内部复制 raw sink 的 endpoint、database
和 timeout 配置给 Event sink；不是另一个独立 ClickHouse 实例。压测数据库
应在复核完成后再决定是否清理，避免把数据完整性证据一并删除。

## 4. 800k / 1M 完整 1 秒窗口结果

### 4.1 Producer、owner 与 Event worker

| 指标 | 800k 目标 | 1M 目标 |
| --- | ---: | ---: |
| callback producer 实测 | 800,000.398 msg/s | 1,000,000.262 msg/s |
| callback 数（测量窗口） | 800,000 | 1,000,000 |
| consumed callbacks | 800,000 | 1,000,000 |
| owner dispatch + EventRuntime | 209,778.498 fact/s | 202,250.168 fact/s |
| Event normal ticks | 800,000 | 1,000,000 |
| Event facts journaled | 800,000 | 1,000,000 |
| Event revisions created | 800,000 | 1,000,000 |
| Event micro-batches applied | 32 | 32 |
| dispatch errors / decode errors / gaps | 0 / 0 / 0 | 0 / 0 / 0 |
| Event worker / runtime health | healthy | healthy |
| benchmark status | `FAIL`（Event 速率未达 99% 门槛） | `FAIL`（Event 速率未达 99% 门槛） |

这里的 owner 速率不是只测 `ApplyBatch` 的 CPU microbenchmark，而是实际 owner
线程从 dispatch 到 EventRuntime append/flush 的处理速率；它解释了为什么
producer 能按目标发出消息，但 Event 不能以同样速率持续消费。

### 4.2 Event sink 与 raw sink

| 指标 | 800k 目标 | 1M 目标 |
| --- | ---: | ---: |
| Event revision rows queued / ACK / released | 800,000 / 800,000 / 800,000 | 1,000,000 / 1,000,000 / 1,000,000 |
| Event revision batches queued / ACK / released | 32 / 32 / 32 | 32 / 32 / 32 |
| recovery markers committed | 32 | 32 |
| Event sink effective ACK rate | 101,030.454 row/s | 106,706.782 row/s |
| Event sink wall time（schedule origin → sink stop） | 7.918 s | 9.371 s |
| Event sink `bytes_sent`（RowBinary payload，当前无 retry） | 532,003,840 B | 665,003,840 B |
| Event RowBinary payload / revision row（含 marker 摊销，近似） | 665.005 B/row | 665.004 B/row |
| raw final rows ACK | 800,000 | 1,000,000 |
| pending raw commits after drain | 0 | 0 |
| retry attempts / unknown outcomes | 0 / 0 | 0 / 0 |
| raw sink / Event sink health | healthy / healthy | healthy / healthy |

短窗口的所有 raw/Event 行最终都 ACK，但 Event sink 需要约 7.9–9.4 秒完成
收尾；这不能表述为 800k 或 1M row/s 的持续 Event 写入能力。

### 4.3 ClickHouse SQL 完整性核验

使用 `count()` 和 `event FINAL` 读取本轮专用数据库，结果如下：

| 数据库 | `raw_tick` | `event_revision_log` | `event` | `event FINAL` | committed recovery runs | unique revision batch IDs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `l2flow_event_800k_1s_20260808` | 800,000 | 800,000 | 800,000 | 800,000 | 32 | 32 |
| `l2flow_event_1m_1s_20260808` | 1,000,000 | 1,000,000 | 1,000,000 | 1,000,000 | 32 | 32 |

`event` 是 materialized view 写入的 `ReplacingMergeTree` 当前表；本 workload
没有重复 EventKey，因此普通 `count()` 与 `FINAL` 结果一致。生产查询仍应按
业务需要使用 `FINAL` 或等价的版本选择策略。

## 5. 30 秒资格窗口：持续能力结果

这两次测试都在完整 measurement window 结束前触发 dispatch queue overflow。
`admission failed: not_running` 是引擎进入 fail-closed 后 producer 看到的终止
状态，不是“正常完成 30 秒”。

两次运行的关键错误文本均为：`admission failed: not_running`，以及
`engine fatal: instrument tick dispatch queue overflow`。

| 指标 | 800k 目标 / 30 s | 1M 目标 / 30 s |
| --- | ---: | ---: |
| 请求总 callbacks（1 s warm-up + measurement） | 24,800,000 | 31,000,000 |
| 实际 admitted | 1,448,610 | 1,548,618 |
| consumed callbacks | 400,013 | 500,013 |
| producer measured rate | 799,999.625 msg/s | 999,999.249 msg/s |
| dispatch queue overflows | 21 | 29 |
| Event facts / revisions / rows ACK | 400,013 / 400,013 / 400,013 | 500,013 / 500,013 / 500,013 |
| Event sink effective ACK rate | 77,168.454 row/s | 80,575.766 row/s |
| raw final rows ACK | 1,448,610 | 1,548,618 |
| retries / unknown outcomes | 0 / 0 | 0 / 0 |
| 30 s qualification status | `FAIL`（fail-closed） | `FAIL`（fail-closed） |

这说明下游处理速率低于输入速率时，有限 dispatch queue 会在短时间内耗尽，
系统会显式停止而不是继续运行并悄悄丢数据。由于窗口没有跑完，不能从该表
推导“稳定 30 秒吞吐率”；它只能证明当前配置无法通过 30 秒资格测试。

对应数据库的 `FINAL` 行数核验：

| 数据库 | `raw_tick` | `event_revision_log` | `event FINAL` | committed recovery runs | unique batch IDs |
| --- | ---: | ---: | ---: | ---: | ---: |
| `l2flow_event_800k_30s_fail_20260808` | 1,448,610 | 400,013 | 400,013 | 16 | 16 |
| `l2flow_event_1m_30s_fail_20260808` | 1,548,618 | 500,013 | 500,013 | 16 | 16 |

## 6. ClickHouse `query_log` 服务端延迟

以下只筛选完整 1 秒窗口对应的 Event writer `query_id` 前缀；Event revision
INSERT 的 writer ID 为：

| 窗口 | writer instance ID |
| --- | --- |
| 800k | `3f4adc54e31ef7054b47e3eb94a6c71f` |
| 1M | `1ba007180bac18218f943fdfb2d2be80` |

### 6.1 `event_revision_log` INSERT

| 窗口 | INSERT 数 | 平均 | p50 | p95 | p99 | 最大 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 800k | 32 | 132.71875 ms | 128.5 ms | 150.9 ms | 154.07 ms | 155 ms |
| 1M | 32 | 155.5625 ms | 151 ms | 173.9 ms | 175.69 ms | 176 ms |

### 6.2 `event_recovery_run` marker INSERT

| 窗口 | INSERT 数 | 平均 | p50 | p95 | 最大 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 800k | 32 | 16 ms | 16 ms | 17.45 ms | 19 ms |
| 1M | 32 | 15.90625 ms | 16 ms | 17.45 ms | 20 ms |

### 6.3 raw ACK gate 的 `raw_tick` INSERT（补充）

raw sink 是 Event revision 提交前的 ACK gate；按数据库名筛选其 query log，得到：

| 窗口 | raw INSERT 数 | 平均 | p50 | p95 | p99 | 最大 | `written_rows` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 800k | 60 | 64.117 ms | 64 ms | 74 ms | 74.82 ms | 76 ms | 800,000 |
| 1M | 64 | 63.797 ms | 64 ms | 71 ms | 72.37 ms | 73 ms | 1,000,000 |

这说明 raw path 在本机可以把完整短窗口的行写完并 ACK；但 raw ACK gate
通过后，Event worker 的计算和 Event revision sink 仍然只有约 202k–210k
fact/s、101k–107k row/s，不能把 raw sink 的结果误认为 Event 端到端能力。

`system.query_log.written_rows` 会把同一 INSERT 触发的 materialized view
写入也算进去，因此 revision INSERT 在 query log 中看起来约为实际 revision
rows 的两倍（800k 为 1,600,000，1M 为 2,000,000）。报告中的真实 revision
行数以 Event sink stats 和 `event_revision_log` SQL `count()` 为准；不要用
`written_rows` 直接替代业务行数。

## 7. 批次参数敏感性 smoke test

为确认默认小批次的影响，另做了 10,000 callback/s、1 秒、4 channels/owners
的 smoke test：

| 参数/指标 | 实测值 |
| --- | ---: |
| Event micro-batch rows | 256 |
| Event INSERT chunk rows | 1,024 |
| Event facts / revisions | 10,000 / 10,000 |
| Event micro-batches | 400 |
| recovery markers | 400 |
| Event revision rows ACK | 10,000 |
| Event sink effective ACK | 948.432 row/s |
| Event sink stop drain | 9,493.788 ms |
| retries / unknown outcomes | 0 / 0 |

每 256 条就生成一次 calculation batch，且每 batch 还要写 recovery marker；这
个配置会把 HTTP INSERT 次数放大，不适合作为 800k–1M 目标的配置。它只是
参数敏感性证据，不是容量结论。

## 8. 构建、测试与复核

本轮重新编译成功：

```bash
cmake --build build-benchmark-clickhouse-release -j 16
```

在本地 ClickHouse 可用且 `L2FLOW_CH_TEST_URL` 已设置时，完整 CTest 结果为：

```text
100% tests passed, 0 tests failed out of 16
```

另外显式运行了真实 loopback 集成测试：

```bash
NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
L2FLOW_CH_TEST_URL=http://127.0.0.1:8123 \
  ./build-benchmark-clickhouse-release/test_clickhouse_event
# ClickHouse Event integration passed
# all ClickHouse Event tests passed

NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost \
L2FLOW_CH_TEST_URL=http://127.0.0.1:8123 \
  ./build-benchmark-clickhouse-release/test_clickhouse_raw
# ClickHouse raw integration passed
# all ClickHouse raw tests passed
```

测试程序、Event schema 和实现文件均保留在工作树中；本轮压测数据库也保留，
便于用 `clickhouse client` 复核。若在受限网络 shell 中直接访问 `127.0.0.1`
被 HTTP proxy 拦截，需要像上面的集成测试一样设置 `NO_PROXY`，或在允许本机
回环 socket 的环境执行。

当前 CTest 另有
`mdl_ingest_event_benchmark_construction_smoke`：它不连接 ClickHouse，只验证
`benchmark_mdl_ingest` 能创建唯一 journal、把同一实例注入全部 Event owners、
完成空 journal 的 `fdatasync` 并删除文件。它是调用路径回归测试，不是吞吐测试。

## 9. 结果边界与未覆盖场景

1. 输入是 synthetic in-process MDL callback，不是 live vendor SDK、网卡或
   网络链路；不能据此推断生产 SDK callback 的 CPU/内存行为。
2. 本次只生成 Shanghai `4.101.24` Tick（type `T`、flag `B`）。未覆盖
   Shanghai Add/Cancel/Status、Shenzhen order/transaction、snapshot、late
   recovery/repair、order-chain 高关联、process restart/bootstrap。
3. phase 没有设置成 `Continuous`，所以本结果主要是 source-event revision，
   `revision_rows_per_callback=1.000`；不能代表连续订单状态投影的行数和成本。
4. ClickHouse 是同机 loopback、本地 `MergeTree`/`ReplacingMergeTree`、
   `insert_quorum=0`，不是复制集群 quorum ACK，也没有测网络抖动、节点故障、
   磁盘故障或副本合并压力。
5. 本报告的 Event sink 结果只有一个 writer thread；2/4/8 lane、不同
   chunk/batch、Native protocol 或异步/合并写入都会改变结果，不能把这里的
   数值当成所有部署配置的上限。
6. 800k/1M 完整窗口只有 1 秒；30 秒资格测试明确 fail-closed。没有完成至少
   30 秒、300 秒或更长的稳定窗口，不能宣称持续容量。
7. query-log 的 INSERT 延迟不包含客户端批次填充和排队；Event effective ACK
   rate 则包含 drain 尾部，两者不能直接相除作为单行端到端延迟。
8. raw/Event 队列是有界且易失的，当前实现的 fail-closed 保护不等价于带 WAL
   的重启恢复能力。

## 10. 最终判断

| 问题 | 判断 |
| --- | --- |
| callback producer 能否按 800k–1M/s 生成？ | 在该 synthetic probe 中可以，实测约等于目标。 |
| Event worker 能否按 800k–1M/s 持续处理？ | 不能；完整短窗口的端到端 owner 处理约 202k–210k fact/s，30 秒窗口队列溢出并 fail-closed。 |
| Event ClickHouse 能否按 800k–1M revision row/s 持续写入？ | 不能；当前单 writer 的最终 ACK 有效速率约 101k–107k row/s。 |
| 1 秒窗口是否丢数据？ | 本轮完整短窗口没有发现丢失：raw、revision、`event FINAL` 行数均与 callback 数一致，batch 全部 ACK/release。 |
| 能否对生产集群作容量承诺？ | 不能；必须补测真实 SDK/network、完整消息族、连续状态/修复、复制 ClickHouse、故障与更长稳态窗口。 |

推荐对外表述：

> 在本机、本地 MergeTree、单 Event writer、仅 Shanghai Trade source-event
> workload 下，800k 和 1M callback/s 的完整 1 秒窗口均可最终完成全部
> raw/Event ACK，但 Event owner 处理速率仅约 202k–210k fact/s，Event
> revision ACK 约 101k–107k row/s；30 秒资格窗口在约 1.45M–1.55M 已接收
> 消息处因 dispatch queue overflow fail-closed。因此当前实现不能宣称可
> 持续承载 800k–1M callback/s。该结果是当前实现、当前配置和本地环境的实测
> 结果，不是生产集群容量承诺。
