# TioolMan

TiFlow [README](README-TiCDC.md)

## 项目介绍

本项目为 TiFlow（原 TiCDC）增加 Key-Range 粒度负载均衡的能力，以提高资源利用率、满足单表大速率写入、纯 Key-Value 等业务场景下 CDC 的需要。

## 背景&动机

TiFlow 目前以 table 为粒度进行任务调度和负载均衡。这个设计在以下场景中存在限制：

#### 1. 不同表之间写入速率差异大。在目前的粒度下，单表的 CDC 只能由 TiFlow 中单个线程（processor）进行处理，因此会导致 TiFlow 集群负载不均、资源浪费
#### 2. 单表大速率写入。表粒度下，当变更数据量太大、同步延迟变长时，无法通过水平扩容提高处理能力（见 tiflow#1207）
#### 3. 无法满足纯 Key-Value 业务场景。直接使用 TiKV 作为 Key-Value 存储的场景下，由于没有 table 概念，因此无法使用 TiFlow 实现 CDC

因此，为了满足上述三个业务场景的需要，本项目为 TiFlow 增加 Key-Range 粒度的任务调度和负载均衡能力，帮助 TiDB、TiKV 进一步完善生态。

## 项目设计

### 1. Why Key Range
Table 之下再进行数据分片，Key Range 是一个显而易见的粒度。事实上 TiDB 本身就是按照 Key Range 进行数据分片，因此 TiFlow 在这个粒度仍然可以很好的与 TiDB / TiKV 配合。

### 2. Why NOT Region
Region 本质上是 TiKV 按照负载均衡的需要划分 Key Range。TiFlow 不直接采用 Region 作为粒度，原因包括：

* Region 的划分同时考虑了存储容量、读写负载、高可用等因素，而 TiFlow 主要考虑写负载。因此按照 Region 的划分很可能不适合 TiFlow
* Region 不稳定，如果采用 Region 与 task 对应，Region merge 会引起 task 失效、Region split 则导致 key range 出现空洞，将大大增加系统的复杂度
但同时需要注意的是，因为 TiFlow 与 TiKV 之间的数据同步以 Region 为单位，获取一个 Region 的部分数据在现有实现下有资源浪费。因此 TiFlow 在进行数据切分的时候，应当尽可能在 Region 边界

### 3. 调度
基于 1 和 2，调度模块应当统计各个 Region 的变更数据量，并根据资源数量，在 Region 边界均衡划分
同时，在每个调度周期，重新获取 Region 分布，结合变更数据量统计调整调度
当出现 Region 变化时：
* Region Split：不影响任务调度
* Region Merge：如果导致捕获 Region 的部分数据，在适当的时机调整任务调度
此外，由于存在 table 粒度的 Metrics，因此 Key Range 的划分应当考虑在 table 边界（可能与 Region 边界冲突。可以暂时优先满足 table 边界，以降低实现复杂度）

### 4. 数据同步一致性
#### 4.1 MySQL sink
目前 TiFlow 在 MySQL sink 上不拆分单表事务，保证[单表事务的原子性](https://docs.pingcap.com/zh/tidb/stable/ticdc-overview/#%E6%95%B0%E6%8D%AE%E5%90%8C%E6%AD%A5%E4%B8%80%E8%87%B4%E6%80%A7)。调度粒度变更为 Key Range 之后，这点保证将被破坏，上游的一个事务可能被拆分为多个事务，并破坏下游的 Snapshot Isolation。解决方案如下：

##### 4.1.1 对于 TiDB
增加 TiDB sink。TiDB sink 将直接在 mvcc 层写入数据，并复制上游数据的 start_ts 和 commit_ts，从而在上下游之间产生完全相同的 snapshot。同时，resolved_ts 也将同步给下游 TiDB。当读请求的 start_ts 小于 resolved_ts，就可以获得与上游 snapshot 一致的数据。
TiDB sink 还有以下优点：
* 直接写入 TiKV，在同步性能上更优
* 保持跨表事务原子性
* 实际上是一个 TiKV sink + DDL sink。TiKV sink 可用于满足 TiKV CDC 的场景

##### 4.1.2 对于其他数据库
目前没有比较好的解决方案，需要用户在同步效率与事务原子性之间权衡。

#### 4.2 Kafka sink
没有变化。

### 5. 其他模块的调整

#### 5.1 Owner
* 根据调度算法生成 Key Range Task。一个 Changefeed 的 Task 数量与 Capture 数量相等，（参考 Region 分布）平均分配到所有 Capture
* 根据 Key Range 收集并汇总 Changefeed 粒度的 Metrics

#### 5.2 Capture
* 接收 Key Range Task 并拉起 Processor

#### 5.3 Processor
* 处理 Key Range Task
* 一个 Processor 处理一个 Task，即`一个Changefeed`在`一个Capture`上的`一个或多个Key Range`
* processor.addTable 需要增加相应的 addKeySpan 版本
* processor.handlePosition 处理 KeySpans 的 position

#### 5.4 Puller
暂无

#### 5.5 Sink
bufferSink 以 tableID 作为缓存粒度，需要修改

