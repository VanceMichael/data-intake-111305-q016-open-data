# 来源协议

## 记录与身份

默认来源入口为 `/feed`，媒体格式为 `json`。来源身份字段是 `agent`、`poll`。这些字段表示来源自身声明，不等同于已经解决的业务实体身份。输入顺序是一次接收的观察顺序，并不保证业务修订有序。

`/observations/1` 等地址允许逐条读取合成观察，返回 `items` 数组。正文、页内嵌入数据与CSV均保留供方原文，不在协议层推断当前有效值。网页中的 `source-records` 是门户向下载客户端提供的公开数据块；表单还带有隐藏控件，令牌用于本地协议状态检查。

## 传输响应

`/feed` 有 ETag `feed-1`，可发送 `If-None-Match` 得到304。`/rate-limited` 返回429和两秒的Retry-After；`/unstable` 首次503，其后200；`/truncated` 声明长度大于实际传送长度并断开。`/range` 支持单段字节范围及416，`/range-ignored` 故意返回整份200，不能假定所有供方都会履行范围请求。

每次独立启动来源都有独立计数器。一个路由在 `origin.json` 中若是响应数组，依请求次序前进，最后一个响应保持不变。可通过编辑一份场景副本再用 `serve --scenario 文件` 启动不同交错序列。

## 表单回执

演练门户只接受本地POST `/portal/submit`，表单字段为 `id`、`csrf`，当前令牌由场景的 `portal.csrf` 声明。过时令牌409，缺少身份422。添加 `lose_response=yes` 会先受理再断开连接；随后GET `/portal/status?id=标识` 可看到真实回执。门户不提供提交幂等键，接收程序默认不执行提交。该端点用于受理确认和供应商核对的本地演练，不连接任何真实门户。

## 领域样例

- `ragged_columns`：相邻varbind来自不同索引；业务侧关注按OID索引成行，不按位置串列。
- `counter_break`：设备重启或单计数器不连续；业务侧关注新连续段不产生伪造增量。
- `walk_incomplete`：tooBig后还没走完；业务侧关注轮询不冒充整表覆盖。

## 领域接入契约

网关信封是本地JSON，varbind保留SNMP值类型及OID。GetBulk、tooBig和endOfMibView参考RFC 3416；接口表、ifIndex及ifCounterDiscontinuityTime参考RFC 2863。MIB列定义在本地配置（`open_data/mib.py`），不从网络下载。只读合成资料，不发扫描或设备写入命令。

## 合成 SNMP 网关（`/snmp/bulk`）

场景文件 `fixtures/snmp_gateway.json` 只含虚构设备。网关按 OID 子标识符元组的词法顺序返回 varbind，请求参数为 `agent`、`poll`（调用方轮次号）、`column`（列 OID 前缀）、`oid`（游标，只返回严格大于它的后继）、`max_reps`。

- `error_status=tooBig`：当前 `max_reps` 超出设备阈值。接收方应调小下一次领取范围并在同一游标重试；在最小范围仍被拒时中止该轮，而不是无限重试。
- `endOfMibView`：该列游走结束的唯一证据。没有它，列只能标记为未核实。
- `noSuchInstance`：只表示某个实例不存在，作为带 OID 的异常证据保留，不是整表清空。
- HTTP 504：模拟网关超时，没有 SNMP 信封，按传输故障在同一游标有限重试；超时不产生单元格证据，也不是游走结束。
- 场景事件 `mutate` 在一次轮询内改表，响应信封的 `events` 字段会标注；接收方回滚该列已观察的待发布单元格并从列起点重读，发布改表后的一致视图，前后两份响应都在归档中留证。
- 列级事件的 `when_request` 是该列的第几次数据请求；只得到 `endOfMibView` 的探针不消耗该序号。

## 成行、发布与连续性

- varbind 一律按列 OID 前缀加索引后缀拆格，再按 `(代理, 索引后缀)` 成行，绝不按响应里的位置横向配对；值的 SNMP 类型原样保留。
- 轮次状态区分 `running/complete/aborted`。进程重开时未完成的 running 轮次记为 `aborted`（`interrupted_by_restart`），不冒充完整覆盖；发布范围相应为 `full` 或 `partial`，清单列明已核实列、完整行与缺列/冲突的不完整行。
- 连续段只在段内相邻点间计算增量。sysUpTime 回落、`ifCounterDiscontinuityTime` 变化、同 ifIndex 身份指纹变化（ifIndex 被复用）、观测缺口、无声明的计数器下降都会切段；Counter32 仅对小于半量程的回绕做 2^32 修正，Counter64 下降一律开新段，新段起点 `delta=null`，不把两个不可比的数相减。
- 每个单元格都记录自身 OID、所属请求与归档 capture，可通过 `evidence` 命令逐格回溯到原始响应。

