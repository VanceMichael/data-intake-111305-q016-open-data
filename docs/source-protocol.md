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

网关信封是本地JSON，varbind保留SNMP值类型及OID。GetBulk、tooBig和endOfMibView参考RFC 3416；接口表、ifIndex及ifCounterDiscontinuityTime参考RFC 2863。MIB列定义在本地配置，不从网络下载。只读合成资料，不发扫描或设备写入命令。

## 合成网关协议

`open_data.gateway` 在回环地址提供合成网关，把场景文件中的接口表按 GetBulk 语义返回为已解码信封，不连接任何真实设备。

`GET /getbulk?agent=名称&poll=轮次&oids=游标1,游标2&max_repetitions=N` 返回 `{agent, poll, sysUpTime, error_status, varbinds}`。每个请求游标按列独立推进：响应该列游标之后最多 N 个实例，列的实例集合互不相同（缺列即稀疏响应）；列耗尽返回该游标OID的 endOfMibView。`max_repetitions` 超过场景声明的 `max_repetitions_limit` 时返回 `error_status: tooBig` 和空 varbinds。场景 `faults` 可声明 `drop`（直接断开，模拟响应丢失）、`http500`、`no_such_instance`（指定列的指定索引）。缺省轮次沿用最近一轮的表，模拟轮间保持。

## 接口表接收口径

- 行按 OID 列前缀加索引后缀组装；响应内 varbind 顺序只作审计，不参与配对。ifTable/ifXTable 的索引是单级 ifIndex，多级后缀按越界杂项记录。
- 列只有在本轮拿到 endOfMibView 才算核实；tooBig 只把下一次领取范围减半，不作废已收到的单元格。
- 超时（传输断开或信封 `error_status: timeout`）与 noSuchInstance 都不是整表已清空：前者重试有界、轮次保持未完成，后者记为该索引该列的空洞。
- 一轮请求预算用尽即暂停，列游标持久化，下次从断点继续；设备之间状态独立，互不影响进度。
- sysUpTime 回落是重启证据，关闭该设备全部开放连续段；ifCounterDiscontinuityTime 前进关闭该接口的开放段；Counter64 段内回落另起新段；Counter32 段内回落按回绕处理。增量只在连续段内计算，不做跨段相减。
- 轮次固定时只用本轮单元格计算行完整性，不从上一轮补列；每行的每个单元格以 capture_id 回指原始响应归档。

