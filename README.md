# 园区网络 SNMP 接口表的成行采集与连续性标记

这是资料接收程序的基础版。它请求来源接口，保留响应状态、头部、原始内容与每次观察的时间，能够从 JSON、CSV 或门户嵌入的数据块读回来源记录。当前不生成面向业务的有效版本，也不自动执行门户提交；重复观察会分别保留，原始内容按摘要复用。

## 运行环境

使用 Python 3.11 及以上，业务运行依赖均为标准库；SQLite 由 Python 自带。时区资料使用运行环境内的系统数据库。运行过程不安装依赖，也不下载数据。本地来源服务器和接收程序在同一进程环境中即可运行。

构建工具及其传递依赖精确锁定在 `requirements-build.txt`。首次准备开发环境时先执行下面命令；之后测试、构建和演示均可离线运行，不在运行命令中安装包。

```sh
python3 -m pip install -r requirements-build.txt
```

```sh
python3 -m unittest discover -s tests -v
python3 -m build --wheel --no-isolation
python3 -m compileall -q open_data tests
python3 -m open_data demo
```

演示从空的临时目录启动本地来源，真实完成一次 HTTP 获取及归档，并输出原样记录；结束自动清理。本地测试、编译和演示合计一般不超过十秒，可直接展示，不依赖历史日志。

## 常驻来源与接收

```sh
python3 -m open_data serve
```

终端会输出本次分配的回环地址。在另一终端将下面地址替换为实际地址：

```sh
python3 -m open_data capture --base-url http://127.0.0.1:实际端口 --archive .runtime/raw.sqlite3 --source local-source --path /feed
```

归档库只追加来源观察，不用最后一条响应覆盖先前响应。`archive.py` 管理原始内容及完整性，`client.py` 执行一次请求，`decoding.py` 负责来源格式解码，`origin.py` 提供协议演练。业务解释可与原始接收独立演进。

## SNMP 接口表成行采集

在原始响应归档之上，`mib.py`、`gateway.py`、`walk.py`、`store.py`、`continuity.py` 实现接口表（ifTable/ifXTable）接收。合成网关只监听回环地址、只读，不扫描真实网络也不写设备。

- **按 OID 成行，不按位置配对**：varbind 按列 OID 前缀与索引后缀拆格，再按 `(代理, 索引后缀)` 组装行；SNMP 值类型（Counter32/Counter64/OctetString/TimeTicks 及 noSuchInstance 等异常）原样保留。
- **有界游走**：逐列 GetBulk，列页预算、整轮请求预算、传输重试都有上限。tooBig 调小 `max_reps` 同游标重试；只有 `endOfMibView` 才标记该列本轮已核实；504 超时与 noSuchInstance 都不是整表走完或清空。
- **轮次记录与发布**：轮次区分 complete/aborted，发布范围为 full/partial，清单列明已核实列、完整行以及缺列或冲突的不完整行；轮内改表会回滚该列待发布单元格并重读，不把上一轮残留列补进本轮。
- **连续段**：sysUpTime 回落、ifCounterDiscontinuityTime 变化、ifIndex 复用导致的身份指纹变化、观测缺口、无声明的计数器下降都切段；Counter32 小回绕按 2^32 修正，Counter64 下降直接开新段，段起点不做减法，避免伪造巨大流量。
- **多设备与断点恢复**：每台设备轮次号独立，互不拖累；进程重开后未完成轮次记为中断。每个单元格都带 OID、请求与归档 capture，可逐格回溯证据。

```sh
python3 -m open_data snmp-demo
```

离线端到端演示覆盖 ragged 列、tooBig、超时重试、noSuchInstance、轮内改表、Counter 回绕、重启复用 ifIndex、单计数器不连续与中断恢复。常驻网关与单轮采集：

```sh
python3 -m open_data gateway
python3 -m open_data walk --base-url http://127.0.0.1:实际端口 \
  --archive .runtime/raw.sqlite3 --store .runtime/tables.sqlite3 \
  --source local-snmp-gateway --agent switch-a
python3 -m open_data poll --store .runtime/tables.sqlite3 --poll-id 1
python3 -m open_data evidence --store .runtime/tables.sqlite3 --poll-id 1 --index 7
python3 -m open_data segments --store .runtime/tables.sqlite3 --agent switch-a
```

`gateway --scenario 文件` 与 `serve --scenario` 行为一致：找不到指定文件直接报错，不回退默认资料。

## 开发资料

来源协议见 `docs/source-protocol.md`，领域观察关系见 `tests/test_source_contract.py`。`fixtures/business_cases.json` 保存业务侧关注的交错情况及可观察口径；基础版测试验证的是原始采集和协议，不代表业务处理已经具备这些能力。所有条目、机构、文件内容和门户回执均为本地合成资料，不包含真实用户身份。

## Wheel 分发

构建产物位于 `dist/`，包含 `open_data` 和命名空间内的合成来源 JSON。安装 wheel 后，即使当前目录不是源码目录，也可执行 `python3 -m open_data demo`。提供 `serve --scenario` 时仍以指定文件为准，找不到指定文件会直接报错，不回退到默认资料。
