"""本地 MIB 列配置与 OID 工具。

MIB 列定义只来自本模块的内置配置，不从网络下载。网关返回的 varbind 必须
能按列 OID 前缀与索引后缀拆开，再按 ``(代理, 索引后缀)`` 重新成行；返回顺序
不参与任何列配对。
"""

from dataclasses import dataclass

#: ifTable（RFC 2863）与 ifXTable 中本接收程序关心的列。
#: kind 取值：identity（接口身份列）、scalar（普通标量）、
#: counter（计数器，width 为 32/64）、ticks（TimeTicks）。
@dataclass(frozen=True)
class Column:
    name: str
    oid: str
    syntax: str
    kind: str
    width: int | None = None

    @property
    def oid_tuple(self):
        return parse_oid(self.oid)


COLUMNS = (
    Column("ifIndex", "1.3.6.1.2.1.2.2.1.1", "Integer", "scalar"),
    Column("ifDescr", "1.3.6.1.2.1.2.2.1.2", "OctetString", "identity"),
    Column("ifType", "1.3.6.1.2.1.2.2.1.3", "Integer", "scalar"),
    Column("ifMtu", "1.3.6.1.2.1.2.2.1.4", "Integer", "scalar"),
    Column("ifPhysAddress", "1.3.6.1.2.1.2.2.1.6", "OctetString", "identity"),
    Column("ifAdminStatus", "1.3.6.1.2.1.2.2.1.7", "Integer", "scalar"),
    Column("ifOperStatus", "1.3.6.1.2.1.2.2.1.8", "Integer", "scalar"),
    Column("ifInOctets", "1.3.6.1.2.1.2.2.1.10", "Counter32", "counter", 32),
    Column("ifOutOctets", "1.3.6.1.2.1.2.2.1.16", "Counter32", "counter", 32),
    Column("ifName", "1.3.6.1.2.1.31.1.1.1.1", "OctetString", "identity"),
    Column("ifHCInOctets", "1.3.6.1.2.1.31.1.1.1.6", "Counter64", "counter", 64),
    Column("ifHCOutOctets", "1.3.6.1.2.1.31.1.1.1.10", "Counter64", "counter", 64),
    Column("ifCounterDiscontinuityTime", "1.3.6.1.2.1.31.1.1.1.19", "TimeTicks", "ticks"),
)

#: 计数器列对应的不连续时间列（RFC 2863 ifCounterDiscontinuityTime）。
DISCONTINUITY_COLUMN = "ifCounterDiscontinuityTime"

#: SNMP 异常语法（RFC 3416），它们带 OID 位置但没有可用值。
EXCEPTION_TYPES = frozenset({"noSuchInstance", "noSuchObject", "endOfMibView"})

#: Counter32 回绕增量若超过半量程，按“未声明的不连续”处理而不是当作合法回绕，
#: 避免两个本不可比的数相减后伪造出接近 2^32 的巨大流量。
WRAP_SANITY = {32: 1 << 31, 64: 1 << 63}


class MibProfile:
    """按最长前缀识别 varbind 所属列及索引后缀。"""

    def __init__(self, columns=COLUMNS):
        self.columns = tuple(columns)
        self.by_name = {column.name: column for column in self.columns}
        # 长前缀优先，保证将来出现嵌套 OID 空间时匹配确定。
        self._ordered = sorted(self.columns, key=lambda item: len(item.oid_tuple), reverse=True)

    def split(self, oid_text):
        """返回 ``(Column, 索引后缀字符串)``；无法识别时返回 ``(None, None)``。

        索引后缀是列前缀之后剩余的全部子标识符（点分字符串），例如
        ``1.3.6.1.2.1.2.2.1.10.8`` 拆成 ifInOctets 与 ``"8"``。
        """
        oid = parse_oid(oid_text)
        for column in self._ordered:
            prefix = column.oid_tuple
            if len(oid) > len(prefix) and oid[: len(prefix)] == prefix:
                return column, ".".join(str(part) for part in oid[len(prefix):])
        return None, None

    def counter_columns(self):
        return tuple(column for column in self.columns if column.kind == "counter")

    def identity_columns(self):
        return tuple(column for column in self.columns if column.kind == "identity")


def parse_oid(oid_text):
    if not isinstance(oid_text, str) or not oid_text.strip():
        raise ValueError("OID 必须是非空字符串")
    parts = []
    for piece in oid_text.strip().split("."):
        if not piece.isdigit():
            raise ValueError("非法 OID 子标识符: " + oid_text)
        parts.append(int(piece))
    if not parts:
        raise ValueError("空 OID")
    return tuple(parts)


def format_oid(parts):
    return ".".join(str(part) for part in parts)


def is_under(oid_text, prefix_text):
    oid, prefix = parse_oid(oid_text), parse_oid(prefix_text)
    return len(oid) > len(prefix) and oid[: len(prefix)] == prefix
