"""接口表列定义与 OID 工具。

MIB 列定义固定在本地配置，不从网络下载。这里只覆盖演练所需的
ifTable/ifXTable 子集（RFC 2863），索引为单级 ifIndex。
"""
import json
from dataclasses import dataclass
from pathlib import Path

# 非异常的 SNMP 值类型；异常类型单独处理，不携带值。
VALUE_TYPES = {
    "OctetString",
    "Integer32",
    "Counter32",
    "Counter64",
    "Gauge32",
    "TimeTicks",
    "IpAddress",
    "Opaque",
}

# RFC 3416 的异常 varbind 类型。
EXCEPTION_TYPES = {"noSuchObject", "noSuchInstance", "endOfMibView"}

UINT32_MAX = 2 ** 32 - 1
UINT64_MAX = 2 ** 64 - 1
INT32_MIN, INT32_MAX = -(2 ** 31), 2 ** 31 - 1


def parse_oid(text):
    """把点分 OID 文本解析为整数元组，非法输入抛 ValueError。"""
    if not isinstance(text, str):
        raise ValueError("OID must be text")
    parts = text.strip().split(".")
    if not parts or any(not part.isdigit() for part in parts):
        raise ValueError("invalid OID: " + repr(text))
    return tuple(int(part) for part in parts)


def format_oid(oid):
    return ".".join(str(part) for part in oid)


@dataclass(frozen=True)
class Column:
    """一个接口表列：名称、OID 前缀、值类型、是否计数器。"""

    name: str
    prefix: tuple
    value_type: str
    counter: bool = False

    @property
    def prefix_text(self):
        return format_oid(self.prefix)


def _column(name, prefix, value_type, counter=False):
    return Column(name, parse_oid(prefix), value_type, counter)


# 默认列集合：ifTable 与 ifXTable 的常用列，ifCounterDiscontinuityTime
# 是单计数器不连续的本地证据（RFC 2863）。
DEFAULT_COLUMNS = [
    _column("ifDescr", "1.3.6.1.2.1.2.2.1.2", "OctetString"),
    _column("ifInOctets", "1.3.6.1.2.1.2.2.1.10", "Counter32", True),
    _column("ifOutOctets", "1.3.6.1.2.1.2.2.1.16", "Counter32", True),
    _column("ifHCInOctets", "1.3.6.1.2.1.31.1.1.1.6", "Counter64", True),
    _column("ifHCOutOctets", "1.3.6.1.2.1.31.1.1.1.10", "Counter64", True),
    _column("ifCounterDiscontinuityTime", "1.3.6.1.2.1.31.1.1.1.19", "TimeTicks"),
]

DISCONTINUITY_COLUMN = "ifCounterDiscontinuityTime"


def load_columns(path=None):
    """加载列配置；未给路径时用内置默认列。配置是本地 JSON，不联网。"""
    if path is None:
        return list(DEFAULT_COLUMNS)
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, list) or not raw:
        raise ValueError("columns config must be a non-empty list")
    columns = []
    for item in raw:
        columns.append(
            Column(
                str(item["name"]),
                parse_oid(str(item["prefix"])),
                str(item["type"]),
                bool(item.get("counter", False)),
            )
        )
    names = [column.name for column in columns]
    if len(set(names)) != len(names):
        raise ValueError("duplicate column names")
    return columns


def match_column(columns, oid):
    """最长前缀匹配；返回 (column, 索引后缀)，不在任何列下返回 (None, None)。"""
    best = None
    for column in columns:
        prefix = column.prefix
        if oid[: len(prefix)] == prefix and (best is None or len(prefix) > len(best.prefix)):
            best = column
    if best is None:
        return None, None
    return best, oid[len(best.prefix):]


def valid_value(value_type, value):
    """按 SNMP 类型校验值；布尔不是整数。"""
    if isinstance(value, bool):
        return False
    if value_type in ("Counter32", "Gauge32", "TimeTicks"):
        return isinstance(value, int) and 0 <= value <= UINT32_MAX
    if value_type == "Counter64":
        return isinstance(value, int) and 0 <= value <= UINT64_MAX
    if value_type == "Integer32":
        return isinstance(value, int) and INT32_MIN <= value <= INT32_MAX
    if value_type in ("OctetString", "IpAddress", "Opaque"):
        return isinstance(value, str)
    return False
