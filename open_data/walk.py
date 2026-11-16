"""按列前缀的有界 GetBulk 游走与轮次发布。

关键约束：

* varbind 一律通过 :class:`~open_data.mib.MibProfile` 按 OID 列前缀与索引后缀
  拆格，再由存储层按索引成行；任何阶段都不按响应里的位置横向配对；
* 每列只有收到 ``endOfMibView`` 才算“本轮已核实”；tooBig 只允许缩小
  ``max_reps`` 后在同一游标重试；超时是传输故障，noSuchInstance 只是某个
  实例不存在，二者都不是整表走完或清空的证据；
* 每次 HTTP 响应都进只追加归档，请求、单元格与归档 capture 互相引用，
  断点恢复后仍能解释每一行的 OID 证据；
* 预算（每列页数、整轮请求数、重试次数）全部有界，故障列不会拖垮其他设备。
"""

import http.client
import json
from dataclasses import dataclass, field
from urllib.error import URLError
from urllib.parse import urlencode

from .mib import EXCEPTION_TYPES, MibProfile


@dataclass
class WalkConfig:
    initial_max_reps: int = 3
    min_max_reps: int = 1
    column_page_budget: int = 50
    poll_request_budget: int = 200
    timeout_retries: int = 1


@dataclass
class PollSummary:
    poll_id: int
    agent: str
    poll_no: int
    state: str
    abort_reason: str | None
    verified_columns: list = field(default_factory=list)
    rows: list = field(default_factory=list)
    request_count: int = 0
    publication_scope: str | None = None

    def to_dict(self):
        return {
            "poll_id": self.poll_id,
            "agent": self.agent,
            "poll_no": self.poll_no,
            "state": self.state,
            "abort_reason": self.abort_reason,
            "verified_columns": list(self.verified_columns),
            "publication_scope": self.publication_scope,
            "request_count": self.request_count,
            "rows": self.rows,
        }


class ProtocolError(RuntimeError):
    """网关响应不符合本地 GetBulk 契约；宁可中止轮次也不猜测配对。"""


class Walker:
    def __init__(self, client, archive, store, source, profile=None, config=None):
        self.client = client
        self.archive = archive
        self.store = store
        self.source = source
        self.profile = profile or MibProfile()
        self.config = config or WalkConfig()

    # ---- 单轮入口 --------------------------------------------------------

    def run_poll(self, agent):
        poll_id, poll_no = self.store.begin_poll(agent, self.profile.columns)
        verified = []
        conflicts = []
        try:
            for column in self.profile.columns:
                result = self._walk_column(poll_id, poll_no, agent, column)
                verified.append(column.name)
                conflicts.extend(result["conflicts"])
                self.store.set_column_state(poll_id, column.name, "verified",
                                            f"以 endOfMibView 结束，共 {result['requests']} 次请求")
        except _Abort as abort:
            for column in self.profile.columns:
                if column.name not in verified:
                    self.store.set_column_state(poll_id, column.name, "incomplete", abort.reason)
            rows = self.store.materialize_rows(poll_id, self.profile, conflicts)
            self.store.finish_poll(poll_id, "aborted", abort.reason)
            manifest = self._manifest(agent, poll_no, verified, rows, abort.reason)
            self.store.publish(poll_id, "partial", manifest)
            return PollSummary(poll_id, agent, poll_no, "aborted", abort.reason,
                               verified, rows, self._total_requests(poll_id), "partial")

        rows = self.store.materialize_rows(poll_id, self.profile, conflicts)
        self.store.finish_poll(poll_id, "complete")
        manifest = self._manifest(agent, poll_no, verified, rows, None)
        self.store.publish(poll_id, "full", manifest)
        return PollSummary(poll_id, agent, poll_no, "complete", None,
                           verified, rows, self._total_requests(poll_id), "full")

    # ---- 单列游走 --------------------------------------------------------

    def _walk_column(self, poll_id, poll_no, agent, column):
        cursor = column.oid
        reps = self.config.initial_max_reps
        page = 0
        conflicts = []
        while True:
            page += 1
            if page > self.config.column_page_budget:
                raise _Abort("column_page_budget_exhausted:" + column.name)
            seq = self._total_requests(poll_id) + 1
            if seq > self.config.poll_request_budget:
                raise _Abort("poll_request_budget_exhausted")
            # _fetch 内部在同一游标上做有限次传输重试，重试占用新的请求序号
            # 但不推进游标，也不产生任何单元格证据。
            envelope, record = self._fetch(poll_id, poll_no, agent, column, cursor, reps, seq)
            status = envelope["error_status"]
            if status == "tooBig":
                if reps <= self.config.min_max_reps:
                    raise _Abort("tooBig_at_min_reps:" + column.name)
                reps = max(self.config.min_max_reps, reps // 2)
                continue
            if status != "noError":
                raise _Abort("unexpected_error_status:" + status)
            varbinds = envelope["varbinds"]
            if not varbinds:
                # 空的 noError 不推进游标；继续请求只会原地打转，必须显式中止。
                raise _Abort("empty_response_no_advance:" + column.name)
            if len(varbinds) == 1 and varbinds[0]["type"] == "endOfMibView":
                return {"requests": self._total_requests(poll_id), "conflicts": conflicts}
            if "mutate" in envelope.get("events", []):
                # 轮内改表：本列此前观察到的是改表前快照。先回滚待发布单元格
                # （原始响应仍在归档中留证），再从列起点重读改表后的整列；
                # 随后 materialize 时同一单元格只看到改表后的一致视图，而改表
                # 事实与两次响应都在 requests/归档里可解释。
                self.store.rollback_column(poll_id, column.name)
                cursor = column.oid
                page += 1
                continue
            cells = []
            for varbind in varbinds:
                matched, index_suffix = self.profile.split(varbind["oid"])
                if matched is None or matched.name != column.name:
                    raise _Abort("varbind_outside_requested_column:" + varbind["oid"])
                is_exception = varbind["type"] in EXCEPTION_TYPES
                cells.append({
                    "agent": agent,
                    "index_suffix": index_suffix,
                    "oid": varbind["oid"],
                    "value_type": varbind["type"],
                    "value": None if is_exception else varbind["value"],
                    "is_exception": is_exception,
                })
            new_conflicts = self.store.record_cells(
                poll_id, record["request_id"], record["capture_id"], column, cells
            )
            conflicts.extend(new_conflicts)
            cursor = varbinds[-1]["oid"]

    # ---- 单次请求（含归档与传输重试）--------------------------------------

    def _fetch(self, poll_id, poll_no, agent, column, cursor, reps, seq):
        query = urlencode({
            "agent": agent, "poll": poll_no, "column": column.oid,
            "oid": cursor, "max_reps": reps,
        })
        path = "/snmp/bulk?" + query
        try:
            response = self.client.fetch(path)
        except (URLError, OSError, http.client.HTTPException) as error:
            self.store.record_request(
                poll_id, column, seq, cursor, reps, "transport_error",
                error=type(error).__name__ + ":" + str(error),
            )
            if self._timeouts_used(poll_id, column) <= self.config.timeout_retries:
                next_seq = self._total_requests(poll_id) + 1
                if next_seq > self.config.poll_request_budget:
                    raise _Abort("poll_request_budget_exhausted")
                envelope, record = self._fetch(
                    poll_id, poll_no, agent, column, cursor, reps, next_seq
                )
                record["retried"] = True
                return envelope, record
            raise _Abort("transport_error:" + column.name)
        capture_id = self.archive.append(
            self.source, response.url, response.status, response.headers, response.body
        )
        if response.status == 504:
            # 网关超时：归档留痕后按传输故障在同一游标重试，不产生任何单元格证据。
            self.store.record_request(
                poll_id, column, seq, cursor, reps, "transport_error",
                capture_id=capture_id, http_status=504, error="gateway_timeout",
            )
            if self._timeouts_used(poll_id, column) <= self.config.timeout_retries:
                next_seq = self._total_requests(poll_id) + 1
                if next_seq > self.config.poll_request_budget:
                    raise _Abort("poll_request_budget_exhausted")
                envelope, record = self._fetch(
                    poll_id, poll_no, agent, column, cursor, reps, next_seq
                )
                record["retried"] = True
                return envelope, record
            raise _Abort("transport_error:" + column.name)
        if response.status != 200:
            self.store.record_request(
                poll_id, column, seq, cursor, reps, "http_error",
                capture_id=capture_id, http_status=response.status,
            )
            raise _Abort("http_status_" + str(response.status))
        try:
            envelope = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            self.store.record_request(
                poll_id, column, seq, cursor, reps, "protocol_error",
                capture_id=capture_id, http_status=response.status, error=str(error),
            )
            raise _Abort("invalid_envelope:" + column.name)
        self._validate_envelope(envelope, agent, poll_no, column, cursor, reps)
        self.store.record_request(
            poll_id, column, seq, cursor, reps, "ok",
            capture_id=capture_id, http_status=response.status,
            sys_uptime=envelope.get("sysUpTime"),
        )
        request_id = self.store.requests_of(poll_id)[-1]["id"]
        return envelope, {"request_id": request_id, "capture_id": capture_id, "retried": False}

    def _timeouts_used(self, poll_id, column):
        return sum(
            1 for item in self.store.requests_of(poll_id)
            if item["column_name"] == column.name and item["result"] == "transport_error"
        )

    def _total_requests(self, poll_id):
        return len(self.store.requests_of(poll_id))

    @staticmethod
    def _validate_envelope(envelope, agent, poll_no, column, cursor, reps):
        for field_name in ("agent", "poll", "error_status", "varbinds"):
            if field_name not in envelope:
                raise ProtocolError("envelope missing field: " + field_name)
        if envelope["agent"] != agent or envelope["poll"] != poll_no:
            raise ProtocolError("envelope identity does not match request")
        requested = envelope.get("requested", {})
        if requested.get("column") != column.oid or requested.get("oid") != cursor \
                or requested.get("max_reps") != reps:
            raise ProtocolError("envelope echo does not match request cursor")
        if not isinstance(envelope["varbinds"], list):
            raise ProtocolError("varbinds must be a list")

    @staticmethod
    def _manifest(agent, poll_no, verified, rows, abort_reason):
        complete = [row["index_suffix"] for row in rows if row["completeness"] == "complete"]
        incomplete = [
            {"index_suffix": row["index_suffix"],
             "missing_columns": row["missing_columns"],
             "conflict_columns": row["conflict_columns"]}
            for row in rows if row["completeness"] == "incomplete"
        ]
        return {
            "agent": agent,
            "poll_no": poll_no,
            "verified_columns": list(verified),
            "complete_rows": complete,
            "incomplete_rows": incomplete,
            "abort_reason": abort_reason,
            "assembly": "oid_column_prefix_plus_index_suffix",
        }


class _Abort(Exception):
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason
