"""接口表接收：信封应用、有界游走、轮次收尾与发布。

核心约定：
- 行按 OID 列前缀加索引后缀组装，绝不按响应内位置横向配对；
- 列只有拿到 endOfMibView 才算本轮核实；tooBig 只调整下一次领取范围；
- 超时、noSuchInstance 都不是整表已清空的证据；
- 计数器只在连续段内比较，重启或 discontinuity 之后另起新段。
"""
import hashlib
import json
import socket
from dataclasses import dataclass, field
from http.client import HTTPException
from urllib.error import URLError
from urllib.parse import urlencode

from .client import collect
from .decoding import records as decode_records
from .ifmib import (
    EXCEPTION_TYPES,
    VALUE_TYPES,
    format_oid,
    match_column,
    parse_oid,
    valid_value,
)

# 传输层失败都按“本次响应未到达”处理：可重试，不是表已清空。
NETWORK_ERRORS = (URLError, HTTPException, OSError, socket.timeout)

# 一轮内允许的最大请求数上限（防止网关异常时无限游走）。
ROUND_REQUEST_CAP = 200


@dataclass
class Applied:
    """一次信封应用的结果摘要，供游走循环判断进度。"""

    duplicate: bool = False
    error_status: str = "noError"
    reboot: bool = False
    cells: int = 0
    holes: int = 0
    verified: list = field(default_factory=list)
    strays: int = 0
    late: bool = False

    @property
    def progressed(self):
        return bool(self.cells or self.holes or self.verified)


def envelope_digest(envelope):
    canonical = json.dumps(envelope, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validate_envelope(envelope):
    if not isinstance(envelope, dict):
        raise ValueError("gateway envelope must be an object")
    agent = envelope.get("agent")
    poll = envelope.get("poll")
    if not isinstance(agent, str) or not agent.strip():
        raise ValueError("envelope agent is required")
    if isinstance(poll, bool) or not isinstance(poll, int) or poll < 1:
        raise ValueError("envelope poll must be a positive integer")
    error_status = envelope.get("error_status", "noError")
    if not isinstance(error_status, str):
        raise ValueError("envelope error_status must be text")
    varbinds = envelope.get("varbinds", [])
    if not isinstance(varbinds, list):
        raise ValueError("envelope varbinds must be a list")
    sys_up_time = envelope.get("sysUpTime")
    if sys_up_time is not None and (
        isinstance(sys_up_time, bool) or not isinstance(sys_up_time, int) or sys_up_time < 0
    ):
        raise ValueError("envelope sysUpTime must be a non-negative integer")
    return agent.strip(), poll, error_status, varbinds, sys_up_time


def classify_varbind(columns, varbind):
    """把单个 varbind 归类为单元格、空洞、列结束或越界杂项。

    返回 (类别, 详情)。类别：cell / hole / end / stray。
    """
    if not isinstance(varbind, dict):
        return ("stray", {"reason": "varbind_not_object", "raw": repr(varbind)[:200]})
    oid_text = varbind.get("oid")
    value_type = varbind.get("type")
    try:
        oid = parse_oid(oid_text)
    except (ValueError, TypeError):
        return ("stray", {"reason": "bad_oid", "raw": repr(oid_text)[:200]})
    if not isinstance(value_type, str):
        return ("stray", {"reason": "bad_type", "oid": format_oid(oid)})
    column, suffix = match_column(columns, oid)
    if value_type == "endOfMibView":
        # 结束证据按 OID 归属到列；越界的 endOfMibView 不算任何列的结束。
        if column is not None:
            return ("end", {"column": column.name})
        return ("stray", {"reason": "end_of_mib_view_outside_scope", "oid": format_oid(oid)})
    if value_type in ("noSuchObject", "noSuchInstance"):
        if column is not None and len(suffix) == 1:
            return (
                "hole",
                {
                    "column": column.name,
                    "idx": str(suffix[0]),
                    "oid": format_oid(oid),
                    "hole_type": value_type,
                },
            )
        return ("stray", {"reason": "exception_outside_scope", "oid": format_oid(oid)})
    if value_type not in VALUE_TYPES:
        return ("stray", {"reason": "unknown_type", "oid": format_oid(oid), "type": value_type})
    if column is None:
        return ("stray", {"reason": "outside_scope", "oid": format_oid(oid)})
    if len(suffix) != 1:
        # ifTable/ifXTable 的索引是单级 ifIndex；多级后缀属于别的表。
        return ("stray", {"reason": "index_not_single_ifindex", "oid": format_oid(oid)})
    value = varbind.get("value")
    if not valid_value(value_type, value):
        return ("stray", {"reason": "bad_value", "oid": format_oid(oid), "type": value_type})
    return (
        "cell",
        {
            "column": column.name,
            "idx": str(suffix[0]),
            "oid": format_oid(oid),
            "type": value_type,
            "value": value,
        },
    )


def apply_envelope(store, envelope, capture_id):
    """把一个网关信封应用到走查状态；按摘要去重，可安全重放。

    只写本轮证据：单元格、空洞、列结束、游标与 sysUpTime 观察。
    """
    agent, poll, error_status, varbinds, sys_up_time = validate_envelope(envelope)
    digest = envelope_digest(envelope)
    if store.envelope_seen(digest):
        return Applied(duplicate=True, error_status=error_status)
    result = Applied(error_status=error_status)
    round_row = store.round_for(agent, poll)
    if round_row is None:
        round_id = store.begin_round(agent, poll, store.agent_state(agent)["max_repetitions"])
        round_row = store.round_row(round_id)
    elif round_row["status"] != "in_progress":
        # 迟到信封：轮次已固定，只留审计痕迹，不改写已发布的行。
        result.late = True
        store.note_event(round_row["id"], "late_envelope", capture=capture_id)
        store.record_envelope(digest, agent, poll, capture_id)
        return result
    round_id = round_row["id"]
    if store.note_sys_up_time(agent, round_id, sys_up_time):
        result.reboot = True
        store.note_event(round_id, "reboot", sys_up_time=sys_up_time, capture=capture_id)
    if error_status == "tooBig":
        store.note_event(round_id, "too_big", capture=capture_id)
    elif error_status == "timeout":
        store.note_event(round_id, "timeout", capture=capture_id, transport="envelope")
    elif error_status != "noError":
        store.note_event(round_id, "error_status", status=error_status, capture=capture_id)
    else:
        for seq, varbind in enumerate(varbinds):
            kind, detail = classify_varbind(store.columns, varbind)
            if kind == "cell":
                store.record_cell(
                    round_id,
                    detail["column"],
                    detail["idx"],
                    detail["oid"],
                    detail["type"],
                    detail["value"],
                    sys_up_time,
                    capture_id,
                    seq,
                )
                result.cells += 1
            elif kind == "hole":
                store.record_hole(
                    round_id,
                    detail["column"],
                    detail["idx"],
                    detail["oid"],
                    detail["hole_type"],
                    capture_id,
                )
                result.holes += 1
            elif kind == "end":
                store.verify_column(round_id, detail["column"], capture_id)
                result.verified.append(detail["column"])
            else:
                store.note_event(round_id, "stray", capture=capture_id, **detail)
                result.strays += 1
    store.record_envelope(digest, agent, poll, capture_id)
    return result


class Walker:
    """单设备有界游走：一轮内按列游标推进，预算用尽即暂停并保留断点。"""

    def __init__(
        self,
        client,
        archive,
        store,
        *,
        gateway_path="/getbulk",
        request_budget=40,
        timeout_retries=3,
        source="ifwalk",
    ):
        self.client = client
        self.archive = archive
        self.store = store
        self.gateway_path = gateway_path
        self.request_budget = request_budget
        self.timeout_retries = timeout_retries
        self.source = source

    def walk_agent(self, agent, rounds=1):
        """推进某台设备至多 rounds 个轮次；返回每轮结果，互不依赖其它设备。"""
        outcomes = []
        for _ in range(rounds):
            state = self.store.agent_state(agent)
            round_row = self.store.in_progress_round(agent)
            if round_row is None:
                round_id = self.store.begin_round(agent, state["next_poll"], state["max_repetitions"])
                round_row = self.store.round_row(round_id)
            outcome = self._drive(agent, round_row)
            outcomes.append(outcome)
            if outcome["result"] != "finished":
                break
        return {"agent": agent, "rounds": outcomes}

    def _drive(self, agent, round_row):
        round_id = round_row["id"]
        attempts = 0
        timeouts = 0
        stalled = 0
        while True:
            columns = self.store.round_columns(round_id)
            pending = [column for column in columns if column["status"] == "walking"]
            if not pending:
                status = self.store.finalize_round(round_id, "complete")
                return {"poll": round_row["poll"], "result": "finished", "status": status}
            current = self.store.round_row(round_id)
            if current["requests"] >= ROUND_REQUEST_CAP:
                self.store.note_event(round_id, "request_cap", cap=ROUND_REQUEST_CAP)
                status = self.store.finalize_round(round_id, "partial")
                return {"poll": round_row["poll"], "result": "finished", "status": status}
            if attempts >= self.request_budget:
                self.store.note_event(round_id, "budget_pause", budget=self.request_budget)
                return {"poll": round_row["poll"], "result": "paused", "reason": "request_budget"}
            repetitions = current["current_repetitions"]
            query = urlencode(
                {
                    "agent": agent,
                    "poll": round_row["poll"],
                    "oids": ",".join(column["cursor"] for column in pending),
                    "max_repetitions": repetitions,
                }
            )
            path = self.gateway_path + "?" + query
            try:
                capture_id, response = collect(
                    self.client, self.archive, self.source + ":" + agent, path
                )
            except NETWORK_ERRORS as error:
                self.store.note_request(round_id)
                self.store.note_event(
                    round_id, "timeout", transport=type(error).__name__
                )
                attempts += 1
                timeouts += 1
                if timeouts > self.timeout_retries:
                    return {"poll": round_row["poll"], "result": "paused", "reason": "timeouts"}
                continue
            self.store.note_request(round_id)
            attempts += 1
            if response.status != 200:
                self.store.note_event(round_id, "http_error", status=response.status, capture=capture_id)
                timeouts += 1
                if timeouts > self.timeout_retries:
                    return {"poll": round_row["poll"], "result": "paused", "reason": "http_error"}
                continue
            try:
                envelope = json.loads(response.body.decode("utf-8"))
                applied = apply_envelope(self.store, envelope, capture_id)
            except (ValueError, UnicodeDecodeError) as error:
                self.store.note_event(round_id, "bad_envelope", error=str(error), capture=capture_id)
                status = self.store.finalize_round(round_id, "partial")
                return {"poll": round_row["poll"], "result": "finished", "status": status}
            if applied.error_status == "tooBig":
                narrowed = max(1, repetitions // 2)
                self.store.set_round_repetitions(round_id, narrowed)
                self.store.note_event(
                    round_id, "narrow", old=repetitions, new=narrowed
                )
                continue
            if applied.error_status == "timeout":
                timeouts += 1
                if timeouts > self.timeout_retries:
                    return {"poll": round_row["poll"], "result": "paused", "reason": "timeouts"}
                continue
            if applied.error_status != "noError":
                status = self.store.finalize_round(round_id, "partial")
                return {"poll": round_row["poll"], "result": "finished", "status": status}
            timeouts = 0
            if applied.duplicate:
                # 同一信封再次出现：网关没有按游标推进，不能无限重试。
                stalled += 1
                if stalled >= 2:
                    self.store.note_event(round_id, "no_progress", reason="duplicate_response")
                    status = self.store.finalize_round(round_id, "partial")
                    return {"poll": round_row["poll"], "result": "finished", "status": status}
                continue
            stalled = 0
            if not applied.progressed:
                self.store.note_event(round_id, "no_progress")
                status = self.store.finalize_round(round_id, "partial")
                return {"poll": round_row["poll"], "result": "finished", "status": status}


def ingest_archived(store, archive, *, source=None, finalize=False):
    """把归档中的来源记录接入走查状态；返回应用与跳过的信封数。

    同一设备出现更新的 poll 时，先把上一轮按已有证据固定；固定只用本轮
    列证据，不从上一轮补列。
    """
    watermark = int(store.get_meta("ingest.watermark", "0"))
    captures = [c for c in archive.captures(source) if c["id"] > watermark]
    applied = skipped = 0
    for capture in captures:
        watermark = max(watermark, capture["id"])
        if capture["status"] != 200:
            continue
        body = archive.body(capture["id"])
        try:
            items = decode_records(body, capture["headers"].get("content-type", ""))
        except ValueError:
            continue
        for item in items:
            try:
                agent, poll, _, _, _ = validate_envelope(item)
            except ValueError:
                skipped += 1
                continue
            # 出现更新的 poll 时，把该设备所有更旧的开放轮次按已有证据固定。
            for open_round in store.in_progress_rounds(agent):
                if poll > open_round["poll"]:
                    _finalize_open_round(store, open_round)
            result = apply_envelope(store, item, capture["id"])
            if result.duplicate or result.late:
                skipped += 1
            else:
                applied += 1
    store.set_meta("ingest.watermark", str(watermark))
    if finalize:
        for agent_row in _all_agents(store):
            for open_round in store.in_progress_rounds(agent_row["agent"]):
                _finalize_open_round(store, open_round)
    return {"applied": applied, "skipped": skipped, "watermark": watermark}


def _all_agents(store):
    rows = store.connection.execute("SELECT agent FROM agents ORDER BY agent").fetchall()
    return [dict(row) for row in rows]


def _finalize_open_round(store, round_row):
    columns = store.round_columns(round_row["id"])
    status = "complete" if all(c["status"] == "verified" for c in columns) else "partial"
    return store.finalize_round(round_row["id"], status)


def publish_document(store, agent, *, archive_name=None):
    """汇总一台设备的可发布视图：断点、轮次记录、本轮行与连续段。

    行只来自最近一轮的单元格；列核实状态逐列给出；连续段给出边界与
    关闭原因，不做跨段相减。
    """
    state = store.agent_state(agent)
    rounds = store.rounds_for(agent)
    latest = rounds[-1] if rounds else None
    document = {
        "agent": agent,
        "checkpoint": {
            "next_poll": state["next_poll"],
            "max_repetitions": state["max_repetitions"],
            "reboots": state["reboots"],
            "last_sys_up_time": state["last_sys_up_time"],
        },
        "round_history": [
            {
                "poll": r["poll"],
                "status": r["status"],
                "requests": r["requests"],
                "too_big": r["too_big"],
                "timeouts": r["timeouts"],
                "reboot_before": bool(r["reboot_before"]),
                "sys_up_time": [r["sys_up_time_first"], r["sys_up_time_last"]],
            }
            for r in rounds
        ],
        "latest_round": None,
        "segments": [],
        "evidence": {
            "archive": archive_name,
            "note": "cells 与列核实均以 capture_id 回指原始响应归档",
        },
    }
    if latest is not None:
        columns = store.round_columns(latest["id"])
        rows = []
        for status_row in store.row_status_for(latest["id"]):
            idx = status_row["idx"]
            cells = {}
            for cell in store.cells_for_round(latest["id"]):
                if cell["idx"] != idx:
                    continue
                cells[cell["column"]] = {
                    "oid": cell["oid"],
                    "type": cell["type"],
                    "value": json.loads(cell["value"]),
                    "capture_id": cell["capture_id"],
                }
            holes = [
                {
                    "column": hole["column"],
                    "oid": hole["oid"],
                    "type": hole["type"],
                    "capture_id": hole["capture_id"],
                }
                for hole in store.holes_for_round(latest["id"])
                if hole["idx"] == idx
            ]
            rows.append(
                {
                    "index": idx,
                    "complete": bool(status_row["complete"]),
                    "missing": json.loads(status_row["missing"]),
                    "cells": cells,
                    "holes": holes,
                }
            )
        document["latest_round"] = {
            "poll": latest["poll"],
            "status": latest["status"],
            "requests": latest["requests"],
            "repetitions": latest["current_repetitions"],
            "reboot_before": bool(latest["reboot_before"]),
            "sys_up_time": [latest["sys_up_time_first"], latest["sys_up_time_last"]],
            "columns": {
                column["column"]: {
                    "status": column["status"],
                    "entries": column["entries"],
                    "evidence_capture": column["evidence_capture"],
                }
                for column in columns
            },
            "rows": rows,
            "incomplete_rows": [row["index"] for row in rows if not row["complete"]],
        }
    for segment in store.segments_for(agent):
        document["segments"].append(
            {
                "index": segment["idx"],
                "column": segment["column"],
                "state": "open" if segment["closed_round"] is None else "closed",
                "close_reason": segment["close_reason"],
                "samples": segment["samples"],
                "first_value": segment["first_value"],
                "last_value": segment["last_value"],
                "delta": segment["delta"],
                "first_sys_up_time": segment["first_up_time"],
                "last_sys_up_time": segment["last_up_time"],
            }
        )
    return document
