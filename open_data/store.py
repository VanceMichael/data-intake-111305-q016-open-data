"""接口表接收的持久化层。

原始 HTTP 响应仍由 :mod:`open_data.archive` 只追加保存；本库存放的是轮次、
请求、按 OID 拆出的单元格证据、成行结果、发布清单与连续段。每个设备的轮次号
与状态各自维护，互不拖累；轮次状态严格区分 running/complete/aborted，进程
重开时 running 轮次会被解释为中断，绝不冒充完整覆盖。
"""

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


def utc_now():
    return datetime.now(timezone.utc).isoformat()


class TableStore:
    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, timeout=10)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(SCHEMA)
        self.recover_stale_polls()

    def close(self):
        self.connection.close()

    # ---- 基础 -----------------------------------------------------------

    def meta_get(self, key):
        row = self.connection.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return None if row is None else row["value"]

    def meta_set(self, key, value):
        with self.connection:
            self.connection.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )

    def recover_stale_polls(self):
        """进程重开时把 running 轮次标记为 interrupted（中断，而非完成）。"""
        with self.connection:
            cursor = self.connection.execute(
                "UPDATE polls SET state='aborted', finished_at=?, "
                "abort_reason=COALESCE(abort_reason,'interrupted_by_restart') "
                "WHERE state='running'",
                (utc_now(),),
            )
            recovered = cursor.rowcount
            self.connection.execute(
                "UPDATE poll_columns SET state='incomplete', "
                "detail=COALESCE(detail,'')||' 轮次在完成前中断' WHERE state='pending'"
            )
        return recovered

    # ---- 轮次与列 --------------------------------------------------------

    def begin_poll(self, agent, columns):
        with self.connection:
            row = self.connection.execute(
                "SELECT next_poll FROM agent_state WHERE agent=?", (agent,)
            ).fetchone()
            poll_no = 1 if row is None else row["next_poll"]
            now = utc_now()
            self.connection.execute(
                "INSERT INTO agent_state(agent,next_poll,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(agent) DO UPDATE SET next_poll=excluded.next_poll,"
                "updated_at=excluded.updated_at",
                (agent, poll_no + 1, now),
            )
            cursor = self.connection.execute(
                "INSERT INTO polls(agent,poll_no,started_at,state) VALUES(?,?,?,'running')",
                (agent, poll_no, now),
            )
            poll_id = cursor.lastrowid
            for column in columns:
                self.connection.execute(
                    "INSERT INTO poll_columns(poll_id,column_name,column_oid,state) "
                    "VALUES(?,?,?,'pending')",
                    (poll_id, column.name, column.oid),
                )
        return poll_id, poll_no

    def set_column_state(self, poll_id, column_name, state, detail=None):
        with self.connection:
            self.connection.execute(
                "UPDATE poll_columns SET state=?, detail=? WHERE poll_id=? AND column_name=?",
                (state, detail, poll_id, column_name),
            )

    def record_request(self, poll_id, column, request_seq, cursor_oid, max_reps, result,
                       capture_id=None, http_status=None, error=None, sys_uptime=None):
        with self.connection:
            cursor = self.connection.execute(
                "INSERT INTO requests(poll_id,column_name,column_oid,request_seq,cursor_oid,"
                "max_reps,result,capture_id,http_status,error,sys_uptime,requested_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (poll_id, column.name, column.oid, request_seq, cursor_oid, max_reps, result,
                 capture_id, http_status, error, sys_uptime, utc_now()),
            )
            return cursor.lastrowid

    # ---- 单元格证据 ------------------------------------------------------

    def record_cells(self, poll_id, request_id, capture_id, column, cells):
        """写入一个响应中的 varbind 证据，返回本轮同格冲突描述列表。

        表是只追加的：同一轮里若再次观察到同一 ``(列, 索引)`` 且取值不同，
        两条证据都保留，同时上报冲突，成行时该单元格不会被静默覆盖。
        """
        conflicts = []
        with self.connection:
            for cell in cells:
                existing = self.connection.execute(
                    "SELECT value_type,value_json,oid FROM cell_observations "
                    "WHERE poll_id=? AND column_name=? AND index_suffix=?",
                    (poll_id, column.name, cell["index_suffix"]),
                ).fetchall()
                payload = json.dumps(cell["value"], ensure_ascii=False, sort_keys=True)
                for prior in existing:
                    if prior["value_type"] != cell["value_type"] or prior["value_json"] != payload:
                        conflicts.append({
                            "column": column.name,
                            "index_suffix": cell["index_suffix"],
                            "first_oid": prior["oid"],
                            "later_oid": cell["oid"],
                        })
                self.connection.execute(
                    "INSERT INTO cell_observations(poll_id,agent,column_name,column_oid,"
                    "index_suffix,oid,value_type,value_json,is_exception,request_id,"
                    "capture_id,observed_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (poll_id, cell["agent"], column.name, column.oid, cell["index_suffix"],
                     cell["oid"], cell["value_type"], payload,
                     1 if cell["is_exception"] else 0, request_id, capture_id, utc_now()),
                )
        return conflicts

    def rollback_column(self, poll_id, column_name):
        """轮内改表后回滚本列已记录的单元格观察。

        回滚只删除待发布的单元格行；这些单元格所属响应仍在只追加归档中，
        随后重读得到的新旧两份证据都能通过 capture_id 回溯。请求记录不删除，
        因此“先读到旧值、检测到改表、再读到新值”的过程完整留痕。
        """
        with self.connection:
            cursor = self.connection.execute(
                "DELETE FROM cell_observations WHERE poll_id=? AND column_name=?",
                (poll_id, column_name),
            )
            return cursor.rowcount

    # ---- 成行与轮次收尾 ---------------------------------------------------

    def materialize_rows(self, poll_id, profile, conflicts):
        """按索引后缀把本轮已核实的单元格组装成行；不补任何上轮残留。"""
        conflict_columns = {}
        for item in conflicts:
            conflict_columns.setdefault(
                (item["index_suffix"], item["column"]), item["later_oid"]
            )
        rows = self.connection.execute(
            "SELECT * FROM cell_observations WHERE poll_id=? ORDER BY index_suffix,column_name,id",
            (poll_id,),
        ).fetchall()
        grouped = {}
        for cell in rows:
            grouped.setdefault(cell["index_suffix"], []).append(cell)
        output = []
        with self.connection:
            self.connection.execute("DELETE FROM poll_rows WHERE poll_id=?", (poll_id,))
            for index_suffix, cells in sorted(grouped.items()):
                values = {}
                present = set()
                for cell in cells:
                    if cell["is_exception"]:
                        continue  # noSuchInstance 等只作证据，不充当列值
                    present.add(cell["column_name"])
                    values.setdefault(cell["column_name"], {
                        "oid": cell["oid"],
                        "type": cell["value_type"],
                        "value": json.loads(cell["value_json"]),
                    })
                missing = [
                    column.name for column in profile.columns
                    if column.name not in present
                ]
                row_conflicts = sorted(
                    name for (suffix, name) in conflict_columns if suffix == index_suffix
                )
                identity_parts = []
                for column in profile.identity_columns():
                    if column.name in values:
                        identity_parts.append(
                            (column.name, values[column.name]["type"], values[column.name]["value"])
                        )
                fingerprint = hashlib.sha256(
                    json.dumps(identity_parts, ensure_ascii=False, sort_keys=True).encode()
                ).hexdigest()
                completeness = "complete" if not missing and not row_conflicts else "incomplete"
                record = {
                    "index_suffix": index_suffix,
                    "completeness": completeness,
                    "missing_columns": missing,
                    "conflict_columns": row_conflicts,
                    "fingerprint": fingerprint,
                    "cells": values,
                }
                self.connection.execute(
                    "INSERT INTO poll_rows(poll_id,agent,index_suffix,completeness,"
                    "missing_columns,conflict_columns,fingerprint,row_json) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (poll_id, cells[0]["agent"], index_suffix, completeness,
                     json.dumps(missing), json.dumps(row_conflicts), fingerprint,
                     json.dumps(record, ensure_ascii=False, sort_keys=True)),
                )
                output.append(record)
        return output

    def finish_poll(self, poll_id, state, abort_reason=None):
        with self.connection:
            self.connection.execute(
                "UPDATE polls SET state=?, finished_at=?, abort_reason=? WHERE id=?",
                (state, utc_now(), abort_reason, poll_id),
            )

    def publish(self, poll_id, scope, manifest):
        poll = self.require_poll(poll_id)
        with self.connection:
            self.connection.execute(
                "INSERT INTO publications(poll_id,agent,poll_no,scope,published_at,manifest_json) "
                "VALUES(?,?,?,?,?,?) ON CONFLICT(poll_id) DO UPDATE SET "
                "scope=excluded.scope,published_at=excluded.published_at,"
                "manifest_json=excluded.manifest_json",
                (poll_id, poll["agent"], poll["poll_no"], scope, utc_now(),
                 json.dumps(manifest, ensure_ascii=False, sort_keys=True)),
            )

    # ---- 查询 ------------------------------------------------------------

    def require_poll(self, poll_id):
        row = self.connection.execute("SELECT * FROM polls WHERE id=?", (poll_id,)).fetchone()
        if row is None:
            raise KeyError(poll_id)
        return dict(row)

    def polls(self, agent=None):
        if agent is None:
            rows = self.connection.execute("SELECT * FROM polls ORDER BY id").fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM polls WHERE agent=? ORDER BY poll_no", (agent,)
            ).fetchall()
        return [dict(row) for row in rows]

    def columns_of(self, poll_id):
        rows = self.connection.execute(
            "SELECT * FROM poll_columns WHERE poll_id=? ORDER BY id", (poll_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    def requests_of(self, poll_id):
        rows = self.connection.execute(
            "SELECT * FROM requests WHERE poll_id=? ORDER BY id", (poll_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    def rows_of(self, poll_id):
        rows = self.connection.execute(
            "SELECT row_json FROM poll_rows WHERE poll_id=? ORDER BY index_suffix", (poll_id,)
        ).fetchall()
        return [json.loads(row["row_json"]) for row in rows]

    def publication(self, poll_id):
        row = self.connection.execute(
            "SELECT * FROM publications WHERE poll_id=?", (poll_id,)
        ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["manifest"] = json.loads(result.pop("manifest_json"))
        return result

    def cell_evidence(self, poll_id, index_suffix):
        """返回某一行每个单元格的 OID 证据链：来源响应与请求均可回溯。"""
        rows = self.connection.execute(
            "SELECT o.column_name,o.oid,o.value_type,o.value_json,o.is_exception,"
            "o.request_id,o.capture_id,r.cursor_oid,r.request_seq,r.result "
            "FROM cell_observations o JOIN requests r ON r.id=o.request_id "
            "WHERE o.poll_id=? AND o.index_suffix=? ORDER BY o.column_name,o.id",
            (poll_id, index_suffix),
        ).fetchall()
        return [dict(row) for row in rows]

    def latest_rows_by_agent(self, agent):
        """每个代理只取已发布轮次中的最新行视图，供连续性分析。"""
        poll = self.connection.execute(
            "SELECT p.* FROM polls p JOIN publications u ON u.poll_id=p.id "
            "WHERE p.agent=? ORDER BY p.poll_no DESC LIMIT 1",
            (agent,),
        ).fetchone()
        if poll is None:
            return None, []
        return dict(poll), self.rows_of(poll["id"])

    def agents(self):
        return [
            row["agent"]
            for row in self.connection.execute("SELECT agent FROM agent_state ORDER BY agent")
        ]


SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS agent_state(
    agent TEXT PRIMARY KEY,
    next_poll INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS polls(
    id INTEGER PRIMARY KEY,
    agent TEXT NOT NULL,
    poll_no INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    state TEXT NOT NULL,
    abort_reason TEXT,
    UNIQUE(agent, poll_no)
);
CREATE TABLE IF NOT EXISTS poll_columns(
    id INTEGER PRIMARY KEY,
    poll_id INTEGER NOT NULL REFERENCES polls(id),
    column_name TEXT NOT NULL,
    column_oid TEXT NOT NULL,
    state TEXT NOT NULL,
    detail TEXT,
    UNIQUE(poll_id, column_name)
);
CREATE TABLE IF NOT EXISTS requests(
    id INTEGER PRIMARY KEY,
    poll_id INTEGER NOT NULL REFERENCES polls(id),
    column_name TEXT NOT NULL,
    column_oid TEXT NOT NULL,
    request_seq INTEGER NOT NULL,
    cursor_oid TEXT NOT NULL,
    max_reps INTEGER NOT NULL,
    result TEXT NOT NULL,
    capture_id INTEGER,
    http_status INTEGER,
    error TEXT,
    sys_uptime INTEGER,
    requested_at TEXT NOT NULL,
    UNIQUE(poll_id, column_name, request_seq)
);
CREATE TABLE IF NOT EXISTS cell_observations(
    id INTEGER PRIMARY KEY,
    poll_id INTEGER NOT NULL REFERENCES polls(id),
    agent TEXT NOT NULL,
    column_name TEXT NOT NULL,
    column_oid TEXT NOT NULL,
    index_suffix TEXT NOT NULL,
    oid TEXT NOT NULL,
    value_type TEXT NOT NULL,
    value_json TEXT,
    is_exception INTEGER NOT NULL,
    request_id INTEGER NOT NULL REFERENCES requests(id),
    capture_id INTEGER,
    observed_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cells_poll ON cell_observations(poll_id);
CREATE TABLE IF NOT EXISTS poll_rows(
    id INTEGER PRIMARY KEY,
    poll_id INTEGER NOT NULL REFERENCES polls(id),
    agent TEXT NOT NULL,
    index_suffix TEXT NOT NULL,
    completeness TEXT NOT NULL,
    missing_columns TEXT NOT NULL,
    conflict_columns TEXT NOT NULL,
    fingerprint TEXT,
    row_json TEXT NOT NULL,
    UNIQUE(poll_id, index_suffix)
);
CREATE TABLE IF NOT EXISTS publications(
    id INTEGER PRIMARY KEY,
    poll_id INTEGER NOT NULL UNIQUE REFERENCES polls(id),
    agent TEXT NOT NULL,
    poll_no INTEGER NOT NULL,
    scope TEXT NOT NULL,
    published_at TEXT NOT NULL,
    manifest_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS segments(
    id INTEGER PRIMARY KEY,
    agent TEXT NOT NULL,
    index_suffix TEXT NOT NULL,
    column_name TEXT NOT NULL,
    start_poll_id INTEGER NOT NULL,
    end_poll_id INTEGER,
    end_reason TEXT,
    width INTEGER,
    UNIQUE(agent, index_suffix, column_name, start_poll_id)
);
CREATE TABLE IF NOT EXISTS segment_points(
    id INTEGER PRIMARY KEY,
    segment_id INTEGER NOT NULL REFERENCES segments(id),
    poll_id INTEGER NOT NULL,
    value INTEGER NOT NULL,
    sys_uptime INTEGER,
    delta INTEGER,
    wrap_adjusted INTEGER NOT NULL DEFAULT 0,
    UNIQUE(segment_id, poll_id)
);
"""
