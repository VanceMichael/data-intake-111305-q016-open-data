"""接口表走查的派生状态库。

与原始响应归档分离存放（业务解释可与原始接收独立演进），单元格通过
capture_id 回指归档中的原始响应，作为每一行的 OID 证据。所有计数器值以
十进制文本保存，避免 64 位无符号数超出 SQLite 整型范围。
"""
import json
import sqlite3
from pathlib import Path

from .ifmib import DISCONTINUITY_COLUMN, format_oid

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS agents (
    agent TEXT PRIMARY KEY,
    next_poll INTEGER NOT NULL DEFAULT 1,
    max_repetitions INTEGER NOT NULL,
    last_sys_up_time INTEGER,
    reboots INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE TABLE IF NOT EXISTS rounds (
    id INTEGER PRIMARY KEY,
    agent TEXT NOT NULL,
    poll INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'in_progress',
    started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    ended_at TEXT,
    sys_up_time_first INTEGER,
    sys_up_time_last INTEGER,
    reboot_before INTEGER NOT NULL DEFAULT 0,
    requests INTEGER NOT NULL DEFAULT 0,
    too_big INTEGER NOT NULL DEFAULT 0,
    timeouts INTEGER NOT NULL DEFAULT 0,
    start_repetitions INTEGER NOT NULL,
    current_repetitions INTEGER NOT NULL,
    detail TEXT NOT NULL DEFAULT '[]',
    UNIQUE (agent, poll)
);
CREATE TABLE IF NOT EXISTS round_columns (
    round_id INTEGER NOT NULL REFERENCES rounds(id) ON DELETE CASCADE,
    column TEXT NOT NULL,
    prefix TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'walking',
    entries INTEGER NOT NULL DEFAULT 0,
    cursor TEXT NOT NULL,
    evidence_capture INTEGER,
    PRIMARY KEY (round_id, column)
);
CREATE TABLE IF NOT EXISTS cells (
    id INTEGER PRIMARY KEY,
    round_id INTEGER NOT NULL REFERENCES rounds(id) ON DELETE CASCADE,
    column TEXT NOT NULL,
    idx TEXT NOT NULL,
    oid TEXT NOT NULL,
    type TEXT NOT NULL,
    value TEXT,
    sys_up_time INTEGER,
    capture_id INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    UNIQUE (round_id, column, idx)
);
CREATE TABLE IF NOT EXISTS holes (
    id INTEGER PRIMARY KEY,
    round_id INTEGER NOT NULL REFERENCES rounds(id) ON DELETE CASCADE,
    column TEXT NOT NULL,
    idx TEXT NOT NULL,
    oid TEXT NOT NULL,
    type TEXT NOT NULL,
    capture_id INTEGER NOT NULL,
    UNIQUE (round_id, column, idx)
);
CREATE TABLE IF NOT EXISTS row_status (
    round_id INTEGER NOT NULL REFERENCES rounds(id) ON DELETE CASCADE,
    idx TEXT NOT NULL,
    present TEXT NOT NULL,
    missing TEXT NOT NULL,
    complete INTEGER NOT NULL,
    PRIMARY KEY (round_id, idx)
);
CREATE TABLE IF NOT EXISTS segments (
    id INTEGER PRIMARY KEY,
    agent TEXT NOT NULL,
    idx TEXT NOT NULL,
    column TEXT NOT NULL,
    opened_round INTEGER NOT NULL REFERENCES rounds(id),
    closed_round INTEGER REFERENCES rounds(id),
    close_reason TEXT,
    first_value TEXT NOT NULL,
    last_value TEXT NOT NULL,
    first_up_time INTEGER,
    last_up_time INTEGER,
    last_discontinuity INTEGER,
    samples INTEGER NOT NULL DEFAULT 1,
    delta TEXT NOT NULL DEFAULT '0'
);
CREATE INDEX IF NOT EXISTS segments_open ON segments(agent, idx, column) WHERE closed_round IS NULL;
CREATE TABLE IF NOT EXISTS envelopes (
    digest TEXT PRIMARY KEY,
    agent TEXT NOT NULL,
    poll INTEGER NOT NULL,
    capture_id INTEGER NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
"""

# 轮次状态：in_progress 可恢复；complete 全部列拿到 endOfMibView；
# partial 有界内无法走完（网关明确失败或响应不再推进）。
ROUND_OPEN = "in_progress"


class IfStore:
    def __init__(self, path, columns, start_repetitions=8):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.columns = list(columns)
        self.columns_by_name = {column.name: column for column in self.columns}
        self.start_repetitions = start_repetitions
        self.connection = sqlite3.connect(self.path, timeout=5)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(SCHEMA)
        self.connection.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', '1')"
        )
        self.connection.commit()

    def close(self):
        self.connection.close()

    # ---- 设备检查点 ----

    def agent_state(self, agent):
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO agents(agent, max_repetitions) VALUES (?, ?)",
                (agent, self.start_repetitions),
            )
        row = self.connection.execute("SELECT * FROM agents WHERE agent=?", (agent,)).fetchone()
        return dict(row)

    def note_sys_up_time(self, agent, round_id, sys_up_time):
        """记录一次 sysUpTime 观察；回落视为重启证据，返回是否重启。"""
        if sys_up_time is None:
            return False
        state = self.agent_state(agent)
        reboot = state["last_sys_up_time"] is not None and sys_up_time < state["last_sys_up_time"]
        with self.connection:
            self.connection.execute(
                "UPDATE agents SET last_sys_up_time=?, reboots=reboots+?, "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE agent=?",
                (sys_up_time, 1 if reboot else 0, agent),
            )
            self.connection.execute(
                "UPDATE rounds SET sys_up_time_first=COALESCE(sys_up_time_first, ?), "
                "sys_up_time_last=?, reboot_before=MAX(reboot_before, ?) WHERE id=?",
                (sys_up_time, sys_up_time, 1 if reboot else 0, round_id),
            )
        return reboot

    def set_agent_plan(self, agent, next_poll, max_repetitions):
        with self.connection:
            self.connection.execute(
                "UPDATE agents SET next_poll=?, max_repetitions=?, "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE agent=?",
                (next_poll, max_repetitions, agent),
            )

    # ---- 轮次 ----

    def begin_round(self, agent, poll, max_repetitions):
        with self.connection:
            cursor = self.connection.execute(
                "INSERT INTO rounds(agent, poll, start_repetitions, current_repetitions) "
                "VALUES (?, ?, ?, ?)",
                (agent, poll, max_repetitions, max_repetitions),
            )
            round_id = cursor.lastrowid
            for column in self.columns:
                self.connection.execute(
                    "INSERT INTO round_columns(round_id, column, prefix, cursor) VALUES (?, ?, ?, ?)",
                    (round_id, column.name, column.prefix_text, column.prefix_text),
                )
        return round_id

    def round_row(self, round_id):
        row = self.connection.execute("SELECT * FROM rounds WHERE id=?", (round_id,)).fetchone()
        return dict(row) if row else None

    def in_progress_round(self, agent):
        row = self.connection.execute(
            "SELECT * FROM rounds WHERE agent=? AND status=? ORDER BY poll ASC LIMIT 1",
            (agent, ROUND_OPEN),
        ).fetchone()
        return dict(row) if row else None

    def in_progress_rounds(self, agent):
        rows = self.connection.execute(
            "SELECT * FROM rounds WHERE agent=? AND status=? ORDER BY poll ASC",
            (agent, ROUND_OPEN),
        ).fetchall()
        return [dict(row) for row in rows]

    def round_for(self, agent, poll):
        row = self.connection.execute(
            "SELECT * FROM rounds WHERE agent=? AND poll=?", (agent, poll)
        ).fetchone()
        return dict(row) if row else None

    def rounds_for(self, agent):
        rows = self.connection.execute(
            "SELECT * FROM rounds WHERE agent=? ORDER BY poll", (agent,)
        ).fetchall()
        return [dict(row) for row in rows]

    def round_columns(self, round_id):
        rows = self.connection.execute(
            "SELECT * FROM round_columns WHERE round_id=? ORDER BY column", (round_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    def note_request(self, round_id):
        with self.connection:
            self.connection.execute(
                "UPDATE rounds SET requests=requests+1 WHERE id=?", (round_id,)
            )

    def note_event(self, round_id, kind, **data):
        round_row = self.round_row(round_id)
        detail = json.loads(round_row["detail"])
        event = {"kind": kind, **data}
        detail.append(event)
        counter = {"too_big": "too_big", "timeout": "timeouts"}.get(kind)
        with self.connection:
            self.connection.execute(
                "UPDATE rounds SET detail=? WHERE id=?", (json.dumps(detail), round_id)
            )
            if counter:
                self.connection.execute(
                    f"UPDATE rounds SET {counter}={counter}+1 WHERE id=?", (round_id,)
                )

    def set_round_repetitions(self, round_id, repetitions):
        with self.connection:
            self.connection.execute(
                "UPDATE rounds SET current_repetitions=? WHERE id=?", (repetitions, round_id)
            )

    # ---- 单元格与列进度 ----

    def record_cell(self, round_id, column, idx, oid, value_type, value, sys_up_time, capture_id, seq):
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO cells(round_id, column, idx, oid, type, value, "
                "sys_up_time, capture_id, seq) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    round_id,
                    column,
                    idx,
                    oid,
                    value_type,
                    json.dumps(value, ensure_ascii=False),
                    sys_up_time,
                    capture_id,
                    seq,
                ),
            )
            self.connection.execute(
                "UPDATE round_columns SET entries=entries+1, cursor=? "
                "WHERE round_id=? AND column=? AND status='walking'",
                (oid, round_id, column),
            )

    def record_hole(self, round_id, column, idx, oid, hole_type, capture_id):
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO holes(round_id, column, idx, oid, type, capture_id) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (round_id, column, idx, oid, hole_type, capture_id),
            )
            self.connection.execute(
                "UPDATE round_columns SET cursor=? WHERE round_id=? AND column=? "
                "AND status='walking'",
                (oid, round_id, column),
            )

    def verify_column(self, round_id, column, capture_id):
        with self.connection:
            self.connection.execute(
                "UPDATE round_columns SET status='verified', evidence_capture=? "
                "WHERE round_id=? AND column=?",
                (capture_id, round_id, column),
            )

    def cells_for_round(self, round_id):
        rows = self.connection.execute(
            "SELECT * FROM cells WHERE round_id=? ORDER BY idx, column", (round_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    def holes_for_round(self, round_id):
        rows = self.connection.execute(
            "SELECT * FROM holes WHERE round_id=? ORDER BY idx, column", (round_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    # ---- 信封去重 ----

    def envelope_seen(self, digest):
        row = self.connection.execute(
            "SELECT 1 FROM envelopes WHERE digest=?", (digest,)
        ).fetchone()
        return row is not None

    def record_envelope(self, digest, agent, poll, capture_id):
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO envelopes(digest, agent, poll, capture_id) VALUES (?, ?, ?, ?)",
                (digest, agent, poll, capture_id),
            )

    # ---- 轮次收尾：行状态与连续段 ----

    def finalize_round(self, round_id, status):
        """固定一轮：按本轮单元格计算行完整性，并把计数器并入连续段。

        只用本轮证据，不从上一轮补列。返回最终状态。
        """
        round_row = self.round_row(round_id)
        if round_row["status"] != ROUND_OPEN:
            return round_row["status"]
        columns = self.round_columns(round_id)
        if status == "complete" and any(c["status"] != "verified" for c in columns):
            status = "partial"
        with self.connection:
            self._build_row_status(round_id)
            self._apply_segments(round_row)
            self.connection.execute(
                "UPDATE rounds SET status=?, ended_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') "
                "WHERE id=?",
                (status, round_id),
            )
            self.connection.execute(
                "UPDATE agents SET next_poll=?, max_repetitions=?, "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE agent=?",
                (
                    round_row["poll"] + 1,
                    round_row["current_repetitions"],
                    round_row["agent"],
                ),
            )
        return status

    def _build_row_status(self, round_id):
        cells = self.cells_for_round(round_id)
        by_index = {}
        for cell in cells:
            by_index.setdefault(cell["idx"], set()).add(cell["column"])
        all_columns = {column.name for column in self.columns}
        for idx, present in sorted(by_index.items(), key=lambda item: _index_key(item[0])):
            missing = sorted(all_columns - present)
            self.connection.execute(
                "INSERT OR REPLACE INTO row_status(round_id, idx, present, missing, complete) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    round_id,
                    idx,
                    json.dumps(sorted(present)),
                    json.dumps(missing),
                    0 if missing else 1,
                ),
            )

    def _open_segment(self, agent, idx, column):
        row = self.connection.execute(
            "SELECT * FROM segments WHERE agent=? AND idx=? AND column=? AND closed_round IS NULL",
            (agent, idx, column),
        ).fetchone()
        return dict(row) if row else None

    def _close_segment(self, segment, round_id, reason):
        self.connection.execute(
            "UPDATE segments SET closed_round=?, close_reason=? WHERE id=?",
            (round_id, reason, segment["id"]),
        )

    def _apply_segments(self, round_row):
        """把本轮计数器观察并入连续段；只在段内计算增量。

        重启（sysUpTime 回落）关闭该设备全部开放段；单接口
        ifCounterDiscontinuityTime 前进关闭该接口的开放段；Counter64 回落
        视为单计数器不连续。Counter32 回落且没有其它断点证据时按回绕处理。
        """
        agent = round_row["agent"]
        round_id = round_row["id"]
        up_time = round_row["sys_up_time_last"]
        cells = self.cells_for_round(round_id)
        by_index = {}
        for cell in cells:
            by_index.setdefault(cell["idx"], {})[cell["column"]] = cell

        if round_row["reboot_before"]:
            open_segments = self.connection.execute(
                "SELECT * FROM segments WHERE agent=? AND closed_round IS NULL", (agent,)
            ).fetchall()
            for segment in open_segments:
                self._close_segment(dict(segment), round_id, "reboot")

        for idx, cols in by_index.items():
            discontinuity_cell = cols.get(DISCONTINUITY_COLUMN)
            if discontinuity_cell is None:
                continue
            current = json.loads(discontinuity_cell["value"])
            for segment in self._open_segments_for(agent, idx):
                last = segment["last_discontinuity"]
                if last is not None and current > last:
                    self._close_segment(segment, round_id, "discontinuity_time")

        for idx, cols in sorted(by_index.items(), key=lambda item: _index_key(item[0])):
            discontinuity = None
            if DISCONTINUITY_COLUMN in cols:
                discontinuity = json.loads(cols[DISCONTINUITY_COLUMN]["value"])
            for column in self.columns:
                if not column.counter or column.name not in cols:
                    continue
                value = json.loads(cols[column.name]["value"])
                segment = self._open_segment(agent, idx, column.name)
                if segment is None:
                    self.connection.execute(
                        "INSERT INTO segments(agent, idx, column, opened_round, first_value, "
                        "last_value, first_up_time, last_up_time, last_discontinuity, samples) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
                        (
                            agent,
                            idx,
                            column.name,
                            round_id,
                            str(value),
                            str(value),
                            up_time,
                            up_time,
                            discontinuity,
                        ),
                    )
                    continue
                last_value = int(segment["last_value"])
                if value < last_value and column.value_type == "Counter64":
                    # Counter64 在段内回落：没有连续性证据支撑，开新段。
                    self._close_segment(segment, round_id, "counter_regressed")
                    self.connection.execute(
                        "INSERT INTO segments(agent, idx, column, opened_round, first_value, "
                        "last_value, first_up_time, last_up_time, last_discontinuity, samples) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
                        (agent, idx, column.name, round_id, str(value), str(value),
                         up_time, up_time, discontinuity),
                    )
                    continue
                if value < last_value:
                    delta = (value - last_value) % (2 ** 32)
                else:
                    delta = value - last_value
                # delta 用十进制文本保存，Python 侧计算，避免 64 位计数器溢出 SQLite 整型。
                total = int(segment["delta"]) + delta
                self.connection.execute(
                    "UPDATE segments SET last_value=?, last_up_time=?, samples=samples+1, "
                    "delta=?, last_discontinuity=COALESCE(?, last_discontinuity) WHERE id=?",
                    (str(value), up_time, str(total), discontinuity, segment["id"]),
                )

    def _open_segments_for(self, agent, idx):
        rows = self.connection.execute(
            "SELECT * FROM segments WHERE agent=? AND idx=? AND closed_round IS NULL",
            (agent, idx),
        ).fetchall()
        return [dict(row) for row in rows]

    def segments_for(self, agent):
        rows = self.connection.execute(
            "SELECT * FROM segments WHERE agent=? ORDER BY id", (agent,)
        ).fetchall()
        return [dict(row) for row in rows]

    def row_status_for(self, round_id):
        rows = self.connection.execute(
            "SELECT * FROM row_status WHERE round_id=? ORDER BY idx", (round_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    # ---- 接收水位 ----

    def get_meta(self, key, default=None):
        row = self.connection.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else default

    def set_meta(self, key, value):
        with self.connection:
            self.connection.execute(
                "INSERT OR REPLACE INTO meta(key, value) VALUES (?, ?)", (key, value)
            )


def _index_key(idx):
    parts = idx.split(".")
    return tuple(int(part) if part.isdigit() else part for part in parts)
