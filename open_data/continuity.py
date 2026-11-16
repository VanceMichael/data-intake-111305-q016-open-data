"""计数器连续段构建。

输入只取已发布轮次的成行结果与每轮的 sysUpTime 证据。下列任一情况都必须切断
连续段，段内相邻点才允许计算增量，绝不把两个不可比的数直接相减：

1. 中间隔着未发布或不完整的轮次（观测缺口，无法证明期间没有变化）；
2. sysUpTime 回落（设备重启）；
3. ifCounterDiscontinuityTime 变化（RFC 2863 的不连续声明）；
4. 同一 ifIndex 的身份指纹变化（ifIndex 被复用为另一个接口）；
5. 计数器值下降：Counter64 没有自然回绕语义，Counter32 只有小回绕才按
   2^32 修正，超过半量程的“增量”按未声明的不连续处理。
"""

from .mib import DISCONTINUITY_COLUMN, WRAP_SANITY


class SegmentBuilder:
    def __init__(self, store, profile):
        self.store = store
        self.profile = profile

    def rebuild(self):
        """幂等重建所有代理的连续段与段内增量。"""
        with self.store.connection:
            self.store.connection.execute("DELETE FROM segment_points")
            self.store.connection.execute("DELETE FROM segments")
        results = []
        for agent in self.store.agents():
            results.extend(self._build_agent(agent))
        return results

    def _build_agent(self, agent):
        polls = [
            poll for poll in self.store.polls(agent)
            if self.store.publication(poll["id"]) is not None
        ]
        rows_by_poll = {poll["id"]: {row["index_suffix"]: row for row in self.store.rows_of(poll["id"])}
                        for poll in polls}
        indexes = sorted({suffix for rows in rows_by_poll.values() for suffix in rows})
        output = []
        for index_suffix in indexes:
            for column in self.profile.counter_columns():
                output.extend(self._build_series(agent, index_suffix, column, polls, rows_by_poll))
        return output

    def _build_series(self, agent, index_suffix, column, polls, rows_by_poll):
        points = []
        previous_poll_no = None
        previous_fingerprint = None
        previous_uptime = None
        previous_disc = None
        previous_value = None
        segment_id = None
        reasons = []

        def open_segment(poll_id, width):
            cursor = self.store.connection.execute(
                "INSERT INTO segments(agent,index_suffix,column_name,start_poll_id,width) "
                "VALUES(?,?,?,?,?)",
                (agent, index_suffix, column.name, poll_id, width),
            )
            return cursor.lastrowid

        def close_segment(poll_id, reason):
            if segment_id is not None:
                self.store.connection.execute(
                    "UPDATE segments SET end_poll_id=?, end_reason=? WHERE id=?",
                    (poll_id, reason, segment_id),
                )

        def add_point(seg, poll_id, value, uptime, delta, wrap_adjusted):
            self.store.connection.execute(
                "INSERT INTO segment_points(segment_id,poll_id,value,sys_uptime,delta,wrap_adjusted) "
                "VALUES(?,?,?,?,?,?)",
                (seg, poll_id, value, uptime, delta, 1 if wrap_adjusted else 0),
            )

        for poll in polls:
            row = rows_by_poll[poll["id"]].get(index_suffix)
            cell = None if row is None else row["cells"].get(column.name)
            uptime = self._poll_uptime(poll["id"])
            if cell is None or not isinstance(cell["value"], int):
                # 该轮未观察到此计数器：下一次观察必须另起一段。
                close_segment(poll["id"], "observation_gap")
                segment_id = None
                previous_poll_no = poll["poll_no"]
                previous_uptime = uptime
                continue
            value = cell["value"]
            fingerprint = row["fingerprint"]
            disc = self._disc_value(row)

            reason = None
            if segment_id is None:
                reason = "segment_start"
            elif poll["poll_no"] != previous_poll_no + 1:
                reason = "observation_gap"
            elif previous_uptime is not None and uptime is not None and uptime < previous_uptime:
                reason = "sysuptime_rollback"
            elif disc is not None and previous_disc is not None and disc != previous_disc:
                reason = "discontinuity_time_changed"
            elif previous_fingerprint is not None and fingerprint != previous_fingerprint:
                reason = "ifindex_reused_identity_changed"
            elif value < previous_value:
                if column.width == 32 and self._looks_like_wrap32(previous_value, value):
                    reason = None  # 合法小回绕，段保持连续
                else:
                    reason = "counter_decrease_without_declared_discontinuity"
            if reason is not None and reason != "segment_start":
                close_segment(poll["id"], reason)
                segment_id = None
            if segment_id is None:
                with self.store.connection:
                    segment_id = open_segment(poll["id"], column.width)
                delta, wrap_adjusted = None, False
                reasons.append({"agent": agent, "index_suffix": index_suffix,
                                "column": column.name, "poll_no": poll["poll_no"], "reason": reason})
            else:
                delta, wrap_adjusted = self._delta(column, previous_value, value)
            with self.store.connection:
                add_point(segment_id, poll["id"], value, uptime, delta, wrap_adjusted)

            previous_poll_no = poll["poll_no"]
            previous_fingerprint = fingerprint
            previous_uptime = uptime
            previous_disc = disc
            previous_value = value

        close_segment(polls[-1]["id"] if polls else None, "end_of_observations")
        return reasons

    def _poll_uptime(self, poll_id):
        row = self.store.connection.execute(
            "SELECT sys_uptime FROM requests WHERE poll_id=? AND sys_uptime IS NOT NULL "
            "ORDER BY id LIMIT 1",
            (poll_id,),
        ).fetchone()
        return None if row is None else row["sys_uptime"]

    def _disc_value(self, row):
        cell = row["cells"].get(DISCONTINUITY_COLUMN)
        if cell is None or not isinstance(cell["value"], int):
            return None
        return cell["value"]

    @staticmethod
    def _looks_like_wrap32(previous_value, value):
        modulus = 1 << 32
        delta = (value - previous_value) % modulus
        return 0 < delta < WRAP_SANITY[32]

    @staticmethod
    def _delta(column, previous_value, value):
        if column.width == 32 and value < previous_value:
            modulus = 1 << 32
            return (value - previous_value) % modulus, True
        return value - previous_value, False

    def describe(self, agent=None):
        """返回面向发布的连续段描述，每点都带原始值与可回溯轮次。"""
        query = (
            "SELECT s.*, p.poll_no, pt.value, pt.sys_uptime, pt.delta, pt.wrap_adjusted "
            "FROM segments s JOIN segment_points pt ON pt.segment_id=s.id "
            "JOIN polls p ON p.id=pt.poll_id"
        )
        params = ()
        if agent is not None:
            query += " WHERE s.agent=?"
            params = (agent,)
        query += " ORDER BY s.agent, s.index_suffix, s.column_name, s.id, p.poll_no"
        segments = {}
        for row in self.store.connection.execute(query, params):
            seg = segments.setdefault(row["id"], {
                "segment_id": row["id"],
                "agent": row["agent"],
                "index_suffix": row["index_suffix"],
                "column": row["column_name"],
                "width": row["width"],
                "end_reason": row["end_reason"],
                "points": [],
            })
            seg["points"].append({
                "poll_no": row["poll_no"],
                "value": row["value"],
                "sys_uptime": row["sys_uptime"],
                "delta": row["delta"],
                "wrap_adjusted": bool(row["wrap_adjusted"]),
            })
        return sorted(segments.values(), key=lambda item: (
            item["agent"], item["index_suffix"], item["column"], item["segment_id"]))
