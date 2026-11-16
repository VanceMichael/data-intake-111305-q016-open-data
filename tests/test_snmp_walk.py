import json
import tempfile
import unittest
from pathlib import Path

from open_data.archive import Archive
from open_data.client import Client
from open_data.continuity import SegmentBuilder
from open_data.gateway import serve_gateway
from open_data.mib import MibProfile, parse_oid
from open_data.store import TableStore
from open_data.walk import Walker, WalkConfig

PROFILE = MibProfile()


def scenario(agents):
    return {"description": "test synthetic", "agents": agents}


def make_walker(gateway, temporary, source="test-source", config=None):
    archive = Archive(Path(temporary) / "raw.sqlite3")
    store = TableStore(Path(temporary) / "tables.sqlite3")
    walker = Walker(Client(gateway.base_url), archive, store, source, PROFILE,
                    config or WalkConfig())
    return walker, archive, store


def base_agent(rows):
    base = {}
    for index, descr, counters in rows:
        base[f"1.3.6.1.2.1.2.2.1.1.{index}"] = {"type": "Integer", "value": index}
        base[f"1.3.6.1.2.1.2.2.1.2.{index}"] = {"type": "OctetString", "value": descr}
        base[f"1.3.6.1.2.1.2.2.1.4.{index}"] = {"type": "Integer", "value": 1500}
        base[f"1.3.6.1.2.1.2.2.1.6.{index}"] = {"type": "OctetString", "value": f"mac-{index}"}
        base[f"1.3.6.1.2.1.2.2.1.10.{index}"] = {"type": "Counter32", "value": counters[0]}
        base[f"1.3.6.1.2.1.2.2.1.16.{index}"] = {"type": "Counter32", "value": counters[1]}
        base[f"1.3.6.1.2.1.31.1.1.1.1.{index}"] = {"type": "OctetString", "value": f"Gi0/{index}"}
        base[f"1.3.6.1.2.1.31.1.1.1.6.{index}"] = {"type": "Counter64", "value": counters[2]}
        base[f"1.3.6.1.2.1.31.1.1.1.10.{index}"] = {"type": "Counter64", "value": counters[3]}
        base[f"1.3.6.1.2.1.31.1.1.1.19.{index}"] = {"type": "TimeTicks", "value": 0}
    return base


class MibSplitTests(unittest.TestCase):
    def test_split_uses_column_prefix_and_index_suffix(self):
        column, suffix = PROFILE.split("1.3.6.1.2.1.2.2.1.10.8")
        self.assertEqual(column.name, "ifInOctets")
        self.assertEqual(suffix, "8")
        column, suffix = PROFILE.split("1.3.6.1.2.1.31.1.1.1.19.7")
        self.assertEqual(column.name, "ifCounterDiscontinuityTime")
        self.assertEqual(suffix, "7")
        self.assertEqual(PROFILE.split("1.2.3.4"), (None, None))

    def test_oid_ordering_is_numeric_tuple_not_string(self):
        # 字符串序里 ".10" 排在 ".7" 前，词法元组序则相反——网关必须用元组序。
        self.assertLess(parse_oid("1.3.6.7"), parse_oid("1.3.6.10"))


class RaggedAssemblyTests(unittest.TestCase):
    def test_rows_are_assembled_by_index_not_response_position(self):
        agent = {"base": base_agent([(7, "uplink", (1000, 800, 1000, 800)),
                                     (8, "desk", (200, 100, 200, 100))]),
                 "polls": {"1": {"sysUpTime": 100}}}
        with serve_gateway(scenario({"ragged": agent})) as gateway, tempfile.TemporaryDirectory() as temporary:
            walker, archive, store = make_walker(gateway, temporary)
            try:
                summary = walker.run_poll("ragged")
                self.assertEqual(summary.state, "complete")
                rows = {row["index_suffix"]: row for row in store.rows_of(summary.poll_id)}
                # 索引 7 的计数器绝不能因返回顺序被拼到索引 8 的行上。
                self.assertEqual(rows["7"]["cells"]["ifInOctets"]["value"], 1000)
                self.assertEqual(rows["8"]["cells"]["ifInOctets"]["value"], 200)
                self.assertEqual(rows["7"]["cells"]["ifDescr"]["value"], "uplink")
                # 值类型原样保留。
                self.assertEqual(rows["7"]["cells"]["ifInOctets"]["type"], "Counter32")
                self.assertEqual(rows["7"]["cells"]["ifDescr"]["type"], "OctetString")
                # 每个单元格都记录自己的完整 OID 证据。
                self.assertEqual(rows["7"]["cells"]["ifInOctets"]["oid"],
                                 "1.3.6.1.2.1.2.2.1.10.7")
            finally:
                store.close()
                archive.close()

    def test_each_successful_response_is_archived_and_referenced(self):
        agent = {"base": base_agent([(1, "p1", (1, 1, 1, 1))]),
                 "polls": {"1": {"sysUpTime": 100}}}
        with serve_gateway(scenario({"a": agent})) as gateway, tempfile.TemporaryDirectory() as temporary:
            walker, archive, store = make_walker(gateway, temporary)
            try:
                summary = walker.run_poll("a")
                request_count = len(store.requests_of(summary.poll_id))
                self.assertGreater(request_count, 0)
                self.assertEqual(len(archive.captures()), request_count)
                evidence = store.cell_evidence(summary.poll_id, "1")
                self.assertTrue(evidence)
                for item in evidence:
                    archived = json.loads(archive.body(item["capture_id"]))
                    self.assertIn(item["oid"], [vb["oid"] for vb in archived["varbinds"]])
            finally:
                store.close()
                archive.close()


class WalkSemanticsTests(unittest.TestCase):
    def _run(self, agent_body, agent_name="dev", config=None):
        temporary = tempfile.TemporaryDirectory()
        context = serve_gateway(scenario({agent_name: agent_body}))
        gateway = context.__enter__()
        walker, archive, store = make_walker(gateway, temporary.name, config=config)
        self.addCleanup(context.__exit__, None, None, None)
        self.addCleanup(temporary.cleanup)
        self.addCleanup(store.close)
        self.addCleanup(archive.close)
        return gateway, walker, archive, store

    def test_toobig_shrinks_max_reps_and_completes_walk(self):
        agent = {
            "base": base_agent([(1, "p1", (10, 5, 10, 5)), (2, "p2", (20, 6, 20, 6))]),
            "polls": {"1": {"sysUpTime": 100, "events": [
                {"when_request": 1, "kind": "toobig", "max_reps_threshold": 3},
            ]}},
        }
        gateway, walker, archive, store = self._run(
            agent, config=WalkConfig(initial_max_reps=3))
        summary = walker.run_poll("dev")
        self.assertEqual(summary.state, "complete")
        requests = store.requests_of(summary.poll_id)
        self.assertIn("tooBig", [
            json.loads(archive.body(r["capture_id"]))["error_status"]
            for r in requests if r["capture_id"]])
        reps_used = sorted({r["max_reps"] for r in requests})
        self.assertEqual(reps_used[0], 1)  # tooBig 后下一次领取范围被调小
        self.assertIn(3, reps_used)  # 首次请求仍按初始范围发出并被拒

    def test_toobig_at_min_reps_aborts_instead_of_looping(self):
        agent = {
            "base": base_agent([(1, "p1", (10, 5, 10, 5))]),
            "polls": {"1": {"sysUpTime": 100, "events": [
                {"when_request": 1, "column": "1.3.6.1.2.1.2.2.1.2",
                 "kind": "toobig", "max_reps_threshold": 1},
            ]}},
        }
        gateway, walker, archive, store = self._run(
            agent, config=WalkConfig(initial_max_reps=1))
        summary = walker.run_poll("dev")
        self.assertEqual(summary.state, "aborted")
        self.assertTrue(summary.abort_reason.startswith("tooBig_at_min_reps"))
        self.assertEqual(store.publication(summary.poll_id)["scope"], "partial")
        # 预算保证请求数有界。
        self.assertLessEqual(len(store.requests_of(summary.poll_id)),
                             WalkConfig().poll_request_budget)

    def test_timeout_is_retried_same_cursor_and_does_not_end_walk(self):
        agent = {
            "base": base_agent([(1, "p1", (10, 5, 10, 5))]),
            "polls": {"1": {"sysUpTime": 100, "events": [
                {"when_request": 1, "column": "1.3.6.1.2.1.31.1.1.1.6", "kind": "timeout"},
            ]}},
        }
        gateway, walker, archive, store = self._run(agent)
        summary = walker.run_poll("dev")
        self.assertEqual(summary.state, "complete")
        errors = [r for r in store.requests_of(summary.poll_id)
                  if r["result"] == "transport_error"]
        self.assertEqual(len(errors), 1)  # 超时留痕但同游标重试成功
        # 超时没有产生任何单元格证据。
        self.assertEqual(store.connection.execute(
            "SELECT count(*) FROM cell_observations WHERE request_id=?",
            (errors[0]["id"],)).fetchone()[0], 0)

    def test_nosuch_instance_is_evidence_not_table_clearing(self):
        agent = {
            "base": base_agent([(1, "p1", (10, 5, 10, 5))]),
            "polls": {"1": {"sysUpTime": 100, "events": [
                {"when_request": 1, "column": "1.3.6.1.2.1.31.1.1.1.6", "kind": "nosuchinstance",
                 "oid": "1.3.6.1.2.1.31.1.1.1.6.9"},
            ]}},
        }
        gateway, walker, archive, store = self._run(agent)
        summary = walker.run_poll("dev")
        self.assertEqual(summary.state, "complete")
        evidence = store.connection.execute(
            "SELECT value_type,is_exception FROM cell_observations "
            "WHERE poll_id=? AND oid='1.3.6.1.2.1.31.1.1.1.6.9'",
            (summary.poll_id,)).fetchone()
        self.assertEqual(tuple(evidence), ("noSuchInstance", 1))
        # 已核实的其它行仍然完整发布，没有被当成整表清空。
        rows = {r["index_suffix"]: r for r in store.rows_of(summary.poll_id)}
        self.assertIn("1", rows)

    def test_in_poll_mutation_rolls_back_column_and_rereads(self):
        agent = {
            "base": base_agent([(7, "uplink", (1000, 800, 1000, 800)),
                                (8, "desk", (200, 100, 200, 100))]),
            "polls": {"1": {"sysUpTime": 100, "events": [
                {"when_request": 1, "column": "1.3.6.1.2.1.2.2.1.10", "kind": "mutate",
                 "put": {"1.3.6.1.2.1.2.2.1.10.7": {"type": "Counter32", "value": 999999}}},
            ]}},
        }
        gateway, walker, archive, store = self._run(
            agent, config=WalkConfig(initial_max_reps=1))
        summary = walker.run_poll("dev")
        self.assertEqual(summary.state, "complete")
        rows = {r["index_suffix"]: r for r in store.rows_of(summary.poll_id)}
        # 发布视图采用改表后的一致取值，不混用改表前后的单元格。
        self.assertEqual(rows["7"]["cells"]["ifInOctets"]["value"], 999999)
        # 改表事件在请求记录中留痕，且该列回滚后从列起点重读。
        mutated = [r for r in store.requests_of(summary.poll_id)
                   if r["column_name"] == "ifInOctets"]
        self.assertEqual(mutated[0]["cursor_oid"], "1.3.6.1.2.1.2.2.1.10")
        self.assertGreaterEqual(len([r for r in mutated if r["capture_id"]]), 2)
        archived_bodies = [
            json.loads(archive.body(r["capture_id"]))
            for r in mutated if r["capture_id"]
        ]
        self.assertTrue(any("mutate" in body.get("events", []) for body in archived_bodies))

    def test_row_missing_columns_is_marked_incomplete(self):
        # 只有 ifDescr 与 ifInOctets 两列的数据，其余列必然缺失。
        base = {
            "1.3.6.1.2.1.2.2.1.2.5": {"type": "OctetString", "value": "lonely"},
            "1.3.6.1.2.1.2.2.1.10.5": {"type": "Counter32", "value": 1},
        }
        gateway, walker, archive, store = self._run(
            {"base": base, "polls": {"1": {"sysUpTime": 100}}})
        summary = walker.run_poll("dev")
        row = store.rows_of(summary.poll_id)[0]
        self.assertEqual(row["index_suffix"], "5")
        self.assertEqual(row["completeness"], "incomplete")
        self.assertIn("ifOutOctets", row["missing_columns"])
        manifest = store.publication(summary.poll_id)["manifest"]
        self.assertEqual(manifest["complete_rows"], [])
        self.assertEqual(manifest["incomplete_rows"][0]["index_suffix"], "5")


class ContinuityTests(unittest.TestCase):
    def _poll_all(self, gateway, temporary_dir, agent, times):
        walker, archive, store = make_walker(gateway, temporary_dir)
        summaries = [walker.run_poll(agent) for _ in range(times)]
        builder = SegmentBuilder(store, PROFILE)
        return builder, store, archive, summaries

    def test_counter32_wrap_is_adjusted_inside_one_segment(self):
        agent = {"base": base_agent([(1, "p1", (4294967000, 10, 4294967000, 10))]),
                 "polls": {
                     "1": {"sysUpTime": 1000},
                     "2": {"sysUpTime": 2000, "changes": {
                         "1.3.6.1.2.1.2.2.1.10.1": {"type": "Counter32", "value": 500},
                         "1.3.6.1.2.1.31.1.1.1.6.1": {"type": "Counter64", "value": 4294967600},
                     }}}}
        with serve_gateway(scenario({"dev": agent})) as gateway, tempfile.TemporaryDirectory() as temporary:
            builder, store, archive, _ = self._poll_all(gateway, temporary, "dev", 2)
            try:
                breaks = builder.rebuild()
                c32 = [s for s in builder.describe() if s["column"] == "ifInOctets"][0]
                self.assertEqual(len(c32["points"]), 2)
                # 4_294_967_296 - 4_294_967_000 + 500 = 796，按 2^32 回绕修正
                self.assertEqual(c32["points"][1]["delta"], 796)
                self.assertTrue(c32["points"][1]["wrap_adjusted"])
                self.assertEqual(c32["end_reason"], "end_of_observations")
                # Counter64 不做回绕，正常增长时同段。
                c64 = [s for s in builder.describe() if s["column"] == "ifHCInOctets"][0]
                self.assertEqual(c64["points"][1]["delta"], 600)
                self.assertFalse(any(b["reason"] == "counter_decrease_without_declared_discontinuity"
                                     for b in breaks))
            finally:
                store.close(); archive.close()

    def test_counter64_decrease_opens_new_segment_without_fake_delta(self):
        agent = {"base": base_agent([(1, "p1", (100, 100, 5000, 5000))]),
                 "polls": {
                     "1": {"sysUpTime": 1000},
                     "2": {"sysUpTime": 2000, "changes": {
                         "1.3.6.1.2.1.31.1.1.1.6.1": {"type": "Counter64", "value": 42},
                     }}}}
        with serve_gateway(scenario({"dev": agent})) as gateway, tempfile.TemporaryDirectory() as temporary:
            builder, store, archive, _ = self._poll_all(gateway, temporary, "dev", 2)
            try:
                builder.rebuild()
                c64 = [s for s in builder.describe() if s["column"] == "ifHCInOctets"]
                self.assertEqual(len(c64), 2)
                self.assertIsNone(c64[1]["points"][0]["delta"])  # 新段起点不做减法
            finally:
                store.close(); archive.close()

    def test_restart_reusing_ifindex_cuts_segment_on_identity_and_uptime(self):
        base = base_agent([(1, "old-port", (100, 100, 100, 100))])
        agent = {"base": base, "polls": {
            "1": {"sysUpTime": 900000},
            "2": {"sysUpTime": 50, "changes": {
                "1.3.6.1.2.1.2.2.1.2.1": {"type": "OctetString", "value": "new-port"},
                "1.3.6.1.2.1.2.2.1.6.1": {"type": "OctetString", "value": "mac-1b"},
                "1.3.6.1.2.1.31.1.1.1.1.1": {"type": "OctetString", "value": "Gi0/1b"},
                "1.3.6.1.2.1.2.2.1.10.1": {"type": "Counter32", "value": 3},
                "1.3.6.1.2.1.31.1.1.1.6.1": {"type": "Counter64", "value": 3},
            }}}}
        with serve_gateway(scenario({"dev": agent})) as gateway, tempfile.TemporaryDirectory() as temporary:
            builder, store, archive, _ = self._poll_all(gateway, temporary, "dev", 2)
            try:
                breaks = builder.rebuild()
                reasons = {b["reason"] for b in breaks if b["poll_no"] == 2}
                self.assertIn("sysuptime_rollback", reasons)
                c32 = [s for s in builder.describe() if s["column"] == "ifInOctets"]
                self.assertEqual(len(c32), 2)
                self.assertIsNone(c32[1]["points"][0]["delta"])
            finally:
                store.close(); archive.close()

    def test_discontinuity_time_change_cuts_segment(self):
        agent = {"base": base_agent([(1, "p1", (100, 100, 100, 100))]),
                 "polls": {
                     "1": {"sysUpTime": 1000},
                     "2": {"sysUpTime": 2000, "changes": {
                         "1.3.6.1.2.1.2.2.1.10.1": {"type": "Counter32", "value": 300},
                         "1.3.6.1.2.1.31.1.1.1.19.1": {"type": "TimeTicks", "value": 1500},
                     }}}}
        with serve_gateway(scenario({"dev": agent})) as gateway, tempfile.TemporaryDirectory() as temporary:
            builder, store, archive, _ = self._poll_all(gateway, temporary, "dev", 2)
            try:
                breaks = builder.rebuild()
                self.assertTrue(any(b["reason"] == "discontinuity_time_changed" for b in breaks))
            finally:
                store.close(); archive.close()


class StoreRecoveryTests(unittest.TestCase):
    def test_interrupted_poll_is_aborted_and_next_poll_keeps_its_own_rows(self):
        agent = {"base": base_agent([(1, "p1", (100, 100, 100, 100))]),
                 "polls": {n: {"sysUpTime": 1000 * n} for n in ("1", "2", "3")}}
        with serve_gateway(scenario({"dev": agent})) as gateway, tempfile.TemporaryDirectory() as temporary:
            store_path = Path(temporary) / "tables.sqlite3"
            archive_path = Path(temporary) / "raw.sqlite3"
            archive = Archive(archive_path)
            store = TableStore(store_path)
            try:
                walker = Walker(Client(gateway.base_url), archive, store, "src", PROFILE)
                first = walker.run_poll("dev")
                self.assertEqual(first.poll_no, 1)
                # 模拟进程在第 2 轮登记后、游走前崩溃。
                store.begin_poll("dev", PROFILE.columns)
                store.close()
                store = TableStore(store_path)
                stale = [p for p in store.polls("dev") if p["state"] == "aborted"]
                self.assertEqual(len(stale), 1)
                self.assertEqual(stale[0]["abort_reason"], "interrupted_by_restart")
                walker = Walker(Client(gateway.base_url), archive, store, "src", PROFILE)
                third = walker.run_poll("dev")
                self.assertEqual(third.poll_no, 3)  # 轮次号不复用
                # 新一轮只含本轮证据，没有把第 1 轮的残留列补进来。
                rows = store.rows_of(third.poll_id)
                evidence_polls = {e for r in store.connection.execute(
                    "SELECT DISTINCT poll_id FROM cell_observations WHERE poll_id=?",
                    (third.poll_id,)).fetchall() for e in r}
                self.assertEqual(evidence_polls, {third.poll_id})
                self.assertTrue(rows)
            finally:
                store.close(); archive.close()

    def test_agent_progress_is_independent(self):
        slow = {"base": base_agent([(1, "p1", (1, 1, 1, 1))]),
                "polls": {"1": {"sysUpTime": 100, "events": [
                    {"when_request": 1, "column": "1.3.6.1.2.1.2.2.1.1",
                     "kind": "toobig", "max_reps_threshold": 1}]}}}
        healthy = {"base": base_agent([(2, "p2", (2, 2, 2, 2))]),
                   "polls": {"1": {"sysUpTime": 100}}}
        with serve_gateway(scenario({"slow": slow, "healthy": healthy})) as gateway, \
                tempfile.TemporaryDirectory() as temporary:
            store_path = Path(temporary) / "tables.sqlite3"
            archive_path = Path(temporary) / "raw.sqlite3"
            archive = Archive(archive_path)
            # slow 在 reps=1 时仍被拒（阈值 1，任何领取范围都 tooBig），必然中止；
            # healthy 用默认配置应完整完成，两个设备共用存储但进度互不拖累。
            slow_store = TableStore(store_path)
            healthy_store = slow_store  # 同一存储，验证设备间状态隔离
            try:
                slow_walker = Walker(Client(gateway.base_url), archive, slow_store, "src",
                                     PROFILE, WalkConfig(initial_max_reps=1))
                bad = slow_walker.run_poll("slow")
                self.assertEqual(bad.state, "aborted")
                good_walker = Walker(Client(gateway.base_url), archive, healthy_store, "src",
                                     PROFILE, WalkConfig())
                good = good_walker.run_poll("healthy")
                self.assertEqual(good.state, "complete")
                self.assertEqual(slow_store.publication(bad.poll_id)["scope"], "partial")
                self.assertEqual(slow_store.publication(good.poll_id)["scope"], "full")
            finally:
                slow_store.close(); archive.close()


if __name__ == "__main__":
    unittest.main()
