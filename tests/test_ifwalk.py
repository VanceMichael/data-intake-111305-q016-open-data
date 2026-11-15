import json
import tempfile
import unittest
from pathlib import Path

from open_data.archive import Archive
from open_data.client import Client, collect
from open_data.gateway import load_gateway_scenario, serve_gateway
from open_data.ifmib import DEFAULT_COLUMNS, load_columns, match_column, parse_oid
from open_data.ifstore import IfStore
from open_data.ifwalk import (
    Walker,
    apply_envelope,
    classify_varbind,
    ingest_archived,
    publish_document,
)
from open_data.origin import serve

SCENARIO = Path(__file__).parents[1] / "fixtures" / "ifwalk_scenario.json"


class WalkHarness:
    def __init__(self, test, budget=40, retries=3):
        self.test = test
        self.temporary = tempfile.TemporaryDirectory()
        self.columns = load_columns()
        self.scenario = load_gateway_scenario(SCENARIO)
        self.gateway_ctx = serve_gateway(self.scenario, self.columns)
        self.gateway = self.gateway_ctx.__enter__()
        self.archive = Archive(Path(self.temporary.name) / "raw.sqlite3")
        self.store = IfStore(Path(self.temporary.name) / "walk.sqlite3", self.columns)
        self.walker = Walker(
            Client(self.gateway.base_url),
            self.archive,
            self.store,
            request_budget=budget,
            timeout_retries=retries,
        )

    def close(self):
        self.store.close()
        self.archive.close()
        self.gateway_ctx.__exit__(None, None, None)

    def requests_for(self, agent, poll):
        return [
            request
            for request in self.gateway.requests
            if request["agent"] == agent and request["poll"] == poll
        ]


class OidAndClassifyTests(unittest.TestCase):
    def test_parse_oid_rejects_bad_text(self):
        for text in ("", "1..2", "1.2.x", "1.2.", None, 12):
            with self.subTest(text=text), self.assertRaises((ValueError, TypeError)):
                parse_oid(text)

    def test_match_column_uses_longest_prefix(self):
        columns = load_columns()
        column, suffix = match_column(columns, parse_oid("1.3.6.1.2.1.31.1.1.1.6.7"))
        self.assertEqual(column.name, "ifHCInOctets")
        self.assertEqual(suffix, (7,))
        column, _ = match_column(columns, parse_oid("1.3.6.1.2.1.2.2.1.10.8"))
        self.assertEqual(column.name, "ifInOctets")

    def test_classify_keeps_type_and_rejects_positional_guesses(self):
        columns = load_columns()
        kind, detail = classify_varbind(
            columns, {"oid": "1.3.6.1.2.1.2.2.1.10.8", "type": "Counter32", "value": 44}
        )
        self.assertEqual(kind, "cell")
        self.assertEqual((detail["column"], detail["idx"], detail["type"]), ("ifInOctets", "8", "Counter32"))
        # 多级索引不属于 ifTable 单级 ifIndex，不能按位置塞进某一行。
        kind, _ = classify_varbind(
            columns, {"oid": "1.3.6.1.2.1.2.2.1.10.8.9", "type": "Counter32", "value": 1}
        )
        self.assertEqual(kind, "stray")
        # 值类型非法、未知类型、越界 OID 都进杂项而不是某一行。
        for varbind in (
            {"oid": "1.3.6.1.2.1.2.2.1.10.8", "type": "Counter32", "value": -1},
            {"oid": "1.3.6.1.2.1.2.2.1.10.8", "type": "Counter32", "value": True},
            {"oid": "1.3.6.1.2.1.2.2.1.10.8", "type": "Mystery", "value": 1},
            {"oid": "1.3.6.1.9.9.9.9.1", "type": "Counter32", "value": 1},
        ):
            with self.subTest(varbind=varbind):
                self.assertEqual(classify_varbind(columns, varbind)[0], "stray")

    def test_classify_exceptions(self):
        columns = load_columns()
        kind, detail = classify_varbind(
            columns, {"oid": "1.3.6.1.2.1.31.1.1.1.6.8", "type": "noSuchInstance", "value": None}
        )
        self.assertEqual((kind, detail["column"], detail["idx"]), ("hole", "ifHCInOctets", "8"))
        kind, detail = classify_varbind(
            columns, {"oid": "1.3.6.1.2.1.2.2.1.2", "type": "endOfMibView", "value": None}
        )
        self.assertEqual((kind, detail["column"]), ("end", "ifDescr"))


class WalkTests(unittest.TestCase):
    def setUp(self):
        self.harness = WalkHarness(self)
        self.addCleanup(self.harness.close)
        self.store = self.harness.store
        self.walker = self.harness.walker

    def walk(self, agent, rounds):
        return self.walker.walk_agent(agent, rounds=rounds)

    def test_ragged_rows_assembled_by_oid_not_position(self):
        outcome = self.walk("switch-a", 1)
        self.assertEqual(outcome["rounds"][0]["status"], "complete")
        round_row = self.store.round_for("switch-a", 1)
        rows = {row["idx"]: row for row in self.store.row_status_for(round_row["id"])}
        self.assertEqual(json.loads(rows["8"]["missing"]), ["ifHCOutOctets"])
        self.assertEqual(json.loads(rows["7"]["missing"]), [])
        cells = {
            (cell["idx"], cell["column"]): cell
            for cell in self.store.cells_for_round(round_row["id"])
        }
        # 位置配对会把 ifHCInOctets.7 错挂到行 8；按 OID 组装各行各列。
        self.assertEqual(json.loads(cells[("7", "ifHCInOctets")]["value"]), 100000)
        self.assertEqual(json.loads(cells[("8", "ifHCInOctets")]["value"]), 60000)
        self.assertEqual(json.loads(cells[("8", "ifInOctets")]["value"]), 3000)
        self.assertNotIn(("8", "ifHCOutOctets"), cells)

    def test_too_big_narrows_next_request(self):
        self.walk("switch-a", 1)
        requests = self.harness.requests_for("switch-a", 1)
        self.assertEqual(requests[0]["max_repetitions"], 8)
        self.assertEqual(requests[1]["max_repetitions"], 4)
        round_row = self.store.round_for("switch-a", 1)
        self.assertEqual(round_row["too_big"], 1)
        self.assertEqual(round_row["current_repetitions"], 4)
        detail = json.loads(round_row["detail"])
        self.assertIn("narrow", {event["kind"] for event in detail})

    def test_end_of_mib_view_is_the_completion_evidence(self):
        self.walk("switch-a", 1)
        round_row = self.store.round_for("switch-a", 1)
        columns = {c["column"]: c for c in self.store.round_columns(round_row["id"])}
        self.assertTrue(all(c["status"] == "verified" for c in columns.values()))
        self.assertTrue(all(c["evidence_capture"] for c in columns.values()))
        # 每个列的结束都能回指归档里的原始响应。
        for column in columns.values():
            body = self.harness.archive.body(column["evidence_capture"])
            self.assertIn(b"endOfMibView", body)

    def test_timeout_is_not_table_cleared(self):
        self.walk("switch-b", 2)
        round_two = self.store.round_for("switch-b", 2)
        self.assertEqual(round_two["status"], "in_progress")
        self.assertGreater(round_two["timeouts"], 0)
        # 上一轮已核实的行不被超时抹掉，连续段保持开放。
        document = publish_document(self.store, "switch-b")
        self.assertEqual(document["latest_round"]["poll"], 2)
        self.assertEqual(document["latest_round"]["status"], "in_progress")
        self.assertEqual(document["latest_round"]["rows"], [])
        self.assertEqual(document["checkpoint"]["next_poll"], 2)
        history = {entry["poll"]: entry["status"] for entry in document["round_history"]}
        self.assertEqual(history[1], "complete")
        self.assertTrue(all(segment["state"] == "open" for segment in document["segments"]))

    def test_no_such_instance_is_a_hole_not_a_clear(self):
        self.walk("switch-a", 4)
        round_four = self.store.round_for("switch-a", 4)
        self.assertEqual(round_four["status"], "complete")
        holes = self.store.holes_for_round(round_four["id"])
        self.assertEqual(
            [(hole["column"], hole["idx"], hole["type"]) for hole in holes],
            [("ifHCInOctets", "8", "noSuchInstance")],
        )
        rows = {row["idx"]: row for row in self.store.row_status_for(round_four["id"])}
        self.assertEqual(json.loads(rows["8"]["missing"]), ["ifHCInOctets"])
        cells = {
            (cell["idx"], cell["column"])
            for cell in self.store.cells_for_round(round_four["id"])
        }
        self.assertIn(("8", "ifInOctets"), cells)
        self.assertIn(("8", "ifOutOctets"), cells)

    def test_reboot_closes_segments_without_fake_delta(self):
        self.walk("switch-a", 4)
        segments = [
            s for s in self.store.segments_for("switch-a")
            if s["idx"] == "7" and s["column"] == "ifHCInOctets"
        ]
        self.assertEqual(len(segments), 2)
        old, new = segments
        self.assertEqual((old["close_reason"], old["delta"]), ("reboot", "50000"))
        self.assertEqual(new["closed_round"], None)
        # 重启后计数器从 300 起步：新段增量只含段内样本，不出现巨大流量。
        self.assertEqual((new["first_value"], new["last_value"], new["delta"]), ("300", "900", "600"))
        agent = self.store.agent_state("switch-a")
        self.assertEqual(agent["reboots"], 1)

    def test_single_counter_discontinuity_opens_fresh_segment(self):
        self.walk("switch-a", 2)
        segments = [
            s for s in self.store.segments_for("switch-a")
            if s["idx"] == "8" and s["column"] == "ifHCInOctets"
        ]
        self.assertEqual(len(segments), 2)
        old, new = segments
        self.assertEqual(old["close_reason"], "discontinuity_time")
        self.assertEqual((new["first_value"], new["samples"]), ("10", 1))
        # 未受波及的接口段不受影响。
        other = [
            s for s in self.store.segments_for("switch-a")
            if s["idx"] == "7" and s["column"] == "ifHCInOctets"
        ]
        self.assertEqual(len(other), 1)
        self.assertEqual((other[0]["samples"], other[0]["delta"]), (2, "50000"))

    def test_incomplete_row_is_not_backfilled_from_last_round(self):
        self.walk("switch-a", 2)
        round_two = self.store.round_for("switch-a", 2)
        rows = {row["idx"]: row for row in self.store.row_status_for(round_two["id"])}
        self.assertEqual(json.loads(rows["8"]["missing"]), ["ifOutOctets"])
        cells = {
            (cell["idx"], cell["column"]): cell
            for cell in self.store.cells_for_round(round_two["id"])
        }
        # 第一轮有 ifOutOctets.8=4000，第二轮缺失就不能拿旧值冒充完整。
        self.assertNotIn(("8", "ifOutOctets"), cells)
        self.assertEqual(json.loads(cells[("9", "ifHCInOctets")]["value"]), 500)

    def test_counter32_wrap_stays_in_segment(self):
        self.walk("switch-a", 4)
        segments = [
            s for s in self.store.segments_for("switch-a")
            if s["idx"] == "7" and s["column"] == "ifInOctets" and s["closed_round"] is None
        ]
        self.assertEqual(len(segments), 1)
        # 回绕按模 2^32 计入段内增量：(50 - 4294967200) mod 2^32 = 146。
        self.assertEqual(
            (segments[0]["first_value"], segments[0]["last_value"], segments[0]["delta"]),
            ("4294967200", "50", "146"),
        )

    def test_agents_progress_independently(self):
        self.walk("switch-b", 4)
        outcome = self.walk("switch-a", 4)
        self.assertTrue(all(r["status"] == "complete" for r in outcome["rounds"]))
        self.assertEqual(self.store.agent_state("switch-a")["next_poll"], 5)
        # switch-b 卡在第二轮不影响 switch-a 已完成的轮次。
        self.assertEqual(self.store.round_for("switch-b", 2)["status"], "in_progress")

    def test_value_types_are_preserved(self):
        self.walk("switch-a", 1)
        document = publish_document(self.store, "switch-a")
        row7 = next(row for row in document["latest_round"]["rows"] if row["index"] == "7")
        self.assertEqual(row7["cells"]["ifDescr"]["type"], "OctetString")
        self.assertEqual(row7["cells"]["ifHCInOctets"]["type"], "Counter64")
        self.assertEqual(row7["cells"]["ifCounterDiscontinuityTime"]["type"], "TimeTicks")
        for cell in row7["cells"].values():
            self.assertIsInstance(cell["capture_id"], int)

    def test_resume_from_checkpoint_continues_cursors(self):
        harness = self.harness
        # 预算 2：tooBig 与首个数据响应后即暂停，断点保留列游标。
        paused = Walker(
            Client(harness.gateway.base_url),
            harness.archive,
            self.store,
            request_budget=2,
        )
        outcome = paused.walk_agent("switch-a", rounds=1)
        self.assertEqual(outcome["rounds"][0]["result"], "paused")
        self.assertEqual(self.store.round_for("switch-a", 1)["status"], "in_progress")
        # 恢复后从断点游标继续，而不是从头再走。
        outcome = self.walker.walk_agent("switch-a", rounds=1)
        self.assertEqual(outcome["rounds"][0]["status"], "complete")
        requests = harness.requests_for("switch-a", 1)
        self.assertEqual([r["max_repetitions"] for r in requests], [8, 4, 4])
        resumed_oids = requests[2]["oids"]
        self.assertIn("1.3.6.1.2.1.2.2.1.2.8", resumed_oids)
        self.assertIn("1.3.6.1.2.1.31.1.1.1.10.7", resumed_oids)
        # 单元格没有因重放而重复。
        round_row = self.store.round_for("switch-a", 1)
        cells = self.store.cells_for_round(round_row["id"])
        keys = [(cell["column"], cell["idx"]) for cell in cells]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(self.store.agent_state("switch-a")["max_repetitions"], 4)

    def test_round_rows_carry_oid_evidence(self):
        self.walk("switch-a", 1)
        document = publish_document(self.store, "switch-a", archive_name="raw.sqlite3")
        row8 = next(row for row in document["latest_round"]["rows"] if row["index"] == "8")
        self.assertEqual(row8["cells"]["ifInOctets"]["oid"], "1.3.6.1.2.1.2.2.1.10.8")
        capture_id = row8["cells"]["ifInOctets"]["capture_id"]
        body = json.loads(self.harness.archive.body(capture_id))
        oids = {varbind["oid"] for varbind in body["varbinds"]}
        self.assertIn("1.3.6.1.2.1.2.2.1.10.8", oids)


class IngestTests(unittest.TestCase):
    def test_ingest_fixture_feed_assembles_by_oid(self):
        with serve() as origin, tempfile.TemporaryDirectory() as temporary:
            archive = Archive(Path(temporary) / "raw.sqlite3")
            store = IfStore(Path(temporary) / "walk.sqlite3", load_columns())
            try:
                collect(Client(origin.base_url), archive, "local-source", "/feed")
                result = ingest_archived(store, archive, source="local-source", finalize=True)
                self.assertEqual(result["applied"], 3)
                # 旧导出脚本按位置拼行的同一批数据，现在按 OID 成行。
                round_one = store.round_for("switch-a", 1)
                self.assertEqual(round_one["status"], "partial")
                cells = {
                    (cell["idx"], cell["column"]): json.loads(cell["value"])
                    for cell in store.cells_for_round(round_one["id"])
                }
                self.assertEqual(cells, {
                    ("7", "ifDescr"): "uplink",
                    ("7", "ifCounterDiscontinuityTime"): 0,
                    ("8", "ifInOctets"): 44,
                })
                # tooBig 只是调整信号；sysUpTime 回落记为重启证据。
                round_two = store.round_for("switch-a", 2)
                self.assertEqual(round_two["too_big"], 1)
                self.assertEqual(round_two["reboot_before"], 1)
                self.assertEqual(store.agent_state("switch-a")["reboots"], 1)
                # endOfMibView 让 switch-b 的 ifDescr 列核实，其余列仍待走。
                round_b = store.round_for("switch-b", 1)
                columns = {c["column"]: c["status"] for c in store.round_columns(round_b["id"])}
                self.assertEqual(columns["ifDescr"], "verified")
                self.assertEqual(columns["ifInOctets"], "walking")
                self.assertEqual(round_b["status"], "partial")
            finally:
                store.close()
                archive.close()

    def test_ingest_is_idempotent_across_runs(self):
        with serve() as origin, tempfile.TemporaryDirectory() as temporary:
            archive = Archive(Path(temporary) / "raw.sqlite3")
            store = IfStore(Path(temporary) / "walk.sqlite3", load_columns())
            try:
                collect(Client(origin.base_url), archive, "local-source", "/feed")
                collect(Client(origin.base_url), archive, "local-source", "/feed")
                first = ingest_archived(store, archive, source="local-source", finalize=True)
                second = ingest_archived(store, archive, source="local-source", finalize=True)
                self.assertEqual(first["applied"], 3)
                self.assertEqual(second["applied"], 0)
                self.assertEqual(len(store.rounds_for("switch-a")), 2)
            finally:
                store.close()
                archive.close()

    def test_apply_envelope_rejects_bad_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = IfStore(Path(temporary) / "walk.sqlite3", load_columns())
            try:
                for envelope in (
                    {"poll": 1, "varbinds": []},
                    {"agent": "x", "poll": 0, "varbinds": []},
                    {"agent": "x", "poll": 1, "varbinds": {}},
                ):
                    with self.subTest(envelope=envelope), self.assertRaises(ValueError):
                        apply_envelope(store, envelope, 1)
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
