import argparse
import json
import tempfile
from pathlib import Path

from .archive import Archive
from .client import Client, collect
from .continuity import SegmentBuilder
from .decoding import records
from .gateway import load_gateway_scenario, serve_gateway
from .mib import MibProfile
from .origin import serve
from .store import TableStore
from .walk import Walker, WalkConfig


def capture(base_url, archive_path, source, path):
    archive = Archive(archive_path)
    try:
        capture_id, response = collect(Client(base_url), archive, source, path)
        result = {"capture_id": capture_id, "status": response.status, "bytes": len(response.body), "url": response.url}
        if response.status == 200:
            result["source_records"] = records(response.body, response.headers.get("content-type", ""))
        result["archived_observations"] = len(archive.captures())
        return result
    finally:
        archive.close()


def walk_once(base_url, archive_path, store_path, source, agent, max_reps):
    client = Client(base_url)
    archive = Archive(archive_path)
    store = TableStore(store_path)
    store.meta_set("archive_path", str(archive_path))
    try:
        walker = Walker(client, archive, store, source, MibProfile(),
                        WalkConfig(initial_max_reps=max_reps))
        summary = walker.run_poll(agent)
        SegmentBuilder(store, MibProfile()).rebuild()
        return summary.to_dict()
    finally:
        store.close()
        archive.close()


def poll_detail(store_path, poll_id):
    store = TableStore(store_path)
    try:
        poll = store.require_poll(poll_id)
        return {
            "poll": poll,
            "columns": store.columns_of(poll_id),
            "requests": store.requests_of(poll_id),
            "rows": store.rows_of(poll_id),
            "publication": store.publication(poll_id),
        }
    finally:
        store.close()


def evidence_chain(store_path, poll_id, index_suffix):
    store = TableStore(store_path)
    try:
        return {"poll_id": poll_id, "index_suffix": index_suffix,
                "evidence": store.cell_evidence(poll_id, index_suffix)}
    finally:
        store.close()


def segments_report(store_path, agent=None):
    store = TableStore(store_path)
    try:
        SegmentBuilder(store, MibProfile()).rebuild()
        return {"segments": SegmentBuilder(store, MibProfile()).describe(agent)}
    finally:
        store.close()


def snmp_demo():
    """离线端到端演示：合成网关 + 有界游走 + 成行发布 + 连续段 + 中断恢复。"""
    profile = MibProfile()
    with tempfile.TemporaryDirectory() as temporary, serve_gateway() as gateway:
        archive_path = Path(temporary) / "raw.sqlite3"
        store_path = Path(temporary) / "tables.sqlite3"
        client = Client(gateway.base_url)
        archive = Archive(archive_path)
        store = TableStore(store_path)
        try:
            walker = Walker(client, archive, store, "local-snmp-gateway", profile)
            summaries = []
            # switch-a：ragged 列、tooBig、超时、轮内改表
            summaries.append(walker.run_poll("switch-a").to_dict())
            summaries.append(walker.run_poll("switch-a").to_dict())
            # switch-b：Counter 回绕、重启复用 ifIndex
            summaries.append(walker.run_poll("switch-b").to_dict())
            summaries.append(walker.run_poll("switch-b").to_dict())
            # 两个设备交错推进，验证进度互不拖累
            summaries.append(walker.run_poll("switch-a").to_dict())
            summaries.append(walker.run_poll("switch-b").to_dict())
            # switch-c：单个计数器异常下降（无声明的不连续）
            summaries.append(walker.run_poll("switch-c").to_dict())
            summaries.append(walker.run_poll("switch-c").to_dict())
            summaries.append(walker.run_poll("switch-c").to_dict())
            # switch-b 第 4 轮：ifCounterDiscontinuityTime 变化
            summaries.append(walker.run_poll("switch-b").to_dict())
            # 模拟进程在一轮游走中途被杀死：只登记轮次就关闭存储。
            store.begin_poll("switch-c", profile.columns)
            store.close()
            store = TableStore(store_path)  # 重开：running 轮次解释为中断而非完成
            interrupted = store.polls("switch-c")[-1]
            walker = Walker(client, archive, store, "local-snmp-gateway", profile)
            # 中断后继续下一轮，新轮次号不复用，连续性分析在缺口处切段。
            summaries.append(walker.run_poll("switch-c").to_dict())
            builder = SegmentBuilder(store, profile)
            breaks = builder.rebuild()
            report = {
                "operation": "snmp_interface_walk",
                "gateway": gateway.base_url,
                "raw_responses": len(archive.captures()),
                "interrupted_poll": {
                    "poll_no": interrupted["poll_no"],
                    "state": interrupted["state"],
                    "abort_reason": interrupted["abort_reason"],
                },
                "polls": [
                    {key: summary[key] for key in
                     ("agent", "poll_no", "state", "abort_reason", "request_count", "publication_scope")}
                    | {"rows": [
                        {"index_suffix": row["index_suffix"], "completeness": row["completeness"],
                         "missing_columns": row["missing_columns"],
                         "conflict_columns": row["conflict_columns"]}
                        for row in summary["rows"]]}
                    for summary in summaries
                ],
                "continuity_breaks": breaks,
                "segments": builder.describe(),
            }
            return report
        finally:
            store.close()
            archive.close()


def main():
    parser = argparse.ArgumentParser(description="来源响应接收、SNMP 接口表成行采集与连续性标记")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("demo")
    collector = subcommands.add_parser("capture")
    collector.add_argument("--base-url", required=True)
    collector.add_argument("--archive", type=Path, required=True)
    collector.add_argument("--source", required=True)
    collector.add_argument("--path", default="/feed")
    server = subcommands.add_parser("serve")
    server.add_argument("--scenario", type=Path)

    subcommands.add_parser("snmp-demo")
    gateway_server = subcommands.add_parser("gateway")
    gateway_server.add_argument("--scenario", type=Path)
    walker = subcommands.add_parser("walk")
    walker.add_argument("--base-url", required=True)
    walker.add_argument("--archive", type=Path, required=True)
    walker.add_argument("--store", type=Path, required=True)
    walker.add_argument("--source", required=True)
    walker.add_argument("--agent", required=True)
    walker.add_argument("--max-reps", type=int, default=3)
    detail = subcommands.add_parser("poll")
    detail.add_argument("--store", type=Path, required=True)
    detail.add_argument("--poll-id", type=int, required=True)
    evidence = subcommands.add_parser("evidence")
    evidence.add_argument("--store", type=Path, required=True)
    evidence.add_argument("--poll-id", type=int, required=True)
    evidence.add_argument("--index", required=True)
    segments = subcommands.add_parser("segments")
    segments.add_argument("--store", type=Path, required=True)
    segments.add_argument("--agent")

    args = parser.parse_args()
    if args.command == "demo":
        with tempfile.TemporaryDirectory() as temporary, serve() as origin:
            result = capture(origin.base_url, Path(temporary) / "archive.sqlite3", "local-source", "/feed")
            print(json.dumps({"operation": "raw_capture", **result}, ensure_ascii=False, indent=2))
    elif args.command == "capture":
        print(json.dumps(capture(args.base_url, args.archive, args.source, args.path), ensure_ascii=False, indent=2))
    elif args.command == "serve":
        from .origin import load_scenario
        with serve(load_scenario(args.scenario)) as origin:
            print(json.dumps({"base_url": origin.base_url}, ensure_ascii=False), flush=True)
            try:
                import threading
                threading.Event().wait()
            except KeyboardInterrupt:
                pass
    elif args.command == "snmp-demo":
        print(json.dumps(snmp_demo(), ensure_ascii=False, indent=2))
    elif args.command == "gateway":
        with serve_gateway(load_gateway_scenario(args.scenario)) as gateway:
            print(json.dumps({"base_url": gateway.base_url, "route": "/snmp/bulk"}, ensure_ascii=False), flush=True)
            try:
                import threading
                threading.Event().wait()
            except KeyboardInterrupt:
                pass
    elif args.command == "walk":
        print(json.dumps(walk_once(args.base_url, args.archive, args.store, args.source,
                                   args.agent, args.max_reps), ensure_ascii=False, indent=2))
    elif args.command == "poll":
        print(json.dumps(poll_detail(args.store, args.poll_id), ensure_ascii=False, indent=2))
    elif args.command == "evidence":
        print(json.dumps(evidence_chain(args.store, args.poll_id, args.index), ensure_ascii=False, indent=2))
    elif args.command == "segments":
        print(json.dumps(segments_report(args.store, args.agent), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
