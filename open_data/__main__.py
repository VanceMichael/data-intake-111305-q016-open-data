import argparse
import json
import tempfile
from pathlib import Path

from .archive import Archive
from .client import Client, collect
from .decoding import records
from .gateway import load_gateway_scenario, serve_gateway
from .ifmib import load_columns
from .ifstore import IfStore
from .ifwalk import Walker, ingest_archived, publish_document
from .origin import serve


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


def walk(gateway_url, archive_path, state_path, agents, *, rounds, budget, retries, columns_path=None, timeout=3):
    columns = load_columns(columns_path)
    archive = Archive(archive_path)
    store = IfStore(state_path, columns)
    try:
        walker = Walker(
            Client(gateway_url, timeout=timeout),
            archive,
            store,
            request_budget=budget,
            timeout_retries=retries,
        )
        summaries = []
        for agent in agents:
            try:
                summaries.append(walker.walk_agent(agent, rounds=rounds))
            except Exception as error:  # 单台设备失败不拖累其它设备
                summaries.append({"agent": agent, "error": f"{type(error).__name__}: {error}"})
        return {"agents": summaries}
    finally:
        store.close()
        archive.close()


def publish(state_path, agents, *, columns_path=None, archive_name=None):
    columns = load_columns(columns_path)
    store = IfStore(state_path, columns)
    try:
        if not agents:
            rows = store.connection.execute("SELECT agent FROM agents ORDER BY agent").fetchall()
            agents = [row["agent"] for row in rows]
        return [publish_document(store, agent, archive_name=archive_name) for agent in agents]
    finally:
        store.close()


def ingest(archive_path, state_path, *, source=None, finalize=False, columns_path=None):
    columns = load_columns(columns_path)
    archive = Archive(archive_path)
    store = IfStore(state_path, columns)
    try:
        return ingest_archived(store, archive, source=source, finalize=finalize)
    finally:
        store.close()
        archive.close()


def interface_demo(scenario_path=None):
    """端到端演练：合成网关、有界游走、断点续走与分设备发布。"""
    columns = load_columns()
    scenario = load_gateway_scenario(scenario_path)
    agents = list(scenario["agents"])
    with tempfile.TemporaryDirectory() as temporary, serve_gateway(scenario, columns) as gateway:
        archive_path = Path(temporary) / "raw.sqlite3"
        state_path = Path(temporary) / "walk.sqlite3"
        archive = Archive(archive_path)
        store = IfStore(state_path, columns)
        try:
            walker = Walker(Client(gateway.base_url), archive, store, request_budget=12)
            rounds_log = {agent: [] for agent in agents}
            # 逐轮交错推进，展示设备之间进度互不拖累。
            for _ in range(4):
                for agent in agents:
                    outcome = walker.walk_agent(agent, rounds=1)
                    rounds_log[agent].extend(outcome["rounds"])
            documents = [publish_document(store, agent, archive_name=archive_path.name) for agent in agents]
            summary = {
                "operation": "interface_walk_demo",
                "gateway_requests": len(gateway.requests),
                "archived_responses": len(archive.captures()),
                "agents": {
                    agent: {
                        "rounds": rounds_log[agent],
                        "checkpoint": document["checkpoint"],
                        "latest_round": _brief_round(document["latest_round"]),
                        "segments": document["segments"],
                    }
                    for agent, document in zip(agents, documents)
                },
            }
            return summary
        finally:
            store.close()
            archive.close()


def _brief_round(latest):
    if latest is None:
        return None
    return {
        "poll": latest["poll"],
        "status": latest["status"],
        "columns": latest["columns"],
        "rows": [
            {"index": row["index"], "complete": row["complete"], "missing": row["missing"]}
            for row in latest["rows"]
        ],
        "incomplete_rows": latest["incomplete_rows"],
    }


def main():
    parser = argparse.ArgumentParser(description="来源响应接收与归档")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("demo")
    collector = subcommands.add_parser("capture")
    collector.add_argument("--base-url", required=True)
    collector.add_argument("--archive", type=Path, required=True)
    collector.add_argument("--source", required=True)
    collector.add_argument("--path", default="/feed")
    server = subcommands.add_parser("serve")
    server.add_argument("--scenario", type=Path)

    gateway_cmd = subcommands.add_parser("gateway", help="启动本地合成 SNMP 网关")
    gateway_cmd.add_argument("--scenario", type=Path, required=True)
    gateway_cmd.add_argument("--columns", type=Path)

    walk_cmd = subcommands.add_parser("walk", help="对网关执行有界接口表游走")
    walk_cmd.add_argument("--gateway", required=True)
    walk_cmd.add_argument("--archive", type=Path, required=True)
    walk_cmd.add_argument("--state", type=Path, required=True)
    walk_cmd.add_argument("--agent", action="append", required=True)
    walk_cmd.add_argument("--rounds", type=int, default=1)
    walk_cmd.add_argument("--requests", type=int, default=40, help="本轮请求预算")
    walk_cmd.add_argument("--retries", type=int, default=3, help="单次轮次内超时重试次数")
    walk_cmd.add_argument("--columns", type=Path)

    ingest_cmd = subcommands.add_parser("ingest", help="把归档中的来源记录接入走查状态")
    ingest_cmd.add_argument("--archive", type=Path, required=True)
    ingest_cmd.add_argument("--state", type=Path, required=True)
    ingest_cmd.add_argument("--source")
    ingest_cmd.add_argument("--finalize", action="store_true")
    ingest_cmd.add_argument("--columns", type=Path)

    publish_cmd = subcommands.add_parser("publish", help="发布每台设备的接口表视图")
    publish_cmd.add_argument("--state", type=Path, required=True)
    publish_cmd.add_argument("--agent", action="append")
    publish_cmd.add_argument("--columns", type=Path)
    publish_cmd.add_argument("--archive-name")

    subcommands.add_parser("ifdemo", help="接口表游走端到端演练")
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
    elif args.command == "gateway":
        columns = load_columns(args.columns)
        with serve_gateway(load_gateway_scenario(args.scenario), columns) as gateway:
            print(json.dumps({"base_url": gateway.base_url}, ensure_ascii=False), flush=True)
            try:
                import threading
                threading.Event().wait()
            except KeyboardInterrupt:
                pass
    elif args.command == "walk":
        result = walk(
            args.gateway,
            args.archive,
            args.state,
            args.agent,
            rounds=args.rounds,
            budget=args.requests,
            retries=args.retries,
            columns_path=args.columns,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif args.command == "ingest":
        result = ingest(
            args.archive,
            args.state,
            source=args.source,
            finalize=args.finalize,
            columns_path=args.columns,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
    elif args.command == "publish":
        documents = publish(
            args.state,
            args.agent or [],
            columns_path=args.columns,
            archive_name=args.archive_name,
        )
        print(json.dumps({"agents": documents}, ensure_ascii=False, indent=2))
    elif args.command == "ifdemo":
        print(json.dumps(interface_demo(), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
