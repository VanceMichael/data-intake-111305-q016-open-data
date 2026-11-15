"""本地合成 SNMP 网关演练服务。

只监听回环地址，把场景文件中的接口表按 GetBulk 语义返回为已解码的 JSON
信封（agent、poll、sysUpTime、error_status、varbinds）。不发起任何真实
网络扫描，也不修改任何设备。场景可声明：

- max_repetitions_limit：超过即 tooBig，演练领取范围调整；
- polls：每轮的 sysUpTime 与接口表（行按 ifIndex，列值可省略类型，
  类型取自列配置）；缺省的轮次沿用最近一轮的表；
- faults：drop（直接断开，模拟响应丢失/超时）、http500、
  no_such_instance（指定列的指定索引返回 noSuchInstance）。
"""
import copy
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .ifmib import format_oid, match_column, parse_oid


def load_gateway_scenario(path=None):
    if path is not None:
        scenario = Path(path)
    else:
        scenario = Path(__file__).parent / "fixtures" / "ifwalk_scenario.json"
        if not scenario.is_file():
            scenario = Path(__file__).parents[1] / "fixtures" / "ifwalk_scenario.json"
    scenario = json.loads(scenario.read_text(encoding="utf-8"))
    if not isinstance(scenario.get("agents"), dict) or not scenario["agents"]:
        raise ValueError("gateway scenario needs a non-empty agents object")
    return scenario


class Gateway(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, scenario, columns):
        self.scenario = copy.deepcopy(scenario)
        self.columns = list(columns)
        self.columns_by_name = {column.name: column for column in self.columns}
        self.counters = {}
        self.requests = []
        self.state_lock = threading.Lock()
        super().__init__(("127.0.0.1", 0), GatewayHandler)

    @property
    def base_url(self):
        return "http://127.0.0.1:" + str(self.server_port)

    def poll_spec(self, agent_spec, poll):
        """取该轮的场景；缺省轮次沿用最近一轮，模拟表在轮间保持。"""
        polls = agent_spec.get("polls", {})
        chosen = None
        for key in sorted(polls, key=lambda item: int(item)):
            if int(key) <= poll:
                chosen = polls[key]
        return chosen or {"sysUpTime": 0, "rows": {}}

    def next_count(self, agent, poll):
        key = (agent, poll)
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]


class GatewayHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format_string, *args):
        return None

    def send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        parsed = urlsplit(self.path)
        if parsed.path != "/getbulk":
            self.send_json(404, {"error": "unknown_route"})
            return
        params = parse_qs(parsed.query)
        agent = params.get("agent", [""])[0]
        try:
            poll = int(params.get("poll", ["0"])[0])
            max_repetitions = int(params.get("max_repetitions", ["0"])[0])
            requested = [oid for oid in params.get("oids", [""])[0].split(",") if oid]
        except ValueError:
            self.send_json(400, {"error": "bad_request"})
            return
        with self.server.state_lock:
            self.server.requests.append(
                {
                    "agent": agent,
                    "poll": poll,
                    "oids": list(requested),
                    "max_repetitions": max_repetitions,
                }
            )
            agent_spec = self.server.scenario["agents"].get(agent)
            if agent_spec is None:
                self.send_json(404, {"error": "unknown_agent"})
                return
            count = self.server.next_count(agent, poll)
            spec = self.server.poll_spec(agent_spec, poll)
            faults = [
                fault
                for fault in agent_spec.get("faults", [])
                if fault.get("poll") == poll
                and ("request" not in fault or fault["request"] == count)
            ]
        for fault in faults:
            if fault.get("action") == "drop":
                # 不返回任何内容直接断开：模拟响应丢失/超时。
                self.close_connection = True
                return
            if fault.get("action") == "http500":
                self.send_json(500, {"error": "gateway_fault"})
                return
        limit = agent_spec.get("max_repetitions_limit")
        if limit is not None and max_repetitions > limit:
            self.send_json(
                200,
                {
                    "agent": agent,
                    "poll": poll,
                    "sysUpTime": spec.get("sysUpTime", 0),
                    "error_status": "tooBig",
                    "varbinds": [],
                },
            )
            return
        rows = spec.get("rows", {})
        holes = {
            (fault.get("column"), int(fault.get("index")))
            for fault in faults
            if fault.get("action") == "no_such_instance"
        }
        varbinds = []
        for oid_text in requested:
            try:
                oid = parse_oid(oid_text)
            except ValueError:
                varbinds.append({"oid": oid_text, "type": "endOfMibView", "value": None})
                continue
            column, suffix = match_column(self.server.columns, oid)
            if column is None:
                varbinds.append({"oid": oid_text, "type": "endOfMibView", "value": None})
                continue
            start = suffix[0] if len(suffix) == 1 else 0
            # 每个列有独立的实例集合：缺列的行形成稀疏（ragged）响应。
            available = sorted(
                int(index)
                for index, row in rows.items()
                if column.name in row and int(index) > start
            )
            if not available:
                varbinds.append({"oid": oid_text, "type": "endOfMibView", "value": None})
                continue
            for index in available[: max_repetitions]:
                full_oid = column.prefix_text + "." + str(index)
                if (column.name, index) in holes:
                    varbinds.append({"oid": full_oid, "type": "noSuchInstance", "value": None})
                    continue
                value = rows[str(index)][column.name]
                if isinstance(value, dict):
                    varbinds.append(
                        {"oid": full_oid, "type": value["type"], "value": value["value"]}
                    )
                else:
                    varbinds.append(
                        {"oid": full_oid, "type": column.value_type, "value": value}
                    )
        self.send_json(
            200,
            {
                "agent": agent,
                "poll": poll,
                "sysUpTime": spec.get("sysUpTime", 0),
                "error_status": "noError",
                "varbinds": varbinds,
            },
        )


@contextmanager
def serve_gateway(scenario, columns):
    server = Gateway(scenario, columns)
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    worker.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)
