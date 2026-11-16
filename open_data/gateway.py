"""本地合成 SNMP 网关。

只在回环地址提供只读的 GetBulk 风格 JSON 响应，不扫描真实网络，也不向设备
发送写操作。网关在每个 ``(代理, 轮次)`` 视图上按 OID 词法顺序返回 varbind，
并按场景脚本注入 tooBig、endOfMibView、noSuchInstance、超时及轮内改表事件。

请求约定（全部为 GET /snmp/bulk）：

* ``agent``  设备标识；
* ``poll``   调用方自己的轮次号（正整数）；
* ``column`` 本次游走的列 OID 前缀；
* ``oid``    游标，本调用只返回严格大于该 OID 的词法后继；
* ``max_reps`` 本次最多返回多少个本子树内的 varbind。

词法顺序按 OID 子标识符元组比较，而不是字符串比较。
"""

import copy
import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from .mib import parse_oid


def load_gateway_scenario(path=None):
    if path is not None:
        scenario_path = Path(path)
    else:
        scenario_path = Path(__file__).parent / "fixtures" / "snmp_gateway.json"
        if not scenario_path.is_file():
            scenario_path = Path(__file__).parents[1] / "fixtures" / "snmp_gateway.json"
    return json.loads(scenario_path.read_text(encoding="utf-8"))


class GatewayScenario:
    """按 ``(代理, 轮次)`` 构造不可信的只读 MIB 视图。"""

    def __init__(self, scenario):
        self.scenario = copy.deepcopy(scenario)
        self.agents = self.scenario["agents"]

    def _declared_polls(self, agent):
        polls = self.agents[agent].get("polls", {})
        return sorted((int(number), body) for number, body in polls.items())

    def build_view(self, agent, poll):
        if agent not in self.agents:
            raise KeyError(agent)
        view = dict(self.agents[agent]["base"])
        uptime = 0
        for number, body in self._declared_polls(agent):
            if number <= poll:
                uptime = body.get("sysUpTime", uptime)
                view.update(copy.deepcopy(body.get("changes", {})))
        return uptime, view

    def events(self, agent, poll):
        for number, body in self._declared_polls(agent):
            if number == poll:
                return copy.deepcopy(body.get("events", []))
        return []


class Gateway(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, scenario):
        self.scenario = GatewayScenario(scenario)
        self.requests = []
        # 每个 (agent, poll) 独立维护请求序号与轮内改表叠加层，互不拖累。
        self.global_seq = {}
        self.column_seq = {}
        self.fired = {}
        self.overlays = {}
        self.state_lock = threading.Lock()
        super().__init__(("127.0.0.1", 0), GatewayHandler)

    @property
    def base_url(self):
        return "http://127.0.0.1:" + str(self.server_port)

    def _key(self, agent, poll):
        return agent, poll

    def answer(self, agent, poll, column, cursor_text, max_reps):
        """计算一次 GetBulk 响应；返回 (http_status, payload)。"""
        key = self._key(agent, poll)
        with self.state_lock:
            self.requests.append(
                {"agent": agent, "poll": poll, "column": column, "oid": cursor_text, "max_reps": max_reps}
            )
            if agent not in self.scenario.agents:
                return 404, {"error": "unknown_agent", "agent": agent}
            try:
                prefix = parse_oid(column)
                cursor = parse_oid(cursor_text)
            except ValueError as error:
                return 400, {"error": str(error)}
            if max_reps <= 0:
                return 400, {"error": "max_reps must be positive"}

            global_number = self.global_seq.get(key, 0) + 1
            self.global_seq[key] = global_number

            fired = self.fired.setdefault(key, set())
            overlay = self.overlays.setdefault(key, {"delete": set(), "put": {}})

            # 先基于当前叠加层判断本请求是否能取到数据；列本地序号只在有数据的
            # 请求上递增，endOfMibView 探针不消耗序号，保证 when_request=1 指向
            # 该列第一页真实数据。
            sys_up_time, preview = self.scenario.build_view(agent, poll)
            for oid in overlay["delete"]:
                preview.pop(oid, None)
            preview.update(overlay["put"])
            has_data = any(
                len(parse_oid(oid_text)) > len(prefix)
                and parse_oid(oid_text)[: len(prefix)] == prefix
                and parse_oid(oid_text) > cursor
                for oid_text in preview
            )
            column_number = self.column_seq.get((key, column), 0) + (1 if has_data else 0)

            events_in_response = []
            for index, event in enumerate(self.scenario.events(agent, poll)):
                # tooBig 是设备对领取范围的持续反应，独立于请求序号：只要该列
                # 请求的 max_reps 仍达到阈值就一直返回 tooBig，调用方缩小范围后
                # 才放行，因此不参与一次性事件的 fired 管理。
                if event["kind"] == "toobig" and (
                    "column" not in event or event["column"] == column
                ):
                    if max_reps >= event.get("max_reps_threshold", 1):
                        return 200, self._envelope(agent, poll, column, cursor_text, max_reps,
                                                   "tooBig", [], request_seq=global_number)
                    continue
                if index in fired:
                    continue
                # 列级事件只在请求它声明的那一列时才允许触发。
                if "column" in event and event["column"] != column:
                    continue
                target = event.get("when_request")
                # 列级事件（带 column）的 when_request 是该列的第几次数据请求；
                # 全局事件的 when_request 是整个 (代理,轮次) 的第几次请求。
                matched = (target == column_number) if "column" in event \
                    else (target == global_number)
                if not matched:
                    continue
                fired.add(index)
                events_in_response.append(event["kind"])
                if event["kind"] == "timeout":
                    # 网关级超时：HTTP 504 且没有 SNMP 信封，调用方按传输故障处理。
                    return 504, {"error": "simulated_timeout"}
                elif event["kind"] == "mutate":
                    for oid in event.get("delete", []):
                        overlay["delete"].add(oid)
                    overlay["put"].update(copy.deepcopy(event.get("put", {})))
                elif event["kind"] == "nosuchinstance":
                    overlay["put"][event["oid"]] = {"type": "noSuchInstance", "value": None}
                # 未命中阈值的一次性事件视为已消化，不重复触发。

            view = preview
            for oid in overlay["delete"]:
                view.pop(oid, None)
            view.update(overlay["put"])

            candidates = []
            for oid_text, varbind in view.items():
                oid = parse_oid(oid_text)
                if len(oid) > len(prefix) and oid[: len(prefix)] == prefix and oid > cursor:
                    candidates.append((oid, oid_text, varbind))
            candidates.sort(key=lambda item: item[0])
            selected = candidates[:max_reps]
            if not selected:
                varbinds = [{"oid": cursor_text, "type": "endOfMibView", "value": None}]
            else:
                self.column_seq[(key, column)] = column_number
                varbinds = [
                    {"oid": oid_text, "type": varbind["type"], "value": varbind.get("value")}
                    for _, oid_text, varbind in selected
                ]
            envelope = self._envelope(agent, poll, column, cursor_text, max_reps,
                                      "noError", varbinds, sys_up_time, global_number)
            envelope["events"] = events_in_response
            return 200, envelope

    @staticmethod
    def _envelope(agent, poll, column, cursor_text, max_reps, error_status, varbinds,
                  sys_up_time=None, request_seq=None):
        envelope = {
            "agent": agent,
            "poll": poll,
            "sysUpTime": sys_up_time,
            "error_status": error_status,
            "requested": {"column": column, "oid": cursor_text, "max_reps": max_reps},
            "varbinds": varbinds,
        }
        if request_seq is not None:
            envelope["request_seq"] = request_seq
        envelope["events"] = []
        return envelope


class GatewayHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format_string, *args):
        return None

    def _send(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlsplit(self.path)
        if parsed.path != "/snmp/bulk":
            self._send(404, {"error": "unknown_route"})
            return
        query = parse_qs(parsed.query)
        try:
            agent = query["agent"][0]
            poll = int(query["poll"][0])
            column = query["column"][0]
            cursor = query["oid"][0]
            max_reps = int(query["max_reps"][0])
        except (KeyError, IndexError, ValueError):
            self._send(400, {"error": "requires agent,poll,column,oid,max_reps"})
            return
        if poll < 1:
            self._send(400, {"error": "poll must be positive"})
            return
        status, payload = self.server.answer(agent, poll, column, cursor, max_reps)
        if status == 504:
            # 模拟网关超时：返回 504 且不带任何 SNMP 信封。
            self._send(504, payload)
            return
        self._send(status, payload)


@contextmanager
def serve_gateway(scenario=None):
    server = Gateway(scenario or load_gateway_scenario())
    worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
    worker.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)
