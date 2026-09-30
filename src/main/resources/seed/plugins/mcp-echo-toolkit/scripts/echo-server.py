#!/usr/bin/env python3
# echo-server.py — mcp-echo-toolkit 的最小 stdio MCP server（官方种子样例）。
#
# 协议形态 = newline-delimited JSON-RPC over stdin/stdout（与 PluginMcpManagerSpec
# 的 stdio fixture 同款，零第三方依赖——python3 标准库即可运行）。仅实现客户端
# 装载链会触碰的三个方法：initialize / tools/list / tools/call；暴露单件 echo 工具
# （把调用入参原样回显，无外部副作用）。
import sys
import json


def send(obj):
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        msg = json.loads(line)
    except Exception:
        continue
    if "id" not in msg or msg.get("id") is None:
        continue
    mid = msg["id"]
    method = msg.get("method", "")
    if method == "initialize":
        send({"id": mid, "result": {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "mcp-echo-toolkit-echo", "version": "1.0.0"},
        }})
    elif method == "tools/list":
        send({"id": mid, "result": {"tools": [{
            "name": "echo",
            "description": "echo back the arguments verbatim (sample tool, no side effects)",
            "inputSchema": {"type": "object", "properties": {"text": {"type": "string"}}},
        }]}})
    elif method == "tools/call":
        args = msg.get("params", {}).get("arguments", {})
        send({"id": mid, "result": {"content": [
            {"type": "text", "text": "echo:" + json.dumps(args, sort_keys=True)}
        ]}})
    else:
        send({"id": mid, "result": {}})
