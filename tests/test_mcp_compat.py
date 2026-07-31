from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pytest

import actionlens as al
from actionlens.sinks import MemorySink

MODERN_VERSION = "2026-07-28"


def _modern_meta(*, capabilities: Mapping[str, object] | None = None) -> dict[str, object]:
    return {
        "io.modelcontextprotocol/protocolVersion": MODERN_VERSION,
        "io.modelcontextprotocol/clientCapabilities": dict(capabilities or {}),
        "io.modelcontextprotocol/clientInfo": {"name": "test-client", "version": "1"},
    }


def _proxy(tmp_path: Path, transport=None, **kwargs):
    lens = al.ActionLens(storage_dir=tmp_path, sink=MemorySink())
    proxy = al.MCPGovernanceProxy(
        lens,
        transport or (lambda name, arguments: {"name": name, **arguments}),
        server_info={"name": "actionlens-test", "version": "1.5.2"},
        **kwargs,
    )
    return lens, proxy


def test_dual_era_initialize_and_tool_listing(tmp_path: Path) -> None:
    _, proxy = _proxy(tmp_path)
    proxy.register_tool(
        "weather",
        title="Weather",
        description="Read weather",
        icons=[{"src": "https://example.test/weather.png"}],
        input_schema={"type": "object"},
        output_schema={"type": "object"},
    )

    initialized = proxy.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": "2025-11-25", "capabilities": {}},
        }
    )
    assert initialized["result"]["protocolVersion"] == "2025-11-25"
    assert "resultType" not in initialized["result"]

    legacy_list = proxy.handle_request(
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
    )
    assert legacy_list["result"]["tools"] == [
        {
            "name": "weather",
            "description": "Read weather",
            "inputSchema": {"type": "object"},
        }
    ]

    modern_list = proxy.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/list",
            "params": {"_meta": _modern_meta()},
        }
    )
    result = modern_list["result"]
    assert result["resultType"] == "complete"
    assert result["ttlMs"] == 300_000
    assert result["cacheScope"] == "private"
    assert result["tools"][0]["title"] == "Weather"
    assert result["tools"][0]["icons"][0]["src"].endswith("weather.png")
    assert result["tools"][0]["outputSchema"] == {"type": "object"}


def test_discover_validates_modern_metadata_and_versions(tmp_path: Path) -> None:
    _, proxy = _proxy(tmp_path)
    discover = {
        "jsonrpc": "2.0",
        "id": "discover",
        "method": "server/discover",
        "params": {"_meta": _modern_meta()},
    }
    response = proxy.handle_request(discover)
    assert response["result"]["resultType"] == "complete"
    assert MODERN_VERSION in response["result"]["supportedVersions"]

    unsupported = {
        **discover,
        "params": {
            "_meta": {
                **_modern_meta(),
                "io.modelcontextprotocol/protocolVersion": "2099-01-01",
            }
        },
    }
    error = proxy.handle_request(unsupported)["error"]
    assert error["code"] == -32022
    assert error["data"]["requested"] == "2099-01-01"

    missing_capabilities = {
        **discover,
        "params": {
            "_meta": {"io.modelcontextprotocol/protocolVersion": MODERN_VERSION}
        },
    }
    assert proxy.handle_request(missing_capabilities)["error"]["code"] == -32602


def test_modern_tool_result_has_complete_and_structured_content(tmp_path: Path) -> None:
    _, proxy = _proxy(tmp_path)
    proxy.register_tool("lookup", input_schema={"type": "object"})
    response = proxy.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                "_meta": _modern_meta(),
                "name": "lookup",
                "arguments": {"query": "x"},
            },
        }
    )
    result = response["result"]
    assert result["resultType"] == "complete"
    assert result["structuredContent"] == {"name": "lookup", "query": "x"}
    assert result["_meta"]["io.modelcontextprotocol/serverInfo"]["name"] == "actionlens-test"


def test_upstream_mcp_envelope_preserves_error_and_inner_structured_content(
    tmp_path: Path,
) -> None:
    _, proxy = _proxy(
        tmp_path,
        lambda _name, _arguments: {
            "content": [{"type": "text", "text": "provider rejected"}],
            "isError": True,
            "structuredContent": {"accepted": False},
        },
    )
    proxy.register_tool(
        "update",
        output_schema={
            "type": "object",
            "properties": {"accepted": {"type": "boolean"}},
            "required": ["accepted"],
        },
    )
    response = proxy.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 10,
            "method": "tools/call",
            "params": {
                "_meta": _modern_meta(),
                "name": "update",
                "arguments": {},
            },
        }
    )
    result = response["result"]
    assert result["isError"] is True
    assert result["structuredContent"] == {"accepted": False}


def test_schema_dialect_output_validation_and_external_ref_guard(tmp_path: Path) -> None:
    _, proxy = _proxy(tmp_path, lambda _name, _arguments: {"count": "invalid"})
    proxy.register_tool(
        "count",
        input_schema={
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
        },
        output_schema={
            "type": "object",
            "properties": {"count": {"type": "integer"}},
            "required": ["count"],
        },
    )
    response = proxy.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 8,
            "method": "tools/call",
            "params": {
                "_meta": _modern_meta(),
                "name": "count",
                "arguments": {},
            },
        }
    )
    assert response["result"]["isError"] is True
    assert "structuredContent" not in response["result"]

    with pytest.raises(ValueError, match="external \\$ref"):
        proxy.register_tool(
            "unsafe-schema",
            input_schema={"$ref": "https://schemas.example.test/tool.json"},
        )


def test_mrtr_retry_handles_params_and_resumes_approval(tmp_path: Path) -> None:
    calls: list[dict[str, object]] = []

    def transport(_name: str, arguments: dict[str, object]) -> dict[str, object]:
        calls.append(arguments)
        return {"accepted": True}

    lens: al.ActionLens

    def input_required(output, _request):
        ticket_id = output.governance["ticket_id"]
        return {
            "inputRequests": {
                "approval": {
                    "method": "elicitation/create",
                    "params": {
                        "mode": "form",
                        "message": "Approve this update?",
                        "requestedSchema": {"type": "object"},
                    },
                }
            },
            "requestState": ticket_id,
        }

    def handle_input(responses, request):
        assert responses["approval"]["action"] == "accept"
        ticket_id = request["params"]["requestState"]
        assert lens.approve(ticket_id=ticket_id, approved_by="mcp-user")

    lens, proxy = _proxy(
        tmp_path,
        transport,
        input_required_factory=input_required,
        input_response_handler=handle_input,
    )
    proxy.register_tool(
        "update",
        risk=al.RiskLevel.MUTATION,
        idempotency=al.IdempotencyPolicy.REQUIRED,
        approval_required=True,
        input_schema={
            "type": "object",
            "properties": {"value": {"type": "string"}},
            "required": ["value"],
            "additionalProperties": False,
        },
    )
    params = {
        "_meta": _modern_meta(capabilities={"elicitation": {}}),
        "name": "update",
        "arguments": {"value": "new", "idempotency_key": "mcp-update-1"},
    }
    first = proxy.handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": params}
    )
    assert first["result"]["resultType"] == "input_required"
    assert calls == []

    retry_params = {
        **params,
        "inputResponses": {"approval": {"action": "accept", "content": {}}},
        "requestState": first["result"]["requestState"],
    }
    second = proxy.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": retry_params,
        }
    )
    assert second["result"]["resultType"] == "complete"
    assert second["result"]["structuredContent"] == {"accepted": True}
    assert calls == [{"value": "new"}]


def test_mrtr_rejects_unadvertised_client_capability(tmp_path: Path) -> None:
    _, proxy = _proxy(
        tmp_path,
        input_required_factory=lambda output, request: {
            "inputRequests": {
                "approval": {"method": "elicitation/create", "params": {}}
            }
        },
    )
    proxy.register_tool("approval", approval_required=True)
    response = proxy.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "_meta": _modern_meta(),
                "name": "approval",
                "arguments": {},
            },
        }
    )
    assert response["error"]["code"] == -32602
    assert "did not declare" in response["error"]["message"]


def test_mrtr_retry_fails_closed_without_response_handler(tmp_path: Path) -> None:
    _, proxy = _proxy(tmp_path)
    proxy.register_tool("lookup")
    response = proxy.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "_meta": _modern_meta(),
                "name": "lookup",
                "arguments": {},
                "inputResponses": {},
                "requestState": "unverified-state",
            },
        }
    )
    assert response["error"]["code"] == -32602
    assert "not configured" in response["error"]["message"]


def test_forwarded_legacy_result_is_normalized_for_modern_client(tmp_path: Path) -> None:
    _, proxy = _proxy(
        tmp_path,
        forward_request=lambda request: {
            "jsonrpc": "2.0",
            "id": request["id"],
            "result": {"prompts": []},
        },
    )
    response = proxy.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 9,
            "method": "prompts/list",
            "params": {"_meta": _modern_meta()},
        }
    )
    assert response["result"]["resultType"] == "complete"
    assert response["result"]["ttlMs"] == 0
    assert response["result"]["cacheScope"] == "private"
