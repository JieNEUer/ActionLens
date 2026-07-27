from __future__ import annotations

import inspect
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from actionlens.models import (
    ConcurrencyPolicy,
    IdempotencyPolicy,
    OutputPolicy,
    RiskLevel,
    StructuredToolOutput,
    ToolCallContext,
)

if TYPE_CHECKING:
    from actionlens.runtime import ActionLens


class MCPToolDefinition(BaseModel):
    """The portable subset of an MCP tools/list item used by the proxy."""

    name: str = Field(min_length=1)
    description: str | None = None
    input_schema: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_mcp(cls, value: Mapping[str, Any]) -> "MCPToolDefinition":
        payload = dict(value)
        if "inputSchema" in payload and "input_schema" not in payload:
            payload["input_schema"] = payload.pop("inputSchema")
        return cls.model_validate(payload)


class MCPGovernanceProxy:
    """Govern ``tools/call`` through ActionLens without owning MCP transport.

    The host supplies either a ``call_tool(name, arguments)`` callable or an
    object exposing that method. Every registered remote tool becomes a normal
    ActionLens tool, so idempotency, approval, leases, output policy, and
    trajectory capture apply before a request reaches the MCP server.
    """

    def __init__(
        self,
        lens: "ActionLens",
        transport: Callable[[str, dict[str, Any]], Any] | Any,
        *,
        forward_request: Callable[[dict[str, Any]], Any] | None = None,
        asynchronous: bool | None = None,
    ) -> None:
        self.lens = lens
        candidate = (
            transport if callable(transport) else getattr(transport, "call_tool", None)
        )
        if not callable(candidate):
            raise TypeError("transport must be callable or expose call_tool(name, arguments)")
        self._call_tool: Callable[[str, dict[str, Any]], Any] = candidate
        self._forward_request = forward_request
        self._asynchronous = (
            inspect.iscoroutinefunction(candidate)
            if asynchronous is None
            else asynchronous
        )
        self._tools: dict[str, Callable[..., Any]] = {}

    @property
    def tools(self) -> dict[str, Callable[..., Any]]:
        return dict(self._tools)

    def register_tool(
        self,
        remote_name: str,
        *,
        description: str | None = None,
        input_schema: Mapping[str, Any] | None = None,
        name: str | None = None,
        risk: RiskLevel | str = RiskLevel.READ,
        idempotency: IdempotencyPolicy | str = IdempotencyPolicy.OFF,
        idempotency_key_param: str = "idempotency_key",
        hash_ignore_keys: list[str] | None = None,
        timeout_sec: float | None = None,
        run_sync_in_thread: bool = False,
        cache_ttl_sec: float = 300.0,
        max_bytes: int | None = None,
        output: OutputPolicy | None = None,
        approval_required: bool = False,
        approval_ttl_sec: float | None = 86400.0,
        concurrency: ConcurrencyPolicy | str = ConcurrencyPolicy.UNKNOWN,
        lease_seconds: float = 30.0,
        fencing_supported: bool = False,
    ) -> Callable[..., Any]:
        if remote_name in self._tools:
            raise ValueError(f"MCP tool {remote_name!r} is already registered")
        schema = dict(input_schema or {})
        validator = _compile_schema_validator(schema) if schema else None
        local_name = name or remote_name

        if self._asynchronous:

            async def remote_tool(**arguments: Any) -> Any:
                response = self._call_tool(remote_name, dict(arguments))
                if not inspect.isawaitable(response):
                    raise TypeError("MCP transport was configured as async but returned a plain value")
                return await response

        else:

            def remote_tool(**arguments: Any) -> Any:
                response = self._call_tool(remote_name, dict(arguments))
                if inspect.isawaitable(response):
                    raise TypeError("MCP transport returned an awaitable; configure asynchronous=True")
                return response

        remote_tool.__name__ = local_name
        remote_tool.__doc__ = description or f"Governed MCP tool proxy for {remote_name}."
        # MCP arguments are dynamic keyword fields rather than Python function
        # parameters. Tell ToolRuntime to expose, validate, and approve them as
        # their JSON-Schema names instead of the internal ``arguments`` mapping.
        remote_tool.actionlens_dynamic_arguments = True  # type: ignore[attr-defined]
        remote_tool.actionlens_schema_fingerprint = schema  # type: ignore[attr-defined]
        if validator is not None:
            remote_tool.actionlens_argument_validator = validator  # type: ignore[attr-defined]
            remote_tool.actionlens_modified_argument_validator = (  # type: ignore[attr-defined]
                _compile_modified_schema_validator(schema)
            )
        wrapped = self.lens.tool(
            name=local_name,
            description=description,
            risk=risk,
            idempotency=idempotency,
            idempotency_key_param=idempotency_key_param,
            hash_ignore_keys=hash_ignore_keys,
            timeout_sec=timeout_sec,
            run_sync_in_thread=run_sync_in_thread,
            cache_ttl_sec=cache_ttl_sec,
            max_bytes=max_bytes,
            output=output,
            approval_required=approval_required,
            approval_ttl_sec=approval_ttl_sec,
            concurrency=concurrency,
            lease_seconds=lease_seconds,
            fencing_supported=fencing_supported,
        )(remote_tool)
        self._tools[remote_name] = wrapped
        return wrapped

    def register_definitions(
        self,
        definitions: Iterable[MCPToolDefinition | Mapping[str, Any]],
        *,
        policies: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, Callable[..., Any]]:
        registered: dict[str, Callable[..., Any]] = {}
        for definition_value in definitions:
            definition = (
                definition_value
                if isinstance(definition_value, MCPToolDefinition)
                else MCPToolDefinition.from_mcp(definition_value)
            )
            registered[definition.name] = self.register_tool(
                definition.name,
                description=definition.description,
                input_schema=definition.input_schema,
                **dict((policies or {}).get(definition.name, {})),
            )
        return registered

    def call(
        self,
        name: str,
        arguments: Mapping[str, Any],
        *,
        context: ToolCallContext | None = None,
    ) -> StructuredToolOutput:
        tool = self._tools.get(name)
        if tool is None:
            raise KeyError(f"MCP tool {name!r} has not been registered")
        result = tool(**dict(arguments), __al_ctx=context)
        if inspect.isawaitable(result):
            raise TypeError("async MCP tool requires await proxy.acall(...)")
        return StructuredToolOutput.model_validate(result)

    async def acall(
        self,
        name: str,
        arguments: Mapping[str, Any],
        *,
        context: ToolCallContext | None = None,
    ) -> StructuredToolOutput:
        tool = self._tools.get(name)
        if tool is None:
            raise KeyError(f"MCP tool {name!r} has not been registered")
        result = tool(**dict(arguments), __al_ctx=context)
        if inspect.isawaitable(result):
            result = await result
        return StructuredToolOutput.model_validate(result)

    def handle_request(
        self,
        request: Mapping[str, Any],
        *,
        context: ToolCallContext | None = None,
    ) -> dict[str, Any]:
        """Handle a synchronous MCP JSON-RPC request or forward non-tool calls."""

        method = request.get("method")
        if method != "tools/call":
            return self._forward_sync(request)
        params = request.get("params")
        if not isinstance(params, Mapping):
            return _jsonrpc_error(request, -32602, "tools/call params must be an object")
        name = params.get("name")
        arguments = params.get("arguments", {})
        if not isinstance(name, str) or not isinstance(arguments, Mapping):
            return _jsonrpc_error(request, -32602, "tools/call requires name and object arguments")
        try:
            output = self.call(name, arguments, context=context)
        except (KeyError, TypeError, ValueError) as exc:
            return _jsonrpc_error(request, -32602, str(exc))
        return _jsonrpc_result(request, output)

    async def ahandle_request(
        self,
        request: Mapping[str, Any],
        *,
        context: ToolCallContext | None = None,
    ) -> dict[str, Any]:
        method = request.get("method")
        if method != "tools/call":
            return await self._forward_async(request)
        params = request.get("params")
        if not isinstance(params, Mapping):
            return _jsonrpc_error(request, -32602, "tools/call params must be an object")
        name = params.get("name")
        arguments = params.get("arguments", {})
        if not isinstance(name, str) or not isinstance(arguments, Mapping):
            return _jsonrpc_error(request, -32602, "tools/call requires name and object arguments")
        try:
            output = await self.acall(name, arguments, context=context)
        except (KeyError, TypeError, ValueError) as exc:
            return _jsonrpc_error(request, -32602, str(exc))
        return _jsonrpc_result(request, output)

    def _forward_sync(self, request: Mapping[str, Any]) -> dict[str, Any]:
        if self._forward_request is None:
            raise ValueError("only tools/call is handled without forward_request")
        response = self._forward_request(dict(request))
        if inspect.isawaitable(response):
            raise TypeError("async forward_request requires await proxy.ahandle_request(...)")
        if not isinstance(response, dict):
            raise TypeError("forward_request must return a JSON-RPC object")
        return response

    async def _forward_async(self, request: Mapping[str, Any]) -> dict[str, Any]:
        if self._forward_request is None:
            raise ValueError("only tools/call is handled without forward_request")
        response = self._forward_request(dict(request))
        if inspect.isawaitable(response):
            response = await response
        if not isinstance(response, dict):
            raise TypeError("forward_request must return a JSON-RPC object")
        return response


def _jsonrpc_result(request: Mapping[str, Any], output: StructuredToolOutput) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request.get("id"),
        "result": {
            "content": [{"type": "text", "text": output.model_dump_json(exclude_none=True)}],
            "isError": output.status in {"FAILED", "DENIED", "TIMEOUT", "UNCERTAIN"},
        },
    }


def _jsonrpc_error(request: Mapping[str, Any], code: int, message: str) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request.get("id"),
        "error": {"code": code, "message": message},
    }


def _compile_schema_validator(schema: Mapping[str, Any]) -> Callable[[dict[str, Any]], None]:
    normalized = dict(schema)
    try:
        from jsonschema import Draft202012Validator
    except ImportError:
        _assert_fallback_schema_supported(normalized)

        def validate(arguments: dict[str, Any]) -> None:
            _validate_schema(arguments, normalized, path="arguments")

        return validate

    validator = Draft202012Validator(normalized)

    def validate(arguments: dict[str, Any]) -> None:
        error = next(iter(validator.iter_errors(arguments)), None)
        if error is not None:
            location = ".".join(str(part) for part in error.absolute_path)
            suffix = f" at {location}" if location else ""
            raise ValueError(f"MCP argument schema validation failed{suffix}: {error.message}")

    return validate


def _compile_modified_schema_validator(
    schema: Mapping[str, Any],
) -> Callable[[dict[str, Any]], None]:
    """Validate a partial approval/policy patch against an MCP input schema."""

    patch_schema = dict(schema)
    # Required fields are validated after applying the patch to the original
    # invocation. At approval time only changed fields are present.
    patch_schema["required"] = []
    return _compile_schema_validator(patch_schema)


def _assert_fallback_schema_supported(schema: Mapping[str, Any]) -> None:
    unsupported = {"$ref", "allOf", "anyOf", "oneOf", "not", "patternProperties"}
    present = unsupported.intersection(schema)
    if present:
        raise ValueError(
            "MCP schema uses features requiring the optional jsonschema dependency: "
            + ", ".join(sorted(present))
        )
    for child in schema.get("properties", {}).values():
        if isinstance(child, Mapping):
            _assert_fallback_schema_supported(child)
    items = schema.get("items")
    if isinstance(items, Mapping):
        _assert_fallback_schema_supported(items)


def _validate_schema(value: Any, schema: Mapping[str, Any], *, path: str) -> None:
    expected = schema.get("type")
    if expected is not None and not _matches_type(value, expected):
        raise ValueError(f"MCP argument schema validation failed at {path}: expected {expected}")
    if "enum" in schema and value not in schema["enum"]:
        raise ValueError(f"MCP argument schema validation failed at {path}: value is not in enum")
    if "const" in schema and value != schema["const"]:
        raise ValueError(f"MCP argument schema validation failed at {path}: value does not match const")

    if isinstance(value, Mapping):
        properties = schema.get("properties", {})
        required = schema.get("required", [])
        for name in required:
            if name not in value:
                raise ValueError(f"MCP argument schema validation failed at {path}: missing {name!r}")
        if schema.get("additionalProperties") is False:
            unknown = set(value) - set(properties)
            if unknown:
                raise ValueError(
                    f"MCP argument schema validation failed at {path}: unknown keys "
                    + ", ".join(sorted(map(str, unknown)))
                )
        for name, item in value.items():
            child_schema = properties.get(name)
            if isinstance(child_schema, Mapping):
                _validate_schema(item, child_schema, path=f"{path}.{name}")
    elif isinstance(value, list) and isinstance(schema.get("items"), Mapping):
        for index, item in enumerate(value):
            _validate_schema(item, schema["items"], path=f"{path}[{index}]")

    if isinstance(value, str):
        minimum = schema.get("minLength")
        maximum = schema.get("maxLength")
        if minimum is not None and len(value) < int(minimum):
            raise ValueError(f"MCP argument schema validation failed at {path}: string is too short")
        if maximum is not None and len(value) > int(maximum):
            raise ValueError(f"MCP argument schema validation failed at {path}: string is too long")
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        minimum = schema.get("minimum")
        maximum = schema.get("maximum")
        if minimum is not None and value < minimum:
            raise ValueError(f"MCP argument schema validation failed at {path}: number is below minimum")
        if maximum is not None and value > maximum:
            raise ValueError(f"MCP argument schema validation failed at {path}: number is above maximum")


def _matches_type(value: Any, expected: Any) -> bool:
    expected_values = expected if isinstance(expected, list) else [expected]
    for item in expected_values:
        if item == "object" and isinstance(value, Mapping):
            return True
        if item == "array" and isinstance(value, list):
            return True
        if item == "string" and isinstance(value, str):
            return True
        if item == "boolean" and isinstance(value, bool):
            return True
        if item == "integer" and isinstance(value, int) and not isinstance(value, bool):
            return True
        if item == "number" and isinstance(value, (int, float)) and not isinstance(value, bool):
            return True
        if item == "null" and value is None:
            return True
    return False
