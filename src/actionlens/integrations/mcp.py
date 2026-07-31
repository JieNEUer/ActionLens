from __future__ import annotations

import inspect
import json
import warnings
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from actionlens.context import get_current_context, make_generated_context
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

# ---------------------------------------------------------------------------
# MCP protocol era constants (2026-07-28 specification, SEP-2575/2322/2549).
#
# The 2026-07-28 revision made the protocol stateless: the initialize
# handshake and Mcp-Session-Id header were removed, every request carries its
# protocol version in ``_meta``, results require a ``resultType`` field, and
# ``server/discover`` became mandatory.  This proxy is "dual-era": it answers
# legacy clients (initialize-era revisions) with the classic result shape and
# modern clients with resultType/structuredContent/_meta.serverInfo.
# ---------------------------------------------------------------------------
LEGACY_PROTOCOL_VERSIONS = (
    "2024-11-05",
    "2025-03-26",
    "2025-06-18",
    "2025-11-25",
)
MODERN_PROTOCOL_VERSIONS = ("2026-07-28",)
DEFAULT_SUPPORTED_PROTOCOL_VERSIONS = LEGACY_PROTOCOL_VERSIONS + MODERN_PROTOCOL_VERSIONS
MODERN_FROM = MODERN_PROTOCOL_VERSIONS[0]

_META_PROTOCOL_VERSION = "io.modelcontextprotocol/protocolVersion"
_META_CLIENT_INFO = "io.modelcontextprotocol/clientInfo"
_META_CLIENT_CAPABILITIES = "io.modelcontextprotocol/clientCapabilities"
_META_SERVER_INFO = "io.modelcontextprotocol/serverInfo"

_UNSUPPORTED_PROTOCOL_VERSION_ERROR = -32022

# Keys on tools/call params reserved by the modern MRTR pattern. They are
# protocol fields beside ``arguments`` and are surfaced through context
# metadata without entering tool input validation.
_MRTR_PARAM_KEYS = ("inputResponses", "requestState")

_MAX_TOOL_NAME_LENGTH = 128


class MCPToolDefinition(BaseModel):
    """The portable subset of an MCP tools/list item used by the proxy.

    Covers both protocol eras: the classic ``name``/``description``/
    ``inputSchema`` triple plus the modern ``title``, ``icons``, and
    ``outputSchema`` fields introduced by the 2026-07-28 revision.
    """

    name: str = Field(min_length=1)
    title: str | None = None
    description: str | None = None
    icons: list[dict[str, Any]] = Field(default_factory=list)
    input_schema: dict[str, Any] = Field(default_factory=dict)
    output_schema: dict[str, Any] | None = None

    @classmethod
    def from_mcp(cls, value: Mapping[str, Any]) -> MCPToolDefinition:
        payload = dict(value)
        if "inputSchema" in payload and "input_schema" not in payload:
            payload["input_schema"] = payload.pop("inputSchema")
        if "outputSchema" in payload and "output_schema" not in payload:
            payload["output_schema"] = payload.pop("outputSchema")
        return cls.model_validate(payload)


class UnsupportedProtocolVersionError(Exception):
    """Raised when a request declares a protocol version we do not support."""

    def __init__(self, requested: str, supported: tuple[str, ...]) -> None:
        super().__init__(f"unsupported MCP protocol version {requested!r}")
        self.requested = requested
        self.supported = tuple(supported)


class MCPGovernanceProxy:
    """Govern ``tools/call`` through ActionLens without owning MCP transport.

    The host supplies either a ``call_tool(name, arguments)`` callable or an
    object exposing that method. Every registered remote tool becomes a normal
    ActionLens tool, so idempotency, approval, leases, output policy, and
    trajectory capture apply before a request reaches the MCP server.
    """

    def __init__(
        self,
        lens: ActionLens,
        transport: Callable[[str, dict[str, Any]], Any] | Any,
        *,
        forward_request: Callable[[dict[str, Any]], Any] | None = None,
        asynchronous: bool | None = None,
        supported_protocol_versions: Iterable[str] | None = None,
        server_info: Mapping[str, Any] | None = None,
        capabilities: Mapping[str, Any] | None = None,
        instructions: str | None = None,
        cache_ttl_ms: int = 300_000,
        cache_scope: str = "private",
        input_required_factory: Callable[
            [StructuredToolOutput, Mapping[str, Any]], Mapping[str, Any] | None
        ]
        | None = None,
        input_response_handler: Callable[[Mapping[str, Any], Mapping[str, Any]], Any]
        | None = None,
    ) -> None:
        """Govern MCP ``tools/call`` requests through ActionLens.

        ``supported_protocol_versions`` selects which protocol revisions the
        proxy advertises (``server/discover``) and accepts.  Requests that
        declare an unsupported version are rejected with
        ``UnsupportedProtocolVersionError`` (JSON-RPC ``-32022``).  Requests
        without a declared version are treated as legacy (initialize-era)
        clients and answered with the classic result shape.

        ``input_required_factory`` lets the host emit a modern MRTR
        ``resultType: "input_required"`` result (e.g. to surface an ActionLens
        approval prompt as an elicitation) instead of a plain result.  It is
        called with the governed ``StructuredToolOutput`` and the raw request;
        return a mapping with ``inputRequests``/``requestState`` or ``None``
        to fall back to a regular result.

        ``input_response_handler`` receives the client's ``inputResponses``
        and raw retry request before governance runs again. A host can use it
        to verify the opaque ``requestState`` and persist an ActionLens
        approval decision. Integrity protection and replay policy for
        ``requestState`` remain host responsibilities.
        """
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
        if cache_ttl_ms < 0:
            raise ValueError("cache_ttl_ms must be non-negative")
        if cache_scope not in {"public", "private"}:
            raise ValueError("cache_scope must be 'public' or 'private'")
        self._tools: dict[str, Callable[..., Any]] = {}
        self._definitions: dict[str, MCPToolDefinition] = {}
        self._supported_versions = tuple(
            supported_protocol_versions or DEFAULT_SUPPORTED_PROTOCOL_VERSIONS
        )
        if not self._supported_versions or any(
            not isinstance(version, str) or not version for version in self._supported_versions
        ):
            raise ValueError("supported_protocol_versions must contain non-empty strings")
        self._server_info = dict(server_info or {"name": "actionlens", "version": "unknown"})
        self._capabilities = dict(capabilities or {"tools": {}})
        self._instructions = instructions
        self._cache_ttl_ms = cache_ttl_ms
        self._cache_scope = cache_scope
        self._input_required_factory = input_required_factory
        self._input_response_handler = input_response_handler

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
        title: str | None = None,
        icons: Iterable[Mapping[str, Any]] | None = None,
        output_schema: Mapping[str, Any] | None = None,
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
        local_name = name or remote_name
        if len(remote_name) > _MAX_TOOL_NAME_LENGTH:
            warnings.warn(
                f"MCP tool name {remote_name!r} exceeds {_MAX_TOOL_NAME_LENGTH} "
                "characters; modern (2026-07-28+) clients may reject it",
                UserWarning,
                stacklevel=2,
            )
        schema = dict(input_schema or {})
        icon_values = [dict(icon) for icon in icons] if icons else []
        validator = _compile_schema_validator(schema) if schema else None
        output_validator = (
            _compile_schema_validator(dict(output_schema)) if output_schema else None
        )

        def validate_output(response: Any) -> Any:
            if output_validator is None:
                return response
            candidate = (
                response.get("structuredContent")
                if isinstance(response, Mapping) and "structuredContent" in response
                else response
            )
            try:
                output_validator(candidate)
            except ValueError as exc:
                message = str(exc).replace(
                    "MCP argument schema validation",
                    "MCP output schema validation",
                    1,
                )
                raise ValueError(message) from None
            return response

        if self._asynchronous:

            async def remote_tool(**arguments: Any) -> Any:
                response = self._call_tool(remote_name, dict(arguments))
                if not inspect.isawaitable(response):
                    raise TypeError("MCP transport was configured as async but returned a plain value")
                return validate_output(await response)

        else:

            def remote_tool(**arguments: Any) -> Any:
                response = self._call_tool(remote_name, dict(arguments))
                if inspect.isawaitable(response):
                    raise TypeError("MCP transport returned an awaitable; configure asynchronous=True")
                return validate_output(response)

        remote_tool.__name__ = local_name
        remote_tool.__doc__ = description or f"Governed MCP tool proxy for {remote_name}."
        # MCP arguments are dynamic keyword fields rather than Python function
        # parameters. Tell ToolRuntime to expose, validate, and approve them as
        # their JSON-Schema names instead of the internal ``arguments`` mapping.
        remote_tool.actionlens_dynamic_arguments = True  # type: ignore[attr-defined]
        remote_tool.actionlens_schema_fingerprint = schema  # type: ignore[attr-defined]
        # Modern (2026-07-28+) tools/list metadata, preserved for hosts that
        # render tools/list responses with title/icons/outputSchema.
        remote_tool.actionlens_title = title  # type: ignore[attr-defined]
        remote_tool.actionlens_icons = icon_values  # type: ignore[attr-defined]
        remote_tool.actionlens_output_schema = (  # type: ignore[attr-defined]
            dict(output_schema) if output_schema else None
        )
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
        self._definitions[remote_name] = MCPToolDefinition(
            name=remote_name,
            title=title,
            description=description,
            icons=icon_values,
            input_schema=schema,
            output_schema=dict(output_schema) if output_schema else None,
        )
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
                title=definition.title,
                icons=definition.icons,
                output_schema=definition.output_schema,
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

    # -- protocol era helpers ------------------------------------------------

    @staticmethod
    def _request_meta(request: Mapping[str, Any]) -> dict[str, Any]:
        params = request.get("params")
        if isinstance(params, Mapping):
            meta = params.get("_meta")
            if isinstance(meta, Mapping):
                return dict(meta)
        return {}

    def _check_protocol_version(self, request: Mapping[str, Any]) -> None:
        """Reject requests declaring a protocol version we do not support.

        Requests without a declared version are legacy clients (initialize-era
        revisions never carried per-request metadata) and are served as-is.
        """
        meta = self._request_meta(request)
        version = meta.get(_META_PROTOCOL_VERSION)
        if version is None:
            return
        if not isinstance(version, str):
            raise ValueError("MCP protocol version must be a string")
        if version not in self._supported_versions:
            raise UnsupportedProtocolVersionError(version, self._supported_versions)
        if version >= MODERN_FROM:
            capabilities = meta.get(_META_CLIENT_CAPABILITIES)
            if not isinstance(capabilities, Mapping):
                raise ValueError(
                    f"modern MCP requests require {_META_CLIENT_CAPABILITIES!r} in params._meta"
                )
            client_info = meta.get(_META_CLIENT_INFO)
            if client_info is not None and not isinstance(client_info, Mapping):
                raise ValueError(f"{_META_CLIENT_INFO!r} must be an object when provided")

    def _request_is_modern(self, request: Mapping[str, Any]) -> bool:
        version = self._request_meta(request).get(_META_PROTOCOL_VERSION)
        return isinstance(version, str) and version >= MODERN_FROM

    def handle_discover(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Answer ``server/discover`` (mandatory for 2026-07-28+ servers).

        Discovery is answered directly by the proxy even when no
        ``forward_request`` is configured, because it describes the proxy's
        own protocol surface rather than the upstream server's.
        """
        return {
            "jsonrpc": "2.0",
            "id": request.get("id"),
            "result": {
                "resultType": "complete",
                "supportedVersions": list(self._supported_versions),
                "capabilities": dict(self._capabilities),
                **(
                    {"instructions": self._instructions}
                    if self._instructions is not None
                    else {}
                ),
                "ttlMs": 3600000,
                "cacheScope": "public",
                "_meta": {_META_SERVER_INFO: dict(self._server_info)},
            },
        }

    def handle_initialize(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Answer a legacy initialize handshake for dual-era clients."""
        if self._request_is_modern(request):
            return _jsonrpc_error(request, -32601, "initialize is not used by modern MCP")
        params = request.get("params")
        if not isinstance(params, Mapping):
            return _jsonrpc_error(request, -32602, "initialize params must be an object")
        requested = params.get("protocolVersion")
        legacy_supported = tuple(
            version for version in self._supported_versions if version < MODERN_FROM
        )
        if requested in legacy_supported:
            selected = requested
        elif legacy_supported:
            # Legacy clients negotiate by accepting the server's selected
            # version; choosing the newest supported legacy revision is the
            # interoperable fallback.
            selected = legacy_supported[-1]
        else:
            return _jsonrpc_error(
                request,
                _UNSUPPORTED_PROTOCOL_VERSION_ERROR,
                "legacy initialize is not supported",
                data={"supported": list(self._supported_versions), "requested": requested},
            )
        result: dict[str, Any] = {
            "protocolVersion": selected,
            "capabilities": dict(self._capabilities),
            "serverInfo": dict(self._server_info),
        }
        if self._instructions is not None:
            result["instructions"] = self._instructions
        return {"jsonrpc": "2.0", "id": request.get("id"), "result": result}

    def handle_list_tools(self, request: Mapping[str, Any]) -> dict[str, Any]:
        """Return the governed tool registry in either protocol era shape."""
        modern = self._request_is_modern(request)
        tools = []
        for name in sorted(self._definitions):
            definition = self._definitions[name]
            item: dict[str, Any] = {
                "name": definition.name,
                "inputSchema": dict(definition.input_schema),
            }
            if modern and definition.title is not None:
                item["title"] = definition.title
            if definition.description is not None:
                item["description"] = definition.description
            if modern and definition.icons:
                item["icons"] = [dict(icon) for icon in definition.icons]
            if modern and definition.output_schema is not None:
                item["outputSchema"] = dict(definition.output_schema)
            tools.append(item)
        result: dict[str, Any] = {"tools": tools}
        if modern:
            result.update(
                {
                    "resultType": "complete",
                    "ttlMs": self._cache_ttl_ms,
                    "cacheScope": self._cache_scope,
                    "_meta": {_META_SERVER_INFO: dict(self._server_info)},
                }
            )
        return {"jsonrpc": "2.0", "id": request.get("id"), "result": result}

    def handle_request(
        self,
        request: Mapping[str, Any],
        *,
        context: ToolCallContext | None = None,
    ) -> dict[str, Any]:
        """Handle a synchronous MCP JSON-RPC request or forward non-tool calls."""

        try:
            self._check_protocol_version(request)
        except UnsupportedProtocolVersionError as exc:
            return _jsonrpc_error(
                request,
                _UNSUPPORTED_PROTOCOL_VERSION_ERROR,
                str(exc),
                data={"supported": list(exc.supported), "requested": exc.requested},
            )
        except ValueError as exc:
            return _jsonrpc_error(request, -32602, str(exc))
        method = request.get("method")
        if method == "server/discover":
            if not self._request_is_modern(request):
                return self._forward_sync(request)
            return self.handle_discover(request)
        if method == "initialize":
            return self.handle_initialize(request)
        if method == "tools/list":
            return self.handle_list_tools(request)
        if method != "tools/call":
            return self._forward_sync(request)
        params = request.get("params")
        if not isinstance(params, Mapping):
            return _jsonrpc_error(request, -32602, "tools/call params must be an object")
        name = params.get("name")
        arguments = params.get("arguments", {})
        if not isinstance(name, str) or not isinstance(arguments, Mapping):
            return _jsonrpc_error(request, -32602, "tools/call requires name and object arguments")
        arguments = dict(arguments)
        try:
            self._handle_input_responses(request)
            context = self._prepare_call_context(request, context=context)
            output = self.call(name, arguments, context=context)
        except (KeyError, TypeError, ValueError) as exc:
            return _jsonrpc_error(request, -32602, str(exc))
        return self._respond(request, output)

    async def ahandle_request(
        self,
        request: Mapping[str, Any],
        *,
        context: ToolCallContext | None = None,
    ) -> dict[str, Any]:
        try:
            self._check_protocol_version(request)
        except UnsupportedProtocolVersionError as exc:
            return _jsonrpc_error(
                request,
                _UNSUPPORTED_PROTOCOL_VERSION_ERROR,
                str(exc),
                data={"supported": list(exc.supported), "requested": exc.requested},
            )
        except ValueError as exc:
            return _jsonrpc_error(request, -32602, str(exc))
        method = request.get("method")
        if method == "server/discover":
            if not self._request_is_modern(request):
                return await self._forward_async(request)
            return self.handle_discover(request)
        if method == "initialize":
            return self.handle_initialize(request)
        if method == "tools/list":
            return self.handle_list_tools(request)
        if method != "tools/call":
            return await self._forward_async(request)
        params = request.get("params")
        if not isinstance(params, Mapping):
            return _jsonrpc_error(request, -32602, "tools/call params must be an object")
        name = params.get("name")
        arguments = params.get("arguments", {})
        if not isinstance(name, str) or not isinstance(arguments, Mapping):
            return _jsonrpc_error(request, -32602, "tools/call requires name and object arguments")
        arguments = dict(arguments)
        try:
            await self._ahandle_input_responses(request)
            context = self._prepare_call_context(request, context=context)
            output = await self.acall(name, arguments, context=context)
        except (KeyError, TypeError, ValueError) as exc:
            return _jsonrpc_error(request, -32602, str(exc))
        return self._respond(request, output)

    def _prepare_call_context(
        self,
        request: Mapping[str, Any],
        *,
        context: ToolCallContext | None,
    ) -> ToolCallContext | None:
        """Expose modern MRTR params to the governed call context."""
        if not self._request_is_modern(request):
            return context
        params = request.get("params")
        if not isinstance(params, Mapping):
            return context
        captured: dict[str, Any] = {}
        for key in _MRTR_PARAM_KEYS:
            if key in params:
                captured[key] = params[key]
        if not captured:
            return context
        base = context or get_current_context()
        if base is None:
            base = make_generated_context(self.lens.project, str(request.get("method", "mcp")))
        metadata = dict(base.metadata)
        metadata["mcp_input_responses"] = captured.get("inputResponses")
        metadata["mcp_request_state"] = captured.get("requestState")
        # A contextvar session is a parent/session identity, not a call id.
        # Leave the id blank when we create an explicit bridge context so the
        # runtime allocates a unique id for this retry.
        call_id = base.call_id if context is not None else ""
        return base.model_copy(update={"metadata": metadata, "call_id": call_id})

    def _handle_input_responses(self, request: Mapping[str, Any]) -> None:
        params = request.get("params")
        if not self._request_is_modern(request) or not isinstance(params, Mapping):
            return
        responses = params.get("inputResponses")
        state = params.get("requestState")
        if responses is not None and not isinstance(responses, Mapping):
            raise ValueError("tools/call inputResponses must be an object")
        if state is not None and not isinstance(state, str):
            raise ValueError("tools/call requestState must be an opaque string")
        if responses is not None or state is not None:
            if self._input_response_handler is None:
                raise ValueError(
                    "MRTR retry received but input_response_handler is not configured"
                )
            result = self._input_response_handler(dict(responses or {}), request)
            if inspect.isawaitable(result):
                raise TypeError("input_response_handler must be synchronous")

    async def _ahandle_input_responses(self, request: Mapping[str, Any]) -> None:
        params = request.get("params")
        if not self._request_is_modern(request) or not isinstance(params, Mapping):
            return
        responses = params.get("inputResponses")
        state = params.get("requestState")
        if responses is not None and not isinstance(responses, Mapping):
            raise ValueError("tools/call inputResponses must be an object")
        if state is not None and not isinstance(state, str):
            raise ValueError("tools/call requestState must be an opaque string")
        if responses is not None or state is not None:
            if self._input_response_handler is None:
                raise ValueError(
                    "MRTR retry received but input_response_handler is not configured"
                )
            result = self._input_response_handler(dict(responses or {}), request)
            if inspect.isawaitable(result):
                await result

    def _respond(
        self, request: Mapping[str, Any], output: StructuredToolOutput
    ) -> dict[str, Any]:
        """Serialize a governed result in the request's protocol era shape."""
        if self._request_is_modern(request):
            if (
                output.status == "PENDING_APPROVAL"
                and self._input_required_factory is not None
            ):
                payload = self._input_required_factory(output, request)
                if payload is not None:
                    try:
                        self._validate_input_required_payload(request, payload)
                    except ValueError as exc:
                        return _jsonrpc_error(request, -32602, str(exc))
                    return _jsonrpc_input_required(
                        request,
                        payload,
                        server_info=self._server_info,
                    )
            return _jsonrpc_result(
                request, output, modern=True, server_info=self._server_info
            )
        return _jsonrpc_result(request, output)

    def _validate_input_required_payload(
        self,
        request: Mapping[str, Any],
        payload: Mapping[str, Any],
    ) -> None:
        input_requests = payload.get("inputRequests")
        request_state = payload.get("requestState")
        if input_requests is None and request_state is None:
            raise ValueError(
                "input_required results require inputRequests or requestState"
            )
        if request_state is not None and not isinstance(request_state, str):
            raise ValueError("input_required requestState must be an opaque string")
        if input_requests is None:
            return
        if not isinstance(input_requests, Mapping):
            raise ValueError("input_required inputRequests must be an object")
        capabilities = self._request_meta(request).get(_META_CLIENT_CAPABILITIES, {})
        required_capabilities = {
            "elicitation/create": "elicitation",
            "roots/list": "roots",
            "sampling/createMessage": "sampling",
        }
        for key, value in input_requests.items():
            if not isinstance(key, str) or not isinstance(value, Mapping):
                raise ValueError("inputRequests keys must map to request objects")
            method = value.get("method")
            capability = required_capabilities.get(method)
            if capability is None:
                raise ValueError(f"unsupported MRTR input request method {method!r}")
            if capability not in capabilities:
                raise ValueError(
                    f"client did not declare the {capability!r} capability required by {method}"
                )

    def _forward_sync(self, request: Mapping[str, Any]) -> dict[str, Any]:
        if self._forward_request is None:
            return _jsonrpc_error(request, -32601, f"method not found: {request.get('method')}")
        response = self._forward_request(dict(request))
        if inspect.isawaitable(response):
            raise TypeError("async forward_request requires await proxy.ahandle_request(...)")
        if not isinstance(response, dict):
            raise TypeError("forward_request must return a JSON-RPC object")
        return self._adapt_forwarded_response(request, response)

    async def _forward_async(self, request: Mapping[str, Any]) -> dict[str, Any]:
        if self._forward_request is None:
            return _jsonrpc_error(request, -32601, f"method not found: {request.get('method')}")
        response = self._forward_request(dict(request))
        if inspect.isawaitable(response):
            response = await response
        if not isinstance(response, dict):
            raise TypeError("forward_request must return a JSON-RPC object")
        return self._adapt_forwarded_response(request, response)

    def _adapt_forwarded_response(
        self,
        request: Mapping[str, Any],
        response: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Normalize an upstream result for the requesting protocol era."""
        adapted = dict(response)
        raw_result = adapted.get("result")
        if not isinstance(raw_result, Mapping):
            return adapted
        result = dict(raw_result)
        if self._request_is_modern(request):
            # Earlier servers omit resultType; modern clients MUST interpret
            # that omission as complete, so materialize it at the proxy edge.
            result.setdefault("resultType", "complete")
            if request.get("method") in {
                "tools/list",
                "prompts/list",
                "resources/list",
                "resources/read",
                "resources/templates/list",
            }:
                result.setdefault("ttlMs", 0)
                result.setdefault("cacheScope", "private")
            meta = result.get("_meta")
            result_meta = dict(meta) if isinstance(meta, Mapping) else {}
            result_meta.setdefault(_META_SERVER_INFO, dict(self._server_info))
            result["_meta"] = result_meta
        else:
            result.pop("resultType", None)
            result.pop("ttlMs", None)
            result.pop("cacheScope", None)
        adapted["result"] = result
        return adapted


def _jsonrpc_result(
    request: Mapping[str, Any],
    output: StructuredToolOutput,
    *,
    modern: bool = False,
    server_info: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    has_structured_content, structured_content = _structured_content(output.result)
    result: dict[str, Any] = {
        "content": [{"type": "text", "text": output.model_dump_json(exclude_none=True)}],
        "isError": output.status in {"FAILED", "DENIED", "TIMEOUT", "UNCERTAIN"}
        or _upstream_is_error(output.result),
    }
    if modern:
        # Modern (2026-07-28+) results carry resultType and expose the
        # governed result as structuredContent when it is JSON-serializable.
        result["resultType"] = "complete"
        if has_structured_content:
            try:
                json.dumps(structured_content, allow_nan=False)
            except (TypeError, ValueError):
                pass
            else:
                result["structuredContent"] = structured_content
        if server_info:
            result["_meta"] = {_META_SERVER_INFO: dict(server_info)}
    return {"jsonrpc": "2.0", "id": request.get("id"), "result": result}


def _structured_content(value: Any) -> tuple[bool, Any]:
    """Extract an upstream MCP envelope's structured result when present."""
    if isinstance(value, Mapping) and "structuredContent" in value:
        return True, value["structuredContent"]
    return value is not None, value


def _upstream_is_error(value: Any) -> bool:
    """Preserve an upstream MCP tool error without treating business data as one."""
    return isinstance(value, Mapping) and value.get("isError") is True


def _jsonrpc_input_required(
    request: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    server_info: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Serialize a modern MRTR ``InputRequiredResult`` (resultType required)."""
    result = dict(payload)
    result["resultType"] = "input_required"
    if server_info:
        meta = result.get("_meta")
        result_meta = dict(meta) if isinstance(meta, Mapping) else {}
        result_meta.setdefault(_META_SERVER_INFO, dict(server_info))
        result["_meta"] = result_meta
    return {"jsonrpc": "2.0", "id": request.get("id"), "result": result}


def _jsonrpc_error(
    request: Mapping[str, Any],
    code: int,
    message: str,
    *,
    data: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = dict(data)
    return {
        "jsonrpc": "2.0",
        "id": request.get("id"),
        "error": error,
    }


def _compile_schema_validator(schema: Mapping[str, Any]) -> Callable[[Any], None]:
    normalized = dict(schema)
    _validate_schema_resource_bounds(normalized)
    try:
        from jsonschema.validators import validator_for
    except ImportError:
        _assert_fallback_schema_supported(normalized)

        def validate(arguments: Any) -> None:
            _validate_schema(arguments, normalized, path="arguments")

        return validate

    validator_class = validator_for(normalized)
    validator_class.check_schema(normalized)
    validator = validator_class(normalized)

    def validate(arguments: Any) -> None:
        error = next(iter(validator.iter_errors(arguments)), None)
        if error is not None:
            location = ".".join(str(part) for part in error.absolute_path)
            suffix = f" at {location}" if location else ""
            raise ValueError(f"MCP argument schema validation failed{suffix}: {error.message}")

    return validate


def _validate_schema_resource_bounds(
    schema: Mapping[str, Any],
    *,
    max_depth: int = 64,
    max_nodes: int = 2_048,
) -> None:
    """Reject network references and pathologically large schema trees."""
    stack: list[tuple[Any, int]] = [(schema, 0)]
    nodes = 0
    while stack:
        value, depth = stack.pop()
        nodes += 1
        if depth > max_depth or nodes > max_nodes:
            raise ValueError("MCP schema exceeds validation resource limits")
        if isinstance(value, Mapping):
            ref = value.get("$ref")
            if isinstance(ref, str) and not ref.startswith("#"):
                raise ValueError(
                    "MCP schema external $ref resolution is disabled by default"
                )
            stack.extend((child, depth + 1) for child in value.values())
        elif isinstance(value, list):
            stack.extend((child, depth + 1) for child in value)


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
