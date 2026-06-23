"""MCP proxy — run external MCP servers inside bwrap instances.

Each configured proxied MCP is started inside a bwrap sandbox for a given
instance and connected via stdio.  Tools are discovered at server startup
(outside bwrap) so their schemas are known, then lazy-wrapped: when the user
calls a proxied tool with an ``instance_id``, the proxy starts the MCP server
inside that instance's sandbox and forwards the call.

Proxied MCP connections can be configured with an ``idle_timeout`` (seconds).
After no tool calls for the timeout duration, the connection is automatically
closed.  When set to 0 (the default), connections live forever.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

from fastmcp.client import Client
from fastmcp.client.transports import StdioTransport
from mcp.types import TextContent, Tool

from ns_hpc.config import Config, ProxiedMCP
from ns_hpc.namespace import build_bwrap_args

logger = logging.getLogger("ns-hpc")


async def discover_tools(cfg: ProxiedMCP, config: Config) -> list[Tool]:
    """Discover tools inside a bwrap sandbox that mirrors a real instance.

    The sandbox uses the same ``bind_ro`` and flags as a real instance,
    but ``/workspace`` and ``/output`` are empty tmpfs mounts —
    no host data is exposed to discovery.
    """
    command = [cfg.command] + (cfg.args or [])
    env = {**os.environ, **(cfg.env or {})} if cfg.env else None
    ns = config.namespace

    bargs = build_bwrap_args(
        command=command,
        workspace_host_path="",
        workspace_mount=ns.workspace_mount,
        config=config,
        extra_tmpfs=[ns.output_mount],
    )

    transport = StdioTransport(
        command=bargs[0],
        args=bargs[1:],
        env=env,
    )
    try:
        async with Client(transport) as client:
            return await client.list_tools()
    except Exception as e:
        logger.warning("failed to discover tools for %r: %s", cfg.command, e)
        return []


class ProxiedMCPClient:
    """One connection to a proxied MCP server running inside an instance.

    When configured with an ``idle_timeout`` > 0, an internal monitor task
    tracks the time since the last tool call completed.  If no calls occur
    within the timeout window, the connection is automatically closed and the
    client is removed from the ProxyManager.
    """

    def __init__(
        self,
        proxy_name: str,
        instance_id: str,
        cfg: ProxiedMCP,
        config: Config,
        on_idle_close: callable | None = None,
    ) -> None:
        self.proxy_name = proxy_name
        self.instance_id = instance_id
        self.cfg = cfg
        self.config = config
        self._client: Client | None = None
        self._last_used: float = 0.0       # monotonic time of last call completion
        self._active_calls: int = 0         # concurrent calls in flight
        self._idle_task: asyncio.Task | None = None
        self._on_idle_close = on_idle_close
        self._closed: bool = False

    @property
    def is_connected(self) -> bool:
        """True once ensure_connected() has succeeded at least once."""
        return self._client is not None and not self._closed

    async def ensure_connected(self) -> Client:
        """Start the process inside bwrap and connect if not already connected."""
        if self._closed:
            raise RuntimeError(
                f"ProxiedMCPClient {self.proxy_name!r}/{self.instance_id!r} "
                f"was closed due to idle timeout; obtain a new client"
            )
        if self._client is not None:
            return self._client

        # Validate proxied MCP command
        if not self.cfg.command or "\0" in self.cfg.command:
            raise ValueError(f"Invalid proxied MCP command {self.cfg.command!r}")

        # Build bwrap args directly (avoids --json-status-fd from the CLI path)
        from ns_hpc.instance import Instance

        inst = Instance.load(self.instance_id, self.config)
        if inst is None:
            raise ValueError(f"Instance '{self.instance_id}' not found")

        cmd = [self.cfg.command] + (self.cfg.args or [])
        ns = self.config.namespace
        shared_output_root = self.config.resolve_instances_dir() / "output"
        bargs = build_bwrap_args(
            command=cmd,
            workspace_host_path=str(inst.workspace_dir),
            config=self.config,
            extra_rw_binds=[(str(inst.output_path), ns.output_mount)],
            extra_ro_binds=[(str(shared_output_root), ns.shared_output_mount)],
        )

        transport = StdioTransport(
            command=bargs[0],
            args=bargs[1:],
            env={**os.environ, **(self.cfg.env or {})} if self.cfg.env else None,
            keep_alive=True,
        )
        client = Client(transport)
        await client.__aenter__()
        self._client = client
        self._last_used = time.monotonic()
        return client

    def _start_idle_monitor(self) -> None:
        """Start a background task that closes the connection after idle_timeout."""
        if self._idle_task is not None:
            return
        if self.cfg.idle_timeout <= 0:
            return
        self._idle_task = asyncio.ensure_future(self._idle_loop())

    async def _idle_loop(self) -> None:
        """Background coroutine: check idle state periodically and close if expired."""
        timeout = self.cfg.idle_timeout
        try:
            while not self._closed:
                await asyncio.sleep(timeout)
                # If active calls are in flight, don't close — wait for the next
                # check cycle.  The fact that a call is running counts as activity.
                now = time.monotonic()
                idle_duration = now - self._last_used
                if self._active_calls > 0:
                    # Calls are in flight; reset the idle timer implicitly
                    # (the call completion handler will refresh _last_used)
                    continue
                if idle_duration >= timeout:
                    logger.info(
                        "proxy %r/%r idle for %.1fs (timeout=%.0fs), closing",
                        self.proxy_name, self.instance_id,
                        idle_duration, timeout,
                    )
                    await self.close()
                    break
        except asyncio.CancelledError:
            pass

    async def call_tool(
        self, name: str, arguments: dict[str, Any]
    ) -> list[TextContent]:
        client = await self.ensure_connected()
        self._active_calls += 1
        try:
            result = await client.call_tool(name, arguments)
        finally:
            self._active_calls -= 1
            self._last_used = time.monotonic()
        return result

    async def list_tools(self) -> list[Tool]:
        client = await self.ensure_connected()
        return await client.list_tools()

    async def close(self) -> None:
        """Close the connection and cancel the idle monitor."""
        if self._closed:
            return
        self._closed = True
        # Avoid self-cancellation: when close() is called from _idle_loop
        # the _idle_task *is* the current task.  Cancelling ourselves would
        # prevent the subprocess cleanup and _on_idle_close from running.
        current = asyncio.current_task()
        if self._idle_task is not None:
            if self._idle_task is not current:
                self._idle_task.cancel()
            self._idle_task = None
        if self._client is not None:
            # Use Client.close() to properly terminate the subprocess
            # (__aexit__ alone leaves the subprocess alive with keep_alive=True).
            # Fall back to __aexit__ when _client is a non-Client mock.
            client = self._client
            self._client = None
            if hasattr(client, 'close'):
                await client.close()
            elif hasattr(client, '__aexit__'):
                await client.__aexit__(None, None, None)
        # Notify the manager so it can remove this client from its bookkeeping
        if self._on_idle_close is not None:
            self._on_idle_close(self.proxy_name, self.instance_id)


class ProxyManager:
    """Manages proxied MCP clients across instances.

    ``_clients[proxy_name][instance_id] = ProxiedMCPClient``
    """

    def __init__(self) -> None:
        self._clients: dict[str, dict[str, ProxiedMCPClient]] = {}

    def _remove_client(self, proxy_name: str, instance_id: str) -> None:
        """Remove a closed client from internal bookkeeping."""
        by_instance = self._clients.get(proxy_name)
        if by_instance is not None:
            by_instance.pop(instance_id, None)
            if not by_instance:
                self._clients.pop(proxy_name, None)

    def get_or_start(
        self,
        proxy_name: str,
        instance_id: str,
        cfg: ProxiedMCP,
        config: Config,
    ) -> ProxiedMCPClient:
        """Return an existing client for *proxy_name*/*instance_id* or create one.

        If a previously-closed (idle-timed-out) client exists for this
        proxy_name/instance_id pair, a new one is created.
        """
        by_instance = self._clients.setdefault(proxy_name, {})
        client = by_instance.get(instance_id)
        if client is None or client._closed:
            client = ProxiedMCPClient(
                proxy_name, instance_id, cfg, config,
                on_idle_close=self._remove_client,
            )
            by_instance[instance_id] = client
            client._start_idle_monitor()
        return client

    async def stop_all(self, instance_id: str) -> None:
        """Close all proxied MCPs running in the given instance."""
        for by_instance in self._clients.values():
            client = by_instance.pop(instance_id, None)
            if client is not None:
                await client.close()

    async def close_all(self) -> None:
        """Close every proxied MCP connection."""
        for by_instance in self._clients.values():
            for client in by_instance.values():
                await client.close()
        self._clients.clear()
