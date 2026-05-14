"""MCP proxy — run external MCP servers inside bwrap instances.

Each configured proxied MCP is started inside a bwrap sandbox for a given
instance and connected via stdio.  Tools are discovered at server startup
(outside bwrap) so their schemas are known, then lazy-wrapped: when the user
calls a proxied tool with an ``instance_id``, the proxy starts the MCP server
inside that instance's sandbox and forwards the call.
"""
from __future__ import annotations

import logging
import os
import re
import sys
from typing import Any

from fastmcp.client import Client
from fastmcp.client.transports import StdioTransport
from mcp.types import TextContent, Tool

from ns_hpc.config import Config, ProxiedMCP

logger = logging.getLogger("ns-hpc")


def _build_bwrap_cmd(instance_id: str, command: str, args: list[str] | None) -> list[str]:
    """Build argv for ``python -m ns_hpc bwrap <id> -- <command> <args>``."""
    return [
        sys.executable, "-m", "ns_hpc", "bwrap", instance_id, "--",
        command,
        *(args or []),
    ]


async def discover_tools(cfg: ProxiedMCP, config: Config | None = None) -> list[Tool]:
    """Start the MCP server and list its tools.

    When *config* is provided, the server runs inside a bwrap sandbox
    (disposable temp workspace, empty home).  Otherwise it runs on the host.
    """
    if config is not None:
        return await _discover_tools_bwrap(config, cfg)

    transport = StdioTransport(
        command=cfg.command,
        args=cfg.args or [],
        env={**os.environ, **(cfg.env or {})} if cfg.env else None,
    )
    try:
        async with Client(transport) as client:
            return await client.list_tools()
    except Exception as e:
        logger.warning("failed to discover tools from proxied MCP %r: %s", cfg.command, e)
        return []


async def _discover_tools_bwrap(config: Config, cfg: ProxiedMCP) -> list[Tool]:
    """Discover tools inside a sandbox that mirrors a real instance namespace.

    The sandbox uses the same ``bind_ro`` and flags as a real instance,
    but ``/workspace`` and ``/output`` are empty tmpfs mounts instead of
    real host directories — no host data is exposed to discovery.
    """
    command = [cfg.command] + (cfg.args or [])
    env = {**os.environ, **(cfg.env or {})} if cfg.env else None
    ns = config.namespace_defaults

    args = ["bwrap"]
    args.extend(ns.flags)

    for host_path in ns.bind_ro:
        args.extend(["--ro-bind", host_path, host_path])

    # Empty sandbox — tmpfs instead of real instance directories
    args.extend(["--tmpfs", ns.workspace_mount])
    args.extend(["--tmpfs", "/output"])
    args.extend(["--tmpfs", "/home"])

    args.extend(["--chdir", ns.workspace_mount])
    args.append("--")
    args.extend(command)

    transport = StdioTransport(
        command=args[0],
        args=args[1:],
        env=env,
    )
    try:
        async with Client(transport) as client:
            return await client.list_tools()
    except Exception as e:
        logger.warning("failed to discover tools in bwrap for %r: %s", cfg.command, e)
        return []


class ProxiedMCPClient:
    """One connection to a proxied MCP server running inside an instance."""

    def __init__(self, proxy_name: str, instance_id: str, cfg: ProxiedMCP) -> None:
        self.proxy_name = proxy_name
        self.instance_id = instance_id
        self.cfg = cfg
        self._client: Client | None = None

    async def ensure_connected(self) -> Client:
        """Start the process inside bwrap and connect if not already connected."""
        if self._client is not None:
            return self._client

        # Validate instance_id is a safe filesystem identifier
        if not re.match(r"^[a-zA-Z0-9_.-]+$", self.instance_id):
            raise ValueError(
                f"Invalid instance_id {self.instance_id!r}: must match [a-zA-Z0-9_.-]+"
            )
        if not self.cfg.command or "\0" in self.cfg.command:
            raise ValueError(f"Invalid proxied MCP command {self.cfg.command!r}")

        cmd = _build_bwrap_cmd(self.instance_id, self.cfg.command, self.cfg.args)
        transport = StdioTransport(
            command=cmd[0],
            args=cmd[1:],
            env={**os.environ, **(self.cfg.env or {})} if self.cfg.env else None,
            keep_alive=True,
        )
        client = Client(transport)
        await client.__aenter__()
        self._client = client
        return client

    async def list_tools(self) -> list[Tool]:
        client = await self.ensure_connected()
        return await client.list_tools()

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> list[TextContent]:
        client = await self.ensure_connected()
        return await client.call_tool(name, arguments)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None


class ProxyManager:
    """Manages proxied MCP clients across instances.

    ``_clients[proxy_name][instance_id] = ProxiedMCPClient``
    """

    def __init__(self) -> None:
        self._clients: dict[str, dict[str, ProxiedMCPClient]] = {}

    def get_or_start(
        self, proxy_name: str, instance_id: str, cfg: ProxiedMCP,
    ) -> ProxiedMCPClient:
        """Return an existing client for *proxy_name*/*instance_id* or create one."""
        by_instance = self._clients.setdefault(proxy_name, {})
        client = by_instance.get(instance_id)
        if client is None:
            client = ProxiedMCPClient(proxy_name, instance_id, cfg)
            by_instance[instance_id] = client
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
