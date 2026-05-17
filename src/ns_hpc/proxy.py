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
    """One connection to a proxied MCP server running inside an instance."""

    def __init__(self, proxy_name: str, instance_id: str, cfg: ProxiedMCP, config: Config) -> None:
        self.proxy_name = proxy_name
        self.instance_id = instance_id
        self.cfg = cfg
        self.config = config
        self._client: Client | None = None

    async def ensure_connected(self) -> Client:
        """Start the process inside bwrap and connect if not already connected."""
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
            extra_ro_binds=[(str(shared_output_root), "/shared-output")],
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
        return client

    async def list_tools(self) -> list[Tool]:
        client = await self.ensure_connected()
        return await client.list_tools()

    async def call_tool(
        self, name: str, arguments: dict[str, Any]
    ) -> list[TextContent]:
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
        self,
        proxy_name: str,
        instance_id: str,
        cfg: ProxiedMCP,
        config: Config,
    ) -> ProxiedMCPClient:
        """Return an existing client for *proxy_name*/*instance_id* or create one."""
        by_instance = self._clients.setdefault(proxy_name, {})
        client = by_instance.get(instance_id)
        if client is None:
            client = ProxiedMCPClient(proxy_name, instance_id, cfg, config)
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
