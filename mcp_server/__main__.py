"""Entry point: run the Draupnir MCP server over stdio (``python -m mcp_server``)."""

import asyncio

from mcp_server.server import create_server


async def _run() -> None:
    server = await create_server()
    await server.run_async(transport="stdio")


def main() -> None:
    """Generate the server from the live API spec and run it over stdio."""

    asyncio.run(_run())


if __name__ == "__main__":
    main()
