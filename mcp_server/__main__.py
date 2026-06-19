"""Entry point: run the Draupnir MCP server over stdio (``python -m mcp_server``)."""

from mcp_server.server import create_server


def main() -> None:
    """Run the MCP server over the stdio transport."""

    create_server().run(transport="stdio")


if __name__ == "__main__":
    main()
