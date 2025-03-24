import argparse
from roam_mcp.server import run_server


def main():
                    """Entry point for the Roam MCP server CLI."""
                    parser = argparse.ArgumentParser(
                        description="Roam Research MCP Server")
                    parser.add_argument("--transport",
                                        choices=["stdio", "sse"],
                                        default="stdio",
                                        help="Transport method (stdio or sse)")
                    parser.add_argument(
                        "--port",
                        type=int,
                        default=3000,
                        help="Port for SSE transport (default: 3000)")

                    args = parser.parse_args()

                    # Run the server with the specified transport
                    run_server(
                        transport=args.transport,
                        port=args.port if args.transport == "sse" else None)


if __name__ == "__main__":
                    main()
