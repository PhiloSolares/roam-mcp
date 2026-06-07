"""Command-line interface for the Roam MCP server."""

import argparse
import sys
from roam_mcp.server import run_server

def main():
    """Entry point for the Roam MCP server CLI."""
    parser = argparse.ArgumentParser(description="Roam Research MCP Server")
    
    # Transport options
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
        help="Transport method (stdio or sse)"
    )
    
    # Server configuration
    parser.add_argument(
        "--port",
        type=int,
        default=3000,
        help="Port for SSE transport (default: 3000)"
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host/interface to bind for SSE transport (default: 127.0.0.1; "
             "use 0.0.0.0 to accept connections from other hosts/containers)"
    )

    # Verbosity options
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )

    # Parse arguments
    args = parser.parse_args()

    # Run the server with the specified transport
    try:
        run_server(
            transport=args.transport,
            host=args.host,
            port=args.port if args.transport == "sse" else None,
            verbose=args.verbose
        )
    except KeyboardInterrupt:
        print("\nServer stopped by user", file=sys.stderr)
        sys.exit(0)
    except Exception as e:
        print(f"Error starting server: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()