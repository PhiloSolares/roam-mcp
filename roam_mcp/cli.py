"""Command-line interface for the Roam MCP server."""

import argparse
import sys
import logging # Import logging to potentially catch early errors

# Import the runner function and validation from server module
from roam_mcp.server import run_server, validate_environment_and_log, setup_logging

def main():
    """Entry point for the Roam MCP server CLI."""
    parser = argparse.ArgumentParser(description="Roam Research MCP Server")
    
    # Transport options
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
        help="Transport method (stdio or sse, default: stdio)"
    )
    
    # Server configuration
    parser.add_argument(
        "--port",
        type=int,
        default=3000,
        help="Port for SSE transport (default: 3000)"
    )
    
    # Verbosity options
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose DEBUG logging (default: INFO)"
    )

    # Parse arguments
    args = parser.parse_args()

    # Setup logging early based on verbosity arg
    # Note: run_server also calls setup_logging, but doing it here
    # allows catching potential issues during arg parsing or early setup.
    setup_logging(verbose=args.verbose)
    logger = logging.getLogger("roam-mcp.cli") # Get logger for CLI scope

    # Validate environment before running server - optional but good practice
    # run_server already does this, but checking early can provide faster feedback
    # if validate_environment_and_log():
    #     logger.info("Environment validation successful.")
    # else:
    #     # Validation function prints detailed instructions
    #     logger.error("Environment validation failed. Server might not function correctly.")
    #     # Decide whether to exit or continue
    #     # sys.exit(1) # Optional: Exit if validation fails

    # Run the server (which will re-validate and log)
    try:
        run_server(
            transport=args.transport,
            port=args.port if args.transport == "sse" else None,
            verbose=args.verbose # Pass verbosity to server runner
        )
    except KeyboardInterrupt:
        # run_server handles its own logging for this
        pass # Already handled in run_server
    except Exception as e:
        # Catch any exceptions bubbling up from run_server start
        logger.critical(f"Failed to start or run server: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()