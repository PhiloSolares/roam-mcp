"""Command-line interface for the Roam MCP server."""

import argparse
import sys
import logging
import os
from dotenv import load_dotenv
from pathlib import Path
from roam_mcp.server import run_server
from roam_mcp.api import API_TOKEN, GRAPH_NAME, MEMORIES_TAG

def print_configuration_help():
    """Print detailed configuration help similar to the TypeScript version."""
    config_help = f"""
Missing required environment variables: {[] if API_TOKEN else ['ROAM_API_TOKEN']}{[] if GRAPH_NAME else ['ROAM_GRAPH_NAME']}

Please configure these variables either:
1. In your MCP settings file:
   - For Claude: ~/Library/Application Support/Claude/claude_desktop_config.json
   - For Cline: ~/Library/Application Support/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json

   Example configuration:
   {{
     "mcpServers": {{
       "roam-helper": {{
         "command": "uvx",
         "args": ["git+https://github.com/PhiloSolares/roam-mcp.git"],
         "env": {{
           "ROAM_API_TOKEN": "your-api-token",
           "ROAM_GRAPH_NAME": "your-graph-name"
         }}
       }}
     }}
   }}

2. Or in a .env file in the roam-mcp directory:
   ROAM_API_TOKEN=your-api-token
   ROAM_GRAPH_NAME=your-graph-name
"""
    print(config_help, file=sys.stderr)

def validate_environment():
    """Validate that required environment variables are set."""
    # Try to load from .env file first, if it exists in current directory or parent
    current_dir = Path.cwd()
    env_file = current_dir / '.env'
    
    if not env_file.exists():
        # Try parent directory
        env_file = current_dir.parent / '.env'
    
    if env_file.exists():
        load_dotenv(env_file)
    
    # Check for required variables
    if not API_TOKEN or not GRAPH_NAME:
        print("Configuration error:", file=sys.stderr)
        print_configuration_help()
        return False
    
    return True

def main():
    """Entry point for the Roam MCP server CLI."""
    parser = argparse.ArgumentParser(
        description="Roam Research MCP Server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    # Server configuration group
    server_group = parser.add_argument_group('Server Configuration')
    server_group.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
        help="Transport method (stdio or sse)"
    )
    server_group.add_argument(
        "--port",
        type=int,
        default=3000,
        help="Port for SSE transport"
    )
    
    # Logging configuration group
    logging_group = parser.add_argument_group('Logging Configuration')
    logging_group.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    logging_group.add_argument(
        "--log-file",
        help="Log to specified file instead of stderr"
    )
    logging_group.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set logging level"
    )
    
    # Environment configuration group
    env_group = parser.add_argument_group('Environment Configuration')
    env_group.add_argument(
        "--env-file",
        help="Path to .env file with ROAM_API_TOKEN and ROAM_GRAPH_NAME"
    )
    env_group.add_argument(
        "--print-config",
        action="store_true",
        help="Print current configuration and exit"
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Handle environment file if specified
    if args.env_file:
        if os.path.exists(args.env_file):
            load_dotenv(args.env_file)
            print(f"Loaded environment from {args.env_file}", file=sys.stderr)
        else:
            print(f"Error: Environment file {args.env_file} not found", file=sys.stderr)
            sys.exit(1)
    
    # Validate environment vars
    valid_env = validate_environment()
    
    # Print configuration if requested
    if args.print_config:
        config_info = f"""
Current Configuration:
---------------------
ROAM_API_TOKEN: {"Set" if API_TOKEN else "Not set"}
ROAM_GRAPH_NAME: {GRAPH_NAME if GRAPH_NAME else "Not set"}
MEMORIES_TAG: {MEMORIES_TAG}
Transport: {args.transport}
Port (if using SSE): {args.port}
Log level: {args.log_level}
Verbose logging: {"Enabled" if args.verbose else "Disabled"}
Log file: {args.log_file if args.log_file else "Stderr"}
"""
        print(config_info)
        if not valid_env:
            sys.exit(1)
        sys.exit(0)
    
    # Exit if environment validation failed and we're not just printing config
    if not valid_env:
        sys.exit(1)
    
    # Determine actual log level
    log_level = getattr(logging, args.log_level)
    if args.verbose and log_level > logging.DEBUG:
        log_level = logging.DEBUG
    
    # Run the server with the specified transport
    try:
        run_server(
            transport=args.transport,
            port=args.port if args.transport == "sse" else None,
            verbose=args.verbose,
            log_level=log_level,
            log_file=args.log_file
        )
    except KeyboardInterrupt:
        print("\nServer stopped by user", file=sys.stderr)
        sys.exit(0)
    except Exception as e:
        print(f"Error starting server: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()