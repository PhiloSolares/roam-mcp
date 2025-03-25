"""
Roam Research MCP Server - Python implementation
Connect Claude to your Roam Research database
"""

import os
from pathlib import Path
from dotenv import load_dotenv

__version__ = "0.2.0"

# Try to load environment variables from .env file
env_path = Path(".") / ".env"
if env_path.exists():
    load_dotenv(env_path)

# Additional .env locations to try
additional_paths = [
    Path.home() / ".roam-mcp.env",
    Path.home() / ".config" / "roam-mcp" / ".env",
]

for path in additional_paths:
    if path.exists():
        load_dotenv(path)
        break