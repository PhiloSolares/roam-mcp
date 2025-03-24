## Using with Claude Desktop

1. Install Claude Desktop from [https://claude.ai/download](https://claude.ai/download)
2. Edit your Claude Desktop configuration file:
   - Mac: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - Windows: `%APPDATA%\Claude\claude_desktop_config.json`

3. Add the Roam MCP server configuration:

```json
{
  "mcpServers": {
    "roam-helper": {
      "command": "uvx",
      "args": ["git+https://github.com/PhiloSolares/roam-mcp.git"],
      "env": {
        "ROAM_API_TOKEN": "<your_roam_api_token>",
        "ROAM_GRAPH_NAME": "<your_roam_graph_name>"
      }
    }
  }
}