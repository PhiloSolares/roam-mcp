# Roam Research MCP Server

A Model Context Protocol (MCP) server that connects Claude and other AI assistants to your Roam Research graph.

## What This Does

This server acts as a bridge between AI assistants and your Roam Research database. After setup, you can simply ask Claude to work with your Roam data - no coding required.

For example, you can say:
- "Add these meeting notes to today's daily note in Roam"
- "Search my Roam graph for blocks tagged with #ProjectIdeas"
- "Create a new page in Roam called 'Project Planning'"
- "Find all TODO items I created this month"

## Features

### Content Creation
- Create new pages with nested content and headings
- Add blocks to any page with proper hierarchy
- Create structured outlines with customizable nesting
- Import markdown with proper nesting
- Add todo items with automatic TODO status
- Update existing content individually or in batches
- Modify block content with pattern transformations

### Search and Retrieval
- Find pages and blocks by title, text, or tags
- Search for TODO/DONE items with filtering options
- Find recently modified content
- Search block references and explore block hierarchies
- Search by creation or modification dates
- Navigate parent-child relationships in blocks
- Execute custom Datalog queries for advanced needs

### Memory System
- Store information for Claude to remember across conversations
- Recall stored memories with filtering and sorting options
- Tag memories with custom categories
- Access both recent and older memories with flexible retrieval

### URL Content Processing
- Extract and import content from webpages
- Parse and extract text from PDF documents
- Retrieve YouTube video transcripts
- Intelligently detect content type and process accordingly

## Setup Instructions

1. Install Claude Desktop from [https://claude.ai/download](https://claude.ai/download)

2. Edit your Claude Desktop configuration file:
   - Mac: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - Windows: `%APPDATA%\Claude\claude_desktop_config.json`

3. Add this configuration:

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
```

4. Get your Roam API token:
   - Go to your Roam Research graph settings
   - Navigate to "API tokens"
   - Click "+ New API Token"
   - Copy the token to your configuration

## How to Use

Once set up, simply chat with Claude and ask it to work with your Roam graph. Claude will use the appropriate MCP commands behind the scenes.

Example conversations:

**Creating Content:**
> You: "Claude, please create a new page in my Roam graph called 'Project Ideas' with a section for mobile app ideas."

**Searching Content:**
> You: "Find all blocks in my Roam graph tagged with #ProjectIdeas that also mention mobile apps."
>
> You: "Show me all the TODO items I created this week."

**Using the Memory System:**
> You: "Remember that I want to use spaced repetition for learning JavaScript."
>
> Later:
> You: "What learning techniques have we discussed for programming?"

**Working with External Content:**
> You: "Extract the main points from this PDF and add them to my Roam graph."
>
> You: "Get the transcript from this YouTube video about productivity."

## Advanced Configuration

By default, memories are stored with the tag `#[[Memories]]`. To use a different tag:

```json
"env": {
  "ROAM_API_TOKEN": "your-token",
  "ROAM_GRAPH_NAME": "your-graph",
  "MEMORIES_TAG": "#[[Claude/Memories]]"
}
```

## Docker Support

You can run the Roam MCP server in a Docker container:

### Building the Image

```bash
docker build -t roam-mcp .
```

### Running the Container

Run with environment variables:

```bash
docker run -p 3000:3000 \
  -e ROAM_API_TOKEN="your_api_token" \
  -e ROAM_GRAPH_NAME="your_graph_name" \
  roam-mcp
```

### Using with Claude Desktop

Configure Claude Desktop to use the containerized server:

```json
{
  "mcpServers": {
    "roam-helper": {
      "command": "docker",
      "args": ["run", "--rm", "-p", "3000:3000",
               "-e", "ROAM_API_TOKEN=your_token",
               "-e", "ROAM_GRAPH_NAME=your_graph",
               "roam-mcp"],
      "env": {}
    }
  }
}
```

## License

MIT License