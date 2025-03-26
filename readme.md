# Roam Research MCP Server

A powerful Model Context Protocol (MCP) server that provides comprehensive access to Roam Research's API functionality. This server enables AI assistants like Claude to interact with your Roam Research graph through a standardized interface.

## Features

The Roam MCP server provides a wide range of tools for interacting with your Roam Research graph:

### Content Creation and Manipulation

- `roam_create_page`: Create new pages with optional nested content
- `roam_create_block`: Add blocks to any page (defaults to today's daily note)
- `roam_create_outline`: Create structured outlines with proper nesting
- `roam_import_markdown`: Import nested markdown content with proper conversion
- `roam_add_todo`: Add todo items to today's daily note
- `roam_update_block`: Update existing blocks directly or with pattern transformations
- `roam_update_multiple_blocks`: Batch update multiple blocks in a single operation

### Search and Retrieval

- `roam_fetch_page_by_title`: Retrieve complete page contents with resolved references
- `roam_search_by_text`: Search for text across all blocks
- `roam_search_for_tag`: Search for blocks with specific tags
- `roam_search_by_status`: Find TODO/DONE items with optional filters
- `roam_search_block_refs`: Find block references anywhere in your graph
- `roam_search_hierarchy`: Navigate parent-child relationships between blocks
- `roam_search_by_date`: Find content based on creation/modification dates
- `roam_find_pages_modified_today`: Get a list of pages modified today
- `roam_datomic_query`: Execute custom Datalog queries for advanced retrieval

### Optional Memory System

- `roam_remember`: Store important information with automatic tagging (optional)
- `roam_recall`: Retrieve stored memories with optional filtering and sorting (optional)

### Other Tools

- `get_youtube_transcript`: Fetch transcripts from YouTube videos
- `get_roam_graph_info`: Get information about your Roam graph
- `summarize_page`: Generate a prompt to summarize a Roam page

### URL Content Parsing

The Roam MCP server supports extracting content from web pages, PDFs, and YouTube videos:
use_mcp_tool roam-helper parse_url {
"url": "https://example.com/article.html"
}
Or use the specific parsers for each type:
use_mcp_tool roam-helper fetch_webpage_content {
"url": "https://example.com/article.html"
}
use_mcp_tool roam-helper fetch_pdf_content {
"url": "https://example.com/document.pdf"
}
use_mcp_tool roam-helper get_youtube_transcript {
"url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
}

## Installation and Setup with Claude Desktop

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
```

That's it! No additional configuration needed.

## Getting Your Roam API Token

1. Go to your Roam Research graph settings
2. Navigate to the "API tokens" section
3. Click the "+ New API Token" button
4. Copy the token and add it to your configuration

## Usage Examples

Here are some examples of how to use the Roam MCP server with Claude:

### Creating Content

```
use_mcp_tool roam-helper roam_create_page {
  "title": "Project Ideas",
  "content": [
    {
      "text": "New Project Ideas",
      "level": 1
    },
    {
      "text": "Mobile App for Task Management",
      "level": 2
    },
    {
      "text": "Key Features",
      "level": 3
    }
  ]
}
```

### Searching Content

```
use_mcp_tool roam-helper roam_search_for_tag {
  "primary_tag": "ProjectIdeas",
  "near_tag": "Mobile"
}
```

### Advanced Queries

```
use_mcp_tool roam-helper roam_datomic_query {
  "query": "[:find ?title (count ?children) :where [?p :node/title ?title] [?p :block/children ?children]]"
}
```

### Using the Optional Memory System

If you want to use the memory system to let Claude remember important information across conversations:

```
use_mcp_tool roam-helper roam_remember {
  "memory": "Claude suggested using spaced repetition for learning new programming languages",
  "categories": ["Learning", "Programming"]
}
```

Later, recall stored memories:

```
use_mcp_tool roam-helper roam_recall {
  "sort_by": "newest",
  "filter_tag": "Programming"
}
```

By default, memories are stored with the tag `#[[Memories]]`. If you want to use a different tag, you can add a `MEMORIES_TAG` environment variable to your configuration:

```json
"env": {
  "ROAM_API_TOKEN": "your-token",
  "ROAM_GRAPH_NAME": "your-graph",
  "MEMORIES_TAG": "#[[Claude/Memories]]"
}
```

## License

MIT License