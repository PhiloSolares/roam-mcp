"""Core server module for Roam MCP server."""

import os
import sys
import logging
import traceback
from typing import Dict, List, Any, Optional, Union
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled
from mcp.server.fastmcp import FastMCP
from datetime import datetime

# Import operations
from roam_mcp.api import (
    API_TOKEN,
    GRAPH_NAME,
    MEMORIES_TAG,
    get_page_content,
    ValidationError,
    QueryError,
    PageNotFoundError,
    BlockNotFoundError,
    TransactionError,
    AuthenticationError,
    RateLimitError
)
from roam_mcp.search import (
    search_by_text,
    search_by_tag,
    search_by_status,
    search_block_refs,
    search_hierarchy,
    search_by_date,
    find_pages_modified_today,
    execute_datomic_query
)
from roam_mcp.content import (
    create_page,
    create_block,
    create_outline,
    import_markdown,
    add_todos,
    update_content,
    update_multiple_contents
)
from roam_mcp.memory import (
    remember,
    recall
)
from roam_mcp.utils import (
    extract_youtube_video_id,
    detect_url_type
)
from roam_mcp.content_parsers import parse_webpage, parse_pdf

# Initialize FastMCP server
mcp = FastMCP("roam-research")

# Configure logging
logger = logging.getLogger("roam-mcp")


def setup_logging(verbose=False):
    """Configure logging with appropriate level of detail."""
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear any existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)


def validate_environment():
    """Validate that required environment variables are set."""
    if not API_TOKEN or not GRAPH_NAME:
        missing = []
        if not API_TOKEN:
            missing.append("ROAM_API_TOKEN")
        if not GRAPH_NAME:
            missing.append("ROAM_GRAPH_NAME")
            
        error_msg = f"""
Missing required environment variables: {', '.join(missing)}

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
        logger.error(error_msg)
        return False
    
    return True


def format_error_response(error: Exception) -> str:
    """Format an error for user-friendly display."""
    if isinstance(error, ValidationError):
        return f"Validation error: {str(error)}"
    elif isinstance(error, PageNotFoundError):
        return f"Page not found: {str(error)}"
    elif isinstance(error, BlockNotFoundError):
        return f"Block not found: {str(error)}"
    elif isinstance(error, QueryError):
        return f"Query error: {str(error)}"
    elif isinstance(error, TransactionError):
        return f"Transaction error: {str(error)}"
    elif isinstance(error, AuthenticationError):
        return f"Authentication error: {str(error)}"
    elif isinstance(error, RateLimitError):
        return f"Rate limit exceeded: {str(error)}"
    else:
        return f"Error: {str(error)}"


@mcp.tool()
async def search_roam(search_terms: List[str]) -> str:
    """Search Roam database for content containing the specified terms.

    Args:
        search_terms: List of keywords to search for
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not search_terms:
            return "Please provide at least one search term"
        
        all_results = []
        for term in search_terms:
            result = search_by_text(term)
            if result["success"]:
                all_results.extend(result["matches"])
        
        # Limit to 3000 words
        word_count = 0
        max_word_count = 3000
        filtered_results = []
        
        for match in all_results:
            content = match["content"]
            block_word_count = len(content.split())
            
            if word_count + block_word_count <= max_word_count:
                filtered_results.append(f"Page: {match.get('page_title', 'Unknown')}\n{content}")
                word_count += block_word_count
            else:
                break
        
        if not filtered_results:
            return f"No results found for terms: {', '.join(search_terms)}"
            
        return "\n\n".join(filtered_results)
    except Exception as e:
        logger.error(f"Error searching Roam: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_fetch_page_by_title(title: str) -> str:
    """Retrieve complete page contents by exact title, including all nested blocks and resolved block references.

    Args:
        title: Title of the page
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not title:
            return "Error: title is required"
        
        content = get_page_content(title)
        return content
    except Exception as e:
        logger.error(f"Error fetching page: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_create_page(title: str, content: Optional[List[Dict[str, Any]]] = None) -> str:
    """Create a new page in Roam Research with optional content using explicit nesting levels.

    Args:
        title: Title of the new page
        content: Initial content for the page as an array of blocks with explicit nesting levels.
               Each block must have a 'text' field with the content as a string.
               Example:
               [
                 {"text": "Heading", "level": 0},
                 {"text": "- Bullet point", "level": 1},
                 {"text": "- Another point", "level": 1, "children": [
                   {"text": "- Nested point", "level": 2}
                 ]}
               ]
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not title:
            return "Error: title is required"
        
        result = create_page(title, content)
        if result["success"]:
            return f"Page created successfully: {result['page_url']}"
        else:
            return f"Error creating page: {result.get('error', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error creating page: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_create_block(content: str, page_uid: Optional[str] = None, title: Optional[str] = None) -> str:
    """Add a new block to an existing Roam page. If no page specified, adds to today's daily note.

    Args:
        content: Content of the block
        page_uid: Optional: UID of the page to add block to
        title: Optional: Title of the page to add block to
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not content:
            return "Error: content is required"
        
        result = create_block(content, page_uid, title)
        if result["success"]:
            block_uid = result.get("block_uid", "unknown")
            parent_uid = result.get("parent_uid", "unknown")
            return f"Block created successfully with UID: {block_uid} under parent: {parent_uid}"
        else:
            return f"Error creating block: {result.get('error', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error creating block: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_create_outline(outline: List[Dict[str, Any]], page_title_uid: Optional[str] = None, block_text_uid: Optional[str] = None) -> str:
    """Add a structured outline to an existing page or block with customizable nesting levels.

    Args:
        outline: Array of outline items with block text and explicit nesting level
        page_title_uid: Title or UID of the page. Leave blank to use the default daily page
        block_text_uid: A title heading for the outline or the UID of the block under which content will be nested
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not outline:
            return "Error: outline is required and cannot be empty"
        
        result = create_outline(outline, page_title_uid, block_text_uid)
        if result["success"]:
            created_count = len(result.get("created_uids", []))
            page_uid = result.get("page_uid", "unknown")
            parent_uid = result.get("parent_uid", "unknown")
            return f"Outline created successfully with {created_count} blocks on page {page_uid} under parent {parent_uid}"
        else:
            return f"Error creating outline: {result.get('error', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error creating outline: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_import_markdown(content: str, page_uid: Optional[str] = None, page_title: Optional[str] = None,
                            parent_uid: Optional[str] = None, parent_string: Optional[str] = None, 
                            order: str = "last") -> str:
    """Import nested markdown content into Roam under a specific block.

    Args:
        content: Nested markdown content to import
        page_uid: Optional: UID of the page containing the parent block
        page_title: Optional: Title of the page containing the parent block
        parent_uid: Optional: UID of the parent block to add content under
        parent_string: Optional: Exact string content of the parent block to add content under
        order: Optional: Where to add the content under the parent ("first" or "last")
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not content:
            return "Error: content is required and cannot be empty"
        
        result = import_markdown(content, page_uid, page_title, parent_uid, parent_string, order)
        if result["success"]:
            created_count = len(result.get("created_uids", []))
            page_uid = result.get("page_uid", "unknown")
            parent_uid = result.get("parent_uid", "unknown")
            return f"Markdown imported successfully with {created_count} blocks on page {page_uid} under parent {parent_uid}"
        else:
            return f"Error importing markdown: {result.get('error', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error importing markdown: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_add_todo(todos: List[str]) -> str:
    """Add a list of todo items as individual blocks to today's daily page in Roam.

    Args:
        todos: List of todo items to add
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not todos:
            return "Error: todos list cannot be empty"
        
        result = add_todos(todos)
        if result["success"]:
            return f"Added {len(todos)} todo items to today's daily page"
        else:
            return f"Error adding todos: {result.get('error', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error adding todos: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_search_for_tag(primary_tag: str, page_title_uid: Optional[str] = None, near_tag: Optional[str] = None) -> str:
    """Search for blocks containing a specific tag and optionally filter by blocks that also contain another tag nearby.

    Args:
        primary_tag: The main tag to search for (without the [[ ]] brackets)
        page_title_uid: Optional: Title or UID of the page to search in
        near_tag: Optional: Another tag to filter results by - will only return blocks where both tags appear
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not primary_tag:
            return "Error: primary_tag is required"
        
        result = search_by_tag(primary_tag, page_title_uid, near_tag)
        if result["success"]:
            # Format the results
            formatted = f"{result['message']}\n\n"
            
            for match in result["matches"]:
                page_info = f" (in page: {match['page_title']})" if "page_title" in match else ""
                formatted += f"- {match['content']}{page_info}\n"
            
            return formatted
        else:
            return f"Error searching for tag: {result.get('message', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error searching for tag: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_search_by_status(status: str, page_title_uid: Optional[str] = None, 
                              include: Optional[str] = None, exclude: Optional[str] = None) -> str:
    """Search for blocks with a specific status (TODO/DONE) across all pages or within a specific page.

    Args:
        status: Status to search for (TODO or DONE)
        page_title_uid: Optional: Title or UID of the page to search in
        include: Optional: Comma-separated list of terms to filter results by inclusion
        exclude: Optional: Comma-separated list of terms to filter results by exclusion
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not status or status not in ["TODO", "DONE"]:
            return "Error: status must be either 'TODO' or 'DONE'"
        
        result = search_by_status(status, page_title_uid, include, exclude)
        if result["success"]:
            # Format the results
            formatted = f"{result['message']}\n\n"
            
            for match in result["matches"]:
                page_info = f" (in page: {match['page_title']})" if "page_title" in match else ""
                formatted += f"- {match['content']}{page_info}\n"
            
            return formatted
        else:
            return f"Error searching by status: {result.get('message', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error searching by status: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_search_block_refs(block_uid: Optional[str] = None, page_title_uid: Optional[str] = None) -> str:
    """Search for block references within a page or across the entire graph.

    Args:
        block_uid: Optional: UID of the block to find references to
        page_title_uid: Optional: Title or UID of the page to search in
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        result = search_block_refs(block_uid, page_title_uid)
        if result["success"]:
            # Format the results
            formatted = f"{result['message']}\n\n"
            
            for match in result["matches"]:
                page_info = f" (in page: {match['page_title']})" if "page_title" in match else ""
                formatted += f"- {match['content']}{page_info}\n"
            
            return formatted
        else:
            return f"Error searching block references: {result.get('message', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error searching block references: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_search_hierarchy(parent_uid: Optional[str] = None, child_uid: Optional[str] = None,
                              page_title_uid: Optional[str] = None, max_depth: int = 1) -> str:
    """Search for parent or child blocks in the block hierarchy.

    Args:
        parent_uid: Optional: UID of the block to find children of
        child_uid: Optional: UID of the block to find parents of
        page_title_uid: Optional: Title or UID of the page to search in
        max_depth: Optional: How many levels deep to search (default: 1)
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not parent_uid and not child_uid:
            return "Error: Either parent_uid or child_uid must be provided"
        
        result = search_hierarchy(parent_uid, child_uid, page_title_uid, max_depth)
        if result["success"]:
            # Format the results
            formatted = f"{result['message']}\n\n"
            
            for match in result["matches"]:
                page_info = f" (in page: {match['page_title']})" if "page_title" in match else ""
                depth_info = f" (depth: {match['depth']})"
                formatted += f"- {match['content']}{page_info}{depth_info}\n"
            
            return formatted
        else:
            return f"Error searching hierarchy: {result.get('message', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error searching hierarchy: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_find_pages_modified_today(max_num_pages: int = 50) -> str:
    """Find pages that have been modified today (since midnight).

    Args:
        max_num_pages: Max number of pages to retrieve (default: 50)
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if max_num_pages < 1:
            return "Error: max_num_pages must be at least 1"
        
        result = find_pages_modified_today(max_num_pages)
        if result["success"]:
            # Format the results
            formatted = f"{result['message']}\n\n"
            
            for page in result["pages"]:
                formatted += f"- {page}\n"
            
            return formatted
        else:
            return f"Error finding modified pages: {result.get('message', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error finding modified pages: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_search_by_text(text: str, page_title_uid: Optional[str] = None) -> str:
    """Search for blocks containing specific text across all pages or within a specific page.

    Args:
        text: The text to search for
        page_title_uid: Optional: Title or UID of the page to search in
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not text:
            return "Error: text is required"
        
        result = search_by_text(text, page_title_uid)
        if result["success"]:
            # Format the results
            formatted = f"{result['message']}\n\n"
            
            for match in result["matches"]:
                page_info = f" (in page: {match['page_title']})" if "page_title" in match else ""
                formatted += f"- {match['content']}{page_info}\n"
            
            return formatted
        else:
            return f"Error searching by text: {result.get('message', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error searching by text: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_update_block(block_uid: str, content: Optional[str] = None, 
                          transform_pattern: Optional[Dict[str, Any]] = None) -> str:
    """Update a single block identified by its UID.

    Args:
        block_uid: UID of the block to update
        content: New content for the block
        transform_pattern: Pattern to transform the current content
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not block_uid:
            return "Error: block_uid is required"
        
        if not content and not transform_pattern:
            return "Error: Either content or transform_pattern must be provided"
        
        result = update_content(block_uid, content, transform_pattern)
        if result["success"]:
            return f"Block updated successfully: {result['content']}"
        else:
            return f"Error updating block: {result.get('error', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error updating block: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_update_multiple_blocks(updates: List[Dict[str, Any]]) -> str:
    """Efficiently update multiple blocks in a single batch operation.

    Args:
        updates: Array of block updates to perform
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not updates or not isinstance(updates, list):
            return "Error: updates must be a non-empty list"
        
        result = update_multiple_contents(updates)
        if result["success"]:
            successful = sum(1 for r in result["results"] if r.get("success"))
            return f"Updated {successful}/{len(updates)} blocks successfully"
        else:
            return f"Error updating blocks: {result.get('error', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error updating blocks: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_search_by_date(start_date: str, end_date: Optional[str] = None,
                            type_filter: str = "created", scope: str = "blocks",
                            include_content: bool = True) -> str:
    """Search for blocks or pages based on creation or modification dates.

    Args:
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: Optional: End date in ISO format (YYYY-MM-DD)
        type_filter: Whether to search by "created", "modified", or "both"
        scope: Whether to search "blocks", "pages", or "both"
        include_content: Whether to include the content of matching blocks/pages
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not start_date:
            return "Error: start_date is required"
        
        if type_filter not in ["created", "modified", "both"]:
            return "Error: type_filter must be 'created', 'modified', or 'both'"
        
        if scope not in ["blocks", "pages", "both"]:
            return "Error: scope must be 'blocks', 'pages', or 'both'"
        
        result = search_by_date(start_date, end_date, type_filter, scope, include_content)
        if result["success"]:
            # Format the results
            formatted = f"{result['message']}\n\n"
            
            for match in result["matches"]:
                date_info = datetime.fromtimestamp(match["time"] / 1000).strftime("%Y-%m-%d %H:%M:%S")
                
                if match["type"] == "block":
                    page_info = f" (in page: {match.get('page_title', 'Unknown')})"
                    content_info = f": {match.get('content', '')}" if include_content else ""
                    formatted += f"- Block {match['uid']} {date_info}{page_info}{content_info}\n"
                else:  # page
                    title_info = f" (title: {match.get('title', 'Unknown')})"
                    content_info = f": {match.get('content', '')}" if include_content else ""
                    formatted += f"- Page {match['uid']} {date_info}{title_info}{content_info}\n"
            
            return formatted
        else:
            return f"Error searching by date: {result.get('message', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error searching by date: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_remember(memory: str, categories: Optional[List[str]] = None) -> str:
    """Add a memory or piece of information to remember, stored on the daily page with tag.

    Args:
        memory: The memory detail or information to remember
        categories: Optional categories to tag the memory with
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not memory:
            return "Error: memory is required"
        
        result = remember(memory, categories)
        if result["success"]:
            return f"Memory stored successfully: {result['content']}"
        else:
            return f"Error storing memory: {result.get('error', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error storing memory: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_recall(sort_by: str = "newest", filter_tag: Optional[str] = None) -> str:
    """Retrieve stored memories, optionally filtered by tag and sorted by creation date.

    Args:
        sort_by: Sort order for memories based on creation date
        filter_tag: Include only memories with a specific filter tag
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if sort_by not in ["newest", "oldest"]:
            return "Error: sort_by must be 'newest' or 'oldest'"
        
        result = recall(sort_by, filter_tag)
        if result["success"]:
            # Format the results
            formatted = f"{result['message']}\n\n"
            
            for memory in result["memories"]:
                formatted += f"- {memory}\n"
            
            return formatted
        else:
            return f"Error recalling memories: {result.get('error', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error recalling memories: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_datomic_query(query: str, inputs: Optional[List[Any]] = None) -> str:
    """Execute a custom Datomic query on the Roam graph beyond the available search tools.

    Args:
        query: The Datomic query to execute (in Datalog syntax)
        inputs: Optional array of input parameters for the query
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        if not query:
            return "Error: query is required"
        
        result = execute_datomic_query(query, inputs)
        if result["success"]:
            # Format the results
            formatted = f"{result['message']}\n\n"
            
            for match in result["matches"]:
                formatted += f"- {match['content']}\n"
            
            return formatted
        else:
            return f"Error executing query: {result.get('message', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def get_youtube_transcript(url: str) -> str:
    """Fetch and return the transcript of a YouTube video.

    Args:
        url: URL of the YouTube video
    """
    video_id = extract_youtube_video_id(url)
    if not video_id:
        return "Invalid YouTube URL. Unable to extract video ID."

    try:
        # Define the prioritized list of language codes
        languages = [
            'en', 'en-US', 'en-GB', 'de', 'es', 'hi', 'zh', 'ar', 'bn', 'pt',
            'ru', 'ja', 'pa'
        ]

        # Attempt to retrieve the available transcripts
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        # Try to find a transcript in the prioritized languages
        for language in languages:
            try:
                transcript = transcript_list.find_transcript([language])
                # Check if the transcript is manually created or generated, prefer manually created
                if transcript.is_generated:
                    continue
                text = " ".join([line["text"] for line in transcript.fetch()])
                return text
            except Exception:
                continue

        # If no suitable transcript is found in the specified languages, try to fetch a generated transcript
        try:
            generated_transcript = transcript_list.find_generated_transcript(
                languages)
            text = " ".join(
                [line["text"] for line in generated_transcript.fetch()])
            return text
        except Exception:
            return "No suitable transcript found for this video."

    except TranscriptsDisabled:
        return "Transcripts are disabled for this video."
    except Exception as e:
        logger.error(f"Error fetching YouTube transcript: {str(e)}", exc_info=True)
        return f"An error occurred while fetching the transcript: {str(e)}"


@mcp.tool()
async def fetch_webpage_content(url: str) -> str:
    """Fetch and extract the main content from a web page.

    Args:
        url: URL of the web page to fetch
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        logger.debug(f"Fetching webpage content: {url}")
        result = await parse_webpage(url)
        
        if result["success"]:
            return f"# {result['title']}\n\nSource: {url}\n\n{result['content']}"
        else:
            return f"Error fetching webpage: {result.get('error', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error in fetch_webpage_content: {str(e)}", exc_info=True)
        return f"Error fetching webpage: {str(e)}"


@mcp.tool()
async def fetch_pdf_content(url: str) -> str:
    """Fetch and extract the content from a PDF file.

    Args:
        url: URL of the PDF file to fetch
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        logger.debug(f"Fetching PDF content: {url}")
        result = await parse_pdf(url)
        
        if result["success"]:
            return f"# {result['title']}\n\nSource: {url}\n\n{result['content']}"
        else:
            return f"Error fetching PDF: {result.get('error', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error in fetch_pdf_content: {str(e)}", exc_info=True)
        return f"Error fetching PDF: {str(e)}"


@mcp.tool()
async def parse_url(url: str) -> str:
    """Intelligently parse content from a URL - supports webpages, PDFs, and YouTube videos.

    Args:
        url: URL to parse
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        # Detect URL type
        url_type = detect_url_type(url)
        
        if url_type == "youtube":
            # Use existing YouTube transcript function
            return await get_youtube_transcript(url)
        elif url_type == "pdf":
            return await fetch_pdf_content(url)
        else:  # webpage or unknown
            return await fetch_webpage_content(url)
    except Exception as e:
        logger.error(f"Error parsing URL: {str(e)}", exc_info=True)
        return f"Error parsing URL: {str(e)}"


@mcp.tool()
async def get_roam_graph_info() -> str:
    """Get information about your Roam Research graph.
    """
    if not validate_environment():
        return "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
    
    try:
        # Get page count
        query = """[:find (count ?p)
                    :where [?p :node/title]]"""
        
        result = execute_datomic_query(query)
        
        if result["success"] and result["matches"]:
            page_count = result["matches"][0]["content"]
        else:
            page_count = "Unknown"
        
        # Get block count
        query = """[:find (count ?b)
                    :where [?b :block/string]]"""
        
        result = execute_datomic_query(query)
        
        if result["success"] and result["matches"]:
            block_count = result["matches"][0]["content"]
        else:
            block_count = "Unknown"
        
        # Format the output
        memory_tag = MEMORIES_TAG if MEMORIES_TAG else "Not set (using default #[[Memories]])"
        
        formatted_info = f"""
Graph Name: {GRAPH_NAME}
Pages: {page_count}
Blocks: {block_count}
API Access: Enabled
Memory Tag: {memory_tag}
"""
        
        return formatted_info
    except Exception as e:
        logger.error(f"Error retrieving graph information: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.prompt()
async def summarize_page(page_title: str) -> dict:
    """
    Create a prompt to summarize a page in Roam Research.

    Args:
        page_title: Title of the page to summarize
    """
    if not validate_environment():
        return {
            "messages": [{
                "role": "user",
                "content": "Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME environment variables must be set"
            }]
        }
    
    try:
        content = get_page_content(page_title)
        
        return {
            "messages": [{
                "role": "user",
                "content": f"Please provide a concise summary of the following page content from my Roam Research database:\n\n{content}"
            }]
        }
    except Exception as e:
        logger.error(f"Error creating summary prompt: {str(e)}", exc_info=True)
        return {
            "messages": [{
                "role": "user",
                "content": f"I wanted to summarize my Roam page titled '{page_title}', but there was an error retrieving the content: {format_error_response(e)}. Can you help me troubleshoot this issue with my Roam Research integration?"
            }]
        }


def run_server(transport="stdio", port=None, verbose=False):
    """Run the MCP server with the specified transport."""
    # Configure logging based on verbosity
    setup_logging(verbose)
    
    logger.info("Server starting...")
    
    # Validate environment variables
    valid_env = validate_environment()
    if valid_env:
        logger.info(f"API token and graph name are set")
        logger.info(f"MEMORIES_TAG is set to: {MEMORIES_TAG}")
    else:
        logger.warning("Missing required environment variables")
    
    # Run the server
    try:
        if transport == "stdio":
            logger.info("Starting server with stdio transport")
            mcp.run(transport="stdio")
        elif transport == "sse":
            if not port:
                port = 3000
            logger.info(f"Starting server with SSE transport on port {port}")
            mcp.run(transport="sse", port=port)
        else:
            logger.error(f"Unsupported transport: {transport}")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as e:
        logger.error(f"Error running server: {str(e)}")
        traceback.print_exc()