"""Core server module for Roam MCP server."""

import os
import sys
import logging
import traceback
from typing import Dict, List, Any, Optional, Union
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled
from mcp.server.fastmcp import FastMCP
from datetime import datetime

# Import client getter and specific errors
from roam_mcp.api import (
    get_client, # Use this to get client instance
    get_api_token,
    get_graph_name,
    get_memories_tag,
    get_page_content, # This now uses the client
    ValidationError,
    QueryError,
    PageNotFoundError,
    BlockNotFoundError,
    TransactionError,
    AuthenticationError,
    RateLimitError,
    RoamAPIError # Base error
)
# Import operations modules
from roam_mcp import search, content as content_ops, memory as memory_ops

# Import utils needed
from roam_mcp.utils import extract_youtube_video_id

# Initialize FastMCP server
mcp = FastMCP("roam-research")

# Configure logging
logger = logging.getLogger("roam-mcp")


# --- Server Setup ---

def setup_logging(verbose=False):
    """Configure logging with appropriate level of detail."""
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Use basicConfig to set up root logger simply
    logging.basicConfig(level=log_level, format=log_format, stream=sys.stderr)
    
    # Optionally disable logging from libraries if too verbose
    # logging.getLogger("requests").setLevel(logging.WARNING)
    # logging.getLogger("urllib3").setLevel(logging.WARNING)

    logger.info(f"Logging setup complete. Level: {logging.getLevelName(log_level)}")


def validate_environment_and_log() -> bool:
    """
    Validate required environment variables and log status or detailed setup instructions.
    Returns True if valid, False otherwise.
    """
    api_token = get_api_token()
    graph_name = get_graph_name()

    if api_token and graph_name:
        logger.info("ROAM_API_TOKEN and ROAM_GRAPH_NAME are set.")
        # Attempt to initialize client early to catch auth errors?
        try:
             _ = get_client() # Initializes if not already done
             logger.info(f"Successfully initialized client for graph '{graph_name}'.")
             logger.info(f"MEMORIES_TAG is set to: '{get_memories_tag()}'")
             return True
        except AuthenticationError as e:
             logger.error(f"Authentication failed during initial client setup: {e}")
             # Print detailed setup help for auth errors too
             print_setup_instructions(api_token_found=bool(api_token), graph_name_found=bool(graph_name), auth_error=str(e))
             return False
        except Exception as e:
             logger.error(f"Unexpected error during initial client setup: {e}", exc_info=True)
             print_setup_instructions(api_token_found=bool(api_token), graph_name_found=bool(graph_name), auth_error=f"Unexpected error: {e}")
             return False
    else:
        missing = []
        if not api_token: missing.append("ROAM_API_TOKEN")
        if not graph_name: missing.append("ROAM_GRAPH_NAME")
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        print_setup_instructions(api_token_found=bool(api_token), graph_name_found=bool(graph_name))
        return False

def print_setup_instructions(api_token_found: bool, graph_name_found: bool, auth_error: Optional[str] = None):
    """Prints detailed setup instructions to stderr."""
    lines = [
        "--- Roam MCP Server Configuration Error ---",
        "Please set the required environment variables:",
    ]
    if not api_token_found:
        lines.append("  - ROAM_API_TOKEN: Your Roam Research API token.")
    if not graph_name_found:
        lines.append("  - ROAM_GRAPH_NAME: The exact name of your Roam graph.")
        
    if auth_error:
         lines.append(f"\nAuthentication Error: {auth_error}")
         lines.append("Suggestion: Double-check your API token and graph name spelling/case.")

    lines.extend([
        "\nYou can configure these using your MCP client (e.g., Claude Desktop) or by creating a .env file.",
        "\nExample using Claude Desktop config (~/Library/Application Support/Claude/claude_desktop_config.json):",
        '''
{
  "mcpServers": {
    "roam-helper": {
      "command": "uvx",
      "args": ["git+https://github.com/PhiloSolares/roam-mcp.git"],
      "env": {
        "ROAM_API_TOKEN": "your-roam-api-token-here",
        "ROAM_GRAPH_NAME": "your-roam-graph-name-here",
        "MEMORIES_TAG": "#[[Optional/MemoryTag]]"
      }
    }
  }
}
        ''',
        "Note: Replace placeholders with your actual token and graph name.",
        "Ensure the 'command' and 'args' point correctly to how you run this server (uvx example shown).",
        "Restart your MCP client after modifying the configuration.",
        "---------------------------------------------"
    ])
    print("\n".join(lines), file=sys.stderr)

def format_error_response(error: Exception) -> str:
    """Format an error into a user-friendly string for MCP response."""
    # Use the custom error hierarchy if possible
    if isinstance(error, RoamAPIError):
        # Include code, message, and remediation suggestion
        msg = f"Error Code: {error.code}\nMessage: {error.message}"
        if error.remediation:
            msg += f"\nSuggestion: {error.remediation}"
        # Optionally include details for debugging if needed, but might be too verbose for AI
        # if error.details: msg += f"\nDetails: {json.dumps(error.details)}"
        return msg
    elif isinstance(error, (YouTubeTranscriptApi.CouldNotRetrieveTranscript, TranscriptsDisabled)):
         return f"YouTube Transcript Error: {str(error)}"
    else:
        # Generic fallback
        return f"An unexpected error occurred: {str(error)}"


# --- MCP Tool Definitions ---

@mcp.tool()
async def search_roam(search_terms: List[str]) -> str:
    """Search Roam database for content containing the specified terms. Returns combined results.

    Args:
        search_terms: List of keywords to search for. Case-sensitivity depends on Roam setup.
    """
    if not validate_environment_and_log(): # Ensure env vars are checked on each call? Or just at start? Let's check each time.
        return "Configuration Error: ROAM_API_TOKEN and ROAM_GRAPH_NAME must be set. See server logs for details."
    
    try:
        if not search_terms or not isinstance(search_terms, list):
            raise ValidationError("Please provide a list of search terms.", "search_terms")
        
        all_matches = []
        combined_message = ""
        
        # Run search for each term
        for term in search_terms:
            if not isinstance(term, str) or not term.strip(): continue # Skip empty terms
            
            result = search.search_by_text(term.strip()) # Use module function
            if result["success"]:
                all_matches.extend(result["matches"])
            # Combine messages or just use the last one? Let's build a summary.
            combined_message += result.get("message", f"Search for '{term}' completed.") + "\n"
            
        # Deduplicate results based on block_uid
        unique_matches = {match['block_uid']: match for match in all_matches}.values()
        
        # Limit total output size (e.g., by word count or number of results)
        # Simple limit by number of results for now
        MAX_RESULTS = 50
        limited_matches = list(unique_matches)[:MAX_RESULTS]
        
        if not limited_matches:
            return f"No results found containing any of the terms: {', '.join(search_terms)}"
            
        # Format the output
        output_lines = [f"Found {len(limited_matches)} unique blocks (limit {MAX_RESULTS}) matching terms: {', '.join(search_terms)}"]
        for match in limited_matches:
            page_info = f"Page: {match.get('page_title', 'Unknown')}"
            content = match.get('content', '')
            output_lines.append(f"\n---\n{page_info}\nUID: {match.get('block_uid', 'N/A')}\n{content}")
            
        return "\n".join(output_lines)
        
    except Exception as e:
        logger.error(f"Error in search_roam tool: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_fetch_page_by_title(title: str) -> str:
    """Retrieve complete page contents by exact title (case-insensitive fallback), including nested blocks and resolved references.

    Args:
        title: Title of the page (e.g., "My Project Notes", "January 1st, 2024").
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
    
    try:
        if not title or not isinstance(title, str):
            raise ValidationError("Page title must be provided as a string.", "title")
        
        # get_page_content uses the client and handles finding/errors
        content = get_page_content(title.strip())
        return content
        
    except Exception as e: # Catch errors from get_page_content
        logger.error(f"Error in roam_fetch_page_by_title: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_create_page(title: str, content: Optional[List[Dict[str, Any]]] = None) -> str:
    """Create a new page. Optionally add initial content specified with text and nesting level.

    Args:
        title: Title for the new page.
        content: Optional list of blocks, e.g., [{"text": "Item 1", "level": 1}, {"text": "Sub A", "level": 2}].
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
    
    try:
        if not title or not isinstance(title, str):
            raise ValidationError("Page title must be provided as a non-empty string.", "title")
        
        result = content_ops.create_page(title.strip(), content) # Use module function
        
        if result["success"]:
            msg = f"Page '{title}' created/found successfully."
            if "page_url" in result: msg += f" URL: {result['page_url']}"
            if "created_uids" in result and result["created_uids"]: msg += f" Added {len(result['created_uids'])} content blocks."
            return msg
        else:
            # Use the error message from the result dict
            return f"Error creating page '{title}': {result.get('error', 'Unknown error')}"
            
    except Exception as e:
        logger.error(f"Error in roam_create_page tool: {str(e)}", exc_info=True)
        # Format error using the custom hierarchy if possible
        return format_error_response(e)


@mcp.tool()
async def roam_create_block(content: str, page_uid: Optional[str] = None, title: Optional[str] = None) -> str:
    """Add a new block. Defaults to today's daily note if no page context is given. Handles multi-line markdown input.

    Args:
        content: Content for the block (can include markdown).
        page_uid: Optional UID of the target page.
        title: Optional title of the target page (used if page_uid is absent, finds or creates page).
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
        
    try:
        if not content or not isinstance(content, str):
             raise ValidationError("Block content must be provided as a non-empty string.", "content")
             
        result = content_ops.create_block(content, page_uid, title) # Use module function
        
        if result["success"]:
            block_uid = result.get("block_uid", "N/A")
            parent_uid = result.get("parent_uid", "N/A")
            num_created = len(result.get("created_uids", [block_uid])) # Count all UIDs if nested
            return f"Block(s) created successfully (Top UID: {block_uid}, Count: {num_created}) under parent: {parent_uid}"
        else:
            return f"Error creating block: {result.get('error', 'Unknown error')}"
            
    except Exception as e:
        logger.error(f"Error in roam_create_block tool: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_create_outline(outline: List[Dict[str, Any]], page_title_uid: Optional[str] = None, block_text_uid: Optional[str] = None) -> str:
    """Add a structured outline under a page or block. Specify items with 'text' and 'level'.

    Args:
        outline: List of outline items, e.g., [{"text": "Topic 1", "level": 1}, {"text": "Detail A", "level": 2}].
        page_title_uid: Optional target page (title or UID). Defaults to daily page.
        block_text_uid: Optional parent block (text or UID) to nest under. If text, finds/creates block. Defaults to page root.
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
        
    try:
        if not outline or not isinstance(outline, list):
            raise ValidationError("Outline must be provided as a non-empty list.", "outline")
            
        result = content_ops.create_outline(outline, page_title_uid, block_text_uid) # Use module function
        
        if result["success"]:
            count = len(result.get("created_uids", []))
            parent_display = result.get('parent_uid', 'N/A')
            page_display = result.get('page_uid', 'N/A')
            # Optionally lookup parent/page titles for better message? Maybe too slow.
            return f"Outline created successfully with {count} top-level items under parent {parent_display} on page {page_display}."
        else:
            return f"Error creating outline: {result.get('error', 'Unknown error')}"
            
    except Exception as e:
        logger.error(f"Error in roam_create_outline tool: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_import_markdown(content: str, page_uid: Optional[str] = None, page_title: Optional[str] = None,
                            parent_uid: Optional[str] = None, parent_string: Optional[str] = None, 
                            order: str = "last") -> str:
    """Import nested markdown content under a specific block or page. Converts common markdown.

    Args:
        content: Markdown text (can be multi-line, nested lists, etc.).
        page_uid: Optional UID of the target page.
        page_title: Optional title of the target page (finds/creates if no page_uid).
        parent_uid: Optional UID of the parent block to nest under.
        parent_string: Optional exact text of the parent block (requires page context).
        order: Where to add top-level items ("first" or "last"). Default: "last".
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
        
    try:
        if not content or not isinstance(content, str):
             # Allow empty content import? Let's treat as success no-op.
             return "Content was empty, nothing imported."
             # raise ValidationError("Markdown content must be provided as a non-empty string.", "content")

        if order not in ["first", "last"]:
             raise ValidationError("Order must be 'first' or 'last'.", "order")
             
        result = content_ops.import_markdown(content, page_uid, page_title, parent_uid, parent_string, order) # Use module function
        
        if result["success"]:
            count = len(result.get("created_uids", []))
            parent_display = result.get('parent_uid', 'N/A')
            page_display = result.get('page_uid', 'N/A')
            message = result.get("message", f"Markdown imported successfully, creating {count} top-level blocks under parent {parent_display} on page {page_display}.")
            return message
        else:
            return f"Error importing markdown: {result.get('error', 'Unknown error')}"
            
    except Exception as e:
        logger.error(f"Error in roam_import_markdown tool: {str(e)}", exc_info=True)
        return format_error_response(e)

@mcp.tool()
async def roam_add_todo(todos: List[str]) -> str:
    """Add multiple todo items (each as a block with {{[[TODO]]}}) to today's daily page.

    Args:
        todos: List of strings, each representing a todo item.
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
        
    try:
        if not todos or not isinstance(todos, list):
             raise ValidationError("Provide a list of todo strings.", "todos")
             
        result = content_ops.add_todos(todos) # Use module function
        
        if result["success"]:
            count = len(result.get("created_uids", []))
            page_uid = result.get("page_uid", "N/A")
            return f"Added {count} TODO items to daily page {page_uid}."
        else:
            return f"Error adding todos: {result.get('error', 'Unknown error')}"
            
    except Exception as e:
        logger.error(f"Error in roam_add_todo tool: {str(e)}", exc_info=True)
        return format_error_response(e)


# --- Search Tools ---

def _format_search_results(result_dict: Dict[str, Any]) -> str:
    """Helper to format search results for MCP response."""
    if not result_dict["success"]:
        return f"Search failed: {result_dict.get('message', 'Unknown error')}"
        
    matches = result_dict.get("matches", [])
    message = result_dict.get("message", f"Found {len(matches)} results.")
    
    if not matches:
        return message # Return "No results found..." message

    output_lines = [message]
    MAX_RESULTS_DISPLAY = 30 # Limit display length
    for i, match in enumerate(matches):
         if i >= MAX_RESULTS_DISPLAY:
              output_lines.append(f"\n... (truncated {len(matches) - MAX_RESULTS_DISPLAY} more results)")
              break
              
         page_info = f"Page: {match.get('page_title', 'Unknown')}"
         uid_info = f"UID: {match.get('block_uid', 'N/A')}"
         extra_info = ""
         if "depth" in match: extra_info += f" Depth: {match['depth']}"
         if "time" in match:
             ts = match['time'] / 1000 # Convert ms to s
             dt_obj = datetime.fromtimestamp(ts)
             time_type = match.get('time_type', 'Time')
             extra_info += f" {time_type.capitalize()}: {dt_obj.strftime('%Y-%m-%d %H:%M')}"
         
         content = match.get('content', '')
         output_lines.append(f"\n---\n{page_info} ({uid_info}){extra_info}\n{content}")
         
    return "\n".join(output_lines)


@mcp.tool()
async def roam_search_for_tag(primary_tag: str, page_title_uid: Optional[str] = None, near_tag: Optional[str] = None) -> str:
    """Search for blocks referencing a specific tag (page). Optionally filter if another tag is nearby in the block string.

    Args:
        primary_tag: The tag to search for (e.g., "My Project"). Do not include # or [[ ]].
        page_title_uid: Optional: Title or UID of a page to limit the search scope.
        near_tag: Optional: Another tag name. Results will only include blocks containing both the primary tag reference and this near tag text.
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
    try:
        if not primary_tag or not isinstance(primary_tag, str):
            raise ValidationError("primary_tag must be provided as a string.", "primary_tag")
            
        result = search.search_by_tag(primary_tag, page_title_uid, near_tag)
        return _format_search_results(result)
    except Exception as e:
        logger.error(f"Error in roam_search_for_tag tool: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_search_by_status(status: str, page_title_uid: Optional[str] = None, 
                              include: Optional[str] = None, exclude: Optional[str] = None) -> str:
    """Find blocks marked with {{[[TODO]]}} or {{[[DONE]]}}. Optionally filter by included/excluded keywords in the block content.

    Args:
        status: Status to search for ("TODO" or "DONE").
        page_title_uid: Optional: Title or UID of a page to limit scope.
        include: Optional: Comma-separated keywords. Block must contain at least one.
        exclude: Optional: Comma-separated keywords. Block must contain none of these.
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
    try:
        # Validation is handled within search_by_status
        result = search.search_by_status(status, page_title_uid, include, exclude)
        return _format_search_results(result)
    except Exception as e:
        logger.error(f"Error in roam_search_by_status tool: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_search_block_refs(block_uid: Optional[str] = None, page_title_uid: Optional[str] = None) -> str:
    """Find blocks containing block references `((...))`. If block_uid is given, finds references *to* that specific block.

    Args:
        block_uid: Optional: Find references pointing to this specific 9-char block UID.
        page_title_uid: Optional: Title or UID of a page to limit scope.
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
    try:
        result = search.search_block_refs(block_uid, page_title_uid)
        return _format_search_results(result)
    except Exception as e:
        logger.error(f"Error in roam_search_block_refs tool: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_search_hierarchy(parent_uid: Optional[str] = None, child_uid: Optional[str] = None,
                              page_title_uid: Optional[str] = None, max_depth: int = 1) -> str:
    """Explore block hierarchy. Provide parent_uid to find descendants, or child_uid to find ancestors.

    Args:
        parent_uid: Optional: Find children/descendants of this block UID.
        child_uid: Optional: Find parents/ancestors of this block UID.
        page_title_uid: Optional: Title or UID of a page to limit scope (searches blocks ON this page).
        max_depth: Max hierarchy levels to traverse (1-10). Default: 1.
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
    try:
        result = search.search_hierarchy(parent_uid, child_uid, page_title_uid, max_depth)
        # Formatting includes depth info
        return _format_search_results(result)
    except Exception as e:
        logger.error(f"Error in roam_search_hierarchy tool: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_find_pages_modified_today(max_num_pages: int = 50) -> str:
    """Get a list of page titles that have been modified since midnight today.

    Args:
        max_num_pages: Max number of page titles to return. Default: 50.
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
    try:
        result = search.find_pages_modified_today(max_num_pages)
        if result["success"]:
            pages = result.get("pages", [])
            message = result.get("message", f"Found {len(pages)} pages.")
            if not pages: return message
            output = message + "\n\n" + "\n".join([f"- {title}" for title in pages])
            return output
        else:
             return f"Error finding modified pages: {result.get('message', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error in roam_find_pages_modified_today tool: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_search_by_text(text: str, page_title_uid: Optional[str] = None) -> str:
    """Search for blocks containing specific text. Default is case-sensitive (like Roam).

    Args:
        text: The text string to search for within block content.
        page_title_uid: Optional: Title or UID of a page to limit scope.
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
    try:
        if not text or not isinstance(text, str):
             raise ValidationError("Search text must be provided as a non-empty string.", "text")
             
        # Using case_sensitive=True by default, matching TS behavior and likely Roam's default
        result = search.search_by_text(text, page_title_uid, case_sensitive=True)
        return _format_search_results(result)
    except Exception as e:
        logger.error(f"Error in roam_search_by_text tool: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_update_block(block_uid: str, content: Optional[str] = None, 
                          transform_pattern: Optional[Dict[str, Any]] = None) -> str:
    """Update a single block's content. Use 'content' to replace, or 'transform_pattern' to modify with regex.

    Args:
        block_uid: The 9-character UID of the block to update.
        content: Optional: The new string content for the block. Replaces existing content.
        transform_pattern: Optional: Modify existing content. Dict with {'find': 'regex', 'replace': 'string', 'global'?: bool (default True)}.
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
    try:
        # Validation is handled within update_content
        result = content_ops.update_content(block_uid, content, transform_pattern)
        if result["success"]:
            final_content = result.get('content', '[Content Unavailable]')
            # Limit length for response message
            display_content = final_content[:200] + ('...' if len(final_content) > 200 else '')
            return f"Block {block_uid} updated successfully. New content:\n{display_content}"
        else:
            return f"Error updating block {block_uid}: {result.get('error', 'Unknown error')}"
    except Exception as e:
        logger.error(f"Error in roam_update_block tool: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_update_multiple_blocks(updates: List[Dict[str, Any]]) -> str:
    """Update multiple blocks efficiently in batches. Each item needs 'block_uid' and either 'content' or 'transform'.

    Args:
        updates: List of update operations. Ex: [{"block_uid": "...", "content": "New"}, {"block_uid": "...", "transform": {"find": "old", "replace": "new"}}].
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
    try:
        result = content_ops.update_multiple_contents(updates)
        
        # Provide a summary message and maybe details of failures
        message = result.get("message", "Batch update process finished.")
        if not result.get("success", True): # If overall success is false
             failed_updates = [res for res in result.get("results", []) if not res.get("success")]
             if failed_updates:
                  message += f" Failures occurred: {len(failed_updates)} blocks failed to update."
                  # Optionally list first few errors
                  errors_preview = [f"UID {f.get('block_uid', 'N/A')}: {f.get('error', 'Unknown')}" for f in failed_updates[:3]]
                  message += " First few errors: " + "; ".join(errors_preview)
                  
        return message # Return summary message
        # Alternatively, return JSON string of full results if needed
        # return json.dumps(result, indent=2)
        
    except Exception as e:
        logger.error(f"Error in roam_update_multiple_blocks tool: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_search_by_date(start_date: str, end_date: Optional[str] = None,
                            type_filter: str = "created", scope: str = "blocks",
                            include_content: bool = True) -> str:
    """Search for blocks or pages by creation or modification date range (YYYY-MM-DD format).

    Args:
        start_date: Start date (YYYY-MM-DD). Required.
        end_date: Optional end date (YYYY-MM-DD). Defaults to today.
        type_filter: Filter by 'created', 'modified', or 'both'. Default: 'created'.
        scope: Search 'blocks', 'pages', or 'both'. Default: 'blocks'.
        include_content: Include block content or page title in results. Default: True.
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
    try:
        # Validation is done within search_by_date
        result = search.search_by_date(start_date, end_date, type_filter, scope, include_content)
        # Formatting includes type and time info
        return _format_search_results(result)
    except Exception as e:
        logger.error(f"Error in roam_search_by_date tool: {str(e)}", exc_info=True)
        return format_error_response(e)


# --- Memory Tools ---

@mcp.tool()
async def roam_remember(memory: str, categories: Optional[List[str]] = None) -> str:
    """Store a memory or piece of information on today's daily page, tagged with a configured memory tag (default #[[Memories]]).

    Args:
        memory: The text content of the memory to store.
        categories: Optional list of category strings to add as additional tags (e.g., ["Work", "Project Alpha"]).
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
    try:
        if not memory or not isinstance(memory, str):
             raise ValidationError("Memory text must be provided as a non-empty string.", "memory")
             
        result = memory_ops.remember(memory, categories) # Use module function
        
        if result["success"]:
            content = result.get('content', '[Content Unavailable]')
            display_content = content[:200] + ('...' if len(content) > 200 else '')
            return f"Memory stored successfully with UID {result.get('block_uid', 'N/A')}. Content:\n{display_content}"
        else:
            return f"Error storing memory: {result.get('error', 'Unknown error')}"
            
    except Exception as e:
        logger.error(f"Error in roam_remember tool: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def roam_recall(sort_by: str = "newest", filter_tag: Optional[str] = None) -> str:
    """Retrieve stored memories. Searches blocks with the memory tag AND blocks on the dedicated memory page. Deduplicates results.

    Args:
        sort_by: Sort order: "newest" or "oldest" (by creation time). Default: "newest".
        filter_tag: Optional: Only include memories that also contain this tag (text search within memory block).
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
    try:
        if sort_by not in ["newest", "oldest"]:
             raise ValidationError("sort_by must be 'newest' or 'oldest'.", "sort_by")
             
        result = memory_ops.recall(sort_by, filter_tag) # Use module function
        
        if result["success"]:
             memories = result.get("memories", [])
             message = result.get("message", f"Found {len(memories)} memories.")
             if not memories: return message
             
             # Format output list
             output = message + "\n\n" + "\n".join([f"- {mem}" for mem in memories])
             # Limit total length?
             MAX_LEN = 8000 # Example limit
             if len(output) > MAX_LEN:
                  output = output[:MAX_LEN] + "\n... (truncated)"
             return output
        else:
            return f"Error recalling memories: {result.get('error', 'Unknown error')}"
            
    except Exception as e:
        logger.error(f"Error in roam_recall tool: {str(e)}", exc_info=True)
        return format_error_response(e)


# --- Other Tools ---

@mcp.tool()
async def roam_datomic_query(query: str, inputs: Optional[List[Any]] = None) -> str:
    """Execute a custom Datalog query against the Roam graph. Use with caution for advanced data retrieval.

    Args:
        query: The Datalog query string (e.g., "[:find ?title :where [?p :node/title ?title]]").
        inputs: Optional list of input parameters for parameterized queries (e.g., ["My Page Title"]).
    """
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
    try:
        if not query or not isinstance(query, str):
             raise ValidationError("Datalog query must be provided as a non-empty string.", "query")
             
        result = search.execute_datomic_query(query, inputs) # Use module function
        # Format results
        return _format_search_results(result)
    except Exception as e:
        logger.error(f"Error in roam_datomic_query tool: {str(e)}", exc_info=True)
        return format_error_response(e)


@mcp.tool()
async def get_youtube_transcript(url: str) -> str:
    """Fetch and return the transcript of a YouTube video using its URL.

    Args:
        url: The full URL of the YouTube video (e.g., "https://www.youtube.com/watch?v=dQw4w9WgXcQ").
    """
    # No Roam environment needed for this tool
    logger.info(f"Fetching transcript for YouTube URL: {url}")
    video_id = extract_youtube_video_id(url)
    if not video_id:
        return "Invalid YouTube URL provided. Could not extract video ID."

    try:
        # Prioritize manually created English transcripts
        preferred_langs = ['en', 'en-US', 'en-GB']
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)
        
        transcript = None
        try:
             # Try finding manual transcript in preferred languages
             transcript = transcript_list.find_manually_created_transcript(preferred_langs)
             logger.debug(f"Found manual transcript in {transcript.language}")
        except YouTubeTranscriptApi.NoTranscriptFound:
             logger.debug("No manual English transcript found. Trying generated...")
             try:
                  # Try finding generated transcript in preferred languages
                  transcript = transcript_list.find_generated_transcript(preferred_langs)
                  logger.debug(f"Found generated transcript in {transcript.language}")
             except YouTubeTranscriptApi.NoTranscriptFound:
                  logger.warning(f"No English transcript (manual or generated) found for video {video_id}. Trying any language.")
                  # Fallback: Try fetching *any* available transcript
                  try:
                       # Get the first available transcript regardless of language
                       transcript = next(iter(transcript_list))
                       logger.debug(f"Found transcript in language: {transcript.language}")
                  except StopIteration:
                        return f"No transcripts available for this video (ID: {video_id})."

        # Fetch and format the transcript
        if transcript:
            transcript_data = transcript.fetch()
            full_text = " ".join([line["text"] for line in transcript_data])
            logger.info(f"Successfully fetched transcript for video {video_id} ({transcript.language}). Length: {len(full_text)}")
            # Limit length? Transcripts can be very long.
            MAX_TRANSCRIPT_LEN = 15000 # Example limit
            if len(full_text) > MAX_TRANSCRIPT_LEN:
                 full_text = full_text[:MAX_TRANSCRIPT_LEN] + "... (Transcript truncated)"
            return full_text
        else:
             # Should have been caught by inner exceptions, but safety net.
             return f"Could not find any transcript for video {video_id}."

    except TranscriptsDisabled:
        logger.warning(f"Transcripts disabled for video {video_id}.")
        return "Transcripts are disabled for this video."
    except Exception as e:
        logger.error(f"Error fetching YouTube transcript for {url}: {str(e)}", exc_info=True)
        return f"An error occurred while fetching the transcript: {str(e)}"


@mcp.tool()
async def get_roam_graph_info() -> str:
    """Get basic information about the configured Roam Research graph (name, page/block counts)."""
    if not validate_environment_and_log():
        return "Configuration Error: Required Roam credentials missing."
        
    client = get_client() # Get client instance
    
    try:
        page_count = "Error"
        block_count = "Error"

        # Get page count
        try:
            query_page = "[:find (count ?p) . :where [?p :node/title]]" # Added '.' for scalar result
            count_result = client.query(query_page)
            if isinstance(count_result, int): page_count = str(count_result)
            else: logger.warning(f"Unexpected page count result type: {type(count_result)}")
        except QueryError as e:
             logger.error(f"Failed to get page count: {e}")

        # Get block count
        try:
            query_block = "[:find (count ?b) . :where [?b :block/string]]" # Added '.' for scalar result
            count_result = client.query(query_block)
            if isinstance(count_result, int): block_count = str(count_result)
            else: logger.warning(f"Unexpected block count result type: {type(count_result)}")
        except QueryError as e:
             logger.error(f"Failed to get block count: {e}")
        
        # Format output
        graph_name = client.graph_name # Get from client instance
        memory_tag = get_memories_tag()
        
        output = f"""--- Roam Graph Info ---
Graph Name: {graph_name}
Page Count: {page_count}
Block Count: {block_count}
Memory Tag: {memory_tag}
-----------------------"""
        return output
        
    except Exception as e:
        logger.error(f"Error retrieving graph information: {str(e)}", exc_info=True)
        return format_error_response(e)


# --- MCP Prompts ---

@mcp.prompt()
async def summarize_page(page_title: str) -> dict:
    """Generates a prompt asking the AI to summarize a specific Roam page's content.

    Args:
        page_title: The title of the Roam page to summarize.
    """
    if not validate_environment_and_log():
        # Return an error message within the prompt structure
        return {
            "messages": [{
                "role": "user",
                "content": "Configuration Error: Cannot summarize Roam page because ROAM_API_TOKEN and ROAM_GRAPH_NAME are not set correctly. Please check the server configuration."
            }]
        }
    
    try:
        # Fetch the page content using the helper
        page_content = get_page_content(page_title.strip())
        
        # Create the prompt for the AI
        return {
            "messages": [{
                "role": "user",
                "content": f"Please provide a concise summary of the following content from my Roam Research page titled '{page_title}':\n\n```markdown\n{page_content}\n```"
            }]
        }
    except PageNotFoundError as e:
         logger.warning(f"Page not found for summary prompt: {page_title}")
         return {
             "messages": [{
                 "role": "user",
                 "content": f"I asked to summarize my Roam page titled '{page_title}', but the page could not be found. Please double-check the title or confirm the page exists."
             }]
         }
    except Exception as e:
        # Generic error message if fetching content fails
        logger.error(f"Error creating summary prompt for page '{page_title}': {str(e)}", exc_info=True)
        error_info = format_error_response(e)
        return {
            "messages": [{
                "role": "user",
                "content": f"I wanted to summarize my Roam page titled '{page_title}', but an error occurred while retrieving the content:\n\n{error_info}\n\nCan you help me understand this error or suggest how to fix my Roam integration?"
            }]
        }


# --- Server Runner ---

def run_server(transport="stdio", port=None, verbose=False):
    """Configure logging, validate environment, and run the MCP server."""
    setup_logging(verbose)
    logger.info("--- Starting Roam MCP Server ---")
    
    # Validate environment and log initial status/instructions
    is_env_valid = validate_environment_and_log()
    
    # Even if env is invalid, we might still run the server to allow
    # potentially fixing it or using non-Roam tools?
    # For now, let's proceed but log a strong warning.
    if not is_env_valid:
         logger.error("!!! Environment validation failed. Roam tools will likely fail until configuration is corrected. See logs above for details. !!!")
         # sys.exit(1) # Optionally exit immediately

    # Run the FastMCP server
    try:
        if transport == "stdio":
            logger.info("Starting server with STDIO transport.")
            mcp.run(transport="stdio")
        elif transport == "sse":
            port = port or 3000
            logger.info(f"Starting server with SSE transport on port {port}.")
            mcp.run(transport="sse", port=int(port)) # Ensure port is int
        else:
            logger.error(f"Unsupported transport specified: '{transport}'. Use 'stdio' or 'sse'.")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Server stopped by user (KeyboardInterrupt).")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Critical error running server: {str(e)}", exc_info=True)
        traceback.print_exc(file=sys.stderr) # Ensure traceback is visible
        sys.exit(1)
    finally:
        logger.info("--- Roam MCP Server stopped ---")