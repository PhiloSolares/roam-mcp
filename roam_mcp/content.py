"""Content operations for the Roam MCP server (pages, blocks, and outlines)."""

from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import re
import logging

# Use client from api module
from roam_mcp.api import (
    get_client, # Import function to get client instance
    find_or_create_page,
    get_daily_page,
    add_block_to_page,
    update_block,
    batch_update_blocks,
    # execute_query, # No longer used directly
    # execute_write_action, # No longer used directly
    execute_batch_actions, # Still used, wraps client.write
    ValidationError,
    BlockNotFoundError,
    PageNotFoundError,
    TransactionError
)
# Import utils needed here
from roam_mcp.utils import (
    format_roam_date,
    convert_to_roam_markdown,
    parse_markdown_list,
    process_nested_content, # This now needs the client passed
    # find_block_uid, # Now in utils, needs client
    create_block_action
)
# Import finders from utils
from roam_mcp.utils import find_block_uid as find_block_uid_util
from roam_mcp.utils import find_page_by_title as find_page_by_title_util

# Set up logging
logger = logging.getLogger("roam-mcp.content")


def create_page(title: str, content: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """
    Create a new page in Roam Research, potentially with initial content.
    Uses find_or_create_page helper which utilizes the RoamClient.
    Handles batch creation of initial content.

    Args:
        title: Title for the new page.
        content: Optional list of content dictionaries ({text: str, level: int, heading_level?: int}).

    Returns:
        Dictionary with success status, UID, URL, and optionally created UIDs.
    """
    if not title:
        return {"success": False, "error": "Title is required"}
    
    client = get_client() # Get client instance
    graph_name = client.graph_name # Get graph name from client

    try:
        # find_or_create_page handles finding or creating via the client
        page_uid = find_or_create_page(title)
        
        created_content_uids = []
        if content:
            logger.info(f"Adding initial content ({len(content)} items) to new/existing page '{title}' (UID: {page_uid})")
            # Validate content structure
            invalid_items = [
                item for item in content 
                if not isinstance(item.get("text"), str) or not isinstance(item.get("level"), int)
            ]
            if invalid_items:
                raise ValidationError("Invalid content structure - each item must have text (string) and level (integer)", details={"invalid_items": invalid_items[:3]})

            # Check for invalid level jumps (Roam supports level 1 directly under page)
            prev_level = 0
            for item in content:
                level = item["level"]
                if level < 1:
                     raise ValidationError(f"Invalid level {level}. Levels must be 1 or greater.", "level", {"item": item})
                # Allow level 1 under page (level 0). Level > 1 needs parent at level-1.
                if level > 1 and level > prev_level + 1:
                    raise ValidationError(f"Invalid outline structure - level {level} cannot follow level {prev_level}", "level", {"item": item})
                prev_level = level

            # Use process_nested_content utility which handles batching and dependencies
            # Pass the client instance to it
            created_content_uids = process_nested_content_util(content, page_uid, client)

        return {
            "success": True,
            "uid": page_uid,
            "created_uids": created_content_uids, # UIDs of blocks added as content
            "page_url": f"https://roamresearch.com/#/app/{graph_name}/page/{page_uid}"
        }
        
    except (ValidationError, TransactionError, PageNotFoundError) as e:
         logger.error(f"Failed to create page '{title}' or add content: {e}", exc_info=True)
         return {"success": False, "error": str(e)}
    except Exception as e:
         logger.error(f"Unexpected error creating page '{title}': {e}", exc_info=True)
         return {"success": False, "error": f"Unexpected error: {e}"}


def create_block(content: str, page_uid: Optional[str] = None, page_title: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a new block in Roam Research, handling single or multi-line content.
    Uses helpers which utilize the RoamClient.

    Args:
        content: Block content (can be multi-line markdown).
        page_uid: Optional UID of the target page.
        page_title: Optional title of the target page (used if page_uid is not provided).
        
    Returns:
        Dictionary with success status, block UID, parent UID, and potentially more created UIDs for multi-line.
    """
    if not content:
        return {"success": False, "error": "Content is required"}

    client = get_client() # Get client instance
    
    try:
        # Determine target page UID using helpers that use the client
        target_page_uid: Optional[str] = None
        if page_uid:
            # Verify page_uid exists? Optional, find_or_create_page does validation if title is used.
            # For now, assume provided UID is correct. Add verification if needed.
             target_page_uid = page_uid
        elif page_title:
            target_page_uid = find_or_create_page(page_title) # Handles find/create
        else:
            target_page_uid = get_daily_page() # Handles find/create

        if not target_page_uid:
             # Should not happen if helpers work correctly, but safety check.
             raise PageNotFoundError("Could not determine target page UID.")

        # Handle multi-line content via markdown parsing and batching
        if "\n" in content or content.strip().startswith(("- ", "* ", "+ ")): # Check for list markers too
            logger.debug("Content contains newline or list marker, processing as nested structure.")
            # Convert and parse the markdown
            roam_markdown = convert_to_roam_markdown(content)
            parsed_structure = parse_markdown_list(roam_markdown)

            if not parsed_structure:
                raise ValidationError("Failed to parse multi-line content into a structure.", details={"content": content[:100]})

            # Use process_nested_content for creation via batching
            created_uids = process_nested_content_util(parsed_structure, target_page_uid, client)
            
            if not created_uids:
                 # This might happen if parsing yielded structure but creation failed or returned nothing
                 raise TransactionError("Block creation process completed but returned no UIDs.", "batch-create", {"content": content[:100]})

            return {
                "success": True,
                "block_uid": created_uids[0], # UID of the first top-level block created
                "parent_uid": target_page_uid,
                "created_uids": created_uids # All UIDs created
            }
        else:
            # Single line content, create a simple block using the helper
            logger.debug("Content is single line, creating simple block.")
            block_uid = add_block_to_page(target_page_uid, content) # Helper uses client
            return {
                "success": True,
                "block_uid": block_uid,
                "parent_uid": target_page_uid
            }

    except (ValidationError, PageNotFoundError, BlockNotFoundError, TransactionError) as e:
        logger.error(f"Failed to create block: {e}", exc_info=True)
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error creating block: {e}", exc_info=True)
        return {"success": False, "error": f"Unexpected error: {e}"}


def create_outline(outline: List[Dict[str, Any]], page_title_uid: Optional[str] = None, block_text_uid: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a structured outline under a specified page or block.
    Uses helpers which utilize the RoamClient.

    Args:
        outline: List of dictionaries [{text: str, level: int, heading_level?: int}].
        page_title_uid: Optional target page (title or UID). Defaults to daily page.
        block_text_uid: Optional parent block (text or UID) under the target page. Defaults to page root.

    Returns:
        Dictionary with success status, page UID, parent UID, and created UIDs.
    """
    if not outline:
        return {"success": False, "error": "Outline cannot be empty"}
    
    # Basic validation of outline structure
    if not all(isinstance(item, dict) and isinstance(item.get("text"), str) and isinstance(item.get("level"), int) for item in outline):
         raise ValidationError("Invalid outline structure: Each item must be a dict with 'text' (str) and 'level' (int).", "outline")

    client = get_client() # Get client instance

    try:
        # Determine target page UID
        target_page_uid: Optional[str] = None
        if page_title_uid:
            target_page_uid = find_or_create_page(page_title_uid) # Handles find/create/validation
        else:
            target_page_uid = get_daily_page() # Handles find/create

        if not target_page_uid:
             raise PageNotFoundError("Could not determine target page for outline.")

        # Determine parent block UID (either page root or specified block)
        parent_block_uid = target_page_uid # Default to page root

        if block_text_uid:
            logger.debug(f"Looking for or creating parent block '{block_text_uid}' under page {target_page_uid}")
            # Check if it's a UID
            if len(block_text_uid) == 9 and re.match(r'^[a-zA-Z0-9_-]{9}$', block_text_uid):
                 # Verify block exists (optional but good practice)
                 try:
                     block_data = client.pull("[:block/uid]", block_text_uid)
                     if block_data and block_data.get(':block/uid') == block_text_uid:
                          parent_block_uid = block_text_uid
                          logger.debug(f"Found existing block by UID: {block_text_uid}")
                     else:
                          raise BlockNotFoundError(block_text_uid, {"message": "Provided UID does not exist or is not a block."})
                 except QueryError as e: # Includes pull errors
                      raise BlockNotFoundError(block_text_uid, {"message": f"Error verifying block UID: {e}"})
            else:
                 # Not a UID, treat as text. Find or create this block under the page.
                 # Try finding first
                 found_uid = find_block_uid_util(client, block_text_uid) # Find anywhere first? Or scoped to page? Let's scope.
                 
                 # Scoped find query:
                 query = f'''[:find ?uid .
                            :in $ ?page_uid ?text
                            :where [?p :block/uid ?page_uid]
                                   [?b :block/page ?p] ; or :block/parents ?p for direct child?
                                   [?b :block/string ?text]
                                   [?b :block/uid ?uid]]'''
                 scoped_result = client.query(query, inputs=[target_page_uid, block_text_uid])

                 if scoped_result and isinstance(scoped_result, str):
                      parent_block_uid = scoped_result
                      logger.debug(f"Found existing block by text '{block_text_uid}' on page: {parent_block_uid}")
                 else:
                      # Create the block if not found
                      logger.debug(f"Creating header block '{block_text_uid}' on page {target_page_uid}")
                      parent_block_uid = add_block_to_page(target_page_uid, block_text_uid) # Uses client

        # Validate outline levels before creating
        prev_level = 0
        for item in outline:
            level = item["level"]
            if level < 1: raise ValidationError("Levels must be 1 or greater.", "level", {"item": item})
            # Allow level 1 under the target parent.
            # Subsequent levels must be <= prev_level + 1
            if level > 1 and level > prev_level + 1:
                 raise ValidationError(f"Invalid outline structure - level {level} cannot follow level {prev_level}", "level", {"item": item})
            prev_level = level

        # Process the nested outline structure using the utility
        logger.info(f"Creating outline under parent UID: {parent_block_uid}")
        created_uids = process_nested_content_util(outline, parent_block_uid, client)

        if not created_uids:
             # Should ideally not happen if outline was valid, but indicates potential issue.
             logger.warning(f"Outline creation process finished but yielded no UIDs under {parent_block_uid}.")
             # Return success but with empty list? Or error? Let's return success with warning logged.

        return {
            "success": True,
            "page_uid": target_page_uid,
            "parent_uid": parent_block_uid, # The UID where the outline starts
            "created_uids": created_uids # UIDs of the top-level outline items created
        }

    except (ValidationError, PageNotFoundError, BlockNotFoundError, TransactionError) as e:
        logger.error(f"Failed to create outline: {e}", exc_info=True)
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error creating outline: {e}", exc_info=True)
        return {"success": False, "error": f"Unexpected error: {e}"}


def import_markdown(content: str, page_uid: Optional[str] = None, page_title: Optional[str] = None,
                   parent_uid: Optional[str] = None, parent_string: Optional[str] = None,
                   order: str = "last") -> Dict[str, Any]:
    """
    Import markdown content into a specified location in Roam.
    Uses helpers which utilize the RoamClient.

    Args:
        content: Markdown content string.
        page_uid: Optional target page UID.
        page_title: Optional target page title (used if page_uid is None).
        parent_uid: Optional target parent block UID (takes precedence over parent_string).
        parent_string: Optional target parent block's exact string content (requires page context).
        order: Where to add the top-level items ("first" or "last"). Defaults to "last".

    Returns:
        Dictionary with success status, page UID, parent UID, and created UIDs.
    """
    if not content:
        return {"success": False, "error": "Content cannot be empty"}
    if order not in ["first", "last"]:
        raise ValidationError("Order must be 'first' or 'last'", "order")

    client = get_client() # Get client instance

    try:
        # 1. Determine Target Page UID
        target_page_uid: Optional[str] = None
        if page_uid:
            target_page_uid = page_uid # Assume valid if provided
        elif page_title:
            target_page_uid = find_or_create_page(page_title)
        elif parent_string and not parent_uid:
             # Need page context if finding parent by string
             raise ValidationError("page_uid or page_title is required when using parent_string without parent_uid.", "page_title_uid")
        else:
            # Default to daily page if no context given
            target_page_uid = get_daily_page()
            
        if not target_page_uid:
             raise PageNotFoundError("Could not determine target page for import.")

        # 2. Determine Parent Block UID
        parent_block_uid = target_page_uid # Default to page root

        if parent_uid:
            # Verify parent_uid exists (optional, assume valid for now)
            parent_block_uid = parent_uid
            logger.debug(f"Using specified parent block UID: {parent_uid}")
        elif parent_string:
            # Find parent block by string within the target page
            logger.debug(f"Searching for parent block with text '{parent_string}' on page {target_page_uid}")
            query = f'''[:find ?uid .
                       :in $ ?page_uid ?text
                       :where [?p :block/uid ?page_uid]
                              [?b :block/page ?p] ; Or :block/parents ?p ?
                              [?b :block/string ?text]
                              [?b :block/uid ?uid]]'''
            scoped_result = client.query(query, inputs=[target_page_uid, parent_string])
            if scoped_result and isinstance(scoped_result, str):
                 parent_block_uid = scoped_result
                 logger.debug(f"Found parent block by text: {parent_block_uid}")
            else:
                 raise BlockNotFoundError(f"Content: '{parent_string}'", {"page_uid": target_page_uid, "message": "Parent block string not found on the specified page."})

        # 3. Convert and Parse Markdown
        logger.debug("Converting and parsing markdown content.")
        roam_markdown = convert_to_roam_markdown(content)
        parsed_structure = parse_markdown_list(roam_markdown)

        if not parsed_structure:
             # Allow importing empty string? Maybe just log warning.
             logger.warning("Markdown content parsed into an empty structure. Nothing to import.")
             return {
                 "success": True,
                 "page_uid": target_page_uid,
                 "parent_uid": parent_block_uid,
                 "created_uids": [],
                 "message": "Markdown parsed as empty, no blocks created."
             }

        # 4. Process Nested Content (Handles Batching)
        # Note: process_nested_content needs modification to accept 'order' for top-level items.
        # Simplified: process_nested_content currently uses 'last' internally.
        # We need a way to handle 'first' or pass order down.
        # For now, the 'order' parameter only influences the *conceptual* placement,
        # the actual implementation in process_nested_content needs update.
        # Let's simulate 'first' by reversing and using 'last', then reversing result? Hacky.
        # TODO: Refactor process_nested_content to accept top-level order.
        if order == "first":
             logger.warning("Using 'first' order for import is not fully supported yet by process_nested_content, may behave like 'last'.")
             # parsed_structure.reverse() # Hacky attempt, might mess up children

        logger.info(f"Importing {len(parsed_structure)} top-level markdown items under parent {parent_block_uid}")
        created_uids = process_nested_content_util(parsed_structure, parent_block_uid, client)

        # if order == "first": created_uids.reverse() # Reverse UIDs if input was reversed

        return {
            "success": True,
            "page_uid": target_page_uid,
            "parent_uid": parent_block_uid,
            "created_uids": created_uids
        }

    except (ValidationError, PageNotFoundError, BlockNotFoundError, TransactionError) as e:
        logger.error(f"Failed to import markdown: {e}", exc_info=True)
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error importing markdown: {e}", exc_info=True)
        return {"success": False, "error": f"Unexpected error: {e}"}


def add_todos(todos: List[str]) -> Dict[str, Any]:
    """
    Add multiple todo items to today's daily page using batch actions.
    Uses helpers which utilize the RoamClient.

    Args:
        todos: List of todo item strings.

    Returns:
        Dictionary with success status, created UIDs, and page UID.
    """
    if not todos:
        return {"success": False, "error": "Todo list cannot be empty"}
    if not all(isinstance(todo, str) for todo in todos):
        raise ValidationError("All todo items must be strings.", "todos")

    client = get_client() # Get client instance

    try:
        daily_page_uid = get_daily_page() # Handles find/create

        actions = []
        for i, todo_text in enumerate(todos):
            if not todo_text.strip(): continue # Skip empty todos
            
            todo_content = f"{{{{[[TODO]]}}}} {todo_text.strip()}"
            action = create_block_action(
                parent_uid=daily_page_uid,
                content=todo_content,
                order="last" # Add todos to the end
            )
            actions.append(action)

        if not actions:
            return {"success": True, "created_uids": [], "page_uid": daily_page_uid, "message": "No non-empty todos provided."}

        logger.info(f"Adding {len(actions)} TODOs to daily page {daily_page_uid}")
        # Use execute_batch_actions which handles chunking and uses the client
        result = execute_batch_actions(actions)

        return {
            "success": result.get("success", False), # Reflect actual batch success
            "created_uids": result.get("created_uids", []),
            "page_uid": daily_page_uid
        }

    except (ValidationError, PageNotFoundError, TransactionError) as e:
        logger.error(f"Failed to add todos: {e}", exc_info=True)
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error adding todos: {e}", exc_info=True)
        return {"success": False, "error": f"Unexpected error: {e}"}


def update_content(block_uid: str, content: Optional[str] = None, transform_pattern: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Update a block's content directly or via transformation.
    Uses helpers which utilize the RoamClient.

    Args:
        block_uid: UID of the block to update.
        content: Optional new content string.
        transform_pattern: Optional dictionary with {'find': str, 'replace': str, 'global'?: bool}.

    Returns:
        Dictionary with success status and the final content.
    """
    if not block_uid:
        return {"success": False, "error": "Block UID is required"}
    if content is None and transform_pattern is None:
        return {"success": False, "error": "Either 'content' or 'transform_pattern' must be provided"}
    if content is not None and transform_pattern is not None:
         return {"success": False, "error": "Provide either 'content' or 'transform_pattern', not both"}

    # client = get_client() # Client is used within update_block/transform_block helpers

    try:
        if transform_pattern:
            # Validate transform structure
            if not isinstance(transform_pattern, dict) or "find" not in transform_pattern or "replace" not in transform_pattern:
                 raise ValidationError("Invalid 'transform_pattern' structure. Required keys: 'find', 'replace'. Optional: 'global'.", "transform_pattern")
            
            logger.debug(f"Transforming block {block_uid}")
            # transform_block helper uses the client
            final_content = transform_block(
                block_uid,
                transform_pattern["find"],
                transform_pattern["replace"],
                transform_pattern.get("global", True)
            )
            return {"success": True, "content": final_content}
            
        elif content is not None:
            logger.debug(f"Updating block {block_uid} with new content.")
            # update_block helper uses the client
            success = update_block(block_uid, content)
            if success:
                 return {"success": True, "content": content}
            else:
                 # Should not happen if update_block raises error, but safety catch
                 raise TransactionError("update_block reported failure.", "update-block", {"block_uid": block_uid})
        else:
             # Should be caught by initial validation
             raise ValidationError("Internal error: No operation specified for update_content.")

    except (ValidationError, BlockNotFoundError, TransactionError, QueryError) as e:
        logger.error(f"Failed to update content for block {block_uid}: {e}", exc_info=True)
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error updating content for block {block_uid}: {e}", exc_info=True)
        return {"success": False, "error": f"Unexpected error: {e}"}


def update_multiple_contents(updates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Update multiple blocks using batch_update_blocks helper.
    Uses helpers which utilize the RoamClient.

    Args:
        updates: List of update specifications [{'block_uid': str, 'content'?: str, 'transform'?: dict}].

    Returns:
        Dictionary with overall success status and list of individual results.
    """
    if not updates or not isinstance(updates, list):
        return {"success": False, "error": "Updates must be a non-empty list"}

    # client = get_client() # Used within batch_update_blocks helper
    
    try:
        # batch_update_blocks handles validation, preparation, execution, and results
        individual_results = batch_update_blocks(updates)
        
        successful_count = sum(1 for r in individual_results if r.get("success"))
        overall_success = successful_count == len(updates)
        
        return {
            "success": overall_success,
            "results": individual_results,
            "message": f"Attempted {len(updates)} updates. Successful: {successful_count}."
        }
    except (ValidationError, TransactionError) as e: # Catch errors from batch_update_blocks itself
         logger.error(f"Batch update process failed: {e}", exc_info=True)
         # Create a generic error result for all inputs if the process fails early
         error_results = [{"block_uid": u.get("block_uid", "N/A"), "success": False, "error": str(e)} for u in updates]
         return {"success": False, "results": error_results, "error": f"Batch update failed: {e}"}
    except Exception as e:
         logger.error(f"Unexpected error during multiple updates: {e}", exc_info=True)
         error_results = [{"block_uid": u.get("block_uid", "N/A"), "success": False, "error": f"Unexpected error: {e}"} for u in updates]
         return {"success": False, "results": error_results, "error": f"Unexpected error: {e}"}