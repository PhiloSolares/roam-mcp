"""Content operations for the Roam MCP server (pages, blocks, and outlines)."""

from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import re
import logging
import uuid
import time

from roam_mcp.api import (
    execute_query,
    execute_write_action,
    execute_batch_actions,
    get_session_and_headers,
    GRAPH_NAME,
    find_or_create_page,
    get_daily_page,
    add_block_to_page,
    update_block,
    batch_update_blocks,
    find_page_by_title,
    ValidationError,
    BlockNotFoundError,
    PageNotFoundError,
    TransactionError
)
from roam_mcp.utils import (
    format_roam_date,
    convert_to_roam_markdown,
    parse_markdown_list,
    process_nested_content,
    find_block_uid,
    create_block_action
)

# Set up logging
logger = logging.getLogger("roam-mcp.content")


def create_page(title: str, content: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """
    Create a new page in Roam Research with optional nested content.
    
    Args:
        title: Title for the new page
        content: Optional content as a list of dicts with 'text', optional 'level', and optional 'children'
        
    Returns:
        Result with page UID and created block UIDs
    """
    if not title:
        return {
            "success": False,
            "error": "Title is required"
        }
    
    session, headers = get_session_and_headers()
    
    try:
        # Create the page
        page_uid = find_or_create_page(title)
        
        # Add content if provided
        if content:
            # Validate content structure
            def validate_item(item, parent_level=0):
                if not isinstance(item.get("text"), str):
                    return "Each item must have 'text' as a string"
                level = item.get("level", parent_level + 1)
                if not isinstance(level, int):
                    return "'level' must be an integer"
                if level < 0:
                    return "'level' must be non-negative"
                # Don't enforce strict level validation as the debug script shows this works
                children = item.get("children", [])
                if not isinstance(children, list):
                    return "'children' must be a list"
                for child in children:
                    error = validate_item(child, level)
                    if error:
                        return error
                return None
            
            for item in content:
                error = validate_item(item, -1)  # Root level starts at -1 (page is 0)
                if error:
                    return {"success": False, "error": f"Invalid content structure - {error}"}
            
            # Process content in levels
            created_uids = []
            
            # Flatten the hierarchical content structure
            flattened_content = []
            
            def flatten_content(items, parent_level=-1):
                for item in items:
                    text = item.get("text", "")
                    level = item.get("level", parent_level + 1)
                    heading_level = item.get("heading_level", 0)
                    
                    flattened_content.append({
                        "text": text,
                        "level": level,
                        "heading_level": heading_level
                    })
                    
                    children = item.get("children", [])
                    if children:
                        flatten_content(children, level)
            
            flatten_content(content)
            
            # Sort by level
            flattened_content.sort(key=lambda x: x.get("level", 0))
            
            # Group by level
            level_items = {}
            for item in flattened_content:
                level = item.get("level", 0)
                if level not in level_items:
                    level_items[level] = []
                level_items[level].append(item)
            
            # Process level by level
            level_parent_map = {-1: page_uid}
            
            for level in sorted(level_items.keys()):
                batch_actions = []
                level_uids = []
                
                for item in level_items[level]:
                    text = item.get("text", "")
                    heading_level = item.get("heading_level", 0)
                    
                    # Find parent from previous level
                    parent_level = level - 1
                    if parent_level < -1:
                        parent_level = -1
                        
                    parent_uid = level_parent_map.get(parent_level, page_uid)
                    
                    # Generate UID
                    block_uid = str(uuid.uuid4())[:9]
                    level_uids.append(block_uid)
                    
                    # Create action
                    action = {
                        "action": "create-block",
                        "location": {
                            "parent-uid": parent_uid,
                            "order": "last"
                        },
                        "block": {
                            "string": text,
                            "uid": block_uid
                        }
                    }
                    
                    if heading_level and heading_level > 0 and heading_level <= 3:
                        action["block"]["heading"] = heading_level
                        
                    batch_actions.append(action)
                
                # Execute batch for this level
                if batch_actions:
                    result = execute_write_action(batch_actions)
                    
                    if "created_uids" in result:
                        created_uids.extend(result.get("created_uids", []))
                    elif result.get("success", False):
                        # If no UIDs returned but success, use our generated UIDs
                        created_uids.extend(level_uids)
                        
                    # Store last UID at this level as parent for next level
                    if level_uids:
                        level_parent_map[level] = level_uids[-1]
                        
                    # Add delay between levels
                    time.sleep(0.5)
            
            return {
                "success": True,
                "uid": page_uid,
                "created_uids": created_uids,
                "page_url": f"https://roamresearch.com/#/app/{GRAPH_NAME}/page/{page_uid}"
            }
        
        return {
            "success": True,
            "uid": page_uid,
            "page_url": f"https://roamresearch.com/#/app/{GRAPH_NAME}/page/{page_uid}"
        }
    except ValidationError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except TransactionError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def create_block(content: str, page_uid: Optional[str] = None, page_title: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a new block in Roam Research.
    
    Args:
        content: Block content
        page_uid: Optional page UID
        page_title: Optional page title
        
    Returns:
        Result with block UID
    """
    if not content:
        return {
            "success": False,
            "error": "Content is required"
        }
    
    session, headers = get_session_and_headers()
    
    try:
        # Determine target page
        target_page_uid = None
        
        if page_uid:
            # Use provided page UID
            target_page_uid = page_uid
        elif page_title:
            # Find or create page by title
            target_page_uid = find_or_create_page(page_title)
        else:
            # Use today's daily page
            target_page_uid = get_daily_page()
        
        # Handle multi-line content
        if "\n" in content:
            # Parse as nested structure
            markdown_content = convert_to_roam_markdown(content)
            parsed_content = parse_markdown_list(markdown_content)
            
            # Check if there's any content
            if not parsed_content:
                return {
                    "success": False,
                    "error": "Failed to parse content"
                }
            
            # Process nested content with top-down approach
            created_uids = []
            level_parent_map = {-1: target_page_uid}
            
            # Group by level
            level_items = {}
            for item in parsed_content:
                level = item.get("level", 0)
                if level not in level_items:
                    level_items[level] = []
                level_items[level].append(item)
            
            # Process level by level
            for level in sorted(level_items.keys()):
                actions = []
                level_uids = []
                
                for item in level_items[level]:
                    text = item.get("text", "")
                    heading_level = item.get("heading_level", 0)
                    
                    # Find parent for this level
                    parent_level = level - 1
                    if parent_level < -1:
                        parent_level = -1
                        
                    parent_uid = level_parent_map.get(parent_level, target_page_uid)
                    
                    # Generate UID
                    block_uid = str(uuid.uuid4())[:9]
                    level_uids.append(block_uid)
                    
                    # Create action
                    action = {
                        "action": "create-block",
                        "location": {
                            "parent-uid": parent_uid,
                            "order": "last"
                        },
                        "block": {
                            "string": text,
                            "uid": block_uid
                        }
                    }
                    
                    if heading_level and heading_level > 0 and heading_level <= 3:
                        action["block"]["heading"] = heading_level
                        
                    actions.append(action)
                
                # Execute batch for this level
                if actions:
                    result = execute_write_action(actions)
                    
                    if "created_uids" in result:
                        created_uids.extend(result.get("created_uids", []))
                    elif result.get("success", False):
                        # If no UIDs returned but success, use our generated UIDs
                        created_uids.extend(level_uids)
                        
                    # Store the last UID at this level as parent for next level
                    if level_uids:
                        level_parent_map[level] = level_uids[-1]
                        
                    # Add delay between levels
                    time.sleep(0.5)
            
            return {
                "success": True,
                "block_uid": created_uids[0] if created_uids else None,
                "parent_uid": target_page_uid,
                "created_uids": created_uids
            }
        else:
            # Create a simple block with explicit UID
            block_uid = str(uuid.uuid4())[:9]
            
            action_data = {
                "action": "create-block",
                "location": {
                    "parent-uid": target_page_uid,
                    "order": "last"
                },
                "block": {
                    "string": content,
                    "uid": block_uid
                }
            }
            
            result = execute_write_action(action_data)
            if result.get("success", False):
                # Verify the block exists after a brief delay
                time.sleep(1)
                found_uid = find_block_uid(session, headers, GRAPH_NAME, content)
                
                return {
                    "success": True,
                    "block_uid": found_uid or block_uid,
                    "parent_uid": target_page_uid
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to create block"
                }
    except ValidationError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except PageNotFoundError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except BlockNotFoundError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except TransactionError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def create_outline(outline: List[Dict[str, Any]], page_title_uid: Optional[str] = None, block_text_uid: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a structured outline in Roam Research.
    
    Args:
        outline: List of outline items with text and level
        page_title_uid: Optional page title or UID
        block_text_uid: Optional block text or UID to add outline under
        
    Returns:
        Result with created block UIDs
    """
    # Validate outline
    if not outline:
        return {
            "success": False,
            "error": "Outline cannot be empty"
        }
    
    # Check for valid levels
    invalid_items = [item for item in outline if not item.get("text") or not isinstance(item.get("level"), int)]
    if invalid_items:
        return {
            "success": False,
            "error": "All outline items must have text and a valid level"
        }
    
    session, headers = get_session_and_headers()
    
    try:
        # Determine target page
        target_page_uid = None
        
        if page_title_uid:
            # Find page by title or UID
            page_uid = find_page_by_title(session, headers, GRAPH_NAME, page_title_uid)
            
            if page_uid:
                target_page_uid = page_uid
            else:
                # Create new page if not found
                target_page_uid = find_or_create_page(page_title_uid)
        else:
            # Use today's daily page
            target_page_uid = get_daily_page()
        
        # Determine parent block
        parent_uid = target_page_uid
        
        if block_text_uid:
            # Check if it's a valid block UID (9 characters)
            if len(block_text_uid) == 9 and re.match(r'^[a-zA-Z0-9_-]{9}$', block_text_uid):
                # Verify block exists
                query = f'''[:find ?uid
                           :where [?b :block/uid "{block_text_uid}"]
                                  [?b :block/uid ?uid]]'''
                
                result = execute_query(query)
                
                if result:
                    parent_uid = block_text_uid
                else:
                    return {
                        "success": False,
                        "error": f"Block with UID {block_text_uid} not found"
                    }
            else:
                # Create a header block with the given text
                action_data = {
                    "action": "create-block",
                    "location": {
                        "parent-uid": target_page_uid,
                        "order": "last"
                    },
                    "block": {
                        "string": block_text_uid,
                        "uid": str(uuid.uuid4())[:9]
                    }
                }
                
                execute_write_action(action_data)
                time.sleep(0.5)  # Add delay to ensure block is created
                header_uid = find_block_uid(session, headers, GRAPH_NAME, block_text_uid)
                
                if not header_uid:
                    return {
                        "success": False,
                        "error": f"Failed to create header block with text: {block_text_uid}"
                    }
                    
                parent_uid = header_uid
        
        # Generate batch actions for outline - but create level by level
        created_uids = []
        level_items = {}
        
        # Group items by level
        for item in outline:
            level = item["level"]
            if level not in level_items:
                level_items[level] = []
            level_items[level].append(item)
        
        # Process levels in order (0, 1, 2, etc.)
        current_level_parents = {-1: parent_uid}  # Start with root parent
        
        for level in sorted(level_items.keys()):
            level_batch = []
            level_uids = []
            
            for item in level_items[level]:
                # Find parent from previous level
                parent_level = level - 1
                if parent_level < 0:
                    parent_for_item = parent_uid
                else:
                    # Use the last created block at the parent level as parent
                    parent_index = len(level_items.get(parent_level, [])) - 1
                    if parent_index >= 0 and parent_level in current_level_parents:
                        parent_for_item = current_level_parents[parent_level]
                    else:
                        parent_for_item = parent_uid
                
                # Generate a unique UID
                block_uid = str(uuid.uuid4())[:9]
                level_uids.append(block_uid)
                
                # Create block action
                action = {
                    "action": "create-block",
                    "location": {
                        "parent-uid": parent_for_item,
                        "order": "last"
                    },
                    "block": {
                        "string": item["text"],
                        "uid": block_uid
                    }
                }
                
                level_batch.append(action)
            
            # Execute batch for this level
            if level_batch:
                result = execute_write_action(level_batch)
                
                if "created_uids" in result:
                    created_uids.extend(result.get("created_uids", []))
                elif result.get("success", False):
                    # If no UIDs returned but success, use our generated UIDs
                    created_uids.extend(level_uids)
                
                # Store the last created UID at this level as parent for next level
                if level_uids:
                    current_level_parents[level] = level_uids[-1]
            
                # Add a small delay between levels
                time.sleep(0.5)
        
        return {
            "success": True,
            "page_uid": target_page_uid,
            "parent_uid": parent_uid,
            "created_uids": created_uids
        }
    except ValidationError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except PageNotFoundError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except BlockNotFoundError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except TransactionError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def import_markdown(content: str, page_uid: Optional[str] = None, page_title: Optional[str] = None,
                   parent_uid: Optional[str] = None, parent_string: Optional[str] = None,
                   order: str = "last") -> Dict[str, Any]:
    """
    Import markdown content into Roam Research.
    
    Args:
        content: Markdown content to import
        page_uid: Optional page UID
        page_title: Optional page title
        parent_uid: Optional parent block UID
        parent_string: Optional parent block text
        order: Position ("first" or "last")
        
    Returns:
        Result with created block UIDs
    """
    if not content:
        return {
            "success": False,
            "error": "Content cannot be empty"
        }
    
    if order not in ["first", "last"]:
        return {
            "success": False,
            "error": "Order must be 'first' or 'last'"
        }
    
    session, headers = get_session_and_headers()
    
    try:
        # Determine target page
        target_page_uid = None
        
        if page_uid:
            # Use provided page UID
            target_page_uid = page_uid
        elif page_title:
            # Find or create page by title
            target_page_uid = find_or_create_page(page_title)
        else:
            # Use today's daily page
            target_page_uid = get_daily_page()
        
        # Determine parent block
        parent_block_uid = target_page_uid
        
        if parent_uid:
            # Verify block exists
            query = f'''[:find ?uid .
                       :where [?b :block/uid "{parent_uid}"]
                              [?b :block/uid ?uid]]'''
            
            result = execute_query(query)
            
            if result:
                parent_block_uid = parent_uid
            else:
                return {
                    "success": False,
                    "error": f"Block with UID {parent_uid} not found"
                }
        elif parent_string:
            # Find block by string
            found_uid = find_block_uid(session, headers, GRAPH_NAME, parent_string)
            
            if found_uid:
                parent_block_uid = found_uid
            else:
                # Create parent block if it doesn't exist
                block_uid = str(uuid.uuid4())[:9]
                
                action_data = {
                    "action": "create-block",
                    "location": {
                        "parent-uid": target_page_uid,
                        "order": "last"
                    },
                    "block": {
                        "string": parent_string,
                        "uid": block_uid
                    }
                }
                
                execute_write_action(action_data)
                time.sleep(1)  # Wait for block to be created
                
                found_uid = find_block_uid(session, headers, GRAPH_NAME, parent_string)
                if found_uid:
                    parent_block_uid = found_uid
                else:
                    parent_block_uid = block_uid
                    logger.debug(f"Created parent block with UID: {block_uid}")
        
        # Convert markdown to Roam format
        roam_markdown = convert_to_roam_markdown(content)
        
        # Parse markdown into hierarchical structure
        parsed_content = parse_markdown_list(roam_markdown)
        
        if not parsed_content:
            return {
                "success": False,
                "error": "Failed to parse markdown content"
            }
        
        # Process items level by level
        created_uids = []
        
        # Sort by level
        parsed_content.sort(key=lambda x: x.get("level", 0))
        
        # Group items by level
        level_items = {}
        for item in parsed_content:
            level = item.get("level", 0)
            if level not in level_items:
                level_items[level] = []
            level_items[level].append(item)
        
        # Create batch actions level by level
        level_parent_map = {-1: parent_block_uid}
        
        for level in sorted(level_items.keys()):
            actions = []
            level_uids = []
            
            for item in level_items[level]:
                content = item.get("text", "")
                heading_level = item.get("heading_level", 0)
                
                # Find parent for this level
                parent_level = level - 1
                if parent_level < -1:
                    parent_level = -1
                    
                parent_for_item = level_parent_map.get(parent_level, parent_block_uid)
                
                # Generate unique UID
                block_uid = str(uuid.uuid4())[:9]
                level_uids.append(block_uid)
                
# Create action
                action = {
                    "action": "create-block",
                    "location": {
                        "parent-uid": parent_for_item,
                        "order": order if level == 0 else "last"
                    },
                    "block": {
                        "string": content,
                        "uid": block_uid
                    }
                }
                
                if heading_level and heading_level > 0 and heading_level <= 3:
                    action["block"]["heading"] = heading_level
                
                actions.append(action)
                
                # Store temporary parent mapping for the next level
                if level in level_parent_map:
                    level_parent_map[level] = block_uid
            
            # Execute batch for this level
            if actions:
                result = execute_write_action(actions)
                
                if "created_uids" in result:
                    created_uids.extend(result.get("created_uids", []))
                elif result.get("success", False):
                    # If no UIDs returned but success, use our generated UIDs
                    created_uids.extend(level_uids)
                
                # Add delay between levels
                time.sleep(0.5)
        
        return {
            "success": True,
            "page_uid": target_page_uid,
            "parent_uid": parent_block_uid,
            "created_uids": created_uids
        }
    except ValidationError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except PageNotFoundError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except BlockNotFoundError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except TransactionError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except Exception as e:
        logger.error(f"Error importing markdown: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }


def add_todos(todos: List[str]) -> Dict[str, Any]:
    """
    Add todo items to today's daily page.
    
    Args:
        todos: List of todo items
        
    Returns:
        Result with success status
    """
    if not todos:
        return {
            "success": False,
            "error": "Todo list cannot be empty"
        }
    
    if not all(isinstance(todo, str) for todo in todos):
        return {
            "success": False,
            "error": "All todo items must be strings"
        }
    
    session, headers = get_session_and_headers()
    
    try:
        # Get today's daily page
        daily_page_uid = get_daily_page()
        
        # Create batch actions for todos
        actions = []
        todo_uids = []
        
        for i, todo in enumerate(todos):
            # Format with TODO syntax
            todo_content = f"{{{{[[TODO]]}}}} {todo}"
            
            # Generate UID
            block_uid = str(uuid.uuid4())[:9]
            todo_uids.append(block_uid)
            
            # Create action
            action = {
                "action": "create-block",
                "location": {
                    "parent-uid": daily_page_uid,
                    "order": "last"
                },
                "block": {
                    "string": todo_content,
                    "uid": block_uid
                }
            }
            
            actions.append(action)
        
        # Execute batch actions
        result = execute_write_action(actions)
        
        if result.get("success", False) or "created_uids" in result:
            return {
                "success": True,
                "created_uids": result.get("created_uids", todo_uids),
                "page_uid": daily_page_uid
            }
        else:
            return {
                "success": False,
                "error": "Failed to create todo items"
            }
    except ValidationError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except PageNotFoundError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except TransactionError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def update_content(block_uid: str, content: Optional[str] = None, transform_pattern: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Update a block's content or transform it using a pattern.
    
    Args:
        block_uid: Block UID
        content: New content
        transform_pattern: Pattern for transformation
        
    Returns:
        Result with updated content
    """
    if not block_uid:
        return {
            "success": False,
            "error": "Block UID is required"
        }
    
    if not content and not transform_pattern:
        return {
            "success": False,
            "error": "Either content or transform_pattern must be provided"
        }
    
    try:
        # Get current content if doing a transformation
        if transform_pattern:
            # Validate transform pattern
            if not isinstance(transform_pattern, dict):
                return {
                    "success": False,
                    "error": "Transform pattern must be an object"
                }
            
            if "find" not in transform_pattern or "replace" not in transform_pattern:
                return {
                    "success": False,
                    "error": "Transform pattern must include 'find' and 'replace' properties"
                }
            
            query = f'''[:find ?string .
                        :where [?b :block/uid "{block_uid}"]
                                [?b :block/string ?string]]'''
            
            current_content = execute_query(query)
            
            if not current_content:
                return {
                    "success": False,
                    "error": f"Block with UID {block_uid} not found"
                }
            
            # Apply transformation
            find = transform_pattern["find"]
            replace = transform_pattern["replace"]
            global_replace = transform_pattern.get("global", True)
            
            try:
                flags = re.MULTILINE
                count = 0 if global_replace else 1
                new_content = re.sub(find, replace, current_content, count=count, flags=flags)
                
                # Update block
                update_block(block_uid, new_content)
                
                return {
                    "success": True,
                    "content": new_content
                }
            except re.error as e:
                return {
                    "success": False,
                    "error": f"Invalid regex pattern: {str(e)}"
                }
        else:
            # Direct content update
            update_block(block_uid, content)
            
            return {
                "success": True,
                "content": content
            }
    except ValidationError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except BlockNotFoundError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except TransactionError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def update_multiple_contents(updates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Update multiple blocks in a single operation.
    
    Args:
        updates: List of update operations
        
    Returns:
        Results of updates
    """
    if not updates or not isinstance(updates, list):
        return {
            "success": False,
            "error": "Updates must be a non-empty list"
        }
    
    try:
        # Validate each update
        for i, update in enumerate(updates):
            if "block_uid" not in update:
                return {
                    "success": False,
                    "error": f"Update at index {i} is missing required 'block_uid' property"
                }
            
            if "content" not in update and "transform" not in update:
                return {
                    "success": False,
                    "error": f"Update at index {i} must include either 'content' or 'transform'"
                }
            
            if "transform" in update:
                transform = update["transform"]
                if not isinstance(transform, dict):
                    return {
                        "success": False,
                        "error": f"Transform at index {i} must be an object"
                    }
                
                if "find" not in transform or "replace" not in transform:
                    return {
                        "success": False,
                        "error": f"Transform at index {i} must include 'find' and 'replace' properties"
                    }
        
        # Batch update blocks in chunks of 50
        CHUNK_SIZE = 50
        results = batch_update_blocks(updates, CHUNK_SIZE)
        
        # Count successful updates
        successful = sum(1 for result in results if result.get("success"))
        
        return {
            "success": successful == len(updates),
            "results": results,
            "message": f"Updated {successful}/{len(updates)} blocks successfully"
        }
    except ValidationError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def create_nested_blocks(parent_uid: str, blocks_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Create nested blocks with proper parent-child relationships.
    
    Args:
        parent_uid: UID of the parent block/page
        blocks_data: List of block data (text, level, children)
        
    Returns:
        Dictionary with success status and created block UIDs
    """
    if not blocks_data:
        return {
            "success": True,
            "created_uids": []
        }
    
    # Method 1: Create blocks one by one, ensuring proper parent-child relationships
    session, headers = get_session_and_headers()
    created_uids = []
    level_to_uid = {-1: parent_uid}  # Start with parent as level -1
    
    try:
        # Process blocks in order
        for block in blocks_data:
            level = block.get("level", 0)
            content = block.get("text", "")
            heading_level = block.get("heading_level", 0)
            
            # Find parent for this level
            parent_level = level - 1
            if parent_level < -1:
                parent_level = -1
                
            parent_for_block = level_to_uid.get(parent_level, parent_uid)
            
            # Create block action
            block_uid = str(uuid.uuid4())[:9]
            
            action_data = {
                "action": "create-block",
                "location": {
                    "parent-uid": parent_for_block,
                    "order": "last"
                },
                "block": {
                    "string": content,
                    "uid": block_uid
                }
            }
            
            if heading_level and heading_level > 0 and heading_level <= 3:
                action_data["block"]["heading"] = heading_level
                
            # Execute action
            result = execute_write_action(action_data)
            
            if result.get("success", False):
                created_uids.append(block_uid)
                level_to_uid[level] = block_uid
                logger.debug(f"Created block at level {level} with UID: {block_uid}")
                
                # Process children if available
                children = block.get("children", [])
                if children:
                    children_result = create_nested_blocks(block_uid, children)
                    if children_result.get("success", False):
                        created_uids.extend(children_result.get("created_uids", []))
                    else:
                        logger.warning(f"Failed to create children blocks: {children_result.get('error')}")
                
                # Add a brief delay between operations
                time.sleep(0.5)
            else:
                logger.error(f"Failed to create block: {result.get('error', 'Unknown error')}")
        
        return {
            "success": True,
            "created_uids": created_uids
        }
    except Exception as e:
        error_msg = f"Failed to create nested blocks: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "created_uids": created_uids  # Return any UIDs created before failure
        }