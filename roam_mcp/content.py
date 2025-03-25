"""Content operations for the Roam MCP server (pages, blocks, and outlines)."""

import logging
from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import re

from roam_mcp.api import (
    execute_query,
    execute_write_action,
    get_session_and_headers,
    GRAPH_NAME,
    find_or_create_page,
    get_daily_page,
    add_block_to_page,
    update_block,
    batch_update_blocks,
    find_page_by_title,
    APIError
)
from roam_mcp.utils import (
    format_roam_date,
    convert_to_roam_markdown,
    parse_markdown_list,
    process_nested_content,
    find_block_uid
)

# Set up logging
logger = logging.getLogger("roam-mcp.content")


def create_page(title: str, content: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """
    Create a new page in Roam Research.
    
    Args:
        title: Title for the new page
        content: Optional content for the page
        
    Returns:
        Result with page UID
    """
    session, headers = get_session_and_headers()
    
    try:
        # Create the page
        page_uid = find_or_create_page(title)
        
        # Add content if provided
        if content:
            created_uids = process_nested_content(content, page_uid, session, headers, GRAPH_NAME)
            
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
    except Exception as e:
        logger.error(f"Error creating page '{title}': {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }


def create_block(content: str, page_uid: Optional[str] = None, title: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a new block in Roam Research.
    
    Args:
        content: Block content
        page_uid: Optional page UID
        title: Optional page title
        
    Returns:
        Result with block UID
    """
    session, headers = get_session_and_headers()
    
    try:
        # Determine target page
        target_page_uid = None
        
        if page_uid:
            # Use provided page UID
            target_page_uid = page_uid
        elif title:
            # Find or create page by title
            target_page_uid = find_or_create_page(title)
        else:
            # Use today's daily page
            target_page_uid = get_daily_page()
        
        # Handle multi-line content
        if "\n" in content:
            # Parse as nested structure
            lines = content.strip().split("\n")
            nested_content = []
            
            current_level = 0
            current_path = [nested_content]
            
            for line in lines:
                if not line.strip():
                    continue
                
                # Calculate indentation level
                indent = len(line) - len(line.lstrip())
                level = indent // 2
                text = line.strip()
                
                # Create block object
                block = {"text": text, "children": []}
                
                # Adjust current path based on level
                if level > current_level:
                    # Going deeper
                    current_path.append(current_path[-1][-1]["children"])
                elif level < current_level:
                    # Going back up
                    for _ in range(current_level - level):
                        current_path.pop()
                
                # Add block to current level
                current_path[-1].append(block)
                current_level = level
            
            # Process nested content
            created_uids = process_nested_content(nested_content, target_page_uid, session, headers, GRAPH_NAME)
            
            return {
                "success": True,
                "block_uid": created_uids[0] if created_uids else None,
                "parent_uid": target_page_uid,
                "created_uids": created_uids
            }
        else:
            # Create a simple block
            block_uid = add_block_to_page(target_page_uid, content)
            
            return {
                "success": True,
                "block_uid": block_uid,
                "parent_uid": target_page_uid
            }
    except Exception as e:
        logger.error(f"Error creating block: {str(e)}")
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
    session, headers = get_session_and_headers()
    
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
                        "string": block_text_uid
                    }
                }
                
                execute_write_action(action_data)
                header_uid = find_block_uid(session, headers, GRAPH_NAME, block_text_uid)
                parent_uid = header_uid
        
        # Convert outline to hierarchical structure
        # First, validate levels (shouldn't skip levels)
        prev_level = 0
        for item in outline:
            level = item["level"]
            if level > prev_level + 1:
                return {
                    "success": False,
                    "error": f"Invalid outline structure - level {level} follows level {prev_level}"
                }
            prev_level = level
        
        # Create nested structure
        structured_outline = []
        level_parents = {0: structured_outline}
        
        for item in outline:
            level = item["level"]
            text = item["text"]
            
            node = {"text": text, "children": []}
            
            # Find parent level
            parent_level = level - 1
            if parent_level not in level_parents:
                return {
                    "success": False,
                    "error": f"Invalid outline structure - level {level} has no parent"
                }
            
            # Add to parent
            level_parents[parent_level].append(node)
            level_parents[level] = node["children"]
        
        # Create blocks
        created_uids = process_nested_content(structured_outline, parent_uid, session, headers, GRAPH_NAME)
        
        return {
            "success": True,
            "page_uid": target_page_uid,
            "parent_uid": parent_uid,
            "created_uids": created_uids
        }
    except Exception as e:
        logger.error(f"Error creating outline: {str(e)}")
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
            query = f'''[:find ?uid
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
            query = f'''[:find ?uid
                       :where [?p :block/uid "{target_page_uid}"]
                              [?b :block/page ?p]
                              [?b :block/string "{parent_string}"]
                              [?b :block/uid ?uid]]'''
            
            result = execute_query(query)
            
            if result:
                parent_block_uid = result[0][0]
            else:
                return {
                    "success": False,
                    "error": f"Block with content '{parent_string}' not found on specified page"
                }
        
        # Convert markdown to Roam format
        roam_markdown = convert_to_roam_markdown(content)
        
        # Parse markdown into hierarchical structure
        parsed_content = parse_markdown_list(roam_markdown)
        
        # Create blocks
        created_uids = process_nested_content(parsed_content, parent_block_uid, session, headers, GRAPH_NAME)
        
        return {
            "success": True,
            "page_uid": target_page_uid,
            "parent_uid": parent_block_uid,
            "created_uids": created_uids
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
    session, headers = get_session_and_headers()
    
    try:
        # Get today's daily page
        daily_page_uid = get_daily_page()
        
        created_uids = []
        
        # Add todos as blocks
        for i, todo in enumerate(todos):
            # Format with TODO syntax
            todo_content = f"{{{{[[TODO]]}}}} {todo}"
            
            # Create block
            action_data = {
                "action": "create-block",
                "location": {
                    "parent-uid": daily_page_uid,
                    "order": "last"
                },
                "block": {
                    "string": todo_content
                }
            }
            
            execute_write_action(action_data)
            uid = find_block_uid(session, headers, GRAPH_NAME, todo_content)
            created_uids.append(uid)
        
        return {
            "success": True,
            "created_uids": created_uids,
            "page_uid": daily_page_uid
        }
    except Exception as e:
        logger.error(f"Error adding todos: {str(e)}")
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
    if not content and not transform_pattern:
        return {
            "success": False,
            "error": "Either content or transform_pattern must be provided"
        }
    
    try:
        # Get current content if doing a transformation
        if transform_pattern:
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
            
            flags = re.MULTILINE
            count = 0 if global_replace else 1
            new_content = re.sub(find, replace, current_content, count=count, flags=flags)
            
            # Update block
            update_block(block_uid, new_content)
            
            return {
                "success": True,
                "content": new_content
            }
        else:
            # Direct content update
            update_block(block_uid, content)
            
            return {
                "success": True,
                "content": content
            }
    except Exception as e:
        logger.error(f"Error updating block {block_uid}: {str(e)}")
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
    try:
        results = batch_update_blocks(updates)
        
        return {
            "success": all(result["success"] for result in results),
            "results": results
        }
    except Exception as e:
        logger.error(f"Error batch updating blocks: {str(e)}")
        return {
            "success": False,
            "error": str(e)
        }