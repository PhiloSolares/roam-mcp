"""Content operations for the Roam MCP server (pages, blocks, and outlines)."""

from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import re
import logging
import uuid
import time
import json

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
               Each item should have:
               - 'text' or 'string': Content text
               - 'level': Nesting level (optional, defaults to parent_level + 1)
               - 'heading_level': Heading level 1-3 (optional)
               - 'children': List of child items (optional)
        
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
            # Use the standardized hierarchical content processor
            result = process_hierarchical_content(page_uid, content)
            
            if result["success"]:
                return {
                    "success": True,
                    "uid": page_uid,
                    "created_uids": result.get("created_uids", []),
                    "page_url": f"https://roamresearch.com/#/app/{GRAPH_NAME}/page/{page_uid}"
                }
            else:
                return {
                    "success": False,
                    "error": result.get("error", "Failed to create content"),
                    "uid": page_uid,
                    "page_url": f"https://roamresearch.com/#/app/{GRAPH_NAME}/page/{page_uid}"
                }
        logger.error(f"Error creating page: {str(e)}")
        
        return {
            "error": f"Error creating page: {str(e)}"
            "uid": page_uid,
            "page_url": f"https://roamresearch.com/#/app/{GRAPH_NAME}/page/{page_uid}"
        }
    def create_block(content: str, page_uid: Optional[str] = None, page_title: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a new block in Roam Research.
    
    Args:
        content: Block content - can be single-line text or multi-line content
                 that will be parsed into a hierarchical structure
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
            
            # Build hierarchical structure
            def build_hierarchy_from_parsed(items):
                # Sort by level first
                sorted_items = sorted(items, key=lambda x: x.get("level", 0))
                
                # Group items by level
                level_groups = {}
                for item in sorted_items:
                    level = item.get("level", 0)
                    if level not in level_groups:
                        level_groups[level] = []
                    level_groups[level].append(item)
                
                # Find the minimum level (root level)
                min_level = min(level_groups.keys()) if level_groups else 0
                root_items = level_groups.get(min_level, [])
                
                # Track parents at each level
                current_parents = {}
                hierarchical_items = []
                
                # Process items level by level
                for level in sorted(level_groups.keys()):
                    for item in level_groups[level]:
                        if level == min_level:
                            # Root level items
                            hierarchical_items.append(item)
                            current_parents[level] = item
                        else:
                            # Find the parent
                            parent_level = level - 1
                            while parent_level >= min_level:
                                if parent_level in current_parents:
                                    parent = current_parents[parent_level]
                                    if "children" not in parent:
                                        parent["children"] = []
                                    parent["children"].append(item)
                                    current_parents[level] = item
                                    break
                                parent_level -= 1
                            
                            # If no parent found, add as root
                            if parent_level < min_level:
                                hierarchical_items.append(item)
                                current_parents[level] = item
                
                return hierarchical_items
            
            # Build hierarchical structure
            hierarchical_content = build_hierarchy_from_parsed(parsed_content)
            
            # Process using the standardized hierarchical content processor
            result = process_hierarchical_content(target_page_uid, hierarchical_content)
            
            if result["success"]:
                return {
                    "success": True,
                    "block_uid": result["created_uids"][0] if result["created_uids"] else None,
                    "parent_uid": target_page_uid,
                    "created_uids": result["created_uids"]
                }
            else:
                return {
                    "success": False,
                    "error": result.get("error", "Failed to create hierarchical blocks"),
                    "parent_uid": target_page_uid
                }
        else:
            # Create a simple block with explicit UID
            block_uid = str(uuid.uuid4())[:9]
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
        logger.error(f"Error creating block: {str(e)}")
        return {
            "success": False,
            "error": f"Error creating block: {str(e)}"
        }
            
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
                time.sleep(0.5)
                found_uid = find_block_uid(session, headers, GRAPH_NAME, content)
                
            def create_outline(outline: List[Dict[str, Any]], page_title_uid: Optional[str] = None, block_text_uid: Optional[str] = None) -> Dict[str, Any]:
    """
    Create a structured outline in Roam Research.
    
    Args:
        outline: List of outline items with text and level
               Each item should have:
                - 'text': Content text (required)
                - 'level': Nesting level (required)
                - 'heading_level': Heading level 1-3 (optional)
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
        
        # Build hierarchical structure from flat outline items
        def build_outline_hierarchy(items):
            # First, sort by level
            sorted_items = sorted(items, key=lambda x: x.get("level", 0))
            
            # Group items by level
            level_groups = {}
            for item in sorted_items:
                level = item.get("level", 0)
                if level not in level_groups:
                    level_groups[level] = []
                level_groups[level].append(item)
            
            # Build parent-child relationships based on item position and level
            min_level = min(level_groups.keys()) if level_groups else 0
            hierarchical_items = []
            
            # Track parent nodes at each level
            level_parents = {}
            
            # Process items in order
            for item in sorted_items:
                level = item.get("level", 0)
                
                # If this is a root-level item, add it to the result directly
                if level == min_level:
                    hierarchical_items.append(item)
                    level_parents[level] = item
                else:
                    # Find the nearest parent level
                    parent_level = level - 1
                    while parent_level >= min_level and parent_level not in level_parents:
                        parent_level -= 1
                    
                    # If we found a parent, add this item as its child
                    if parent_level >= min_level:
                        parent = level_parents[parent_level]
                        if "children" not in parent:
                            parent["children"] = []
                        parent["children"].append(item)
                        level_parents[level] = item
                    else:
                        # If no parent found, add it as a root item
                        hierarchical_items.append(item)
                        level_parents[level] = item
            
            return hierarchical_items
        
        # Build hierarchical structure from outline
        hierarchical_outline = build_outline_hierarchy(outline)
        
        # Use the standardized hierarchical content processor
        result = process_hierarchical_content(parent_uid, hierarchical_outline)
        
        if result["success"]:
            return {
                "success": True,
                "page_uid": target_page_uid,
                "parent_uid": parent_uid,
                "created_uids": result.get("created_uids", [])
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to create outline"),
                "page_uid": target_page_uid,
                "parent_uid": parent_uid
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
        logger.error(f"Error creating outline: {str(e)}")
        return {
            "success": False,
            "error": f"Error creating outline: {str(e)}"
        }
                if level_uids:
                    current_level_parents[level] = level_uids[-1]
            
                # Add a small delay between levels
                time.sleep(0.5)
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
        
        # Build a proper hierarchical structure from the parsed markdown
        def build_hierarchy(items):
            # Group items by level
            level_groups = {}
            for item in items:
                level = item.get("level", 0)
                if level not in level_groups:
                    level_groups[level] = []
                level_groups[level].append(item)
            
            # Start with the root level (usually 0)
            min_level = min(level_groups.keys()) if level_groups else 0
            root_items = level_groups.get(min_level, [])
            
            # Recursive function to build the tree
            def attach_children(parent_items, parent_level):
                for parent in parent_items:
                    children = []
                    child_level = parent_level + 1
                    
                    # If there are items at the next level
                    if child_level in level_groups:
                        # Find children whose current parent would be this item
                        # based on the flattened list's position
                        parent_index = items.index(parent)
                        for potential_child in level_groups[child_level]:
                            child_index = items.index(potential_child)
                            
                            # Is this child positioned after the parent and before the next parent?
                            if child_index > parent_index:
                                # Check if there's another parent of the same level between this parent and the child
                                next_parent_index = float('inf')
                                for next_parent in level_groups[parent_level]:
                                    next_idx = items.index(next_parent)
                                    if next_idx > parent_index and next_idx < child_index:
                                        next_parent_index = next_idx
                                        break
                                
                                if child_index < next_parent_index:
                                    children.append(potential_child)
                    
                    # Set the children
                    if children:
                        parent["children"] = children
                        # Recursively attach children to these children
                        attach_children(children, child_level)
            
            # Start the recursive process
            attach_children(root_items, min_level)
            return root_items
        
        # Build a hierarchical structure that preserves parent-child relationships
        hierarchical_content = build_hierarchy(parsed_content)
        
        # Process the hierarchical content using the standardized utility
        result = process_hierarchical_content(parent_block_uid, hierarchical_content, order)
        
        if result["success"]:
            return {
                "success": True,
                "page_uid": target_page_uid,
                "parent_uid": parent_block_uid,
                "created_uids": result.get("created_uids", [])
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Failed to import markdown"),
                "page_uid": target_page_uid,
                "parent_uid": parent_block_uid
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
            "error": f"Error importing markdown: {str(e)}"
        }
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


def process_hierarchical_content(parent_uid: str, content_data: List[Dict[str, Any]], order: str = "last") -> Dict[str, Any]:
    """
    Process hierarchical content with proper parent-child relationships.
    This is a standardized utility function used across different content creation methods.
    
    Args:
        parent_uid: UID of the parent block/page
        content_data: List of content items with text, level, and optional children/heading_level attributes
        order: Where to add content ("first" or "last")
        
    Returns:
        Dictionary with success status and created block UIDs
    """
    if not content_data:
        return {
            "success": True,
            "created_uids": []
        }
    
    # First, validate the hierarchical structure
    def validate_item(item, path="root"):
        errors = []
        # Check required fields
        if not item.get("text") and not item.get("string"):
            errors.append(f"Item at {path} is missing required 'text' field")
        
        # Ensure level is valid
        level = item.get("level")
        if level is not None and not isinstance(level, int):
            errors.append(f"Item at {path} has invalid 'level', must be an integer")
        
        # Validate heading level
        heading_level = item.get("heading_level", 0)
        if heading_level and (not isinstance(heading_level, int) or heading_level < 0 or heading_level > 3):
            errors.append(f"Item at {path} has invalid 'heading_level', must be an integer between 0 and 3")
            
        # Validate children recursively
        children = item.get("children", [])
        if not isinstance(children, list):
            errors.append(f"Item at {path} has invalid 'children', must be a list")
        else:
            for i, child in enumerate(children):
                child_path = f"{path}.children[{i}]"
                child_errors = validate_item(child, child_path)
                errors.extend(child_errors)
                
        return errors
    
    # Validate all items
    all_errors = []
    for i, item in enumerate(content_data):
        item_path = f"item[{i}]"
        errors = validate_item(item, item_path)
        all_errors.extend(errors)
        
    if all_errors:
        return {
            "success": False,
            "error": f"Invalid content structure: {'; '.join(all_errors)}"
        }
    
    # Process hierarchical content with proper nesting
    session, headers = get_session_and_headers()
    all_created_uids = []
    
    # Define a recursive function to process items
    def process_item(item, parent_uid, level_to_uid, current_level):
        created_uids = []
        
        # Get item properties
        text = item.get("text", item.get("string", ""))
        level = item.get("level", current_level)
        heading_level = item.get("heading_level", 0)
        
        # Find the appropriate parent for this level
        parent_level = level - 1
        if parent_level < -1:
            parent_level = -1
            
        effective_parent = level_to_uid.get(parent_level, parent_uid)
        
        # Create block with a unique UID
        block_uid = str(uuid.uuid4())[:9]
        
        action_data = {
            "action": "create-block",
            "location": {
                "parent-uid": effective_parent,
                "order": order if level == 0 else "last"
            },
            "block": {
                "string": text,
                "uid": block_uid
            }
        }
        
        # Add heading level if specified
        if heading_level and heading_level > 0 and heading_level <= 3:
            action_data["block"]["heading"] = heading_level
            
        # Execute the action
        result = execute_write_action(action_data)
        
        if result.get("success", False):
            created_uids.append(block_uid)
            level_to_uid[level] = block_uid
            logger.debug(f"Created block at level {level} with UID: {block_uid}")
            
            # Process children if any
            children = item.get("children", [])
            if children:
                for child in children:
                    # Process each child with this block as parent
                    child_result = process_item(child, block_uid, level_to_uid, level + 1)
                    created_uids.extend(child_result)
                    
            # Add a brief delay for API stability
            time.sleep(0.3)
        else:
            logger.error(f"Failed to create block: {result.get('error', 'Unknown error')}")
        
        return created_uids
    
    try:
        # Process each top-level item
        level_to_uid = {-1: parent_uid}  # Start with parent as level -1
        
        for item in content_data:
            item_uids = process_item(item, parent_uid, level_to_uid, 0)
            all_created_uids.extend(item_uids)
            
        return {
            "success": True,
            "created_uids": all_created_uids
        }
    except Exception as e:
        error_msg = f"Failed to process hierarchical content: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "created_uids": all_created_uids  # Return any UIDs created before failure
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
    # For backward compatibility, now uses the standardized hierarchical content processor
    return process_hierarchical_content(parent_uid, blocks_data)