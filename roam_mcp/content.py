"""Content operations for the Roam MCP server (pages, blocks, and outlines)."""

from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import re
import logging

from roam_mcp.api import client, ValidationError, BlockNotFoundError, PageNotFoundError, TransactionError
from roam_mcp.utils import (
    format_roam_date,
    convert_to_roam_markdown,
    parse_markdown_list,
    process_nested_content,
    create_block_action,
    prepare_batch_actions
)

# Set up logging
logger = logging.getLogger("roam-mcp.content")


class ContentOperations:
    """Operations for content management in Roam (pages, blocks, outlines)."""
    
    def __init__(self):
        """Initialize content operations."""
        pass
    
    def create_page(self, title: str, content: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Create a new page in Roam Research.
        
        Args:
            title: Title for the new page
            content: Optional content for the page
            
        Returns:
            Result with page UID
        """
        if not title:
            return {
                "success": False,
                "error": "Title is required"
            }
        
        try:
            # Create the page
            page_uid = client.find_or_create_page(title)
            
            # Add content if provided
            if content:
                # First, validate content structure
                invalid_items = [
                    item for item in content 
                    if not isinstance(item.get("text"), str) or not isinstance(item.get("level"), int)
                ]
                
                if invalid_items:
                    return {
                        "success": False,
                        "error": "Invalid content structure - each item must have text (string) and level (integer)"
                    }
                
                # Check for invalid level jumps
                prev_level = 0
                for item in content:
                    level = item["level"]
                    if level > prev_level + 1:
                        return {
                            "success": False,
                            "error": f"Invalid content structure - level {level} follows level {prev_level}"
                        }
                    prev_level = level
                
                # Process the nested content
                created_uids = process_nested_content(content, page_uid)
                
                return {
                    "success": True,
                    "uid": page_uid,
                    "created_uids": created_uids,
                    "page_url": f"https://roamresearch.com/#/app/{client.graph_name}/page/{page_uid}"
                }
            
            return {
                "success": True,
                "uid": page_uid,
                "page_url": f"https://roamresearch.com/#/app/{client.graph_name}/page/{page_uid}"
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
    
    def create_block(self, content: str, page_uid: Optional[str] = None, page_title: Optional[str] = None) -> Dict[str, Any]:
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
        
        try:
            # Determine target page
            target_page_uid = None
            
            if page_uid:
                # Use provided page UID
                target_page_uid = page_uid
            elif page_title:
                # Find or create page by title
                target_page_uid = client.find_or_create_page(page_title)
            else:
                # Use today's daily page
                today = datetime.now()
                date_str = format_roam_date(today)
                target_page_uid = client.find_or_create_page(date_str)
            
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
                
                # Process nested content
                created_uids = process_nested_content(parsed_content, target_page_uid)
                
                return {
                    "success": True,
                    "block_uid": created_uids[0] if created_uids else None,
                    "parent_uid": target_page_uid,
                    "created_uids": created_uids
                }
            else:
                # Create a simple block
                block_uid = client.add_block_to_page(target_page_uid, content)
                
                return {
                    "success": True,
                    "block_uid": block_uid,
                    "parent_uid": target_page_uid
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
            logger.error(f"Error creating block: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def create_outline(self, outline: List[Dict[str, Any]], page_title_uid: Optional[str] = None, block_text_uid: Optional[str] = None) -> Dict[str, Any]:
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
        
        try:
            # Determine target page
            target_page_uid = None
            
            if page_title_uid:
                # Find page by title or UID
                page_uid = client.find_page_by_title(page_title_uid)
                
                if page_uid:
                    target_page_uid = page_uid
                else:
                    # Create new page if not found
                    target_page_uid = client.find_or_create_page(page_title_uid)
            else:
                # Use today's daily page
                today = datetime.now()
                date_str = format_roam_date(today)
                target_page_uid = client.find_or_create_page(date_str)
            
            # Determine parent block
            parent_uid = target_page_uid
            
            if block_text_uid:
                # Check if it's a valid block UID (9 characters)
                if len(block_text_uid) == 9 and re.match(r'^[a-zA-Z0-9_-]{9}$', block_text_uid):
                    # Verify block exists
                    query = f'''[:find ?uid
                               :where [?b :block/uid "{block_text_uid}"]
                                      [?b :block/uid ?uid]]'''
                    
                    result = client.execute_query(query)
                    
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
                    
                    client.execute_write_action(action_data)
                    header_uid = client.find_block_uid(block_text_uid)
                    
                    if not header_uid:
                        return {
                            "success": False,
                            "error": f"Failed to create header block with text: {block_text_uid}"
                        }
                        
                    parent_uid = header_uid
            
            # Validate levels (shouldn't skip levels)
            prev_level = 0
            for item in outline:
                level = item["level"]
                if level > prev_level + 1:
                    return {
                        "success": False,
                        "error": f"Invalid outline structure - level {level} follows level {prev_level}"
                    }
                prev_level = level
            
            # Process the nested content
            created_uids = process_nested_content(outline, parent_uid)
            
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
            logger.error(f"Error creating outline: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def import_markdown(self, content: str, page_uid: Optional[str] = None, page_title: Optional[str] = None,
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
        
        try:
            # Determine target page
            target_page_uid = None
            
            if page_uid:
                # Use provided page UID
                target_page_uid = page_uid
            elif page_title:
                # Find or create page by title
                target_page_uid = client.find_or_create_page(page_title)
            else:
                # Use today's daily page
                today = datetime.now()
                date_str = format_roam_date(today)
                target_page_uid = client.find_or_create_page(date_str)
            
            # Determine parent block
            parent_block_uid = target_page_uid
            
            if parent_uid:
                # Verify block exists
                query = f'''[:find ?uid
                           :where [?b :block/uid "{parent_uid}"]
                                  [?b :block/uid ?uid]]'''
                
                result = client.execute_query(query)
                
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
                
                result = client.execute_query(query)
                
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
            
            if not parsed_content:
                return {
                    "success": False,
                    "error": "Failed to parse markdown content"
                }
            
            # Process nested content
            created_uids = process_nested_content(parsed_content, parent_block_uid)
            
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
            logger.error(f"Error importing markdown: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def add_todos(self, todos: List[str]) -> Dict[str, Any]:
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
        
        try:
            # Get today's daily page
            today = datetime.now()
            date_str = format_roam_date(today)
            daily_page_uid = client.find_or_create_page(date_str)
            
            # Create batch actions for todos
            actions = []
            for i, todo in enumerate(todos):
                # Format with TODO syntax
                todo_content = f"{{{{[[TODO]]}}}} {todo}"
                
                # Create action
                action = create_block_action(
                    parent_uid=daily_page_uid,
                    content=todo_content,
                    order="last"
                )
                
                actions.append(action)
            
            # Execute batch actions
            result = client.execute_batch_actions(actions)
            created_uids = result.get("created_uids", [])
            
            return {
                "success": True,
                "created_uids": created_uids,
                "page_uid": daily_page_uid
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
            logger.error(f"Error adding todos: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def update_content(self, block_uid: str, content: Optional[str] = None, transform_pattern: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
                
                current_content = client.execute_query(query)
                
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
                    client.update_block(block_uid, new_content)
                    
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
                client.update_block(block_uid, content)
                
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
            logger.error(f"Error updating content: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }
    
    def update_multiple_contents(self, updates: List[Dict[str, Any]]) -> Dict[str, Any]:
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
            
            # Process batch operations
            results = []
            actions = []
            
            # First, get current content for all blocks
            block_uids = [update["block_uid"] for update in updates]
            query = f'''[:find ?uid ?string
                        :in $ [?uid ...]
                        :where [?b :block/uid ?uid]
                               [?b :block/string ?string]]'''
            
            block_results = client.execute_query(query, [block_uids])
            
            # Create map of uid -> content
            content_map = {uid: content for uid, content in block_results}
            
            # Process each update
            for update in updates:
                block_uid = update["block_uid"]
                current_content = content_map.get(block_uid)
                
                if not current_content:
                    results.append({
                        "block_uid": block_uid,
                        "success": False,
                        "error": f"Block with UID {block_uid} not found"
                    })
                    continue
                
                try:
                    if "content" in update:
                        # Direct content update
                        new_content = update["content"]
                        actions.append({
                            "action": "update-block",
                            "block": {
                                "uid": block_uid,
                                "string": new_content
                            }
                        })
                        
                        results.append({
                            "block_uid": block_uid,
                            "content": new_content,
                            "success": True
                        })
                    elif "transform" in update:
                        # Pattern transformation
                        transform = update["transform"]
                        find = transform["find"]
                        replace = transform["replace"]
                        global_flag = transform.get("global_flag", True)
                        
                        try:
                            flags = re.MULTILINE
                            count = 0 if global_flag else 1
                            new_content = re.sub(find, replace, current_content, count=count, flags=flags)
                            
                            actions.append({
                                "action": "update-block",
                                "block": {
                                    "uid": block_uid,
                                    "string": new_content
                                }
                            })
                            
                            results.append({
                                "block_uid": block_uid,
                                "content": new_content,
                                "success": True
                            })
                        except re.error as e:
                            results.append({
                                "block_uid": block_uid,
                                "success": False,
                                "error": f"Invalid regex pattern: {str(e)}"
                            })
                except Exception as e:
                    results.append({
                        "block_uid": block_uid,
                        "success": False,
                        "error": str(e)
                    })
            
            # Execute batch update if we have any valid actions
            if actions:
                try:
                    client.execute_batch_actions(actions)
                except Exception as e:
                    # Mark all previously successful results as failed
                    for result in results:
                        if result.get("success"):
                            result["success"] = False
                            result["error"] = f"Batch update failed: {str(e)}"
            
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
            logger.error(f"Error updating multiple blocks: {str(e)}", exc_info=True)
            return {
                "success": False,
                "error": str(e)
            }


# Create global instance
content_ops = ContentOperations()

# Legacy functions that delegate to the instance
def create_page(title: str, content: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Legacy function that delegates to the content_ops instance."""
    return content_ops.create_page(title, content)

def create_block(content: str, page_uid: Optional[str] = None, page_title: Optional[str] = None) -> Dict[str, Any]:
    """Legacy function that delegates to the content_ops instance."""
    return content_ops.create_block(content, page_uid, page_title)

def create_outline(outline: List[Dict[str, Any]], page_title_uid: Optional[str] = None, block_text_uid: Optional[str] = None) -> Dict[str, Any]:
    """Legacy function that delegates to the content_ops instance."""
    return content_ops.create_outline(outline, page_title_uid, block_text_uid)

def import_markdown(content: str, page_uid: Optional[str] = None, page_title: Optional[str] = None,
                   parent_uid: Optional[str] = None, parent_string: Optional[str] = None,
                   order: str = "last") -> Dict[str, Any]:
    """Legacy function that delegates to the content_ops instance."""
    return content_ops.import_markdown(content, page_uid, page_title, parent_uid, parent_string, order)

def add_todos(todos: List[str]) -> Dict[str, Any]:
    """Legacy function that delegates to the content_ops instance."""
    return content_ops.add_todos(todos)

def update_content(block_uid: str, content: Optional[str] = None, transform_pattern: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Legacy function that delegates to the content_ops instance."""
    return content_ops.update_content(block_uid, content, transform_pattern)

def update_multiple_contents(updates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Legacy function that delegates to the content_ops instance."""
    return content_ops.update_multiple_contents(updates)