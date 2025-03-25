"""Core API functions for interacting with Roam Research."""

import os
import re
import sys
import logging
from typing import Dict, List, Any, Optional, Union, Set, Tuple
import requests
from datetime import datetime
import json

from roam_mcp.utils import (
    format_roam_date,
    find_block_uid,
    find_page_by_title,
    process_nested_content,
    resolve_block_references
)

# Set up logging
logger = logging.getLogger("roam-mcp.api")

# Get API credentials from environment variables
API_TOKEN = os.environ.get("ROAM_API_TOKEN")
GRAPH_NAME = os.environ.get("ROAM_GRAPH_NAME")
MEMORIES_TAG = os.environ.get("MEMORIES_TAG", "#[[Memories]]")

# Validate API credentials
if not API_TOKEN:
    logger.warning("ROAM_API_TOKEN environment variable is not set")
    
if not GRAPH_NAME:
    logger.warning("ROAM_GRAPH_NAME environment variable is not set")


class APIError(Exception):
    """Exception raised for API errors."""
    pass


class PreserveAuthSession(requests.Session):
    """Session class that preserves authentication headers during redirects."""
    def rebuild_auth(self, prepared_request, response):
        """Preserve the Authorization header on redirects."""
        return


def get_session_and_headers() -> Tuple[requests.Session, Dict[str, str]]:
    """
    Create a session with authentication headers.
    
    Returns:
        Tuple of (session, headers)
    
    Raises:
        ValueError: If required environment variables are missing
    """
    if not API_TOKEN or not GRAPH_NAME:
        raise ValueError("ROAM_API_TOKEN and ROAM_GRAPH_NAME must be set")
    
    session = PreserveAuthSession()
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }
    
    return session, headers


def execute_query(query: str, inputs: Optional[List[Any]] = None) -> Any:
    """
    Execute a Datalog query against the Roam graph.
    
    Args:
        query: Datalog query string
        inputs: Optional list of query inputs
        
    Returns:
        Query results
        
    Raises:
        APIError: If the query fails
    """
    session, headers = get_session_and_headers()
    
    # Prepare query data
    data = {
        "query": query,
    }
    if inputs:
        data["inputs"] = inputs
    
    # Log query (without inputs for security)
    logger.debug(f"Executing query: {query}")
    
    # Execute query
    try:
        response = session.post(
            f'https://api.roamresearch.com/api/graph/{GRAPH_NAME}/q',
            headers=headers,
            json=data
        )
        
        response.raise_for_status()
        result = response.json().get('result')
        
        # Log result size
        if isinstance(result, list):
            logger.debug(f"Query returned {len(result)} results")
            
        return result
    except requests.RequestException as e:
        error_msg = f"Query failed: {str(e)}"
        if hasattr(e, 'response') and e.response:
            error_msg += f" - Status code: {e.response.status_code}"
            try:
                error_detail = e.response.json()
                error_msg += f" - Details: {json.dumps(error_detail)}"
            except:
                error_msg += f" - Response: {e.response.text[:500]}"
        
        logger.error(error_msg)
        raise APIError(error_msg) from e


def execute_write_action(action_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute a write action on the Roam graph.
    
    Args:
        action_data: The action data to write
        
    Returns:
        Response data
        
    Raises:
        APIError: If the write action fails
    """
    session, headers = get_session_and_headers()
    
    # Log action type
    action_type = action_data.get("action", "unknown")
    logger.debug(f"Executing write action: {action_type}")
    
    # Execute action
    try:
        response = session.post(
            f'https://api.roamresearch.com/api/graph/{GRAPH_NAME}/write',
            headers=headers,
            json=action_data
        )
        
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        error_msg = f"Write action failed: {str(e)}"
        if hasattr(e, 'response') and e.response:
            error_msg += f" - Status code: {e.response.status_code}"
            try:
                error_detail = e.response.json()
                error_msg += f" - Details: {json.dumps(error_detail)}"
            except:
                error_msg += f" - Response: {e.response.text[:500]}"
        
        logger.error(error_msg)
        raise APIError(error_msg) from e


def find_or_create_page(title: str) -> str:
    """
    Find a page by title or create it if it doesn't exist.
    
    Args:
        title: Page title
        
    Returns:
        Page UID
        
    Raises:
        APIError: If page creation fails
    """
    session, headers = get_session_and_headers()
    
    # Try to find the page first
    logger.debug(f"Looking for page: {title}")
    page_uid = find_page_by_title(session, headers, GRAPH_NAME, title)
    
    if page_uid:
        logger.debug(f"Found existing page: {title} (UID: {page_uid})")
        return page_uid
    
    # Create the page if it doesn't exist
    logger.debug(f"Creating new page: {title}")
    action_data = {
        "action": "create-page",
        "page": {"title": title}
    }
    
    response = execute_write_action(action_data)
    
    if "page" in response and "uid" in response["page"]:
        new_uid = response["page"]["uid"]
        logger.debug(f"Created page: {title} (UID: {new_uid})")
        return new_uid
    else:
        # Try to find the page again - sometimes the API creates it but doesn't return the UID
        page_uid = find_page_by_title(session, headers, GRAPH_NAME, title)
        if page_uid:
            logger.debug(f"Found newly created page: {title} (UID: {page_uid})")
            return page_uid
        
        error_msg = f"Failed to create page: {title}"
        logger.error(error_msg)
        raise APIError(error_msg)


def get_daily_page() -> str:
    """
    Get or create today's daily page.
    
    Returns:
        Daily page UID
        
    Raises:
        APIError: If page creation fails
    """
    today = datetime.now()
    date_str = format_roam_date(today)
    
    logger.debug(f"Getting daily page for: {date_str}")
    return find_or_create_page(date_str)


def add_block_to_page(page_uid: str, content: str, order: Union[int, str] = "last") -> str:
    """
    Add a block to a page.
    
    Args:
        page_uid: Parent page UID
        content: Block content
        order: Position ("first", "last", or integer index)
        
    Returns:
        New block UID
        
    Raises:
        APIError: If block creation fails
    """
    action_data = {
        "action": "create-block",
        "location": {
            "parent-uid": page_uid,
            "order": order
        },
        "block": {
            "string": content
        }
    }
    
    logger.debug(f"Adding block to page {page_uid}")
    execute_write_action(action_data)
    
    session, headers = get_session_and_headers()
    uid = find_block_uid(session, headers, GRAPH_NAME, content)
    logger.debug(f"Created block with UID: {uid}")
    
    return uid


def update_block(block_uid: str, content: str) -> bool:
    """
    Update a block's content.
    
    Args:
        block_uid: Block UID
        content: New content
        
    Returns:
        Success flag
        
    Raises:
        APIError: If block update fails
    """
    action_data = {
        "action": "update-block",
        "block": {
            "uid": block_uid,
            "string": content
        }
    }
    
    logger.debug(f"Updating block: {block_uid}")
    execute_write_action(action_data)
    
    return True


def transform_block(block_uid: str, find_pattern: str, replace_with: str, global_replace: bool = True) -> str:
    """
    Transform a block's content using regex pattern replacement.
    
    Args:
        block_uid: Block UID
        find_pattern: Regex pattern to find
        replace_with: Text to replace with
        global_replace: Whether to replace all occurrences
        
    Returns:
        Updated content
        
    Raises:
        APIError: If block retrieval or update fails
    """
    # First get the current content
    query = f'''[:find ?string .
                :where [?b :block/uid "{block_uid}"]
                        [?b :block/string ?string]]'''
    
    logger.debug(f"Getting content for block: {block_uid}")
    current_content = execute_query(query)
    
    if not current_content:
        error_msg = f"Block with UID {block_uid} not found"
        logger.error(error_msg)
        raise APIError(error_msg)
    
    # Apply transformation
    logger.debug(f"Transforming block {block_uid} with pattern: {find_pattern}")
    flags = re.MULTILINE
    count = 0 if global_replace else 1
    new_content = re.sub(find_pattern, replace_with, current_content, count=count, flags=flags)
    
    # Update the block
    update_block(block_uid, new_content)
    
    return new_content


def batch_update_blocks(updates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Update multiple blocks in a single operation.
    
    Args:
        updates: List of update operations
        
    Returns:
        List of results
    """
    session, headers = get_session_and_headers()
    results = []
    
    logger.debug(f"Batch updating {len(updates)} blocks")
    
    for update in updates:
        block_uid = update.get("block_uid")
        if not block_uid:
            results.append({"success": False, "error": "Missing block_uid"})
            continue
            
        try:
            # Handle direct content update
            if "content" in update:
                update_block(block_uid, update["content"])
                results.append({
                    "success": True,
                    "block_uid": block_uid,
                    "content": update["content"]
                })
            # Handle pattern transformation
            elif "transform" in update:
                transform = update["transform"]
                new_content = transform_block(
                    block_uid,
                    transform["find"],
                    transform["replace"],
                    transform.get("global", True)
                )
                results.append({
                    "success": True,
                    "block_uid": block_uid,
                    "content": new_content
                })
            else:
                results.append({
                    "success": False,
                    "block_uid": block_uid,
                    "error": "Neither content nor transform provided"
                })
        except Exception as e:
            logger.error(f"Error updating block {block_uid}: {str(e)}")
            results.append({
                "success": False,
                "block_uid": block_uid,
                "error": str(e)
            })
    
    # Log success rate
    successful = sum(1 for r in results if r.get("success"))
    logger.debug(f"Batch update completed: {successful}/{len(updates)} successful")
    
    return results


def get_page_content(title: str, resolve_refs: bool = True) -> str:
    """
    Get the content of a page with optional block reference resolution.
    
    Args:
        title: Page title
        resolve_refs: Whether to resolve block references
        
    Returns:
        Page content as markdown
        
    Raises:
        APIError: If page retrieval fails
    """
    session, headers = get_session_and_headers()
    
    # First find the page UID
    logger.debug(f"Getting content for page: {title}")
    page_uid = find_page_by_title(session, headers, GRAPH_NAME, title)
    
    if not page_uid:
        error_msg = f"Page '{title}' not found"
        logger.error(error_msg)
        raise APIError(error_msg)
    
    # Define query rule for ancestor relationship
    ancestor_rule = """[
        [(ancestor ?child ?parent)
            [?parent :block/children ?child]]
        [(ancestor ?child ?parent)
            [?p :block/children ?child]
            (ancestor ?p ?parent)]
    ]"""
    
    # Get all blocks on the page with their hierarchy information
    query = f"""[:find ?uid ?string ?order ?parent-uid
                :in $ % ?page-uid
                :where
                [?page :block/uid ?page-uid]
                [?block :block/string ?string]
                [?block :block/uid ?uid]
                [?block :block/order ?order]
                (ancestor ?block ?page)
                [?parent :block/children ?block]
                [?parent :block/uid ?parent-uid]]"""
    
    blocks = execute_query(query, [ancestor_rule, page_uid])
    
    if not blocks:
        logger.debug(f"No content found on page: {title}")
        return f"# {title}\n\nNo content found on this page."
    
    # Build a block hierarchy
    block_map = {}
    top_level_blocks = []
    
    for uid, content, order, parent_uid in blocks:
        # Create block object
        if resolve_refs:
            content = resolve_block_references(session, headers, GRAPH_NAME, content)
            
        block = {
            "uid": uid,
            "content": content,
            "order": order,
            "children": []
        }
        
        block_map[uid] = block
        
        # Add to parent's children or top level if parent is the page
        if parent_uid == page_uid:
            top_level_blocks.append(block)
        elif parent_uid in block_map:
            block_map[parent_uid]["children"].append(block)
    
    # Sort blocks by order
    def sort_blocks(blocks):
        blocks.sort(key=lambda b: b["order"])
        for block in blocks:
            sort_blocks(block["children"])
    
    sort_blocks(top_level_blocks)
    
    # Convert to markdown
    markdown = f"# {title}\n\n"
    
    def blocks_to_md(blocks, level=0):
        result = ""
        for block in blocks:
            indent = "  " * level
            result += f"{indent}- {block['content']}\n"
            if block["children"]:
                result += blocks_to_md(block["children"], level + 1)
        return result
    
    markdown += blocks_to_md(top_level_blocks)
    
    logger.debug(f"Retrieved page content for: {title}")
    return markdown