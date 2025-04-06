"""Core API functions for interacting with Roam Research."""

import os
import re
import sys
import logging
from typing import Dict, List, Any, Optional, Union, Set, Tuple, Callable
import requests
from datetime import datetime
import json
import time
from functools import wraps

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


# Enhanced Error Hierarchy
class RoamAPIError(Exception):
    """Base exception for all Roam API errors."""
    def __init__(self, message: str, code: Optional[str] = None, details: Optional[Dict] = None, remediation: Optional[str] = None):
        self.message = message
        self.code = code or "UNKNOWN_ERROR"
        self.details = details or {}
        self.remediation = remediation
        super().__init__(self._format_message())
        
    def _format_message(self) -> str:
        msg = f"{self.code}: {self.message}"
        if self.details:
            msg += f" - Details: {json.dumps(self.details)}"
        if self.remediation:
            msg += f" - Suggestion: {self.remediation}"
        return msg


class AuthenticationError(RoamAPIError):
    """Exception raised for authentication errors."""
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(
            message=message,
            code="AUTH_ERROR",
            details=details,
            remediation="Check your API token and graph name in environment variables."
        )


class PageNotFoundError(RoamAPIError):
    """Exception raised when a page cannot be found."""
    def __init__(self, title: str, details: Optional[Dict] = None):
        super().__init__(
            message=f"Page '{title}' not found",
            code="PAGE_NOT_FOUND",
            details=details,
            remediation="Check the page title for typos or create the page first."
        )


class BlockNotFoundError(RoamAPIError):
    """Exception raised when a block cannot be found."""
    def __init__(self, uid: str, details: Optional[Dict] = None):
        super().__init__(
            message=f"Block with UID '{uid}' not found",
            code="BLOCK_NOT_FOUND",
            details=details,
            remediation="Check the block UID for accuracy."
        )


class ValidationError(RoamAPIError):
    """Exception raised for input validation errors."""
    def __init__(self, message: str, param: str, details: Optional[Dict] = None):
        super().__init__(
            message=message,
            code="VALIDATION_ERROR",
            details={"parameter": param, **(details or {})},
            remediation="Check the input parameters and correct the formatting."
        )


class QueryError(RoamAPIError):
    """Exception raised for query execution errors."""
    def __init__(self, message: str, query: str, details: Optional[Dict] = None):
        super().__init__(
            message=message,
            code="QUERY_ERROR",
            details={"query": query, **(details or {})},
            remediation="Check the query syntax or parameters."
        )


class RateLimitError(RoamAPIError):
    """Exception raised when rate limits are exceeded."""
    def __init__(self, message: str, details: Optional[Dict] = None):
        super().__init__(
            message=message,
            code="RATE_LIMIT_ERROR",
            details=details,
            remediation="Retry after a delay or reduce the request frequency."
        )


class TransactionError(RoamAPIError):
    """Exception raised for transaction failures."""
    def __init__(self, message: str, action_type: str, details: Optional[Dict] = None):
        super().__init__(
            message=message,
            code="TRANSACTION_ERROR",
            details={"action_type": action_type, **(details or {})},
            remediation="Check the action data or retry the operation."
        )


class PreserveAuthSession(requests.Session):
    """Session class that preserves authentication headers during redirects."""
    def rebuild_auth(self, prepared_request, response):
        """Preserve the Authorization header on redirects."""
        return


# Retry decorator for API calls
def retry_on_error(max_retries=3, base_delay=1, backoff_factor=2, retry_on=(RateLimitError, requests.exceptions.RequestException)):
    """
    Decorator to retry API calls with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        backoff_factor: Multiplier for delay on each retry
        retry_on: Tuple of exception types to retry on
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except retry_on as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"Maximum retries ({max_retries}) exceeded: {str(e)}")
                        raise
                    
                    delay = base_delay * (backoff_factor ** (retries - 1))
                    logger.warning(f"Retrying after error: {str(e)}. Attempt {retries}/{max_retries} in {delay:.2f}s")
                    time.sleep(delay)
        return wrapper
    return decorator


def validate_credentials():
    """
    Validate that required API credentials are set.
    
    Raises:
        AuthenticationError: If required credentials are missing
    """
    if not API_TOKEN or not GRAPH_NAME:
        missing = []
        if not API_TOKEN:
            missing.append("ROAM_API_TOKEN")
        if not GRAPH_NAME:
            missing.append("ROAM_GRAPH_NAME")
            
        raise AuthenticationError(
            f"Missing required credentials: {', '.join(missing)}",
            {"missing": missing}
        )


def get_session_and_headers() -> Tuple[requests.Session, Dict[str, str]]:
    """
    Create a session with authentication headers.
    
    Returns:
        Tuple of (session, headers)
    
    Raises:
        AuthenticationError: If required environment variables are missing
    """
    validate_credentials()
    
    session = PreserveAuthSession()
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }
    
    return session, headers


@retry_on_error()
def execute_query(query: str, inputs: Optional[List[Any]] = None) -> Any:
    """
    Execute a Datalog query against the Roam graph.
    
    Args:
        query: Datalog query string
        inputs: Optional list of query inputs
        
    Returns:
        Query results
        
    Raises:
        QueryError: If the query fails
        AuthenticationError: If authentication fails
        RateLimitError: If rate limits are exceeded
    """
    validate_credentials()
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
        
        if response.status_code == 401:
            raise AuthenticationError("Authentication failed", {"status_code": response.status_code})
        
        if response.status_code == 429:
            raise RateLimitError("Rate limit exceeded", {"status_code": response.status_code})
        
        response.raise_for_status()
        result = response.json().get('result')
        
        # Log result size
        if isinstance(result, list):
            logger.debug(f"Query returned {len(result)} results")
            
        return result
    except requests.RequestException as e:
        error_msg = f"Query failed: {str(e)}"
        error_details = {}
        
        if hasattr(e, 'response') and e.response:
            error_details["status_code"] = e.response.status_code
            try:
                error_details["response"] = e.response.json()
            except:
                error_details["response_text"] = e.response.text[:500]
        
        # Classify error based on status code if available
        if hasattr(e, 'response') and e.response:
            if e.response.status_code == 401:
                raise AuthenticationError("Authentication failed", error_details) from e
            elif e.response.status_code == 429:
                raise RateLimitError("Rate limit exceeded", error_details) from e
        
        logger.error(error_msg, extra={"details": error_details})
        raise QueryError(error_msg, query, error_details) from e


@retry_on_error()
def execute_write_action(action_data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, Any]:
    """
    Execute a write action or a batch of actions on the Roam graph.
    
    Args:
        action_data: The action data to write or a list of actions for batch operation
        
    Returns:
        Response data
        
    Raises:
        TransactionError: If the write action fails
        AuthenticationError: If authentication fails
        RateLimitError: If rate limits are exceeded
    """
    validate_credentials()
    session, headers = get_session_and_headers()
    
    # Check if it's a batch operation or single action
    is_batch = isinstance(action_data, list)
    
    # If it's a batch operation, wrap it in a batch container
    if is_batch:
        # Log batch size
        logger.debug(f"Executing batch write action with {len(action_data)} operations")
        
        # Group operations by type for debugging
        action_types = {}
        for action in action_data:
            action_type = action.get("action", "unknown")
            if action_type in action_types:
                action_types[action_type] += 1
            else:
                action_types[action_type] = 1
                
        logger.debug(f"Batch operation types: {action_types}")
        
        # Prepare batch action
        batch_data = {
            "action": "batch-actions",
            "actions": action_data
        }
        
        action_type = "batch-actions"
        operation_data = batch_data
    else:
        # Log action type
        action_type = action_data.get("action", "unknown")
        logger.debug(f"Executing write action: {action_type}")
        operation_data = action_data
    
    # Debug log the operation data
    logger.debug(f"Sending data: {json.dumps(operation_data)[:100]}...")
    
    # Execute action
    try:
        response = session.post(
            f'https://api.roamresearch.com/api/graph/{GRAPH_NAME}/write',
            headers=headers,
            json=operation_data  # Use json parameter for proper JSON encoding
        )
        
        logger.debug(f"Status code: {response.status_code}")
        logger.debug(f"Response headers: {dict(response.headers)}")
        
        if response.status_code == 401:
            raise AuthenticationError("Authentication failed", {"status_code": response.status_code})
        
        if response.status_code == 429:
            raise RateLimitError("Rate limit exceeded", {"status_code": response.status_code})
        
        # Special handling for empty responses
        if response.status_code == 200 and not response.text:
            logger.debug("Received empty response with status 200 (success)")
            return {"success": True}
        
        response.raise_for_status()
        
        # Try to parse JSON response
        try:
            result = response.json()
            logger.debug(f"Response: {json.dumps(result)[:500]}")
            
            # Success even with error message for batch operations that partly succeed
            if "batch-error-message" in result and "num-actions-successfully-transacted-before-failure" in result:
                num_success = result.get("num-actions-successfully-transacted-before-failure", 0)
                logger.debug(f"Batch partially succeeded with {num_success} actions before failure")
                return result
            
            return result
        except json.JSONDecodeError:
            # Some successful operations return empty responses
            if 200 <= response.status_code < 300:
                logger.debug("Success with non-JSON response")
                return {"success": True}
            else:
                logger.debug(f"Failed to parse response as JSON: {response.text[:500]}")
                raise TransactionError(
                    f"Failed to parse response as JSON",
                    action_type,
                    {"response_text": response.text[:500]}
                )
            
    except requests.RequestException as e:
        error_details = {}
        
        if hasattr(e, 'response') and e.response:
            error_details["status_code"] = e.response.status_code
            try:
                error_details["response"] = e.response.json()
            except:
                error_details["response_text"] = e.response.text[:500]
        
        # Classify error based on status code if available
        if hasattr(e, 'response') and e.response:
            if e.response.status_code == 401:
                raise AuthenticationError("Authentication failed", error_details) from e
            elif e.response.status_code == 429:
                raise RateLimitError("Rate limit exceeded", error_details) from e
        
        error_msg = f"Write action failed: {str(e)}"
        logger.error(error_msg, extra={"details": error_details})
        raise TransactionError(error_msg, action_type, error_details) from e


def execute_batch_actions(actions: List[Dict[str, Any]], chunk_size: int = 50) -> Dict[str, Any]:
    """
    Execute a batch of actions, optionally chunking into multiple requests.
    
    Args:
        actions: List of actions to execute
        chunk_size: Maximum number of actions per request
        
    Returns:
        Combined results of all batch operations
        
    Raises:
        TransactionError: If any batch fails
    """
    if not actions:
        return {"success": True, "created_uids": []}
    
    # Single batch if under chunk size
    if len(actions) <= chunk_size:
        result = execute_write_action(actions)
        
        # Check for tempids-to-uids mapping in response
        if "tempids-to-uids" in result:
            return {"success": True, "created_uids": list(result["tempids-to-uids"].values())}
        elif "successful" in result and result["successful"]:
            return {"success": True, "created_uids": []}
        else:
            return result
    
    # Split into chunks for larger batches
    chunks = [actions[i:i + chunk_size] for i in range(0, len(actions), chunk_size)]
    logger.debug(f"Splitting batch operation into {len(chunks)} chunks of max {chunk_size} actions")
    
    # Track results across chunks
    combined_results = {
        "created_uids": [],
        "success": True
    }
    
    # Track temporary and real UIDs for parent-child relationships
    temp_uid_map = {}
    
    # Execute each chunk
    for i, chunk in enumerate(chunks):
        logger.debug(f"Executing batch chunk {i+1}/{len(chunks)} with {len(chunk)} actions")
        
        # Update parent UIDs with real UIDs from previous chunks
        if i > 0 and temp_uid_map:
            for action in chunk:
                if action["action"] == "create-block":
                    parent_uid = action["location"]["parent-uid"]
                    if parent_uid.startswith("temp_") and parent_uid in temp_uid_map:
                        action["location"]["parent-uid"] = temp_uid_map[parent_uid]
        
        result = execute_write_action(chunk)
        
        # Collect UIDs from this chunk
        created_uids = []
        if "tempids-to-uids" in result:
            created_uids = list(result["tempids-to-uids"].values())
        
        if created_uids:
            # Map temp UIDs to real UIDs for next chunks
            if i < len(chunks) - 1:
                for j, uid in enumerate(created_uids):
                    temp_key = f"temp_{i}_{j}"
                    temp_uid_map[temp_key] = uid
            
            combined_results["created_uids"].extend(created_uids)
        
        # Add delay between batches to ensure ordering
        if i < len(chunks) - 1:
            time.sleep(0.5)
    
    return combined_results


def find_or_create_page(title: str) -> str:
    """
    Find a page by title or create it if it doesn't exist.
    
    Args:
        title: Page title
        
    Returns:
        Page UID
        
    Raises:
        TransactionError: If page creation fails
        ValidationError: If title is invalid
        AuthenticationError: If authentication fails
    """
    validate_credentials()
    session, headers = get_session_and_headers()
    
    # Validate title
    if not title or not isinstance(title, str):
        raise ValidationError("Page title must be a non-empty string", "title")
    
    title = title.strip()
    if not title:
        raise ValidationError("Page title cannot be empty or just whitespace", "title")
    
    # Try to find the page first
    logger.debug(f"Looking for page: {title}")
    query = f'''[:find ?uid .
              :where [?e :node/title "{title}"]
                     [?e :block/uid ?uid]]'''
    
    page_uid = execute_query(query)
    
    if page_uid:
        logger.debug(f"Found existing page: {title} (UID: {page_uid})")
        return page_uid
    
    # Create the page if it doesn't exist
    logger.debug(f"Creating new page: {title}")
    action_data = {
        "action": "create-page",
        "page": {"title": title}
    }
    
    try:
        response = execute_write_action(action_data)
        
        if response.get("success", False):
            # Wait a moment for the page to be created
            time.sleep(1)
            
            # Try to find the page again
            page_uid = execute_query(query)
            if page_uid:
                logger.debug(f"Created page: {title} (UID: {page_uid})")
                return page_uid
                
            # If still not found, try one more time with a longer delay
            time.sleep(2)
            page_uid = execute_query(query)
            if page_uid:
                logger.debug(f"Found newly created page: {title} (UID: {page_uid})")
                return page_uid
            
        # If we get here, something went wrong
        error_msg = f"Failed to create page: {title}"
        logger.error(error_msg)
        raise TransactionError(error_msg, "create-page", {"title": title, "response": response})
    except TransactionError:
        # Rethrow existing TransactionError
        raise
    except Exception as e:
        error_msg = f"Failed to create page: {title}"
        logger.error(error_msg)
        raise TransactionError(error_msg, "create-page", {"title": title, "error": str(e)}) from e


def get_daily_page() -> str:
    """
    Get or create today's daily page.
    
    Returns:
        Daily page UID
        
    Raises:
        TransactionError: If page creation fails
    """
    today = datetime.now()
    date_str = format_roam_date(today)
    
    logger.debug(f"Getting daily page for: {date_str}")
    return find_or_create_page(date_str)


def add_block_to_page(page_uid: str, content: str, order: Union[int, str] = "last") -> Optional[str]:
    """
    Add a block to a page.
    
    Args:
        page_uid: Parent page UID
        content: Block content
        order: Position ("first", "last", or integer index)
        
    Returns:
        New block UID or None if creation failed
        
    Raises:
        BlockNotFoundError: If page does not exist
        ValidationError: If parameters are invalid
        TransactionError: If block creation fails
    """
    # Validate parameters
    if not page_uid:
        raise ValidationError("Parent page UID is required", "page_uid")
    
    if not content:
        raise ValidationError("Block content cannot be empty", "content")
    
    # Generate a unique block UID
    import uuid
    block_uid = str(uuid.uuid4())[:9]
    
    action_data = {
        "action": "create-block",
        "location": {
            "parent-uid": page_uid,
            "order": order
        },
        "block": {
            "string": content,
            "uid": block_uid
        }
    }
    
    logger.debug(f"Adding block to page {page_uid}")
    try:
        result = execute_write_action(action_data)
        
        if result.get("success", False):
            # Add a brief delay to ensure the block is created
            time.sleep(1)
            
            # Verify the block exists
            session, headers = get_session_and_headers()
            found_uid = find_block_uid(session, headers, GRAPH_NAME, content)
            
            if found_uid:
                logger.debug(f"Created block with UID: {found_uid}")
                return found_uid
            
            # If we couldn't find the UID by content, return the one we generated
            logger.debug(f"Block created but couldn't verify, returning generated UID: {block_uid}")
            return block_uid
        else:
            logger.error(f"Failed to create block: {result.get('error', 'Unknown error')}")
            return None
    except Exception as e:
        if isinstance(e, (BlockNotFoundError, ValidationError, TransactionError)):
            raise
        
        error_msg = f"Failed to create block: {str(e)}"
        logger.error(error_msg)
        raise TransactionError(error_msg, "create-block", {"page_uid": page_uid}) from e


def update_block(block_uid: str, content: str) -> bool:
    """
    Update a block's content.
    
    Args:
        block_uid: Block UID
        content: New content
        
    Returns:
        Success flag
        
    Raises:
        BlockNotFoundError: If block does not exist
        ValidationError: If parameters are invalid
        TransactionError: If block update fails
    """
    # Validate parameters
    if not block_uid:
        raise ValidationError("Block UID is required", "block_uid")
    
    if content is None:
        raise ValidationError("Block content cannot be None", "content")
    
    action_data = {
        "action": "update-block",
        "block": {
            "uid": block_uid,
            "string": content
        }
    }
    
    logger.debug(f"Updating block: {block_uid}")
    try:
        execute_write_action(action_data)
        return True
    except Exception as e:
        if isinstance(e, (BlockNotFoundError, ValidationError, TransactionError)):
            raise
            
        error_msg = f"Failed to update block: {str(e)}"
        logger.error(error_msg)
        raise TransactionError(error_msg, "update-block", {"block_uid": block_uid}) from e


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
        BlockNotFoundError: If block does not exist
        ValidationError: If parameters are invalid
        QueryError: If block retrieval fails
        TransactionError: If block update fails
    """
    # Validate parameters
    if not block_uid:
        raise ValidationError("Block UID is required", "block_uid")
    
    if not find_pattern:
        raise ValidationError("Find pattern cannot be empty", "find_pattern")
    
    # First get the current content
    query = f'''[:find ?string .
                :where [?b :block/uid "{block_uid}"]
                        [?b :block/string ?string]]'''
    
    logger.debug(f"Getting content for block: {block_uid}")
    try:
        current_content = execute_query(query)
        
        if not current_content:
            raise BlockNotFoundError(block_uid)
        
        # Apply transformation
        logger.debug(f"Transforming block {block_uid} with pattern: {find_pattern}")
        flags = re.MULTILINE
        count = 0 if global_replace else 1
        
        try:
            new_content = re.sub(find_pattern, replace_with, current_content, count=count, flags=flags)
        except re.error as e:
            raise ValidationError(f"Invalid regex pattern: {str(e)}", "find_pattern", {"pattern": find_pattern})
        
        # Update the block
        update_block(block_uid, new_content)
        
        return new_content
    except (BlockNotFoundError, ValidationError, QueryError, TransactionError):
        # Rethrow existing errors
        raise
    except Exception as e:
        error_msg = f"Failed to transform block: {str(e)}"
        logger.error(error_msg)
        raise TransactionError(error_msg, "transform-block", {"block_uid": block_uid}) from e


def batch_update_blocks(updates: List[Dict[str, Any]], chunk_size: int = 50) -> List[Dict[str, Any]]:
    """
    Update multiple blocks in a single operation.
    
    Args:
        updates: List of update operations
        chunk_size: Maximum number of actions per batch
        
    Returns:
        List of results
        
    Raises:
        ValidationError: If updates are not valid
    """
    if not isinstance(updates, list):
        raise ValidationError("Updates must be a list", "updates")
    
    if not updates:
        return []
    
    session, headers = get_session_and_headers()
    results = []
    batch_actions = []
    
    logger.debug(f"Batch updating {len(updates)} blocks")
    
    # Validate each update and prepare batch actions
    for i, update in enumerate(updates):
        try:
            block_uid = update.get("block_uid")
            if not block_uid:
                results.append({"success": False, "error": "Missing block_uid"})
                continue
                
            # Check block exists
            query = f'''[:find ?string .
                        :where [?b :block/uid "{block_uid}"]
                                [?b :block/string ?string]]'''
            
            current_content = execute_query(query)
            if not current_content:
                results.append({
                    "success": False,
                    "block_uid": block_uid,
                    "error": f"Block with UID {block_uid} not found"
                })
                continue
            
            # Handle direct content update
            if "content" in update:
                batch_actions.append({
                    "action": "update-block",
                    "block": {
                        "uid": block_uid,
                        "string": update["content"]
                    }
                })
                
                results.append({
                    "success": True,
                    "block_uid": block_uid,
                    "content": update["content"]
                })
            # Handle pattern transformation
            elif "transform" in update:
                transform = update["transform"]
                
                try:
                    find_pattern = transform["find"]
                    replace_with = transform["replace"]
                    global_replace = transform.get("global", True)
                    
                    # Apply transformation
                    flags = re.MULTILINE
                    count = 0 if global_replace else 1
                    new_content = re.sub(find_pattern, replace_with, current_content, count=count, flags=flags)
                    
                    batch_actions.append({
                        "action": "update-block",
                        "block": {
                            "uid": block_uid,
                            "string": new_content
                        }
                    })
                    
                    results.append({
                        "success": True,
                        "block_uid": block_uid,
                        "content": new_content
                    })
                except re.error as e:
                                    results.append({
                                        "success": False,
                                        "block_uid": block_uid,
                                        "error": f"Invalid regex pattern: {str(e)}"
                    })
                except KeyError as e:
                    results.append({
                        "success": False,
                        "block_uid": block_uid,
                        "error": f"Missing required transform key: {str(e)}"
                    })
            else:
                results.append({
                    "success": False,
                    "block_uid": block_uid,
                    "error": "Neither content nor transform provided"
                })
        except Exception as e:
            logger.error(f"Error preparing update for block {update.get('block_uid', 'unknown')}: {str(e)}")
            results.append({
                "success": False,
                "block_uid": update.get("block_uid", "unknown"),
                "error": str(e)
            })
    
    # Execute batch updates if we have any valid actions
    if batch_actions:
        try:
            execute_batch_actions(batch_actions, chunk_size)
        except Exception as e:
            logger.error(f"Error executing batch update: {str(e)}")
            # Mark all previously successful results as failed
            for result in results:
                if result.get("success"):
                    result["success"] = False
                    result["error"] = f"Batch update failed: {str(e)}"
    
    # Log success rate
    successful = sum(1 for r in results if r.get("success"))
    logger.debug(f"Batch update completed: {successful}/{len(updates)} successful")
    
    return results


def get_page_content(title: str, resolve_refs: bool = True, max_depth: int = 5) -> str:
    """
    Get the content of a page with optional block reference resolution.
    
    Args:
        title: Page title
        resolve_refs: Whether to resolve block references
        max_depth: Maximum depth of nested blocks to retrieve (default: 5)
        
    Returns:
        Page content as markdown
        
    Raises:
        PageNotFoundError: If page retrieval fails
        QueryError: If query execution fails
    """
    session, headers = get_session_and_headers()
    
    # First find the page UID
    logger.debug(f"Getting content for page: {title}")
    page_uid = find_page_by_title(session, headers, GRAPH_NAME, title)
    
    if not page_uid:
        raise PageNotFoundError(title)
    
    # Build block hierarchy iteratively
    block_map = {}
    top_level_blocks = []
    
    # Query to get immediate children of a parent (page or block)
    def get_children(parent_uid: str, depth: int = 0) -> None:
        if depth >= max_depth:
            return
        
        query = f"""[:find ?uid ?string ?order
                    :where
                    [?parent :block/uid "{parent_uid}"]
                    [?parent :block/children ?child]
                    [?child :block/uid ?uid]
                    [?child :block/string ?string]
                    [?child :block/order ?order]]"""
        
        try:
            results = execute_query(query)
            if not results:
                return
            
            for uid, content, order in results:
                # Resolve references if requested
                if resolve_refs:
                    content = resolve_block_references(session, headers, GRAPH_NAME, content)
                
                # Create block object
                block = {
                    "uid": uid,
                    "content": content,
                    "order": order,
                    "children": []
                }
                
                block_map[uid] = block
                
                # Add to top-level or parent's children
                if parent_uid == page_uid:
                    top_level_blocks.append(block)
                elif parent_uid in block_map:
                    block_map[parent_uid]["children"].append(block)
                
                # Recursively fetch children
                get_children(uid, depth + 1)
                
        except QueryError as e:
            logger.warning(f"Failed to fetch children for {parent_uid}: {str(e)}")
            raise
    
    try:
        # Start with the page's top-level blocks
        get_children(page_uid)
        
        if not top_level_blocks:
            logger.debug(f"No content found on page: {title}")
            return f"# {title}\n\nNo content found on this page."
        
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
    except QueryError:
        # Rethrow existing QueryError
        raise
    except Exception as e:
        error_msg = f"Failed to get page content: {str(e)}"
        logger.error(error_msg)
        raise QueryError(error_msg, "Iterative child fetch", {"page_title": title, "page_uid": page_uid}) from e