"""Core API functions and client for interacting with Roam Research."""

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

# Assuming utils are refactored and available at top level if needed outside client
from roam_mcp.utils import (
    format_roam_date,
    # find_block_uid, # Now part of utils, requires client
    # find_page_by_title, # Now part of utils, requires client
    # process_nested_content, # Now part of utils, requires client
    # resolve_block_references # Now part of utils, requires client
)
# Import utility functions that are now needed within this module
from roam_mcp.utils import find_block_uid as find_block_uid_util
from roam_mcp.utils import find_page_by_title as find_page_by_title_util
from roam_mcp.utils import process_nested_content as process_nested_content_util
from roam_mcp.utils import resolve_block_references as resolve_block_references_util


# Set up logging
logger = logging.getLogger("roam-mcp.api")

# --- Environment Variables ---
# Moved retrieval inside functions/client init to allow override/testing

def get_api_token() -> Optional[str]:
    return os.environ.get("ROAM_API_TOKEN")

def get_graph_name() -> Optional[str]:
    return os.environ.get("ROAM_GRAPH_NAME")

def get_memories_tag() -> str:
    return os.environ.get("MEMORIES_TAG", "#[[Memories]]")


# --- Enhanced Error Hierarchy (Same as before) ---
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
            remediation="Check your API token and graph name in environment variables or configuration."
        )

class PageNotFoundError(RoamAPIError):
    """Exception raised when a page cannot be found."""
    def __init__(self, title_or_uid: str, details: Optional[Dict] = None):
        super().__init__(
            message=f"Page '{title_or_uid}' not found",
            code="PAGE_NOT_FOUND",
            details=details,
            remediation="Check the page title/UID for typos or create the page first."
        )

class BlockNotFoundError(RoamAPIError):
    """Exception raised when a block cannot be found."""
    def __init__(self, uid_or_details: str, details: Optional[Dict] = None):
        super().__init__(
            message=f"Block '{uid_or_details}' not found",
            code="BLOCK_NOT_FOUND",
            details=details,
            remediation="Check the block UID or search criteria."
        )

class ValidationError(RoamAPIError):
    """Exception raised for input validation errors."""
    def __init__(self, message: str, param: Optional[str] = None, details: Optional[Dict] = None):
        final_details = details or {}
        if param:
             final_details["parameter"] = param
        super().__init__(
            message=message,
            code="VALIDATION_ERROR",
            details=final_details,
            remediation="Check the input parameters and correct the formatting or values."
        )

class QueryError(RoamAPIError):
    """Exception raised for query execution errors."""
    def __init__(self, message: str, query: Optional[str] = None, details: Optional[Dict] = None):
        final_details = details or {}
        if query:
             final_details["query"] = query
        super().__init__(
            message=message,
            code="QUERY_ERROR",
            details=final_details,
            remediation="Check the Datalog query syntax or parameters."
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
    """Exception raised for transaction failures (write actions)."""
    def __init__(self, message: str, action_type: str, details: Optional[Dict] = None):
        super().__init__(
            message=message,
            code="TRANSACTION_ERROR",
            details={"action_type": action_type, **(details or {})},
            remediation="Check the action data, ensure dependencies exist, or retry the operation."
        )

# --- Session Management ---
class PreserveAuthSession(requests.Session):
    """Session class that preserves authentication headers during redirects."""
    def rebuild_auth(self, prepared_request, response):
        """Preserve the Authorization header on redirects."""
        # This method is called by requests library during redirects.
        # By default, it strips Authorization. We override to do nothing, preserving it.
        pass

# --- Retry Decorator ---
def retry_on_error(max_retries=3, base_delay=1, backoff_factor=2, retry_on=(RateLimitError, requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
    """Decorator to retry API calls with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            last_exception = None
            while True:
                try:
                    return func(*args, **kwargs)
                except retry_on as e:
                    last_exception = e
                    retries += 1
                    if retries > max_retries:
                        logger.error(f"Maximum retries ({max_retries}) exceeded for {func.__name__}: {str(e)}")
                        # Re-raise the last captured exception
                        raise last_exception from e
                    
                    delay = base_delay * (backoff_factor ** (retries - 1))
                    logger.warning(f"Retrying {func.__name__} after error: {str(e)}. Attempt {retries}/{max_retries} in {delay:.2f}s")
                    time.sleep(delay)
                except Exception as e:
                    # Don't retry on other exceptions (like AuthError, ValidationError etc.)
                    raise e
            # Should not be reachable if an exception occurred and retries failed
            raise last_exception if last_exception else RuntimeError("Retry logic failed unexpectedly")
        return wrapper
    return decorator


# --- Roam API Client ---
class RoamClient:
    """Client for interacting with the Roam Research API."""

    def __init__(self, api_token: Optional[str] = None, graph_name: Optional[str] = None):
        self.api_token = api_token or get_api_token()
        self.graph_name = graph_name or get_graph_name()
        
        if not self.api_token or not self.graph_name:
            missing = []
            if not self.api_token: missing.append("ROAM_API_TOKEN")
            if not self.graph_name: missing.append("ROAM_GRAPH_NAME")
            raise AuthenticationError(f"Missing required credentials: {', '.join(missing)}", {"missing": missing})
        
        self.base_url = f'https://api.roamresearch.com/api/graph/{self.graph_name}'
        self.session = self._create_session()
        logger.info(f"RoamClient initialized for graph: {self.graph_name}")

    def _create_session(self) -> requests.Session:
        """Creates a requests session with persistent headers."""
        session = PreserveAuthSession()
        session.headers.update({
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        })
        return session

    @retry_on_error()
    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Makes an HTTP request to the Roam API."""
        url = f'{self.base_url}/{endpoint}'
        logger.debug(f"Making {method.upper()} request to {url}")
        try:
            response = self.session.request(method, url, **kwargs)

            # Check for common errors
            if response.status_code == 401:
                raise AuthenticationError("Authentication failed (401)", {"status_code": 401, "url": url})
            if response.status_code == 403: # Forbidden might also indicate auth issues
                 raise AuthenticationError("Forbidden (403) - Check token permissions or graph name", {"status_code": 403, "url": url})
            if response.status_code == 404 and endpoint != 'q' and endpoint != 'write': # 404 is expected for non-existent blocks/pages in queries
                 raise RoamAPIError(f"Endpoint not found (404): {endpoint}", code="ENDPOINT_NOT_FOUND", details={"status_code": 404, "url": url})
            if response.status_code == 429:
                raise RateLimitError("Rate limit exceeded (429)", {"status_code": 429, "url": url})
            
            # Raise HTTP errors for other bad statuses (4xx, 5xx)
            response.raise_for_status()
            
            return response
        
        except requests.exceptions.RequestException as e:
             # Handle potential request exceptions (e.g., timeout, connection error)
             error_msg = f"Network request failed: {str(e)}"
             error_details = {"url": url}
             if hasattr(e, 'response') and e.response is not None:
                 error_details["status_code"] = e.response.status_code
                 try:
                     error_details["response_body"] = e.response.text[:500] # Log part of the response
                 except Exception: pass # Ignore if response body cannot be read

             logger.error(error_msg, extra={"details": error_details})
             # Re-classify specific request exceptions if needed (e.g., Timeout)
             if isinstance(e, requests.exceptions.Timeout):
                 raise RateLimitError("Request timed out, possibly due to server load or rate limits.", error_details) from e
             raise RoamAPIError(error_msg, code="NETWORK_ERROR", details=error_details) from e


    def query(self, query: str, inputs: Optional[List[Any]] = None) -> Any:
        """Executes a Datalog query."""
        data = {"query": query}
        if inputs:
            data["inputs"] = inputs
        
        try:
            response = self._request('post', 'q', json=data)
            json_response = response.json()
            result = json_response.get('result')
            if isinstance(result, list):
                 logger.debug(f"Query returned {len(result)} results.")
            return result
        except RoamAPIError as e: # Catch specific API errors
            # Add query context if not already present
            if "query" not in e.details:
                 e.details["query"] = query
            if isinstance(e, AuthenticationError): raise e # Re-raise critical errors
            if isinstance(e, RateLimitError): raise e
            raise QueryError(f"Query execution failed: {e.message}", query=query, details=e.details) from e
        except requests.exceptions.JSONDecodeError as e:
            raise QueryError("Failed to decode JSON response from query.", query=query, details={"response_text": response.text[:500]}) from e
        except Exception as e: # Catch unexpected errors during processing
            raise QueryError(f"Unexpected error during query: {str(e)}", query=query) from e


    def write(self, action_data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Executes a write action or a batch of actions."""
        is_batch = isinstance(action_data, list)
        action_type = "batch-actions" if is_batch else action_data.get("action", "unknown")
        
        payload = {"action": "batch-actions", "actions": action_data} if is_batch else action_data
        
        try:
            if is_batch: logger.debug(f"Executing batch write with {len(action_data)} actions.")
            else: logger.debug(f"Executing single write action: {action_type}")

            response = self._request('post', 'write', json=payload)
            json_response = response.json()

            # Specific validation for batch results
            # Roam returns 200 OK even if individual actions fail within a batch.
            # The 'successful' key (if present) might indicate overall status, but it's not reliable.
            # We rely on the caller to check if expected results (like created_uids) are present.
            # If Roam provides better error reporting in the future, this can be improved.
            
            # Minimal check: log if 'successful: false' is returned
            if isinstance(json_response, dict) and json_response.get("successful") is False:
                 logger.warning("Roam API reported 'successful: false' for a batch operation.")
                 # Potentially raise TransactionError here if needed, but Roam's reporting is inconsistent.
                 # raise TransactionError("Batch operation reported as unsuccessful by Roam API.", action_type, {"response": json_response})
            
            return json_response # Return the full response dict

        except RoamAPIError as e: # Catch specific API errors
             if "action_type" not in e.details:
                  e.details["action_type"] = action_type
             if isinstance(e, AuthenticationError): raise e # Re-raise critical errors
             if isinstance(e, RateLimitError): raise e
             raise TransactionError(f"Write action '{action_type}' failed: {e.message}", action_type=action_type, details=e.details) from e
        except requests.exceptions.JSONDecodeError as e:
            raise TransactionError("Failed to decode JSON response from write action.", action_type=action_type, details={"response_text": response.text[:500]}) from e
        except Exception as e: # Catch unexpected errors during processing
            raise TransactionError(f"Unexpected error during write action '{action_type}': {str(e)}", action_type=action_type) from e


# --- Helper Functions using RoamClient ---
# These functions now primarily act as wrappers around the client methods or utils

_client_instance: Optional[RoamClient] = None

def get_client() -> RoamClient:
    """Gets a shared RoamClient instance, initializing if needed."""
    global _client_instance
    if _client_instance is None:
        _client_instance = RoamClient() # Assumes env vars are set
    return _client_instance

# Expose query and write directly via helpers if desired, or require client usage
def execute_query(query: str, inputs: Optional[List[Any]] = None) -> Any:
    """Helper function to execute a query using the shared client."""
    client = get_client()
    return client.query(query, inputs)

def execute_write_action(action_data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, Any]:
    """Helper function to execute write actions using the shared client."""
    client = get_client()
    return client.write(action_data)


def execute_batch_actions(actions: List[Dict[str, Any]], chunk_size: int = 50) -> Dict[str, Any]:
    """
    Execute a batch of actions using the RoamClient, with chunking.
    Focuses on execution and basic UID collection, improved dependency handling
    is complex and deferred for now, but uses the RoamClient.

    Args:
        actions: List of actions to execute.
        chunk_size: Max actions per API request.

    Returns:
        Dictionary with 'success' status and 'created_uids'.
        Note: 'success' only reflects API call success, not individual action success within batch.

    Raises:
        TransactionError: If any chunk fails at the API level.
    """
    client = get_client()
    if not actions:
        return {"success": True, "created_uids": []}

    if not isinstance(actions, list):
         raise ValidationError("Actions must be a list.", "actions")

    # Simple chunking without complex dependency sorting for now
    chunks = [actions[i:i + chunk_size] for i in range(0, len(actions), chunk_size)]
    logger.debug(f"Splitting batch operation into {len(chunks)} chunks of max {chunk_size} actions")

    all_created_uids = []
    overall_success = True

    for i, chunk in enumerate(chunks):
        logger.debug(f"Executing batch chunk {i+1}/{len(chunks)} with {len(chunk)} actions")
        try:
            # Use the client's write method which handles retries etc.
            result = client.write(chunk)

            # Collect UIDs if available in the response
            # Roam's response format for batch UIDs can vary, check carefully
            created_uids_in_chunk = result.get("created_uids", []) if isinstance(result, dict) else []
            
            if isinstance(created_uids_in_chunk, list):
                 all_created_uids.extend(created_uids_in_chunk)
                 logger.debug(f"Chunk {i+1} successful, got {len(created_uids_in_chunk)} UIDs.")
            else:
                 logger.warning(f"Chunk {i+1} executed but 'created_uids' was not a list: {created_uids_in_chunk}")
                 # Consider this a partial failure? For now, log and continue.
                 overall_success = False # Mark overall as potentially problematic

        except TransactionError as e:
            logger.error(f"Batch chunk {i+1} failed: {str(e)}")
            # Add chunk info to details if possible
            e.details["failed_chunk_index"] = i
            e.details["failed_chunk_size"] = len(chunk)
            overall_success = False
            # Depending on desired behavior, you could stop here or try subsequent chunks
            # For now, we stop on the first chunk failure
            raise e
        except Exception as e:
             logger.error(f"Unexpected error during batch chunk {i+1}: {str(e)}", exc_info=True)
             overall_success = False
             raise TransactionError(f"Unexpected error in batch chunk {i+1}", "batch-actions", {"chunk_index": i}) from e

    # Return combined results
    # 'success' here mainly means no API-level errors stopped the process.
    return {"success": overall_success, "created_uids": all_created_uids}


def find_or_create_page(title: str) -> str:
    """Find a page by title or create it if it doesn't exist using the RoamClient."""
    client = get_client()
    
    if not title or not isinstance(title, str):
        raise ValidationError("Page title must be a non-empty string", "title")
    title = title.strip()
    if not title:
        raise ValidationError("Page title cannot be empty or just whitespace", "title")

    page_uid = find_page_by_title_util(client, title) # Use util version with client

    if page_uid:
        logger.debug(f"Found existing page: '{title}' (UID: {page_uid})")
        return page_uid
    
    logger.info(f"Page '{title}' not found, attempting to create.")
    action_data = {"action": "create-page", "page": {"title": title}}
    
    try:
        response = client.write(action_data)
        
        # Roam *should* return the UID in the response, but it's unreliable.
        # Try to extract UID from response if possible
        new_uid_from_response = None
        if isinstance(response, dict) and isinstance(response.get("page"), dict):
             new_uid_from_response = response["page"].get("uid")
        
        if new_uid_from_response and isinstance(new_uid_from_response, str) and len(new_uid_from_response) == 9:
             logger.info(f"Created page '{title}' (UID: {new_uid_from_response}) via response.")
             return new_uid_from_response
        else:
             logger.warning(f"UID not found directly in create-page response for '{title}'. Attempting lookup.")
             # If UID wasn't in the response, try finding the page again immediately
             time.sleep(0.5) # Short delay to allow index update
             page_uid_after_create = find_page_by_title_util(client, title)
             if page_uid_after_create:
                 logger.info(f"Found newly created page '{title}' (UID: {page_uid_after_create}) via lookup.")
                 return page_uid_after_create
             else:
                 raise TransactionError(f"Page creation command sent for '{title}', but failed to confirm creation or find UID.", "create-page", {"title": title, "response": response})

    except (TransactionError, ValidationError, QueryError) as e:
         logger.error(f"Failed during find_or_create_page for '{title}': {e}", exc_info=True)
         raise # Re-raise specific known errors
    except Exception as e:
         logger.error(f"Unexpected error in find_or_create_page for '{title}': {e}", exc_info=True)
         raise TransactionError(f"Unexpected error creating page '{title}'", "create-page", {"title": title}) from e


def get_daily_page() -> str:
    """Get or create today's daily page UID."""
    today_str = format_roam_date(datetime.now())
    logger.debug(f"Getting or creating daily page for: {today_str}")
    return find_or_create_page(today_str)


def add_block_to_page(page_uid: str, content: str, order: Union[int, str] = "last") -> str:
    """Add a block to a page using the RoamClient."""
    client = get_client()
    if not page_uid: raise ValidationError("Parent page UID is required", "page_uid")
    if not content: raise ValidationError("Block content cannot be empty", "content")

    action_data = {
        "action": "create-block",
        "location": {"parent-uid": page_uid, "order": order},
        "block": {"string": content}
    }
    
    logger.debug(f"Adding block to page/block {page_uid}")
    try:
        # Execute the write action
        client.write(action_data)
        
        # Roam doesn't return the new block UID reliably. Find it by content.
        # Add a small delay to increase chances of finding it
        time.sleep(0.75)
        block_uid = find_block_uid_util(client, content) # Use util version with client
        
        logger.info(f"Successfully added block (UID: {block_uid}) to parent {page_uid}")
        return block_uid

    except BlockNotFoundError as e:
         logger.error(f"Failed to find newly created block with content '{content[:50]}...' under parent {page_uid}", exc_info=True)
         raise TransactionError(f"Block created for '{content[:50]}...' but could not be found afterwards.", "create-block", {"page_uid": page_uid}) from e
    except (TransactionError, ValidationError) as e:
         logger.error(f"Failed to add block to {page_uid}: {e}", exc_info=True)
         raise
    except Exception as e:
         logger.error(f"Unexpected error adding block to {page_uid}: {e}", exc_info=True)
         raise TransactionError(f"Unexpected error adding block", "create-block", {"page_uid": page_uid}) from e


def update_block(block_uid: str, content: str) -> bool:
    """Update a block's content using the RoamClient."""
    client = get_client()
    if not block_uid: raise ValidationError("Block UID is required", "block_uid")
    if content is None: raise ValidationError("Block content cannot be None", "content")

    action_data = {
        "action": "update-block",
        "block": {"uid": block_uid, "string": content}
    }
    
    logger.debug(f"Updating block: {block_uid}")
    try:
        client.write(action_data)
        logger.info(f"Successfully updated block {block_uid}")
        return True
    except (TransactionError, ValidationError) as e:
         logger.error(f"Failed to update block {block_uid}: {e}", exc_info=True)
         raise
    except Exception as e:
         logger.error(f"Unexpected error updating block {block_uid}: {e}", exc_info=True)
         raise TransactionError(f"Unexpected error updating block", "update-block", {"block_uid": block_uid}) from e


def transform_block(block_uid: str, find_pattern: str, replace_with: str, global_replace: bool = True) -> str:
    """Transform block content using regex and the RoamClient."""
    client = get_client()
    if not block_uid: raise ValidationError("Block UID is required", "block_uid")
    if not find_pattern: raise ValidationError("Find pattern cannot be empty", "find_pattern")

    # 1. Get current content
    logger.debug(f"Getting content for block transform: {block_uid}")
    query = f'''[:find ?string . :where [?b :block/uid "{block_uid}"] [?b :block/string ?string]]'''
    try:
        current_content = client.query(query)
        if current_content is None: # Query returns None if block not found
            raise BlockNotFoundError(block_uid)
        if not isinstance(current_content, str): # Safety check
             raise QueryError("Received unexpected data type for block content.", query, {"received": type(current_content).__name__})
    except QueryError as e:
         logger.error(f"Failed to get content for block {block_uid} transform: {e}", exc_info=True)
         raise # Re-raise QueryError
    
    # 2. Apply transformation
    logger.debug(f"Applying transform '{find_pattern}' -> '{replace_with}' to block {block_uid}")
    try:
        flags = re.MULTILINE
        count = 0 if global_replace else 1
        new_content = re.sub(find_pattern, replace_with, current_content, count=count, flags=flags)
    except re.error as e:
        raise ValidationError(f"Invalid regex pattern: {str(e)}", "find_pattern", {"pattern": find_pattern}) from e

    # 3. Update the block if content changed
    if new_content != current_content:
        try:
            update_block(block_uid, new_content) # Use the existing update helper which uses the client
            logger.info(f"Successfully transformed and updated block {block_uid}")
            return new_content
        except (TransactionError, ValidationError) as e:
             logger.error(f"Failed to update block {block_uid} after transform: {e}", exc_info=True)
             raise # Re-raise update error
    else:
         logger.info(f"Block {block_uid} content unchanged after transform, no update needed.")
         return current_content


def batch_update_blocks(updates: List[Dict[str, Any]], chunk_size: int = 50) -> List[Dict[str, Any]]:
    """Update multiple blocks using the RoamClient, preparing actions and handling results."""
    client = get_client()
    if not isinstance(updates, list): raise ValidationError("Updates must be a list", "updates")
    if not updates: return []

    results = [] # Store individual results
    actions_to_batch = [] # Store actions for the API

    logger.info(f"Preparing batch update for {len(updates)} blocks.")

    # Prepare actions and pre-validate / transform
    for i, update_spec in enumerate(updates):
        block_uid = update_spec.get("block_uid")
        if not block_uid:
             results.append({"block_uid": None, "success": False, "error": "Missing block_uid"})
             continue

        try:
            # Get current content - needed for transform or just validation
            query = f'''[:find ?string . :where [?b :block/uid "{block_uid}"] [?b :block/string ?string]]'''
            current_content = client.query(query)
            if current_content is None:
                raise BlockNotFoundError(block_uid)
            if not isinstance(current_content, str):
                 raise QueryError("Invalid content type found", query)

            new_content = None
            # Determine new content based on spec
            if "content" in update_spec:
                new_content = update_spec["content"]
            elif "transform" in update_spec:
                transform = update_spec["transform"]
                if not isinstance(transform, dict) or "find" not in transform or "replace" not in transform:
                     raise ValidationError("Invalid 'transform' structure", "transform", update_spec)
                 
                try:
                    find = transform["find"]
                    replace = transform["replace"]
                    is_global = transform.get("global", True)
                    count = 0 if is_global else 1
                    flags = re.MULTILINE
                    new_content = re.sub(find, replace, current_content, count=count, flags=flags)
                except re.error as e:
                     raise ValidationError(f"Invalid regex in transform: {e}", "transform.find", update_spec)
            else:
                raise ValidationError("Update spec needs 'content' or 'transform'", None, update_spec)

            # If content actually changed, add update action
            if new_content != current_content:
                actions_to_batch.append({
                    "action": "update-block",
                    "block": {"uid": block_uid, "string": new_content}
                })
                # Store expected result
                results.append({"block_uid": block_uid, "success": None, "content": new_content}) # Mark success as None initially
            else:
                 # Content didn't change, report as success (no-op)
                 results.append({"block_uid": block_uid, "success": True, "content": current_content, "message": "No change needed"})

        except (BlockNotFoundError, QueryError, ValidationError) as e:
            logger.warning(f"Skipping update for block {block_uid} due to error: {e}")
            results.append({"block_uid": block_uid, "success": False, "error": str(e)})
        except Exception as e:
            logger.error(f"Unexpected error preparing update for {block_uid}: {e}", exc_info=True)
            results.append({"block_uid": block_uid, "success": False, "error": f"Unexpected error: {e}"})

    # Execute the prepared batch actions if any
    if actions_to_batch:
        logger.info(f"Executing batch update for {len(actions_to_batch)} modified blocks.")
        try:
            # Use the robust execute_batch_actions which handles chunking
            batch_result = execute_batch_actions(actions_to_batch, chunk_size)
            
            # Mark corresponding results as successful
            action_uids_updated = {action['block']['uid'] for action in actions_to_batch}
            for result in results:
                 # Check if this block was part of the successful batch execution
                 if result.get("success") is None and result.get("block_uid") in action_uids_updated:
                      if batch_result.get("success"): # If overall batch call succeeded
                           result["success"] = True
                      else:
                           # If batch call failed, mark these as failed too
                           result["success"] = False
                           result["error"] = "Batch execution failed at API level."

        except TransactionError as e:
            logger.error(f"Batch update API call failed: {e}")
            # Mark all pending updates as failed
            action_uids_in_failed_batch = {action['block']['uid'] for action in actions_to_batch}
            for result in results:
                 if result.get("success") is None and result.get("block_uid") in action_uids_in_failed_batch:
                      result["success"] = False
                      result["error"] = f"Batch execution failed: {e}"
        except Exception as e:
             logger.error(f"Unexpected error during batch update execution: {e}", exc_info=True)
             # Mark all pending updates as failed
             action_uids_in_failed_batch = {action['block']['uid'] for action in actions_to_batch}
             for result in results:
                 if result.get("success") is None and result.get("block_uid") in action_uids_in_failed_batch:
                      result["success"] = False
                      result["error"] = f"Unexpected batch execution error: {e}"

    # Final check: Ensure all results have a success status
    for result in results:
        if result.get("success") is None:
            result["success"] = False # Should not happen, but safety catch
            if "error" not in result: result["error"] = "Update was prepared but not executed."

    return results


def get_page_content(title: str, resolve_refs_depth: int = 3, max_block_depth: int = 5) -> str:
    """Get page content using RoamClient, with reference resolution."""
    client = get_client()
    logger.debug(f"Getting content for page: '{title}'")
    
    page_uid = find_page_by_title_util(client, title)
    if not page_uid:
        raise PageNotFoundError(title)

    # --- Iterative block fetching (similar logic as before, but using client.query) ---
    block_map = {} # uid -> block dict
    top_level_uids_ordered = [] # Store UIDs in order

    # Fetch top-level blocks first to establish order
    try:
         top_query = f'''[:find ?uid ?order
                         :in $ ?page_uid
                         :where [?p :block/uid ?page_uid]
                                [?p :block/children ?c]
                                [?c :block/uid ?uid]
                                [?c :block/order ?order]]'''
         top_results = client.query(top_query, inputs=[page_uid])
         if top_results:
             # Sort by order
             top_results.sort(key=lambda x: x[1])
             top_level_uids_ordered = [uid for uid, order in top_results]
    except QueryError as e:
         logger.error(f"Failed to fetch top-level blocks for page {page_uid}: {e}", exc_info=True)
         raise # Re-raise

    if not top_level_uids_ordered:
        logger.info(f"Page '{title}' (UID: {page_uid}) has no top-level blocks.")
        return f"# {title}\n\n(This page appears to be empty)"

    # Use Roam's pull syntax for efficient hierarchy fetching up to max_block_depth
    pull_pattern = f"[:block/string :block/uid :block/order {{:block/children ...{max_block_depth}}} :children/view-type :block/heading]"
    logger.debug(f"Pulling block hierarchy for {len(top_level_uids_ordered)} top-level blocks with depth {max_block_depth}")

    try:
         # Roam pull_many expects a list of entity IDs (which can be UIDs)
         pulled_data = client.pull_many(pull_pattern, top_level_uids_ordered) # Assuming client has pull_many
         
         if not pulled_data or not isinstance(pulled_data, list):
              logger.warning(f"Pull_many returned unexpected data for page {page_uid}: {pulled_data}")
              # Fallback or error needed here? For now, try to continue if possible.
              pulled_data = [] # Avoid iterating over non-list

         # Process pulled data into the desired structure, resolving refs
         def process_pulled_block(block_data, current_ref_depth=0):
              if not block_data or not isinstance(block_data, dict): return None
              
              uid = block_data.get(":block/uid")
              content = block_data.get(":block/string", "")
              order = block_data.get(":block/order", 0)
              children_data = block_data.get(":block/children", [])

              # Resolve references
              if resolve_refs_depth > 0:
                   content = resolve_block_references_util(client, content, max_depth=resolve_refs_depth, current_depth=current_ref_depth)
              
              processed_children = []
              if children_data and isinstance(children_data, list):
                   # Process children recursively (respecting max_block_depth via pull pattern)
                   processed_children = [process_pulled_block(child, current_ref_depth) for child in children_data]
                   # Filter out None results if processing failed for a child
                   processed_children = [child for child in processed_children if child is not None]
                   # Sort children by order
                   processed_children.sort(key=lambda x: x.get("order", 0))

              return {
                   "uid": uid,
                   "content": content,
                   "order": order,
                   "children": processed_children
              }

         # Process each top-level block from the pulled data
         top_level_blocks_processed = [process_pulled_block(data) for data in pulled_data]
         top_level_blocks_processed = [block for block in top_level_blocks_processed if block is not None]
         # Ensure top-level blocks are sorted correctly (pull_many might not preserve input order)
         top_level_blocks_processed.sort(key=lambda x: x.get("order", 0))

         # Convert final structure to markdown
         markdown_output = f"# {title}\n\n"
         def blocks_to_md(blocks, level=0):
              md = ""
              for block in blocks:
                   indent = "  " * level
                   md += f"{indent}- {block['content']}\n"
                   if block["children"]:
                        md += blocks_to_md(block["children"], level + 1)
              return md
              
         markdown_output += blocks_to_md(top_level_blocks_processed)
         
         logger.info(f"Successfully retrieved and formatted content for page '{title}'")
         return markdown_output

    except AttributeError as e:
         # Handle case where client doesn't have pull_many yet
         if 'pull_many' in str(e):
              logger.error("RoamClient does not implement 'pull_many'. Falling back to basic content retrieval (no hierarchy).")
              # Implement a basic fallback (e.g., query just top-level blocks)
              fallback_query = f'''[:find ?string ?order
                                   :in $ ?page_uid
                                   :where [?p :block/uid ?page_uid]
                                          [?p :block/children ?c]
                                          [?c :block/string ?string]
                                          [?c :block/order ?order]]'''
              fallback_results = client.query(fallback_query, inputs=[page_uid])
              if fallback_results:
                   fallback_results.sort(key=lambda x: x[1])
                   md_out = f"# {title}\n\n" + "\n".join([f"- {res[0]}" for res in fallback_results])
                   return md_out
              else:
                   return f"# {title}\n\n(Failed to retrieve content - pull_many unavailable)"
         else:
              raise # Re-raise other AttributeErrors
    except (QueryError, PageNotFoundError) as e:
        logger.error(f"Failed to get content for page '{title}': {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error getting page content for '{title}': {e}", exc_info=True)
        raise QueryError(f"Unexpected error retrieving page content", details={"page": title}) from e


# --- Add pull_many to RoamClient ---
# Need to modify the RoamClient class definition above

class RoamClient: # Redefine with pull_many added
    """Client for interacting with the Roam Research API."""

    def __init__(self, api_token: Optional[str] = None, graph_name: Optional[str] = None):
        self.api_token = api_token or get_api_token()
        self.graph_name = graph_name or get_graph_name()
        
        if not self.api_token or not self.graph_name:
            missing = []
            if not self.api_token: missing.append("ROAM_API_TOKEN")
            if not self.graph_name: missing.append("ROAM_GRAPH_NAME")
            raise AuthenticationError(f"Missing required credentials: {', '.join(missing)}", {"missing": missing})
        
        self.base_url = f'https://api.roamresearch.com/api/graph/{self.graph_name}'
        self.session = self._create_session()
        logger.info(f"RoamClient initialized for graph: {self.graph_name}")

    def _create_session(self) -> requests.Session:
        """Creates a requests session with persistent headers."""
        session = PreserveAuthSession()
        session.headers.update({
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        })
        return session

    @retry_on_error()
    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Makes an HTTP request to the Roam API."""
        url = f'{self.base_url}/{endpoint}'
        logger.debug(f"Making {method.upper()} request to {url}")
        try:
            # Add timeout to requests
            timeout = kwargs.pop('timeout', 30) # Default 30s timeout
            response = self.session.request(method, url, timeout=timeout, **kwargs)

            # Check for common errors
            if response.status_code == 401:
                raise AuthenticationError("Authentication failed (401)", {"status_code": 401, "url": url})
            if response.status_code == 403:
                 raise AuthenticationError("Forbidden (403) - Check token permissions or graph name", {"status_code": 403, "url": url})
            # Allow 404 for query/write as API uses it sometimes, but raise for others
            if response.status_code == 404 and endpoint not in ['q', 'write', 'pull', 'pull-many']:
                 raise RoamAPIError(f"Endpoint not found (404): {endpoint}", code="ENDPOINT_NOT_FOUND", details={"status_code": 404, "url": url})
            if response.status_code == 429:
                raise RateLimitError("Rate limit exceeded (429)", {"status_code": 429, "url": url})
            
            response.raise_for_status()
            
            return response
        
        except requests.exceptions.RequestException as e:
             error_msg = f"Network request failed: {str(e)}"
             error_details = {"url": url}
             if hasattr(e, 'response') and e.response is not None:
                 error_details["status_code"] = e.response.status_code
                 try: error_details["response_body"] = e.response.text[:500]
                 except Exception: pass
             if isinstance(e, requests.exceptions.Timeout):
                 raise RateLimitError("Request timed out, possibly due to server load or rate limits.", error_details) from e
             raise RoamAPIError(error_msg, code="NETWORK_ERROR", details=error_details) from e


    def query(self, query_str: str, inputs: Optional[List[Any]] = None) -> Any:
        """Executes a Datalog query."""
        data = {"query": query_str}
        if inputs: data["inputs"] = inputs
        endpoint = 'q'
        try:
            response = self._request('post', endpoint, json=data)
            json_response = response.json()
            result = json_response.get('result')
            if isinstance(result, list): logger.debug(f"Query returned {len(result)} results.")
            return result
        except RoamAPIError as e:
            if "query" not in e.details: e.details["query"] = query_str
            if isinstance(e, (AuthenticationError, RateLimitError)): raise e
            raise QueryError(f"Query execution failed: {e.message}", query=query_str, details=e.details) from e
        except requests.exceptions.JSONDecodeError as e:
            raise QueryError("Failed to decode JSON response from query.", query=query_str, details={"response_text": response.text[:500]}) from e
        except Exception as e:
            raise QueryError(f"Unexpected error during query: {str(e)}", query=query_str) from e

    def write(self, action_data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Executes a write action or a batch of actions."""
        is_batch = isinstance(action_data, list)
        action_type = "batch-actions" if is_batch else action_data.get("action", "unknown")
        payload = {"action": "batch-actions", "actions": action_data} if is_batch else action_data
        endpoint = 'write'
        try:
            if is_batch: logger.debug(f"Executing batch write with {len(action_data)} actions.")
            else: logger.debug(f"Executing single write action: {action_type}")
            response = self._request('post', endpoint, json=payload)
            json_response = response.json()
            if isinstance(json_response, dict) and json_response.get("successful") is False:
                 logger.warning("Roam API reported 'successful: false' for a batch operation.")
            return json_response
        except RoamAPIError as e:
             if "action_type" not in e.details: e.details["action_type"] = action_type
             if isinstance(e, (AuthenticationError, RateLimitError)): raise e
             raise TransactionError(f"Write action '{action_type}' failed: {e.message}", action_type=action_type, details=e.details) from e
        except requests.exceptions.JSONDecodeError as e:
            raise TransactionError("Failed to decode JSON response from write action.", action_type=action_type, details={"response_text": response.text[:500]}) from e
        except Exception as e:
            raise TransactionError(f"Unexpected error during write action '{action_type}': {str(e)}", action_type=action_type) from e

    def pull(self, pattern: str, eid: Union[str, int]) -> Optional[Dict[str, Any]]:
         """Executes a pull expression for a single entity ID."""
         # EID can be internal ID (int) or UID (str)
         data = {"eid": eid, "selector": pattern}
         endpoint = 'pull'
         logger.debug(f"Executing pull for EID: {eid} with pattern: {pattern}")
         try:
             response = self._request('post', endpoint, json=data)
             json_response = response.json()
             # pull endpoint returns the result directly, not nested under 'result'
             return json_response if isinstance(json_response, dict) else None
         except RoamAPIError as e:
             if "eid" not in e.details: e.details["eid"] = eid
             if "pattern" not in e.details: e.details["pattern"] = pattern
             if isinstance(e, (AuthenticationError, RateLimitError)): raise e
             # Treat 404 specifically for pull (means entity not found)
             if e.details.get("status_code") == 404:
                  logger.warning(f"Pull failed: Entity '{eid}' not found (404).")
                  return None # Return None instead of raising error for not found
             raise QueryError(f"Pull execution failed: {e.message}", query=pattern, details=e.details) from e
         except requests.exceptions.JSONDecodeError as e:
             raise QueryError("Failed to decode JSON response from pull.", query=pattern, details={"eid": eid, "response_text": response.text[:500]}) from e
         except Exception as e:
             raise QueryError(f"Unexpected error during pull: {str(e)}", query=pattern, details={"eid": eid}) from e

    def pull_many(self, pattern: str, eids: List[Union[str, int]]) -> List[Optional[Dict[str, Any]]]:
         """Executes a pull expression for multiple entity IDs."""
         if not eids: return []
         data = {"eids": eids, "selector": pattern}
         endpoint = 'pull-many'
         logger.debug(f"Executing pull-many for {len(eids)} EIDs with pattern: {pattern}")
         try:
             response = self._request('post', endpoint, json=data)
             json_response = response.json()
             # pull-many returns a list of results, corresponding to eids
             if isinstance(json_response, list):
                  # Result might contain nulls if an eid wasn't found
                  return [res if isinstance(res, dict) else None for res in json_response]
             else:
                  logger.error(f"Pull-many returned unexpected data type: {type(json_response)}")
                  raise QueryError("Pull-many did not return a list.", query=pattern, details={"response_type": type(json_response).__name__})
         except RoamAPIError as e:
             if "eids" not in e.details: e.details["eids"] = eids[:10] # Log first few eids
             if "pattern" not in e.details: e.details["pattern"] = pattern
             if isinstance(e, (AuthenticationError, RateLimitError)): raise e
             raise QueryError(f"Pull-many execution failed: {e.message}", query=pattern, details=e.details) from e
         except requests.exceptions.JSONDecodeError as e:
             raise QueryError("Failed to decode JSON response from pull-many.", query=pattern, details={"num_eids": len(eids), "response_text": response.text[:500]}) from e
         except Exception as e:
             raise QueryError(f"Unexpected error during pull-many: {str(e)}", query=pattern, details={"num_eids": len(eids)}) from e

# Update get_page_content to use client.pull_many
def get_page_content(title: str, resolve_refs_depth: int = 3, max_block_depth: int = 5) -> str:
    """Get page content using RoamClient, using pull_many for efficiency."""
    client = get_client()
    logger.debug(f"Getting content for page: '{title}'")
    
    page_uid = find_page_by_title_util(client, title)
    if not page_uid:
        raise PageNotFoundError(title)

    # Use pull to get the top-level children UIDs and order
    page_data = client.pull("[{:block/children [:block/uid :block/order]}]", page_uid)
    
    top_level_children_info = []
    if page_data and isinstance(page_data.get(':block/children'), list):
         top_level_children_info = page_data[':block/children']
         # Sort by order
         top_level_children_info.sort(key=lambda x: x.get(':block/order', 0))
         top_level_uids_ordered = [child.get(':block/uid') for child in top_level_children_info if child.get(':block/uid')]
    else:
         top_level_uids_ordered = []

    if not top_level_uids_ordered:
        logger.info(f"Page '{title}' (UID: {page_uid}) has no top-level blocks.")
        return f"# {title}\n\n(This page appears to be empty)"

    # Define the pull pattern for fetching the hierarchy
    # Generate nested children pattern dynamically based on max_block_depth
    children_pull = ":block/children"
    for _ in range(max_block_depth):
        children_pull = f"{{ {children_pull} ... }}" # Nest further: {:block/children ...}
    
    # Final pattern including string, uid, order, heading, and nested children
    pull_pattern = f"[:block/string :block/uid :block/order :block/heading {children_pull}]"
    
    logger.debug(f"Pulling block hierarchy for {len(top_level_uids_ordered)} top-level blocks using pattern: {pull_pattern}")

    try:
         pulled_data = client.pull_many(pull_pattern, top_level_uids_ordered)
         
         if not isinstance(pulled_data, list):
              logger.error(f"Pull_many returned non-list data: {pulled_data}")
              raise QueryError("pull_many did not return a list.", pull_pattern)

         # Process pulled data into the desired structure, resolving refs
         def process_pulled_block(block_data, current_ref_depth=0):
              if not block_data or not isinstance(block_data, dict): return None
              
              uid = block_data.get(":block/uid")
              content = block_data.get(":block/string", "")
              order = block_data.get(":block/order", 0)
              heading = block_data.get(":block/heading") # May be None or 1, 2, 3
              children_data = block_data.get(":block/children", [])

              # Resolve references
              if resolve_refs_depth > 0:
                   content = resolve_block_references_util(client, content, max_depth=resolve_refs_depth, current_depth=current_ref_depth)
              
              processed_children = []
              if children_data and isinstance(children_data, list):
                   processed_children = [process_pulled_block(child, current_ref_depth) for child in children_data]
                   processed_children = [child for child in processed_children if child is not None]
                   processed_children.sort(key=lambda x: x.get("order", 0)) # Sort children by order

              block_result = {
                   "uid": uid,
                   "content": content,
                   "order": order,
                   "children": processed_children
              }
              # Add heading info if present
              if heading is not None: block_result["heading"] = heading
              return block_result

         # Process each top-level block from the pulled data
         # Match pulled data back to original order if pull_many doesn't preserve it
         # Create a map of uid -> pulled_block_data
         pulled_map = {item[':block/uid']: item for item in pulled_data if item and isinstance(item, dict) and ':block/uid' in item}
         
         top_level_blocks_processed = []
         for uid in top_level_uids_ordered:
              if uid in pulled_map:
                   processed = process_pulled_block(pulled_map[uid])
                   if processed: top_level_blocks_processed.append(processed)
              else:
                   logger.warning(f"Data for top-level block UID {uid} not found in pull_many result.")

         # Convert final structure to markdown
         markdown_output = f"# {title}\n\n"
         def blocks_to_md(blocks, level=0):
              md = ""
              for block in blocks:
                   indent = "  " * level
                   heading_prefix = ""
                   # Add markdown heading markers based on Roam level if desired for output
                   # if block.get("heading"): heading_prefix = "#" * block["heading"] + " "
                   md += f"{indent}- {heading_prefix}{block['content']}\n"
                   if block["children"]:
                        md += blocks_to_md(block["children"], level + 1)
              return md
              
         markdown_output += blocks_to_md(top_level_blocks_processed)
         
         logger.info(f"Successfully retrieved and formatted content for page '{title}'")
         return markdown_output

    except (QueryError, PageNotFoundError, TransactionError) as e: # Include TransactionError if pull causes writes? Unlikely.
        logger.error(f"Failed to get content for page '{title}': {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error getting page content for '{title}': {e}", exc_info=True)
        raise QueryError(f"Unexpected error retrieving page content", details={"page": title}) from e