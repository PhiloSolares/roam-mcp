"""Core API client for interacting with Roam Research."""

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
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

# Set up logging
logger = logging.getLogger("roam-mcp.api")

# Get API credentials from environment variables
API_TOKEN = os.environ.get("ROAM_API_TOKEN")
GRAPH_NAME = os.environ.get("ROAM_GRAPH_NAME")
MEMORIES_TAG = os.environ.get("MEMORIES_TAG", "#[[Memories]]")


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
            remediation="Check your API token and graph name in environment variables or .env file."
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


class RoamClient:
    """Abstraction over raw Roam API HTTP interactions."""
    
    def __init__(self, token: Optional[str] = None, graph_name: Optional[str] = None):
        """
        Initialize the Roam API client.
        
        Args:
            token: Roam API token (defaults to ROAM_API_TOKEN env var)
            graph_name: Roam graph name (defaults to ROAM_GRAPH_NAME env var)
        """
        self.token = token or API_TOKEN
        self.graph_name = graph_name or GRAPH_NAME
        self.session = PreserveAuthSession()
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        
        # Validate credentials
        self.validate_credentials()
    
    def validate_credentials(self):
        """
        Validate that required API credentials are set.
        
        Raises:
            AuthenticationError: If required credentials are missing
        """
        if not self.token or not self.graph_name:
            missing = []
            if not self.token:
                missing.append("ROAM_API_TOKEN")
            if not self.graph_name:
                missing.append("ROAM_GRAPH_NAME")
                
            raise AuthenticationError(
                f"Missing required credentials: {', '.join(missing)}",
                {"missing": missing}
            )
    
    @retry_on_error()
    def execute_query(self, query: str, inputs: Optional[List[Any]] = None) -> Any:
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
            response = self.session.post(
                f'https://api.roamresearch.com/api/graph/{self.graph_name}/q',
                headers=self.headers,
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
    def execute_write_action(self, action_data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, Any]:
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
        
        # Execute action
        try:
            response = self.session.post(
                f'https://api.roamresearch.com/api/graph/{self.graph_name}/write',
                headers=self.headers,
                json=operation_data
            )
            
            if response.status_code == 401:
                raise AuthenticationError("Authentication failed", {"status_code": response.status_code})
            
            if response.status_code == 429:
                raise RateLimitError("Rate limit exceeded", {"status_code": response.status_code})
            
            response.raise_for_status()
            result = response.json()
            
            # Validate response for batch operations
            if is_batch and "successful" in result:
                if not result["successful"]:
                    error_details = {"failed_actions": result.get("failed_actions", [])}
                    raise TransactionError(
                        f"Batch operation failed: {len(error_details['failed_actions'])} actions failed",
                        action_type,
                        error_details
                    )
                    
            return result
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
    
    def execute_batch_actions(self, actions: List[Dict[str, Any]], chunk_size: int = 50) -> Dict[str, Any]:
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
        
        # Apply optimizations to batch
        optimized_actions = self.optimize_batch_actions(actions)
        
        # Single batch if under chunk size
        if len(optimized_actions) <= chunk_size:
            result = self.execute_write_action(optimized_actions)
            return {
                "success": True,
                "created_uids": result.get("created_uids", [])
            }
        
        # Split into chunks for larger batches
        chunks = [optimized_actions[i:i + chunk_size] for i in range(0, len(optimized_actions), chunk_size)]
        logger.debug(f"Splitting batch operation into {len(chunks)} chunks of max {chunk_size} actions")
        
        # Track results across chunks
        combined_results = {
            "created_uids": [],
            "success": True
        }
        
        # Execute each chunk
        for i, chunk in enumerate(chunks):
            logger.debug(f"Executing batch chunk {i+1}/{len(chunks)} with {len(chunk)} actions")
            result = self.execute_write_action(chunk)
            
            # Collect UIDs from this chunk
            if "created_uids" in result:
                combined_results["created_uids"].extend(result["created_uids"])
        
        return combined_results
    
    def optimize_batch_actions(self, actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Optimize batch actions for better performance.
        
        Args:
            actions: List of actions to optimize
            
        Returns:
            Optimized list of actions
        """
        # Group actions by type for more efficient processing
        action_groups = {
            "create-page": [],
            "create-block": [],
            "update-block": [],
            "delete-block": [],
            "other": []
        }
        
        for action in actions:
            action_type = action.get("action", "other")
            if action_type in action_groups:
                action_groups[action_type].append(action)
            else:
                action_groups["other"].append(action)
        
        # Return optimized order: create pages first, then blocks, then updates
        optimized = []
        optimized.extend(action_groups["create-page"])
        optimized.extend(action_groups["create-block"])
        optimized.extend(action_groups["update-block"])
        optimized.extend(action_groups["delete-block"])
        optimized.extend(action_groups["other"])
        
        return optimized
    
    def find_page_by_title(self, title: str) -> Optional[str]:
        """
        Find a page by title, with case-insensitive matching.
        
        Args:
            title: Page title to search for
            
        Returns:
            Page UID or None if not found
        """
        # Clean up the title
        title = title.strip()
        
        # First try direct page lookup (more reliable than case-insensitive queries in Roam)
        query = f'''[:find ?uid .
                    :where [?e :node/title "{title}"]
                            [?e :block/uid ?uid]]'''
        
        result = self.execute_query(query)
        if result:
            return result
        
        # If not found, try checking if it's a UID
        if len(title) == 9 and re.match(r'^[a-zA-Z0-9_-]{9}$', title):
            # This looks like a UID, check if it's a valid page UID
            uid_query = f'''[:find ?title .
                            :where [?e :block/uid "{title}"]
                                    [?e :node/title ?title]]'''
            
            uid_result = self.execute_query(uid_query)
            if uid_result:
                return title
        
        # If still not found, try case-insensitive match by getting all pages
        all_pages_query = f'''[:find ?title ?uid
                             :where [?e :node/title ?title]
                                     [?e :block/uid ?uid]]'''
        
        all_pages_result = self.execute_query(all_pages_query)
        if all_pages_result:
            for page_title, uid in all_pages_result:
                if page_title.lower() == title.lower():
                    return uid
        
        return None
        
    def find_or_create_page(self, title: str) -> str:
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
        # Validate title
        if not title or not isinstance(title, str):
            raise ValidationError("Page title must be a non-empty string", "title")
        
        title = title.strip()
        if not title:
            raise ValidationError("Page title cannot be empty or just whitespace", "title")
        
        # Try to find the page first
        logger.debug(f"Looking for page: {title}")
        page_uid = self.find_page_by_title(title)
        
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
            response = self.execute_write_action(action_data)
            
            if "page" in response and "uid" in response["page"]:
                new_uid = response["page"]["uid"]
                logger.debug(f"Created page: {title} (UID: {new_uid})")
                return new_uid
            else:
                # Try to find the page again - sometimes the API creates it but doesn't return the UID
                page_uid = self.find_page_by_title(title)
                if page_uid:
                    logger.debug(f"Found newly created page: {title} (UID: {page_uid})")
                    return page_uid
                
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
    
    def resolve_block_references(self, content: str, max_depth: int = 3, current_depth: int = 0) -> str:
        """
        Resolve block references in content recursively.
        
        Args:
            content: Content with potential block references
            max_depth: Maximum recursion depth
            current_depth: Current recursion depth
            
        Returns:
            Content with block references resolved
        """
        if current_depth >= max_depth:
            return content
        
        # Find all block references
        ref_pattern = r'\(\(([a-zA-Z0-9_-]{9})\)\)'
        refs = re.findall(ref_pattern, content)
        
        if not refs:
            return content
        
        # For each reference, get its content
        for ref in refs:
            try:
                query = f'''[:find ?string .
                            :where [?b :block/uid "{ref}"]
                                    [?b :block/string ?string]]'''
                
                ref_content = self.execute_query(query)
                
                if ref_content:
                    # Recursively resolve nested references
                    resolved_ref = self.resolve_block_references(
                        ref_content, max_depth, current_depth + 1
                    )
                    
                    # Replace reference with content
                    content = content.replace(f"(({ref}))", resolved_ref)
            except Exception as e:
                logger.warning(f"Failed to resolve reference (({ref})): {str(e)}")
        
        return content
    
    def find_block_uid(self, block_content: str) -> str:
        """
        Search for a block by its content to find its UID.
        
        Args:
            block_content: Content to search for
            
        Returns:
            Block UID
        """
        # Escape quotes in content
        escaped_content = block_content.replace('"', '\\"')
        
        search_query = f'''[:find (pull ?e [:block/uid])
                          :where [?e :block/string "{escaped_content}"]]'''
        
        search_result = self.execute_query(search_query)
        
        if search_result and len(search_result) > 0:
            try:
                block_uid = search_result[0][0][':block/uid']
                return block_uid
            except (KeyError, IndexError):
                logger.error("Unexpected response format when finding block UID")
                raise Exception("Failed to find the block UID due to unexpected response format")
        else:
            # Try a more relaxed search if we can't find an exact match
            # This can happen if there are subtle whitespace or formatting differences
            logger.debug(f"Exact block match not found, trying a more relaxed search")
            try:
                # Get a list of recent blocks sorted by creation time
                time_query = f'''[:find ?uid ?string ?time
                                 :where [?b :block/string ?string]
                                        [?b :block/uid ?uid]
                                        [?b :create/time ?time]]
                                 :order :desc
                                 :limit 5'''
                
                time_result = self.execute_query(time_query)
                
                if time_result:
                    # Check if any of these recent blocks match our content
                    clean_content = block_content.strip()
                    for uid, content, time in time_result:
                        if content.strip() == clean_content:
                            return uid
                
                logger.error("Could not find block UID with relaxed search")
                raise Exception("Failed to find the block UID even with relaxed search")
            except Exception as e:
                logger.error(f"Error in relaxed block search: {str(e)}")
                raise Exception(f"Failed to find the block UID: {str(e)}")
    
    def add_block_to_page(self, page_uid: str, content: str, order: Union[int, str] = "last") -> str:
        """
        Add a block to a page.
        
        Args:
            page_uid: Parent page UID
            content: Block content
            order: Position ("first", "last", or integer index)
            
        Returns:
            New block UID
            
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
        try:
            self.execute_write_action(action_data)
            
            uid = self.find_block_uid(content)
            
            if not uid:
                raise BlockNotFoundError(f"Newly created block with content: {content[:50]}...")
                
            logger.debug(f"Created block with UID: {uid}")
            
            return uid
        except Exception as e:
            if isinstance(e, (BlockNotFoundError, ValidationError, TransactionError)):
                raise
            
            error_msg = f"Failed to create block: {str(e)}"
            logger.error(error_msg)
            raise TransactionError(error_msg, "create-block", {"page_uid": page_uid}) from e
    
    def update_block(self, block_uid: str, content: str) -> bool:
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
            self.execute_write_action(action_data)
            return True
        except Exception as e:
            if isinstance(e, (BlockNotFoundError, ValidationError, TransactionError)):
                raise
                
            error_msg = f"Failed to update block: {str(e)}"
            logger.error(error_msg)
            raise TransactionError(error_msg, "update-block", {"block_uid": block_uid}) from e


# Create a global client instance
client = RoamClient()

# Legacy API functions that delegate to the client
def execute_query(query: str, inputs: Optional[List[Any]] = None) -> Any:
    """Legacy function that delegates to the client."""
    return client.execute_query(query, inputs)

def execute_write_action(action_data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Dict[str, Any]:
    """Legacy function that delegates to the client."""
    return client.execute_write_action(action_data)

def execute_batch_actions(actions: List[Dict[str, Any]], chunk_size: int = 50) -> Dict[str, Any]:
    """Legacy function that delegates to the client."""
    return client.execute_batch_actions(actions, chunk_size)

def find_page_by_title(session, headers, graph_name: str, title: str) -> Optional[str]:
    """Legacy function that delegates to the client."""
    return client.find_page_by_title(title)

def find_or_create_page(title: str) -> str:
    """Legacy function that delegates to the client."""
    return client.find_or_create_page(title)

def get_daily_page() -> str:
    """
    Get or create today's daily page.
    
    Returns:
        Daily page UID
    """
    from roam_mcp.utils import format_roam_date
    today = datetime.now()
    date_str = format_roam_date(today)
    
    logger.debug(f"Getting daily page for: {date_str}")
    return find_or_create_page(date_str)

def add_block_to_page(page_uid: str, content: str, order: Union[int, str] = "last") -> str:
    """Legacy function that delegates to the client."""
    return client.add_block_to_page(page_uid, content, order)

def update_block(block_uid: str, content: str) -> bool:
    """Legacy function that delegates to the client."""
    return client.update_block(block_uid, content)

def resolve_block_references(session, headers, graph_name: str, content: str, max_depth: int = 3, current_depth: int = 0) -> str:
    """Legacy function that delegates to the client."""
    return client.resolve_block_references(content, max_depth, current_depth)

def find_block_uid(session, headers, graph_name: str, block_content: str) -> str:
    """Legacy function that delegates to the client."""
    return client.find_block_uid(block_content)