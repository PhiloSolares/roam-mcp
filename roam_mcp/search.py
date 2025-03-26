"""Search operations for the Roam MCP server."""

from typing import Dict, List, Any, Optional, Union, Set
from datetime import datetime, timedelta
import re
import logging

from roam_mcp.api import (
    execute_query,
    get_session_and_headers,
    GRAPH_NAME,
    find_page_by_title,
    ValidationError,
    QueryError,
    PageNotFoundError,
    BlockNotFoundError
)
from roam_mcp.utils import (
    format_roam_date,
    resolve_block_references
)

# Set up logging
logger = logging.getLogger("roam-mcp.search")


def validate_search_params(text: Optional[str] = None, tag: Optional[str] = None, 
                          status: Optional[str] = None, page_title_uid: Optional[str] = None):
    """
    Validate common search parameters.
    
    Args:
        text: Optional text to search for
        tag: Optional tag to search for
        status: Optional status to search for
        page_title_uid: Optional page title or UID
        
    Raises:
        ValidationError: If parameters are invalid
    """
    if status and status not in ["TODO", "DONE"]:
        raise ValidationError("Status must be 'TODO' or 'DONE'", "status")


def search_by_text(text: str, page_title_uid: Optional[str] = None, case_sensitive: bool = True) -> Dict[str, Any]:
    """
    Search for blocks containing specific text.
    
    Args:
        text: Text to search for
        page_title_uid: Optional page title or UID to scope the search
        case_sensitive: Whether to perform case-sensitive search
        
    Returns:
        Search results
    """
    if not text:
        return {
            "success": False,
            "matches": [],
            "message": "Search text cannot be empty"
        }
    
    session, headers = get_session_and_headers()
    
    # Prepare the query
    if case_sensitive:
        text_condition = f'(clojure.string/includes? ?s "{text}")'
    else:
        text_condition = f'(clojure.string/includes? (clojure.string/lower-case ?s) "{text.lower()}")'
    
    try:
        if page_title_uid:
            # Try to find the page UID if a title was provided
            page_uid = find_page_by_title(session, headers, GRAPH_NAME, page_title_uid)
            
            if not page_uid:
                return {
                    "success": False,
                    "matches": [],
                    "message": f"Page '{page_title_uid}' not found"
                }
                
            query = f"""[:find ?uid ?s ?order
                      :where
                      [?p :block/uid "{page_uid}"]
                      [?b :block/page ?p]
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/order ?order]
                      [{text_condition}]]"""
        else:
            query = f"""[:find ?uid ?s ?page-title
                      :where
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      [{text_condition}]]"""
        
        # Execute the query
        logger.debug(f"Executing text search for: {text}")
        results = execute_query(query)
        
        # Process the results
        matches = []
        if page_title_uid:
            # For page-specific search, results are [uid, content, order]
            for uid, content, order in results:
                # Resolve references if present
                resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
                
                matches.append({
                    "block_uid": uid,
                    "content": resolved_content,
                    "page_title": page_title_uid
                })
        else:
            # For global search, results are [uid, content, page_title]
            for uid, content, page_title in results:
                # Resolve references if present
                resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
                
                matches.append({
                    "block_uid": uid,
                    "content": resolved_content,
                    "page_title": page_title
                })
        
        return {
            "success": True,
            "matches": matches,
            "message": f"Found {len(matches)} block(s) containing \"{text}\""
        }
    except PageNotFoundError as e:
        return {
            "success": False,
            "matches": [],
            "message": str(e)
        }
    except QueryError as e:
        return {
            "success": False,
            "matches": [],
            "message": str(e)
        }
    except Exception as e:
        logger.error(f"Error searching by text: {str(e)}")
        return {
            "success": False,
            "matches": [],
            "message": f"Error searching by text: {str(e)}"
        }


def search_by_tag(tag: str, page_title_uid: Optional[str] = None, near_tag: Optional[str] = None) -> Dict[str, Any]:
    """
    Search for blocks containing a specific tag.
    
    Args:
        tag: Tag to search for (without # or [[ ]])
        page_title_uid: Optional page title or UID to scope the search
        near_tag: Optional second tag that must appear in the same block
        
    Returns:
        Search results
    """
    if not tag:
        return {
            "success": False,
            "matches": [],
            "message": "Tag cannot be empty"
        }
    
    session, headers = get_session_and_headers()
    
    # Format the tag for searching
    # Remove any existing formatting
    clean_tag = tag.replace('#', '').replace('[[', '').replace(']]', '')
    tag_variants = [f"#{clean_tag}", f"#[[{clean_tag}]]", f"[[{clean_tag}]]"]
    
    # Build tag conditions
    tag_conditions = []
    for variant in tag_variants:
        tag_conditions.append(f'(clojure.string/includes? ?s "{variant}")')
    
    tag_condition = f"(or {' '.join(tag_conditions)})"
    
    # Add near_tag condition if provided
    if near_tag:
        clean_near_tag = near_tag.replace('#', '').replace('[[', '').replace(']]', '')
        near_tag_variants = [f"#{clean_near_tag}", f"#[[{clean_near_tag}]]", f"[[{clean_near_tag}]]"]
        
        near_tag_conditions = []
        for variant in near_tag_variants:
            near_tag_conditions.append(f'(clojure.string/includes? ?s "{variant}")')
        
        near_tag_condition = f"(or {' '.join(near_tag_conditions)})"
        combined_condition = f"(and {tag_condition} {near_tag_condition})"
    else:
        combined_condition = tag_condition
    
    try:
        # Build query based on whether we're searching in a specific page
        if page_title_uid:
            # Try to find the page UID if a title was provided
            page_uid = find_page_by_title(session, headers, GRAPH_NAME, page_title_uid)
            
            if not page_uid:
                return {
                    "success": False,
                    "matches": [],
                    "message": f"Page '{page_title_uid}' not found"
                }
                
            query = f"""[:find ?uid ?s
                      :where
                      [?p :block/uid "{page_uid}"]
                      [?b :block/page ?p]
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [{combined_condition}]]"""
        else:
            query = f"""[:find ?uid ?s ?page-title
                      :where
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      [{combined_condition}]]"""
        
        # Execute the query
        logger.debug(f"Executing tag search for: {tag}")
        if near_tag:
            logger.debug(f"With near tag: {near_tag}")
            
        results = execute_query(query)
        
        # Process the results
        matches = []
        if page_title_uid:
            # For page-specific search, results are [uid, content]
            for uid, content in results:
                # Resolve references if present
                resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
                
                matches.append({
                    "block_uid": uid,
                    "content": resolved_content,
                    "page_title": page_title_uid
                })
        else:
            # For global search, results are [uid, content, page_title]
            for uid, content, page_title in results:
                # Resolve references if present
                resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
                
                matches.append({
                    "block_uid": uid,
                    "content": resolved_content,
                    "page_title": page_title
                })
        
        # Build message
        message = f"Found {len(matches)} block(s) with tag #{clean_tag}"
        if near_tag:
            message += f" near #{clean_near_tag}"
        
        return {
            "success": True,
            "matches": matches,
            "message": message
        }
    except PageNotFoundError as e:
        return {
            "success": False,
            "matches": [],
            "message": str(e)
        }
    except QueryError as e:
        return {
            "success": False,
            "matches": [],
            "message": str(e)
        }
    except Exception as e:
        logger.error(f"Error searching by tag: {str(e)}")
        return {
            "success": False,
            "matches": [],
            "message": f"Error searching by tag: {str(e)}"
        }


def search_by_status(status: str, page_title_uid: Optional[str] = None, include: Optional[str] = None, exclude: Optional[str] = None) -> Dict[str, Any]:
    """
    Search for blocks with a specific status (TODO/DONE).
    
    Args:
        status: Status to search for ("TODO" or "DONE")
        page_title_uid: Optional page title or UID to scope the search
        include: Optional comma-separated keywords to include
        exclude: Optional comma-separated keywords to exclude
        
    Returns:
        Search results
    """
    if status not in ["TODO", "DONE"]:
        return {
            "success": False,
            "matches": [],
            "message": "Status must be either 'TODO' or 'DONE'"
        }
    
    session, headers = get_session_and_headers()
    
    # Status pattern
    status_pattern = f"{{{{[[{status}]]}}}}"
    
    try:
        # Build query based on whether we're searching in a specific page
        if page_title_uid:
            # Try to find the page UID if a title was provided
            page_uid = find_page_by_title(session, headers, GRAPH_NAME, page_title_uid)
            
            if not page_uid:
                return {
                    "success": False,
                    "matches": [],
                    "message": f"Page '{page_title_uid}' not found"
                }
                
            query = f"""[:find ?uid ?s
                      :where
                      [?p :block/uid "{page_uid}"]
                      [?b :block/page ?p]
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [(clojure.string/includes? ?s "{status_pattern}")]]"""
        else:
            query = f"""[:find ?uid ?s ?page-title
                      :where
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      [(clojure.string/includes? ?s "{status_pattern}")]]"""
        
        # Execute the query
        logger.debug(f"Executing status search for: {status}")
        results = execute_query(query)
        
        # Process the results
        matches = []
        if page_title_uid:
            # For page-specific search, results are [uid, content]
            for uid, content in results:
                # Resolve references if present
                resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
                
                # Apply include/exclude filters
                if include:
                    include_terms = [term.strip().lower() for term in include.split(',')]
                    if not any(term in resolved_content.lower() for term in include_terms):
                        continue
                        
                if exclude:
                    exclude_terms = [term.strip().lower() for term in exclude.split(',')]
                    if any(term in resolved_content.lower() for term in exclude_terms):
                        continue
                
                matches.append({
                    "block_uid": uid,
                    "content": resolved_content,
                    "page_title": page_title_uid
                })
        else:
            # For global search, results are [uid, content, page_title]
            for uid, content, page_title in results:
                # Resolve references if present
                resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
                
                # Apply include/exclude filters
                if include:
                    include_terms = [term.strip().lower() for term in include.split(',')]
                    if not any(term in resolved_content.lower() for term in include_terms):
                        continue
                        
                if exclude:
                    exclude_terms = [term.strip().lower() for term in exclude.split(',')]
                    if any(term in resolved_content.lower() for term in exclude_terms):
                        continue
                
                matches.append({
                    "block_uid": uid,
                    "content": resolved_content,
                    "page_title": page_title
                })
        
        # Build message
        message = f"Found {len(matches)} block(s) with status {status}"
        if include:
            message += f" including '{include}'"
        if exclude:
            message += f" excluding '{exclude}'"
        
        return {
            "success": True,
            "matches": matches,
            "message": message
        }
    except PageNotFoundError as e:
        return {
            "success": False,
            "matches": [],
            "message": str(e)
        }
    except QueryError as e:
        return {
            "success": False,
            "matches": [],
            "message": str(e)
        }
    except Exception as e:
        logger.error(f"Error searching by status: {str(e)}")
        return {
            "success": False,
            "matches": [],
            "message": f"Error searching by status: {str(e)}"
        }


def search_block_refs(block_uid: Optional[str] = None, page_title_uid: Optional[str] = None) -> Dict[str, Any]:
    """
    Search for block references.
    
    Args:
        block_uid: Optional UID of the block to find references to
        page_title_uid: Optional page title or UID to scope the search
        
    Returns:
        Search results
    """
    session, headers = get_session_and_headers()
    
    # Determine what kind of search we're doing
    if block_uid:
        block_ref_pattern = f"(({block_uid}))"
        description = f"referencing block (({block_uid}))"
    else:
        block_ref_pattern = "\\(\\([^)]+\\)\\)"
        description = "containing block references"
    
    try:
        # Build query based on whether we're searching in a specific page
        if page_title_uid:
            # Try to find the page UID if a title was provided
            page_uid = find_page_by_title(session, headers, GRAPH_NAME, page_title_uid)
            
            if not page_uid:
                return {
                    "success": False,
                    "matches": [],
                    "message": f"Page '{page_title_uid}' not found"
                }
                
            if block_uid:
                query = f"""[:find ?uid ?s
                          :where
                          [?p :block/uid "{page_uid}"]
                          [?b :block/page ?p]
                          [?b :block/string ?s]
                          [?b :block/uid ?uid]
                          [(clojure.string/includes? ?s "{block_ref_pattern}")]]"""
            else:
                query = f"""[:find ?uid ?s
                          :where
                          [?p :block/uid "{page_uid}"]
                          [?b :block/page ?p]
                          [?b :block/string ?s]
                          [?b :block/uid ?uid]
                          [(re-find #"\\(\\([^)]+\\)\\)" ?s)]]"""
        else:
            if block_uid:
                query = f"""[:find ?uid ?s ?page-title
                          :where
                          [?b :block/string ?s]
                          [?b :block/uid ?uid]
                          [?b :block/page ?p]
                          [?p :node/title ?page-title]
                          [(clojure.string/includes? ?s "{block_ref_pattern}")]]"""
            else:
                query = f"""[:find ?uid ?s ?page-title
                          :where
                          [?b :block/string ?s]
                          [?b :block/uid ?uid]
                          [?b :block/page ?p]
                          [?p :node/title ?page-title]
                          [(re-find #"\\(\\([^)]+\\)\\)" ?s)]]"""
        
        # Execute the query
        logger.debug(f"Executing block reference search")
        results = execute_query(query)
        
        # Process the results
        matches = []
        if page_title_uid:
            # For page-specific search, results are [uid, content]
            for uid, content in results:
                # Resolve references if present
                resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
                
                matches.append({
                    "block_uid": uid,
                    "content": resolved_content,
                    "page_title": page_title_uid
                })
        else:
            # For global search, results are [uid, content, page_title]
            for uid, content, page_title in results:
                # Resolve references if present
                resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
                
                matches.append({
                    "block_uid": uid,
                    "content": resolved_content,
                    "page_title": page_title
                })
        
        return {
            "success": True,
            "matches": matches,
            "message": f"Found {len(matches)} block(s) {description}"
        }
    except PageNotFoundError as e:
        return {
            "success": False,
            "matches": [],
            "message": str(e)
        }
    except QueryError as e:
        return {
            "success": False,
            "matches": [],
            "message": str(e)
        }
    except Exception as e:
        logger.error(f"Error searching block references: {str(e)}")
        return {
            "success": False,
            "matches": [],
            "message": f"Error searching block references: {str(e)}"
        }


def search_hierarchy(parent_uid: Optional[str] = None, child_uid: Optional[str] = None, 
                     page_title_uid: Optional[str] = None, max_depth: int = 1) -> Dict[str, Any]:
    """
    Search for parents or children in the block hierarchy.
    
    Args:
        parent_uid: Optional UID of the block to find children of
        child_uid: Optional UID of the block to find parents of
        page_title_uid: Optional page title or UID to scope the search
        max_depth: Maximum depth to search
        
    Returns:
        Search results
    """
    if not parent_uid and not child_uid:
        return {
            "success": False,
            "matches": [],
            "message": "Either parent_uid or child_uid must be provided"
        }
    
    if max_depth < 1:
        return {
            "success": False,
            "matches": [],
            "message": "max_depth must be at least 1"
        }
        
    if max_depth > 10:
        max_depth = 10
        logger.warning("max_depth limited to 10")
    
    session, headers = get_session_and_headers()
    
    # Define ancestor rule
    ancestor_rule = """[
        [(ancestor ?child ?parent ?depth)
            [?parent :block/children ?child]
            [(identity 1) ?depth]]
        [(ancestor ?child ?parent ?depth)
            [?mid :block/children ?child]
            (ancestor ?mid ?parent ?prev-depth)
            [(+ ?prev-depth 1) ?depth]]
    ]"""
    
    try:
        # Determine search type and build query
        if parent_uid:
            # Searching for children
            if page_title_uid:
                # Try to find the page UID if a title was provided
                page_uid = find_page_by_title(session, headers, GRAPH_NAME, page_title_uid)
                
                if not page_uid:
                    return {
                        "success": False,
                        "matches": [],
                        "message": f"Page '{page_title_uid}' not found"
                    }
                    
                query = f"""[:find ?uid ?s ?depth
                          :in $ % ?parent-uid ?max-depth
                          :where
                          [?parent :block/uid ?parent-uid]
                          [?p :block/uid "{page_uid}"]
                          (ancestor ?b ?parent ?depth)
                          [?b :block/string ?s]
                          [?b :block/uid ?uid]
                          [?b :block/page ?p]
                          [(<= ?depth ?max-depth)]]"""
                inputs = [ancestor_rule, parent_uid, max_depth]
            else:
                query = f"""[:find ?uid ?s ?page-title ?depth
                          :in $ % ?parent-uid ?max-depth
                          :where
                          [?parent :block/uid ?parent-uid]
                          (ancestor ?b ?parent ?depth)
                          [?b :block/string ?s]
                          [?b :block/uid ?uid]
                          [?b :block/page ?p]
                          [?p :node/title ?page-title]
                          [(<= ?depth ?max-depth)]]"""
                inputs = [ancestor_rule, parent_uid, max_depth]
            
            description = f"descendants of block {parent_uid}"
        else:
            # Searching for parents
            if page_title_uid:
                # Try to find the page UID if a title was provided
                page_uid = find_page_by_title(session, headers, GRAPH_NAME, page_title_uid)
                
                if not page_uid:
                    return {
                        "success": False,
                        "matches": [],
                        "message": f"Page '{page_title_uid}' not found"
                    }
                    
                query = f"""[:find ?uid ?s ?depth
                          :in $ % ?child-uid ?max-depth
                          :where
                          [?child :block/uid ?child-uid]
                          [?p :block/uid "{page_uid}"]
                          (ancestor ?child ?b ?depth)
                          [?b :block/string ?s]
                          [?b :block/uid ?uid]
                          [?b :block/page ?p]
                          [(<= ?depth ?max-depth)]]"""
                inputs = [ancestor_rule, child_uid, max_depth]
            else:
                query = f"""[:find ?uid ?s ?page-title ?depth
                          :in $ % ?child-uid ?max-depth
                          :where
                          [?child :block/uid ?child-uid]
                          (ancestor ?child ?b ?depth)
                          [?b :block/string ?s]
                          [?b :block/uid ?uid]
                          [?b :block/page ?p]
                          [?p :node/title ?page-title]
                          [(<= ?depth ?max-depth)]]"""
                inputs = [ancestor_rule, child_uid, max_depth]
            
            description = f"ancestors of block {child_uid}"
        
        # Execute the query
        logger.debug(f"Executing hierarchy search with max_depth: {max_depth}")
        results = execute_query(query, inputs)
        
        # Process the results
        matches = []
        if page_title_uid:
            # For page-specific search, results are [uid, content, depth]
            for uid, content, depth in results:
                # Resolve references if present
                resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
                
                matches.append({
                    "block_uid": uid,
                    "content": resolved_content,
                    "depth": depth,
                    "page_title": page_title_uid
                })
        else:
            # For global search, results are [uid, content, page_title, depth]
            for uid, content, page_title, depth in results:
                # Resolve references if present
                resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
                
                matches.append({
                    "block_uid": uid,
                    "content": resolved_content,
                    "depth": depth,
                    "page_title": page_title
                })
        
        return {
            "success": True,
            "matches": matches,
            "message": f"Found {len(matches)} block(s) as {description}"
        }
    except PageNotFoundError as e:
        return {
            "success": False,
            "matches": [],
            "message": str(e)
        }
    except BlockNotFoundError as e:
        return {
            "success": False,
            "matches": [],
            "message": str(e)
        }
    except QueryError as e:
        return {
            "success": False,
            "matches": [],
            "message": str(e)
        }
    except Exception as e:
        logger.error(f"Error searching hierarchy: {str(e)}")
        return {
            "success": False,
            "matches": [],
            "message": f"Error searching hierarchy: {str(e)}"
        }


def search_by_date(start_date: str, end_date: Optional[str] = None, 
                   type_filter: str = "created", scope: str = "blocks",
                   include_content: bool = True) -> Dict[str, Any]:
    """
    Search for blocks or pages based on creation or modification dates.
    
    Args:
        start_date: Start date in ISO format (YYYY-MM-DD)
        end_date: Optional end date in ISO format (YYYY-MM-DD)
        type_filter: Whether to search by "created", "modified", or "both"
        scope: Whether to search "blocks", "pages", or "both"
        include_content: Whether to include block/page content
        
    Returns:
        Search results
    """
    # Validate inputs
    if type_filter not in ["created", "modified", "both"]:
        return {
            "success": False,
            "matches": [],
            "message": "Type must be 'created', 'modified', or 'both'"
        }
    
    if scope not in ["blocks", "pages", "both"]:
        return {
            "success": False,
            "matches": [],
            "message": "Scope must be 'blocks', 'pages', or 'both'"
        }
    
    # Parse dates
    try:
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        
        if end_date:
            # Set end_date to end of day
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            end_dt = end_dt.replace(hour=23, minute=59, second=59)
            end_timestamp = int(end_dt.timestamp() * 1000)
        else:
            # Default to now if no end date
            end_timestamp = int(datetime.now().timestamp() * 1000)
    except ValueError:
        return {
            "success": False,
            "matches": [],
            "message": "Invalid date format. Dates should be in YYYY-MM-DD format."
        }
    
    session, headers = get_session_and_headers()
    
    # Track matches across all queries to handle sorting
    all_matches = []
    logger.debug(f"Executing date search: {start_date} to {end_date or 'now'}")
    
    try:
        # Build and execute queries based on scope and type
        # Block queries for creation time
        if scope in ["blocks", "both"] and type_filter in ["created", "both"]:
            logger.debug("Searching blocks by creation time")
            query = f"""[:find ?uid ?s ?page-title ?time
                      :where
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      [?b :create/time ?time]
                      [(>= ?time {start_timestamp})]
                      [(<= ?time {end_timestamp})]]
                      :limit 1000"""
            
            block_created_results = execute_query(query)
            
            for uid, content, page_title, time in block_created_results:
                match_data = {
                    "uid": uid,
                    "type": "block",
                    "time": time,
                    "time_type": "created",
                    "page_title": page_title
                }
                
                if include_content:
                    resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
                    match_data["content"] = resolved_content
                
                all_matches.append(match_data)
        
        # Block queries for modification time
        if scope in ["blocks", "both"] and type_filter in ["modified", "both"]:
            logger.debug("Searching blocks by modification time")
            query = f"""[:find ?uid ?s ?page-title ?time
                      :where
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      [?b :edit/time ?time]
                      [(>= ?time {start_timestamp})]
                      [(<= ?time {end_timestamp})]]
                      :limit 1000"""
            
            block_modified_results = execute_query(query)
            
            for uid, content, page_title, time in block_modified_results:
                match_data = {
                    "uid": uid,
                    "type": "block",
                    "time": time,
                    "time_type": "modified",
                    "page_title": page_title
                }
                
                if include_content:
                    resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
                    match_data["content"] = resolved_content
                
                all_matches.append(match_data)
        
        # Page queries for creation time
        if scope in ["pages", "both"] and type_filter in ["created", "both"]:
            logger.debug("Searching pages by creation time")
            query = f"""[:find ?uid ?title ?time
                      :where
                      [?p :node/title ?title]
                      [?p :block/uid ?uid]
                      [?p :create/time ?time]
                      [(>= ?time {start_timestamp})]
                      [(<= ?time {end_timestamp})]]
                      :limit 500"""
            
            page_created_results = execute_query(query)
            
            for uid, title, time in page_created_results:
                match_data = {
                    "uid": uid,
                    "type": "page",
                    "time": time,
                    "time_type": "created",
                    "title": title
                }
                
                if include_content:
                    # Get a sample of page content (first few blocks)
                    sample_query = f"""[:find ?s
                                    :where
                                    [?p :block/uid "{uid}"]
                                    [?b :block/page ?p]
                                    [?b :block/string ?s]
                                    [?b :block/order ?o]
                                    [(< ?o 3)]]
                                    :limit 3"""
                    
                    page_blocks = execute_query(sample_query)
                    page_sample = "\n".join([content[0] for content in page_blocks[:3]])
                    
                    if page_blocks:
                        match_data["content"] = f"# {title}\n{page_sample}"
                        if len(page_blocks) > 3:
                            match_data["content"] += "\n..."
                    else:
                        match_data["content"] = f"# {title}\n(No content)"
                
                all_matches.append(match_data)
        
        # Page queries for modification time
        if scope in ["pages", "both"] and type_filter in ["modified", "both"]:
            logger.debug("Searching pages by modification time")
            query = f"""[:find ?uid ?title ?time
                      :where
                      [?p :node/title ?title]
                      [?p :block/uid ?uid]
                      [?p :edit/time ?time]
                      [(>= ?time {start_timestamp})]
                      [(<= ?time {end_timestamp})]]
                      :limit 500"""
            
            page_modified_results = execute_query(query)
            
            for uid, title, time in page_modified_results:
                match_data = {
                    "uid": uid,
                    "type": "page",
                    "time": time,
                    "time_type": "modified",
                    "title": title
                }
                
                if include_content:
                    # Get a sample of page content (first few blocks)
                    sample_query = f"""[:find ?s
                                    :where
                                    [?p :block/uid "{uid}"]
                                    [?b :block/page ?p]
                                    [?b :block/string ?s]
                                    [?b :block/order ?o]
                                    [(< ?o 3)]]
                                    :limit 3"""
                    
                    page_blocks = execute_query(sample_query)
                    page_sample = "\n".join([content[0] for content in page_blocks[:3]])
                    
                    if page_blocks:
                        match_data["content"] = f"# {title}\n{page_sample}"
                        if len(page_blocks) > 3:
                            match_data["content"] += "\n..."
                    else:
                        match_data["content"] = f"# {title}\n(No content)"
                
                all_matches.append(match_data)
        
        # Sort by time (newest first)
        all_matches.sort(key=lambda x: x["time"], reverse=True)
        
        # Deduplicate by UID and time_type
        seen = set()
        unique_matches = []
        
        for match in all_matches:
            key = (match["uid"], match["time_type"])
            if key not in seen:
                seen.add(key)
                unique_matches.append(match)
        
        return {
            "success": True,
            "matches": unique_matches,
            "message": f"Found {len(unique_matches)} matches for the given date range and criteria"
        }
    except QueryError as e:
        return {
            "success": False,
            "matches": [],
            "message": str(e)
        }
    except Exception as e:
        logger.error(f"Error searching by date: {str(e)}")
        return {
            "success": False,
            "matches": [],
            "message": f"Error searching by date: {str(e)}"
        }


def find_pages_modified_today(max_num_pages: int = 50) -> Dict[str, Any]:
    """
    Find pages that have been modified today.
    
    Args:
        max_num_pages: Maximum number of pages to return
        
    Returns:
        List of modified pages
    """
    if max_num_pages < 1:
        return {
            "success": False,
            "pages": [],
            "message": "max_num_pages must be at least 1"
        }
    
    # Define ancestor rule
    ancestor_rule = """[
        [(ancestor ?b ?a)
          [?a :block/children ?b]]
        [(ancestor ?b ?a)
          [?parent :block/children ?b]
          (ancestor ?parent ?a)]
    ]"""
    
    # Get start of today
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_timestamp = int(today.timestamp() * 1000)
    
    try:
        # Query for pages modified today
        logger.debug(f"Finding pages modified today (since {today.isoformat()})")
        query = f"""[:find ?title
                    :in $ ?start_timestamp %
                    :where
                    [?page :node/title ?title]
                    (ancestor ?block ?page)
                    [?block :edit/time ?time]
                    [(> ?time ?start_timestamp)]]
                    :limit {max_num_pages}"""
        
        results = execute_query(query, [start_timestamp, ancestor_rule])
        
        # Extract unique page titles
        unique_pages = list(set([title[0] for title in results]))[:max_num_pages]
        
        return {
            "success": True,
            "pages": unique_pages,
            "message": f"Found {len(unique_pages)} page(s) modified today"
        }
    except QueryError as e:
        return {
            "success": False,
            "pages": [],
            "message": str(e)
        }
    except Exception as e:
        logger.error(f"Error finding pages modified today: {str(e)}")
        return {
            "success": False,
            "pages": [],
            "message": f"Error finding pages modified today: {str(e)}"
        }


def execute_datomic_query(query: str, inputs: Optional[List[Any]] = None) -> Dict[str, Any]:
    """
    Execute a custom Datomic query.
    
    Args:
        query: The Datomic query
        inputs: Optional list of inputs
        
    Returns:
        Query results
    """
    if not query:
        return {
            "success": False,
            "matches": [],
            "message": "Query cannot be empty"
        }
    
    try:
        # Validate query format (basic check)
        if not query.strip().startswith('[:find'):
            logger.warning("Query doesn't start with [:find, may not be valid Datalog syntax")
        
        logger.debug(f"Executing custom Datomic query")
        results = execute_query(query, inputs or [])
        
        # Format results for display
        formatted_results = []
        for result in results:
            if isinstance(result, (list, tuple)):
                formatted_result = " | ".join(str(item) for item in result)
            else:
                formatted_result = str(result)
                
            formatted_results.append({
                "content": formatted_result,
                "block_uid": "",
                "page_title": ""
            })
        
        return {
            "success": True,
            "matches": formatted_results,
            "message": f"Query executed successfully. Found {len(formatted_results)} results."
        }
    except QueryError as e:
        return {
            "success": False,
            "matches": [],
            "message": str(e)
        }
    except Exception as e:
        logger.error(f"Error executing datomic query: {str(e)}")
        return {
            "success": False,
            "matches": [],
            "message": f"Failed to execute query: {str(e)}"
        }