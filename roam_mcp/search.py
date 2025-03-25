"""Search operations for the Roam MCP server."""

import os
import logging
from typing import Dict, List, Any, Optional, Union, Set
from datetime import datetime, timedelta
import re

from roam_mcp.api import (
    execute_query,
    get_session_and_headers,
    GRAPH_NAME,
    find_page_by_title,
    APIError
)
from roam_mcp.utils import (
    format_roam_date,
    resolve_block_references
)

# Set up logging
logger = logging.getLogger("roam-mcp.search")


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
    session, headers = get_session_and_headers()
    
    # Prepare the query
    if case_sensitive:
        text_condition = f'(clojure.string/includes? ?s "{text}")'
    else:
        text_condition = f'(clojure.string/includes? (clojure.string/lower-case ?s) "{text.lower()}")'
    
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
                   :in $ ?page-uid
                   :where
                   [?p :block/uid ?page-uid]
                   [?b :block/page ?p]
                   [?b :block/string ?s]
                   [?b :block/uid ?uid]
                   [{text_condition}]]"""
                   
        results = execute_query(query, [page_uid])
        
        # Process results without page titles
        matches = []
        for uid, content in results:
            # Resolve references if present
            resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
            
            matches.append({
                "block_uid": uid,
                "content": resolved_content,
                "page_title": page_title_uid
            })
            
    else:
        query = f"""[:find ?uid ?s ?page-title
                   :where
                   [?b :block/string ?s]
                   [?b :block/uid ?uid]
                   [?b :block/page ?p]
                   [?p :node/title ?page-title]
                   [{text_condition}]]"""
                   
        results = execute_query(query)
        
        # Process the results with page titles
        matches = []
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
                   :in $ ?page-uid
                   :where
                   [?p :block/uid ?page-uid]
                   [?b :block/page ?p]
                   [?b :block/string ?s]
                   [?b :block/uid ?uid]
                   [{combined_condition}]]"""
                   
        results = execute_query(query, [page_uid])
        
        # Process results without page titles
        matches = []
        for uid, content in results:
            # Resolve references if present
            resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
            
            matches.append({
                "block_uid": uid,
                "content": resolved_content,
                "page_title": page_title_uid
            })
    else:
        query = f"""[:find ?uid ?s ?page-title
                   :where
                   [?b :block/string ?s]
                   [?b :block/uid ?uid]
                   [?b :block/page ?p]
                   [?p :node/title ?page-title]
                   [{combined_condition}]]"""
                   
        results = execute_query(query)
        
        # Process the results with page titles
        matches = []
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
                   :in $ ?page-uid
                   :where
                   [?p :block/uid ?page-uid]
                   [?b :block/page ?p]
                   [?b :block/string ?s]
                   [?b :block/uid ?uid]
                   [(clojure.string/includes? ?s "{status_pattern}")]]"""
                   
        results = execute_query(query, [page_uid])
        
        # Process results without page titles
        matches = []
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
        query = f"""[:find ?uid ?s ?page-title
                   :where
                   [?b :block/string ?s]
                   [?b :block/uid ?uid]
                   [?b :block/page ?p]
                   [?p :node/title ?page-title]
                   [(clojure.string/includes? ?s "{status_pattern}")]]"""
                   
        results = execute_query(query)
        
        # Process the results with page titles
        matches = []
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
        block_ref_pattern = f"\\(\\({block_uid}\\)\\)"
        description = f"referencing block (({block_uid}))"
    else:
        description = "containing block references"
    
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
                      :in $ ?page-uid
                      :where
                      [?p :block/uid ?page-uid]
                      [?b :block/page ?p]
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [(clojure.string/includes? ?s "{block_ref_pattern}")]]"""
            results = execute_query(query, [page_uid])
        else:
            query = f"""[:find ?uid ?s
                      :in $ ?page-uid
                      :where
                      [?p :block/uid ?page-uid]
                      [?b :block/page ?p]
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [(re-find #"\\(\\([^)]+\\)\\)" ?s)]]"""
            results = execute_query(query, [page_uid])
        
        # Process results without page titles
        matches = []
        for uid, content in results:
            # Resolve references if present
            resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
            
            matches.append({
                "block_uid": uid,
                "content": resolved_content,
                "page_title": page_title_uid
            })
    else:
        if block_uid:
            query = f"""[:find ?uid ?s ?page-title
                      :in $
                      :where
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      [(clojure.string/includes? ?s "{block_ref_pattern}")]]"""
            results = execute_query(query)
        else:
            query = f"""[:find ?uid ?s ?page-title
                      :where
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      [(re-find #"\\(\\([^)]+\\)\\)" ?s)]]"""
            results = execute_query(query)
        
        # Process the results with page titles
        matches = []
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
    
    session, headers = get_session_and_headers()
    
    # Define ancestor rule
    ancestor_rule = """[
        [(ancestor ?child ?parent)
            [?parent :block/children ?child]]
        [(ancestor ?child ?parent)
            [?p :block/children ?child]
            (ancestor ?p ?parent)]
    ]"""
    
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
                
            query = """[:find ?uid ?s ?depth
                      :in $ % ?parent-uid ?page-uid ?max-depth
                      :where
                      [?parent :block/uid ?parent-uid]
                      [?p :block/uid ?page-uid]
                      (ancestor ?b ?parent)
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [(get-else $ ?b :block/path-length 1) ?depth]
                      [(< ?depth ?max-depth)]]"""
            inputs = [ancestor_rule, parent_uid, page_uid, max_depth + 1]
        else:
            query = """[:find ?uid ?s ?page-title ?depth
                      :in $ % ?parent-uid ?max-depth
                      :where
                      [?parent :block/uid ?parent-uid]
                      (ancestor ?b ?parent)
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      [(get-else $ ?b :block/path-length 1) ?depth]
                      [(< ?depth ?max-depth)]]"""
            inputs = [ancestor_rule, parent_uid, max_depth + 1]
        
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
                
            query = """[:find ?uid ?s ?depth
                      :in $ % ?child-uid ?page-uid ?max-depth
                      :where
                      [?child :block/uid ?child-uid]
                      [?p :block/uid ?page-uid]
                      (ancestor ?child ?b)
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [(get-else $ ?b :block/path-length 1) ?depth]
                      [(< ?depth ?max-depth)]]"""
            inputs = [ancestor_rule, child_uid, page_uid, max_depth + 1]
        else:
            query = """[:find ?uid ?s ?page-title ?depth
                      :in $ % ?child-uid ?max-depth
                      :where
                      [?child :block/uid ?child-uid]
                      (ancestor ?child ?b)
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      [(get-else $ ?b :block/path-length 1) ?depth]
                      [(< ?depth ?max-depth)]]"""
            inputs = [ancestor_rule, child_uid, max_depth + 1]
        
        description = f"ancestors of block {child_uid}"
    
    # Execute the query
    results = execute_query(query, inputs)
    
    # Process the results
    matches = []
    for result in results:
        uid = result[0]
        content = result[1]
        
        if len(result) == 3:
            # Format is [uid, content, depth]
            page_title = page_title_uid
            depth = result[2]
        else:
            # Format is [uid, content, page_title, depth]
            page_title = result[2]
            depth = result[3]
        
        # Resolve references if present
        resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
        
        match_data = {
            "block_uid": uid,
            "content": resolved_content,
            "depth": depth
        }
        
        if page_title:
            match_data["page_title"] = page_title
            
        matches.append(match_data)
    
    return {
        "success": True,
        "matches": matches,
        "message": f"Found {len(matches)} block(s) as {description}"
    }


def search_by_date(start_date: str, end_date: Optional[str] = None, 
                   type_filter: str = "created", scope: str = "blocks",
                   include_content: bool = True) -> Dict[str, Any]:
    """
    Search for blocks or pages based on creation or modification dates.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
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
    
    # Build queries based on scope and type
    results = []
    
    # Fix for the "Insufficient bindings" error by properly handling create/edit time variables
    if scope in ["blocks", "both"]:
        # Blocks scoped query
        if type_filter == "created":
            query = f"""[:find ?uid ?s ?page-title ?time
                      :where
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      [?b :create/time ?time]
                      [(>= ?time {start_timestamp})]
                      [(<= ?time {end_timestamp})]]"""
        elif type_filter == "modified":
            query = f"""[:find ?uid ?s ?page-title ?time
                      :where
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      [?b :edit/time ?time]
                      [(>= ?time {start_timestamp})]
                      [(<= ?time {end_timestamp})]]"""
        else:  # both
            # We need two separate queries for created and modified times
            create_query = f"""[:find ?uid ?s ?page-title ?time
                            :where
                            [?b :block/string ?s]
                            [?b :block/uid ?uid]
                            [?b :block/page ?p]
                            [?p :node/title ?page-title]
                            [?b :create/time ?time]
                            [(>= ?time {start_timestamp})]
                            [(<= ?time {end_timestamp})]]"""
                            
            edit_query = f"""[:find ?uid ?s ?page-title ?time
                           :where
                           [?b :block/string ?s]
                           [?b :block/uid ?uid]
                           [?b :block/page ?p]
                           [?p :node/title ?page-title]
                           [?b :edit/time ?time]
                           [(>= ?time {start_timestamp})]
                           [(<= ?time {end_timestamp})]]"""
            
            # Execute both queries and combine results
            create_results = execute_query(create_query)
            edit_results = execute_query(edit_query)
            
            # Process created time results
            for uid, content, page_title, time in create_results:
                results.append({
                    "uid": uid,
                    "type": "block",
                    "content": resolve_block_references(session, headers, GRAPH_NAME, content) if include_content else None,
                    "page_title": page_title,
                    "time": time,
                    "time_type": "created"
                })
            
            # Process edit time results, avoiding duplicates
            seen_uids = set(r["uid"] for r in results)
            for uid, content, page_title, time in edit_results:
                if uid not in seen_uids:
                    results.append({
                        "uid": uid,
                        "type": "block",
                        "content": resolve_block_references(session, headers, GRAPH_NAME, content) if include_content else None,
                        "page_title": page_title,
                        "time": time,
                        "time_type": "modified"
                    })
                    seen_uids.add(uid)
            
            # Skip the standard query for "both" since we've handled it separately
            query = None
        
        # Execute the query if not "both"
        if query:
            query_results = execute_query(query)
            
            # Process results
            for uid, content, page_title, time in query_results:
                results.append({
                    "uid": uid,
                    "type": "block",
                    "content": resolve_block_references(session, headers, GRAPH_NAME, content) if include_content else None,
                    "page_title": page_title,
                    "time": time,
                    "time_type": type_filter
                })
    
    if scope in ["pages", "both"]:
        # Pages scoped query
        if type_filter == "created":
            query = f"""[:find ?uid ?title ?time
                      :where
                      [?p :node/title ?title]
                      [?p :block/uid ?uid]
                      [?p :create/time ?time]
                      [(>= ?time {start_timestamp})]
                      [(<= ?time {end_timestamp})]]"""
        elif type_filter == "modified":
            query = f"""[:find ?uid ?title ?time
                      :where
                      [?p :node/title ?title]
                      [?p :block/uid ?uid]
                      [?p :edit/time ?time]
                      [(>= ?time {start_timestamp})]
                      [(<= ?time {end_timestamp})]]"""
        else:  # both
            # We need two separate queries for created and modified times
            create_query = f"""[:find ?uid ?title ?time
                            :where
                            [?p :node/title ?title]
                            [?p :block/uid ?uid]
                            [?p :create/time ?time]
                            [(>= ?time {start_timestamp})]
                            [(<= ?time {end_timestamp})]]"""
                            
            edit_query = f"""[:find ?uid ?title ?time
                           :where
                           [?p :node/title ?title]
                           [?p :block/uid ?uid]
                           [?p :edit/time ?time]
                           [(>= ?time {start_timestamp})]
                           [(<= ?time {end_timestamp})]]"""
            
            # Execute both queries and combine results
            create_results = execute_query(create_query)
            edit_results = execute_query(edit_query)
            
            # Process created time results
            for uid, title, time in create_results:
                # Only get page content if requested
                page_content = None
                if include_content:
                    try:
                        # Get a sample of page content (first 3 blocks)
                        content_query = f"""[:find (pull ?b [:block/string])
                                         :where
                                         [?p :block/uid "{uid}"]
                                         [?b :block/page ?p]
                                         [?b :block/order ?o]
                                         [(< ?o 3)]]"""
                        
                        page_blocks = execute_query(content_query)
                        if page_blocks:
                            block_contents = [b[0].get(":block/string", "") for b in page_blocks[:3]]
                            page_content = f"{title}\n" + "\n".join(block_contents)
                            if len(page_blocks) > 3:
                                page_content += "\n..."
                        else:
                            page_content = f"{title}\n(No content)"
                    except Exception as e:
                        page_content = f"{title}\n(Error retrieving content: {str(e)})"
                
                results.append({
                    "uid": uid,
                    "type": "page",
                    "title": title,
                    "content": page_content,
                    "time": time,
                    "time_type": "created"
                })
            
            # Process edit time results, avoiding duplicates
            seen_uids = set(r["uid"] for r in results)
            for uid, title, time in edit_results:
                if uid not in seen_uids:
                    # Only get page content if requested
                    page_content = None
                    if include_content:
                        try:
                            # Get a sample of page content (first 3 blocks)
                            content_query = f"""[:find (pull ?b [:block/string])
                                             :where
                                             [?p :block/uid "{uid}"]
                                             [?b :block/page ?p]
                                             [?b :block/order ?o]
                                             [(< ?o 3)]]"""
                            
                            page_blocks = execute_query(content_query)
                            if page_blocks:
                                block_contents = [b[0].get(":block/string", "") for b in page_blocks[:3]]
                                page_content = f"{title}\n" + "\n".join(block_contents)
                                if len(page_blocks) > 3:
                                    page_content += "\n..."
                            else:
                                page_content = f"{title}\n(No content)"
                        except Exception as e:
                            page_content = f"{title}\n(Error retrieving content: {str(e)})"
                    
                    results.append({
                        "uid": uid,
                        "type": "page",
                        "title": title,
                        "content": page_content,
                        "time": time,
                        "time_type": "modified"
                    })
                    seen_uids.add(uid)
            
            # Skip the standard query for "both" since we've handled it separately
            query = None
        
        # Execute the query if not "both"
        if query:
            query_results = execute_query(query)
            
            # Process results
            for uid, title, time in query_results:
                # Only get page content if requested
                page_content = None
                if include_content:
                    try:
                        # Get a sample of page content (first 3 blocks)
                        content_query = f"""[:find (pull ?b [:block/string])
                                         :where
                                         [?p :block/uid "{uid}"]
                                         [?b :block/page ?p]
                                         [?b :block/order ?o]
                                         [(< ?o 3)]]"""
                        
                        page_blocks = execute_query(content_query)
                        if page_blocks:
                            block_contents = [b[0].get(":block/string", "") for b in page_blocks[:3]]
                            page_content = f"{title}\n" + "\n".join(block_contents)
                            if len(page_blocks) > 3:
                                page_content += "\n..."
                        else:
                            page_content = f"{title}\n(No content)"
                    except Exception as e:
                        page_content = f"{title}\n(Error retrieving content: {str(e)})"
                
                results.append({
                    "uid": uid,
                    "type": "page",
                    "title": title,
                    "content": page_content,
                    "time": time,
                    "time_type": type_filter
                })
    
    # Sort by time
    results.sort(key=lambda x: x["time"], reverse=True)
    
    return {
        "success": True,
        "matches": results,
        "message": f"Found {len(results)} matches for the given date range and criteria"
    }


def find_pages_modified_today(max_num_pages: int = 50) -> Dict[str, Any]:
    """
    Find pages that have been modified today.
    
    Args:
        max_num_pages: Maximum number of pages to return
        
    Returns:
        List of modified pages
    """
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
    
    # Query for pages modified today
    query = """[:find ?title
               :in $ ?start_timestamp %
               :where
               [?page :node/title ?title]
               (ancestor ?block ?page)
               [?block :edit/time ?time]
               [(> ?time ?start_timestamp)]]"""
    
    results = execute_query(query, [start_timestamp, ancestor_rule])
    
    # Extract unique page titles
    unique_pages = list(set([title[0] for title in results]))[:max_num_pages]
    
    return {
        "success": True,
        "pages": unique_pages,
        "message": f"Found {len(unique_pages)} page(s) modified today"
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
    try:
        results = execute_query(query, inputs or [])
        
        return {
            "success": True,
            "matches": [{"content": str(result)} for result in results],
            "message": f"Query executed successfully. Found {len(results)} results."
        }
    except Exception as e:
        return {
            "success": False,
            "matches": [],
            "message": f"Failed to execute query: {str(e)}"
        }