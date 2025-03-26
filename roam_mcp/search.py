"""Search operations for the Roam MCP server."""

from typing import Dict, List, Any, Optional, Union, Set
from datetime import datetime, timedelta
import re
import logging

# Use client from api module
from roam_mcp.api import (
    get_client, # Import function to get client instance
    # execute_query, # No longer used directly
    ValidationError,
    QueryError,
    PageNotFoundError,
    BlockNotFoundError
)
# Import utils needed here
from roam_mcp.utils import (
    format_roam_date,
    resolve_block_references as resolve_block_references_util # Rename to avoid conflict
)
# Import finders from utils
from roam_mcp.utils import find_page_by_title as find_page_by_title_util

# Set up logging
logger = logging.getLogger("roam-mcp.search")


def validate_search_params(text: Optional[str] = None, tag: Optional[str] = None, 
                          status: Optional[str] = None, page_title_uid: Optional[str] = None):
    """
    Validate common search parameters. (Currently only validates status)
    
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
    Search for blocks containing specific text. Uses RoamClient.
    
    Args:
        text: Text to search for
        page_title_uid: Optional page title or UID to scope the search
        case_sensitive: Whether to perform case-sensitive search (Roam default is often case-sensitive)
        
    Returns:
        Search results dictionary.
    """
    if not text:
        return {"success": False, "matches": [], "message": "Search text cannot be empty"}
    
    client = get_client() # Get client instance

    # Escape double quotes in search text for Datalog query
    escaped_text = text.replace('"', '\\"')
    
    # Prepare the query condition
    if case_sensitive:
        # Use Clojure's string includes function for case-sensitive search
        text_condition = f'(clojure.string/includes? ?s "{escaped_text}")'
    else:
        # Use lower-case conversion for case-insensitive search
        text_condition = f'(clojure.string/includes? (clojure.string/lower-case ?s) "{escaped_text.lower()}")'
    
    try:
        target_page_uid: Optional[str] = None
        if page_title_uid:
            target_page_uid = find_page_by_title_util(client, page_title_uid)
            if not target_page_uid:
                 raise PageNotFoundError(page_title_uid) # Raise error if specific page not found
                 
            # Scoped query
            query = f"""[:find ?uid ?s ?order
                      :in $ ?page_uid
                      :where
                      [?p :block/uid ?page_uid]
                      [?b :block/page ?p]
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/order ?order]
                      [{text_condition}]]"""
            inputs = [target_page_uid]
        else:
            # Global query
            query = f"""[:find ?uid ?s ?page-title
                      :where
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      [{text_condition}]]"""
            inputs = None # No inputs needed for global query
        
        # Execute the query using the client
        logger.debug(f"Executing text search for: '{text}' (case_sensitive={case_sensitive}) Scope: {page_title_uid or 'Global'}")
        results = client.query(query, inputs)
        
        # Process results
        matches = []
        if not results: results = [] # Ensure results is iterable

        for result_item in results:
             uid = result_item[0]
             content = result_item[1]
             page_title = target_page_uid if target_page_uid else result_item[2] # Use known page UID or result

             # Resolve references if present
             resolved_content = resolve_block_references_util(client, content)
             
             matches.append({
                 "block_uid": uid,
                 "content": resolved_content,
                 "page_title": page_title # Could be UID or Title depending on query
             })
        
        return {
            "success": True,
            "matches": matches,
            "message": f"Found {len(matches)} block(s) containing \"{text}\""
        }
        
    except (PageNotFoundError, QueryError, ValidationError) as e: # Catch specific errors
        logger.error(f"Search by text failed: {e}", exc_info=True)
        return {"success": False, "matches": [], "message": str(e)}
    except Exception as e: # Catch unexpected errors
        logger.error(f"Unexpected error searching by text: {e}", exc_info=True)
        return {"success": False, "matches": [], "message": f"Unexpected error: {e}"}


def search_by_tag(tag: str, page_title_uid: Optional[str] = None, near_tag: Optional[str] = None) -> Dict[str, Any]:
    """
    Search for blocks containing a specific tag, with optional secondary tag filter. Uses RoamClient.
    
    Args:
        tag: Primary tag (without formatting).
        page_title_uid: Optional scope (page title or UID).
        near_tag: Optional secondary tag that must be in the same block.
        
    Returns:
        Search results dictionary.
    """
    if not tag:
        return {"success": False, "matches": [], "message": "Primary tag cannot be empty"}

    client = get_client() # Get client instance

    # Normalize and prepare primary tag condition
    clean_tag = tag.replace('#', '').replace('[[', '').replace(']]', '').strip()
    if not clean_tag: return {"success": False, "matches": [], "message": "Primary tag cannot be empty after cleaning"}
    # Roam uses :block/refs for links, need to find the page entity for the tag
    primary_tag_page_title = f"[[{clean_tag}]]" # Assume tag corresponds to a page title

    # Prepare near_tag condition if provided
    near_tag_condition = ""
    if near_tag:
        clean_near_tag = near_tag.replace('#', '').replace('[[', '').replace(']]', '').strip()
        if clean_near_tag:
             # Check if near_tag content is included in block string
             # Create variants for matching #tag, #[[tag]], [[tag]]
             near_tag_variants = [f"#{clean_near_tag}", f"#[[{clean_near_tag}]]", f"[[{clean_near_tag}]]"]
             near_tag_conditions_list = [f'(clojure.string/includes? ?s "{variant}")' for variant in near_tag_variants]
             near_tag_condition = f"(or {' '.join(near_tag_conditions_list)})"
             logger.debug(f"Adding near_tag condition: {near_tag_condition}")
        else:
             logger.warning("Near tag provided but was empty after cleaning.")

    try:
        target_page_uid: Optional[str] = None
        if page_title_uid:
            target_page_uid = find_page_by_title_util(client, page_title_uid)
            if not target_page_uid: raise PageNotFoundError(page_title_uid)
        
        # Query using :block/refs based on the tag's corresponding page title
        # Find the entity ID (:db/id) of the tag page first
        tag_page_query = f'''[:find ?p . :in $ ?title :where [?p :node/title ?title]]'''
        tag_page_eid = client.query(tag_page_query, inputs=[clean_tag])

        if not tag_page_eid:
             logger.warning(f"Could not find page/entity for primary tag: '{clean_tag}'. Search may yield no results.")
             # Proceed? Or return error? Let's proceed, Roam might handle refs differently sometimes.
             # Alternative: Search based on string inclusion like before? Less accurate for tags.
             # Let's stick to :block/refs if possible, but maybe add string search as fallback?
             # For now, return empty if tag page not found.
             return {"success": True, "matches": [], "message": f"No page found for tag '{clean_tag}', cannot search by reference."}

        # Build the main query
        where_clauses = [
            f'[?tag_page :db/id {tag_page_eid}]', # Use the found EID
            '[?b :block/refs ?tag_page]',      # Block must reference the tag page
            '[?b :block/string ?s]',
            '[?b :block/uid ?uid]',
            '[?b :block/page ?page_entity]',
            '[?page_entity :node/title ?page_title]'
        ]
        
        inputs = None # Reset inputs for main query

        if target_page_uid:
            where_clauses.append(f'[?page_entity :block/uid "{target_page_uid}"]')
            query_find = '[:find ?uid ?s :in $ ?tag_page_eid' # Only need uid, string if scoped
            # inputs = [tag_page_eid] # Input is tag page EID
        else:
            query_find = '[:find ?uid ?s ?page_title :in $ ?tag_page_eid' # Need page_title if global
            # inputs = [tag_page_eid]

        if near_tag_condition:
             where_clauses.append(near_tag_condition)

        query = f"{query_find} :where {' '.join(where_clauses)}]"
        
        logger.debug(f"Executing tag ref search for EID: {tag_page_eid} ('{clean_tag}')")
        results = client.query(query, inputs=[tag_page_eid]) # Pass EID as input

        # Process results
        matches = []
        if not results: results = []

        for result_item in results:
             uid = result_item[0]
             content = result_item[1]
             page_title = target_page_uid if target_page_uid else result_item[2]

             resolved_content = resolve_block_references_util(client, content)
             
             matches.append({
                 "block_uid": uid,
                 "content": resolved_content,
                 "page_title": page_title
             })

        message = f"Found {len(matches)} block(s) referencing tag '{clean_tag}'"
        if near_tag and clean_near_tag: message += f" with near tag '{clean_near_tag}'"
        
        return {"success": True, "matches": matches, "message": message}

    except (PageNotFoundError, QueryError, ValidationError) as e:
        logger.error(f"Search by tag failed: {e}", exc_info=True)
        return {"success": False, "matches": [], "message": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error searching by tag: {e}", exc_info=True)
        return {"success": False, "matches": [], "message": f"Unexpected error: {e}"}


def search_by_status(status: str, page_title_uid: Optional[str] = None, include: Optional[str] = None, exclude: Optional[str] = None) -> Dict[str, Any]:
    """
    Search for blocks with TODO/DONE status. Uses RoamClient.
    
    Args:
        status: "TODO" or "DONE".
        page_title_uid: Optional scope (page title or UID).
        include: Optional keywords to require in content.
        exclude: Optional keywords to filter out from content.
        
    Returns:
        Search results dictionary.
    """
    validate_search_params(status=status) # Raises ValidationError if invalid
    
    client = get_client() # Get client instance
    status_pattern = f"{{{{[[{status}]]}}}}"
    # Escape pattern for query if needed? Clojure string/includes handles it okay.

    try:
        target_page_uid: Optional[str] = None
        if page_title_uid:
            target_page_uid = find_page_by_title_util(client, page_title_uid)
            if not target_page_uid: raise PageNotFoundError(page_title_uid)
            
            # Scoped query
            query = f"""[:find ?uid ?s
                      :in $ ?page_uid
                      :where
                      [?p :block/uid ?page_uid]
                      [?b :block/page ?p]
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [(clojure.string/includes? ?s "{status_pattern}")]]"""
            inputs = [target_page_uid]
        else:
            # Global query
            query = f"""[:find ?uid ?s ?page-title
                      :where
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      [(clojure.string/includes? ?s "{status_pattern}")]]"""
            inputs = None
            
        logger.debug(f"Executing status search for: {status}, Scope: {page_title_uid or 'Global'}")
        results = client.query(query, inputs)

        # Process and filter results
        matches = []
        if not results: results = []
        
        include_terms = [term.strip().lower() for term in include.split(',')] if include else []
        exclude_terms = [term.strip().lower() for term in exclude.split(',')] if exclude else []

        for result_item in results:
             uid = result_item[0]
             content = result_item[1]
             page_title = target_page_uid if target_page_uid else result_item[2]
             
             resolved_content = resolve_block_references_util(client, content)
             content_lower = resolved_content.lower()
             
             # Apply filters
             if include_terms and not any(term in content_lower for term in include_terms):
                  continue
             if exclude_terms and any(term in content_lower for term in exclude_terms):
                  continue
                  
             matches.append({
                 "block_uid": uid,
                 "content": resolved_content,
                 "page_title": page_title
             })

        message = f"Found {len(matches)} block(s) with status {status}"
        if include: message += f" including '{include}'"
        if exclude: message += f" excluding '{exclude}'"
        
        return {"success": True, "matches": matches, "message": message}

    except (PageNotFoundError, QueryError, ValidationError) as e:
        logger.error(f"Search by status failed: {e}", exc_info=True)
        return {"success": False, "matches": [], "message": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error searching by status: {e}", exc_info=True)
        return {"success": False, "matches": [], "message": f"Unexpected error: {e}"}


def search_block_refs(block_uid: Optional[str] = None, page_title_uid: Optional[str] = None) -> Dict[str, Any]:
    """
    Search for blocks containing references `((...))` or references to a specific block UID. Uses RoamClient.
    
    Args:
        block_uid: Optional specific block UID to find references to.
        page_title_uid: Optional scope (page title or UID).
        
    Returns:
        Search results dictionary.
    """
    client = get_client() # Get client instance
    
    # Determine query condition based on whether specific block_uid is given
    if block_uid:
        if not (isinstance(block_uid, str) and len(block_uid) == 9):
             raise ValidationError("Invalid block_uid format. Must be 9 characters.", "block_uid")
        ref_pattern = f"(({block_uid}))"
        condition = f'(clojure.string/includes? ?s "{ref_pattern}")'
        description = f"referencing block (({block_uid}))"
    else:
        # Regex to find any block reference `((...))`
        # Need to escape backslashes for Datalog string: \\(\\( ... \\)\\)
        condition = r'[(re-find #"\\(\\([^)]+\\)\\)" ?s)]'
        description = "containing block references"

    try:
        target_page_uid: Optional[str] = None
        if page_title_uid:
            target_page_uid = find_page_by_title_util(client, page_title_uid)
            if not target_page_uid: raise PageNotFoundError(page_title_uid)
            
            # Scoped query
            query = f"""[:find ?uid ?s
                      :in $ ?page_uid
                      :where
                      [?p :block/uid ?page_uid]
                      [?b :block/page ?p]
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      {condition}]"""
            inputs = [target_page_uid]
        else:
            # Global query
            query = f"""[:find ?uid ?s ?page-title
                      :where
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      {condition}]"""
            inputs = None
            
        logger.debug(f"Executing block reference search: {description}, Scope: {page_title_uid or 'Global'}")
        results = client.query(query, inputs)

        matches = []
        if not results: results = []

        for result_item in results:
             uid = result_item[0]
             content = result_item[1]
             page_title = target_page_uid if target_page_uid else result_item[2]

             resolved_content = resolve_block_references_util(client, content)
             
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

    except (PageNotFoundError, QueryError, ValidationError) as e:
        logger.error(f"Search block references failed: {e}", exc_info=True)
        return {"success": False, "matches": [], "message": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error searching block references: {e}", exc_info=True)
        return {"success": False, "matches": [], "message": f"Unexpected error: {e}"}


def search_hierarchy(parent_uid: Optional[str] = None, child_uid: Optional[str] = None, 
                     page_title_uid: Optional[str] = None, max_depth: int = 1) -> Dict[str, Any]:
    """
    Search block hierarchy (ancestors or descendants) up to max_depth. Uses RoamClient.
    
    Args:
        parent_uid: Find descendants of this block UID.
        child_uid: Find ancestors of this block UID.
        page_title_uid: Optional scope (page title or UID).
        max_depth: Max levels to traverse (1-10). Defaults to 1.
        
    Returns:
        Search results dictionary including depth.
    """
    if not parent_uid and not child_uid:
        return {"success": False, "matches": [], "message": "Either parent_uid or child_uid must be provided"}
    if parent_uid and child_uid:
         return {"success": False, "matches": [], "message": "Provide either parent_uid OR child_uid, not both"}

    max_depth = max(1, min(max_depth, 10)) # Clamp depth 1-10

    client = get_client() # Get client instance
    
    # Datalog rule for ancestry with depth calculation
    # Note: Roam's direct parent/child might be complex (page vs block). This rule assumes :block/children link.
    # Roam also has :block/parents which might be more direct for ancestors? Testing needed.
    # This rule traverses :block/children relationship downwards.
    ancestor_rule = """[
        [(ancestor ?child ?parent ?depth)
            [?parent :block/children ?child]
            [(identity 1) ?depth]] ; Direct child is depth 1
        [(ancestor ?child ?ancestor ?depth)
            [?parent :block/children ?child]
            (ancestor ?parent ?ancestor ?prev_depth)
            [(inc ?prev_depth) ?depth]] ; Increment depth
    ]"""

    try:
        target_page_uid: Optional[str] = None
        if page_title_uid:
            target_page_uid = find_page_by_title_util(client, page_title_uid)
            if not target_page_uid: raise PageNotFoundError(page_title_uid)

        query: str
        inputs: List[Any]
        description: str

        if parent_uid: # Find descendants
            if not (isinstance(parent_uid, str) and len(parent_uid) == 9): raise ValidationError("Invalid parent_uid format.", "parent_uid")
            description = f"descendants of block {parent_uid}"
            
            find_vars = "?b_uid ?b_s ?depth"
            base_where = [
                 f'[?parent :block/uid "{parent_uid}"]',
                 '(ancestor ?b ?parent ?depth)', # ?b is descendant, ?parent is ancestor
                 '[?b :block/string ?b_s]',
                 '[?b :block/uid ?b_uid]',
                 f'[(<= ?depth {max_depth})]' # Filter by max depth
            ]
            if target_page_uid:
                 find_vars = "?b_uid ?b_s ?depth" # Only need these if scoped
                 base_where.append(f'[?page :block/uid "{target_page_uid}"]')
                 base_where.append('[?b :block/page ?page]') # Ensure descendant is on the target page
                 query = f"[:find {find_vars} :in $ % :where {' '.join(base_where)}]"
                 inputs = [ancestor_rule]
            else:
                 find_vars = "?b_uid ?b_s ?page_title ?depth" # Need page title if global
                 base_where.append('[?b :block/page ?page]')
                 base_where.append('[?page :node/title ?page_title]')
                 query = f"[:find {find_vars} :in $ % :where {' '.join(base_where)}]"
                 inputs = [ancestor_rule]

        else: # Find ancestors (child_uid is provided)
            if not (isinstance(child_uid, str) and len(child_uid) == 9): raise ValidationError("Invalid child_uid format.", "child_uid")
            description = f"ancestors of block {child_uid}"

            find_vars = "?anc_uid ?anc_s ?depth"
            # Ancestor rule finds (descendant ancestor depth)
            # So we need (ancestor child_block ancestor_block depth)
            base_where = [
                 f'[?child :block/uid "{child_uid}"]',
                 '(ancestor ?child ?anc ?depth)', # ?anc is ancestor, ?child is descendant
                 '[?anc :block/string ?anc_s]',
                 '[?anc :block/uid ?anc_uid]',
                 f'[(<= ?depth {max_depth})]' # Filter by max depth
            ]
            if target_page_uid:
                 find_vars = "?anc_uid ?anc_s ?depth"
                 base_where.append(f'[?page :block/uid "{target_page_uid}"]')
                 base_where.append('[?anc :block/page ?page]') # Ensure ancestor is on the target page
                 query = f"[:find {find_vars} :in $ % :where {' '.join(base_where)}]"
                 inputs = [ancestor_rule]
            else:
                 find_vars = "?anc_uid ?anc_s ?page_title ?depth"
                 base_where.append('[?anc :block/page ?page]')
                 base_where.append('[?page :node/title ?page_title]')
                 query = f"[:find {find_vars} :in $ % :where {' '.join(base_where)}]"
                 inputs = [ancestor_rule]

        logger.debug(f"Executing hierarchy search: {description}, Max Depth: {max_depth}, Scope: {page_title_uid or 'Global'}")
        results = client.query(query, inputs)

        matches = []
        if not results: results = []

        for result_item in results:
             uid = result_item[0]
             content = result_item[1]
             depth = result_item[-1] # Depth is always last
             page_title = target_page_uid if target_page_uid else result_item[2] # Page title is 3rd if global

             resolved_content = resolve_block_references_util(client, content)
             
             matches.append({
                 "block_uid": uid,
                 "content": resolved_content,
                 "depth": depth,
                 "page_title": page_title
             })
        
        # Sort by depth? Optional. Current query doesn't guarantee order.
        matches.sort(key=lambda x: x["depth"])

        return {
            "success": True,
            "matches": matches,
            "message": f"Found {len(matches)} block(s) as {description} (up to depth {max_depth})"
        }

    except (PageNotFoundError, QueryError, ValidationError) as e:
        logger.error(f"Hierarchy search failed: {e}", exc_info=True)
        return {"success": False, "matches": [], "message": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error searching hierarchy: {e}", exc_info=True)
        return {"success": False, "matches": [], "message": f"Unexpected error: {e}"}


def search_by_date(start_date: str, end_date: Optional[str] = None, 
                   type_filter: str = "created", scope: str = "blocks",
                   include_content: bool = True) -> Dict[str, Any]:
    """
    Search blocks/pages by creation/modification date range. Uses RoamClient.
    
    Args:
        start_date: Start date (YYYY-MM-DD).
        end_date: Optional end date (YYYY-MM-DD).
        type_filter: 'created', 'modified', or 'both'.
        scope: 'blocks', 'pages', or 'both'.
        include_content: If True, fetches content (potentially slow).
        
    Returns:
        Search results dictionary.
    """
    if type_filter not in ["created", "modified", "both"]: raise ValidationError("type_filter must be 'created', 'modified', or 'both'", "type_filter")
    if scope not in ["blocks", "pages", "both"]: raise ValidationError("scope must be 'blocks', 'pages', or 'both'", "scope")

    client = get_client() # Get client instance

    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        # Roam time is ms since epoch
        start_timestamp = int(start_dt.timestamp() * 1000)
        
        if end_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            # Include the whole end day
            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999999)
            end_timestamp = int(end_dt.timestamp() * 1000)
        else:
            # If no end date, search up to now
            end_timestamp = int(datetime.now().timestamp() * 1000)
            
        if start_timestamp > end_timestamp:
             raise ValidationError("Start date cannot be after end date.", "start_date/end_date")

    except ValueError:
        raise ValidationError("Invalid date format. Use YYYY-MM-DD.", "start_date/end_date")

    all_matches = []
    logger.debug(f"Executing date search: Type='{type_filter}', Scope='{scope}', Range='{start_date}' to '{end_date or 'now'}'")

    # Helper to build date range condition
    def date_condition(time_var: str) -> str:
        return f"[(>= {time_var} {start_timestamp})] [(<= {time_var} {end_timestamp})]"

    try:
        # --- Block Queries ---
        if scope in ["blocks", "both"]:
            if type_filter in ["created", "both"]:
                logger.debug("Querying blocks by creation time...")
                query = f"""[:find ?uid ?s ?page-title ?time
                           :where [?b :block/string ?s] [?b :block/uid ?uid]
                                  [?b :block/page ?p] [?p :node/title ?page-title]
                                  [?b :create/time ?time] {date_condition('?time')}]"""
                results = client.query(query) or []
                for uid, content, page_title, time in results:
                     match_data = {"uid": uid, "type": "block", "time": time, "time_type": "created", "page_title": page_title}
                     if include_content: match_data["content"] = resolve_block_references_util(client, content)
                     all_matches.append(match_data)

            if type_filter in ["modified", "both"]:
                logger.debug("Querying blocks by modification time...")
                query = f"""[:find ?uid ?s ?page-title ?time
                           :where [?b :block/string ?s] [?b :block/uid ?uid]
                                  [?b :block/page ?p] [?p :node/title ?page-title]
                                  [?b :edit/time ?time] {date_condition('?time')}]"""
                results = client.query(query) or []
                for uid, content, page_title, time in results:
                     match_data = {"uid": uid, "type": "block", "time": time, "time_type": "modified", "page_title": page_title}
                     if include_content: match_data["content"] = resolve_block_references_util(client, content)
                     all_matches.append(match_data)

        # --- Page Queries ---
        if scope in ["pages", "both"]:
            if type_filter in ["created", "both"]:
                 logger.debug("Querying pages by creation time...")
                 query = f"""[:find ?uid ?title ?time
                            :where [?p :node/title ?title] [?p :block/uid ?uid]
                                   [?p :create/time ?time] {date_condition('?time')}]"""
                 results = client.query(query) or []
                 for uid, title, time in results:
                      match_data = {"uid": uid, "type": "page", "time": time, "time_type": "created", "title": title}
                      # Fetching full page content here could be very slow for many pages.
                      # Consider only adding title or maybe first block if include_content is True.
                      if include_content: match_data["content"] = f"# {title}" # Placeholder
                      all_matches.append(match_data)

            if type_filter in ["modified", "both"]:
                 logger.debug("Querying pages by modification time...")
                 query = f"""[:find ?uid ?title ?time
                            :where [?p :node/title ?title] [?p :block/uid ?uid]
                                   [?p :edit/time ?time] {date_condition('?time')}]"""
                 results = client.query(query) or []
                 for uid, title, time in results:
                      match_data = {"uid": uid, "type": "page", "time": time, "time_type": "modified", "title": title}
                      if include_content: match_data["content"] = f"# {title}" # Placeholder
                      all_matches.append(match_data)

        # --- Process & Return ---
        # Deduplicate based on UID and time_type (e.g., block modified and created in range)
        seen = set()
        unique_matches = []
        for match in all_matches:
            key = (match["uid"], match["time_type"])
            if key not in seen:
                seen.add(key)
                unique_matches.append(match)
                
        # Sort by time (newest first)
        unique_matches.sort(key=lambda x: x["time"], reverse=True)

        # Limit results? Maybe add a limit parameter. For now, return all.
        # if len(unique_matches) > 100: # Example limit
        #      logger.warning(f"Date search returned {len(unique_matches)} results, truncating to 100.")
        #      unique_matches = unique_matches[:100]

        return {
            "success": True,
            "matches": unique_matches,
            "message": f"Found {len(unique_matches)} unique matches for the date range and criteria"
        }

    except (QueryError, ValidationError) as e:
        logger.error(f"Search by date failed: {e}", exc_info=True)
        return {"success": False, "matches": [], "message": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error searching by date: {e}", exc_info=True)
        return {"success": False, "matches": [], "message": f"Unexpected error: {e}"}


def find_pages_modified_today(max_num_pages: int = 50) -> Dict[str, Any]:
    """
    Find pages modified since midnight today. Uses RoamClient.
    
    Args:
        max_num_pages: Max pages to return.
        
    Returns:
        Dictionary with success status and list of page titles.
    """
    if max_num_pages < 1:
         raise ValidationError("max_num_pages must be at least 1", "max_num_pages")
    
    client = get_client() # Get client instance
    
    # Datalog rule for ancestry (needed to find modifications in nested blocks)
    ancestor_rule = """[
        [(ancestor ?b ?a) [?a :block/children ?b]]
        [(ancestor ?b ?a) [?p :block/children ?b] (ancestor ?p ?a)]
    ]"""
    
    # Get start of today timestamp (ms)
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_timestamp = int(today_start.timestamp() * 1000)
    
    try:
        logger.debug(f"Querying pages modified since {today_start.isoformat()} ({start_timestamp})")
        # Query finds titles of pages where *any* block under them was edited after start_timestamp
        query = f"""[:find ?title
                    :in $ ?start_timestamp % ; Use % for rules input
                    :where [?page :node/title ?title]
                           (ancestor ?block ?page) ; ?block is descendant of ?page
                           [?block :edit/time ?time]
                           [(> ?time ?start_timestamp)]]"""
                           # No explicit limit in query, Roam might have internal limits.
                           # We limit after fetching all results.

        results = client.query(query, inputs=[start_timestamp, ancestor_rule]) or []
        
        # Extract unique page titles from results [ [title1], [title2], [title1], ... ]
        unique_pages = sorted(list(set([item[0] for item in results if item])))
        
        # Apply limit
        limited_pages = unique_pages[:max_num_pages]
        
        return {
            "success": True,
            "pages": limited_pages,
            "message": f"Found {len(limited_pages)} page(s) modified today (limit {max_num_pages}). Total found: {len(unique_pages)}."
        }
        
    except QueryError as e:
        logger.error(f"Finding pages modified today failed: {e}", exc_info=True)
        return {"success": False, "pages": [], "message": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error finding pages modified today: {e}", exc_info=True)
        return {"success": False, "pages": [], "message": f"Unexpected error: {e}"}


def execute_datomic_query(query: str, inputs: Optional[List[Any]] = None) -> Dict[str, Any]:
    """
    Execute a raw Datomic query provided by the user. Uses RoamClient.
    
    Args:
        query: Datalog query string.
        inputs: Optional list of query inputs.
        
    Returns:
        Dictionary with success status and formatted results.
    """
    if not query:
        return {"success": False, "matches": [], "message": "Query cannot be empty"}
        
    client = get_client() # Get client instance
    
    try:
        # Basic validation - check if it looks like a query
        if not query.strip().startswith("[:find"):
             logger.warning("Provided query does not start with [:find. Executing anyway.")
             # Optionally raise ValidationError here if strict validation desired

        logger.info(f"Executing custom Datomic query.")
        results = client.query(query, inputs) or [] # Use client's query method
        
        # Format results for consistency
        formatted_matches = []
        for result_item in results:
             # Convert result item (could be list, scalar, etc.) to string representation
             if isinstance(result_item, (list, tuple)):
                 content_str = " | ".join(map(str, result_item))
             else:
                 content_str = str(result_item)
                 
             formatted_matches.append({
                 "content": content_str,
                 # No block/page context available from raw query generally
                 "block_uid": "",
                 "page_title": ""
             })

        return {
            "success": True,
            "matches": formatted_matches,
            "message": f"Query executed successfully. Found {len(formatted_matches)} results."
        }
        
    except QueryError as e:
        logger.error(f"Custom Datomic query failed: {e}", exc_info=True)
        return {"success": False, "matches": [], "message": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error executing custom query: {e}", exc_info=True)
        return {"success": False, "matches": [], "message": f"Unexpected error: {e}"}