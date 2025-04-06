"""Memory system operations for the Roam MCP server."""

from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import logging

from roam_mcp.api import (
    execute_query,
    execute_write_action,
    get_session_and_headers,
    GRAPH_NAME,
    get_daily_page,
    add_block_to_page,
    MEMORIES_TAG,
    ValidationError,
    PageNotFoundError,
    QueryError
)
from roam_mcp.utils import (
    format_roam_date,
    resolve_block_references
)

# Set up logging
logger = logging.getLogger("roam-mcp.memory")


def remember(memory: str, categories: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Store a memory with the specified MEMORIES_TAG.
    
    Args:
        memory: The memory to store
        categories: Optional list of categories to tag the memory with
        
    Returns:
        Result with success status
    """
    if not memory:
        return {
            "success": False,
            "error": "Memory cannot be empty"
        }
    
    session, headers = get_session_and_headers()
    
    try:
        # Validate and normalize categories
        normalized_categories = []
        if categories:
            # Ensure all categories are strings
            invalid_categories = [cat for cat in categories if not isinstance(cat, str)]
            if invalid_categories:
                return {
                    "success": False,
                    "error": "All categories must be strings"
                }
            
            # Normalize category formats
            for category in categories:
                category = category.strip()
                if not category:
                    continue
                
                # Remove any existing tag syntax
                clean_category = category.replace('#', '').replace('[[', '').replace(']]', '')
                
                # Add to normalized list
                normalized_categories.append(clean_category)
        
        # Get today's daily page
        daily_page_uid = get_daily_page()
        
        # Format memory with tags
        formatted_memory = MEMORIES_TAG
        
        # Add the memory text
        formatted_memory += f" {memory}"
        
        # Add category tags
        for category in normalized_categories:
            # Format category as Roam tag
            if " " in category or "/" in category:
                tag = f"#[[{category}]]"
            else:
                tag = f"#{category}"
            
            formatted_memory += f" {tag}"
        
        # Create memory block
        block_uid = add_block_to_page(daily_page_uid, formatted_memory)
        
        return {
            "success": True,
            "block_uid": block_uid,
            "content": formatted_memory
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
    except Exception as e:
        logger.error(f"Error storing memory: {str(e)}")
        return {
            "success": False,
            "error": f"Error storing memory: {str(e)}"
        }


def recall(sort_by: str = "newest", filter_tag: Optional[str] = None) -> Dict[str, Any]:
    """
    Recall stored memories, optionally filtered by tag.
    
    Args:
        sort_by: Sort order ("newest" or "oldest")
        filter_tag: Optional tag to filter memories by
        
    Returns:
        List of memory contents
    """
    if sort_by not in ["newest", "oldest"]:
        return {
            "success": False,
            "error": "sort_by must be 'newest' or 'oldest'"
        }
    
    session, headers = get_session_and_headers()
    
    # Clean and normalize the MEMORIES_TAG for queries
    clean_tag = MEMORIES_TAG.replace('#', '').replace('[[', '').replace(']]', '')
    
    # Prepare filter tag conditions if needed
    filter_conditions = ""
    if filter_tag:
        # Clean and normalize filter tag
        clean_filter = filter_tag.replace('#', '').replace('[[', '').replace(']]', '')
        
        # Generate filter tag variants
        filter_variants = []
        if " " in clean_filter or "/" in clean_filter:
            filter_variants = [f"#{clean_filter}", f"#[[{clean_filter}]]", f"[[{clean_filter}]]"]
        else:
            filter_variants = [f"#{clean_filter}", f"#[[{clean_filter}]]", f"[[{clean_filter}]]"]
        
        # Build filter conditions
        filter_conditions_list = []
        for variant in filter_variants:
            filter_conditions_list.append(f'(clojure.string/includes? ?s "{variant}")')
        
        if filter_conditions_list:
            filter_conditions = f" AND (or {' '.join(filter_conditions_list)})"
    
    try:
        logger.debug(f"Recalling memories with sort_by={sort_by}")
        if filter_tag:
            logger.debug(f"Filtering by tag: {filter_tag}")
        
        # Method 1: Search for blocks containing the MEMORIES_TAG across the database
        # Generate tag variants
        tag_variants = []
        if " " in clean_tag or "/" in clean_tag:
            tag_variants = [f"#{clean_tag}", f"#[[{clean_tag}]]", f"[[{clean_tag}]]"]
        else:
            tag_variants = [f"#{clean_tag}", f"#[[{clean_tag}]]", f"[[{clean_tag}]]"]
        
        # Build tag conditions
        tag_conditions = []
        for variant in tag_variants:
            tag_conditions.append(f'(clojure.string/includes? ?s "{variant}")')
        
        tag_condition = f"(or {' '.join(tag_conditions)})"
        
        # Create combined condition with filter if needed
        combined_condition = tag_condition
        if filter_conditions:
            combined_condition = f"(and {tag_condition}{filter_conditions})"
        
        # Query blocks with tag
        tag_query = f"""[:find ?uid ?s ?time ?page-title
                      :where
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :create/time ?time]
                      [?b :block/page ?p]
                      [?p :node/title ?page-title]
                      [{combined_condition}]]"""
        
        tag_results = execute_query(tag_query)
        
        # Method 2: Also check for dedicated page with the clean tag name
        page_query = f"""[:find ?uid ?s ?time
                      :where
                      [?p :node/title "{clean_tag}"]
                      [?b :block/page ?p]
                      [?b :block/string ?s]
                      [?b :block/uid ?uid]
                      [?b :create/time ?time]]"""
        
        # Add filter if needed
        if filter_conditions:
            page_query = f"""[:find ?uid ?s ?time
                          :where
                          [?p :node/title "{clean_tag}"]
                          [?b :block/page ?p]
                          [?b :block/string ?s]
                          [?b :block/uid ?uid]
                          [?b :create/time ?time]
                          [{filter_conditions.replace('AND ', '')}]]"""
        
        page_results = execute_query(page_query)
        
        # Process and combine results
        memories = []
        
        # Process tag results
        for uid, content, time, page_title in tag_results:
            # Resolve references
            resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
            
            memories.append({
                "content": resolved_content,
                "time": time,
                "page_title": page_title,
                "block_uid": uid
            })
        
        # Process page results
        for uid, content, time in page_results:
            # Resolve references
            resolved_content = resolve_block_references(session, headers, GRAPH_NAME, content)
            
            memories.append({
                "content": resolved_content,
                "time": time,
                "page_title": clean_tag,
                "block_uid": uid
            })
        
        # Sort by time
        memories.sort(key=lambda x: x["time"], reverse=(sort_by == "newest"))
        
        # Clean up content - remove the MEMORIES_TAG
        for memory in memories:
            content = memory["content"]
            for variant in tag_variants:
                content = content.replace(variant, "")
            memory["content"] = content.strip()
        
        # Remove duplicates while preserving order
        seen_contents = set()
        unique_memories = []
        
        for memory in memories:
            content = memory["content"]
            if content and content not in seen_contents:
                seen_contents.add(content)
                unique_memories.append(memory)
        
        # Return just the content strings
        memory_contents = [memory["content"] for memory in unique_memories]
        
        return {
            "success": True,
            "memories": memory_contents,
            "message": f"Found {len(memory_contents)} memories"
        }
    except QueryError as e:
        return {
            "success": False,
            "error": str(e)
        }
    except Exception as e:
        logger.error(f"Error recalling memories: {str(e)}")
        return {
            "success": False,
            "error": f"Error recalling memories: {str(e)}"
        }