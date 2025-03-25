"""Memory system operations for the Roam MCP server."""

from typing import Dict, List, Any, Optional, Union
from datetime import datetime

from roam_mcp.api import (
    execute_query,
    get_session_and_headers,
    GRAPH_NAME,
    get_daily_page,
    add_block_to_page,
    MEMORIES_TAG
)
from roam_mcp.utils import (
    format_roam_date,
    resolve_block_references
)


def remember(memory: str, categories: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Store a memory with the specified MEMORIES_TAG.
    
    Args:
        memory: The memory to store
        categories: Optional list of categories to tag the memory with
        
    Returns:
        Result with success status
    """
    session, headers = get_session_and_headers()
    
    try:
        # Get today's daily page
        daily_page_uid = get_daily_page()
        
        # Format memory with tags
        formatted_memory = MEMORIES_TAG
        
        # Add the memory text
        formatted_memory += f" {memory}"
        
        # Add category tags
        if categories:
            for category in categories:
                # Format category as Roam tag
                if " " in category:
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
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
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
    session, headers = get_session_and_headers()
    
    # Clean the MEMORIES_TAG
    clean_tag = MEMORIES_TAG.replace('#', '').replace('[[', '').replace(']]', '')
    
    try:
        # Method 1: Search for blocks containing the tag
        # This could be blocks anywhere in the graph
        tag_condition = f'(clojure.string/includes? ?s "{MEMORIES_TAG}")'
        
        query = f"""[:find ?uid ?s ?time ?page-title
                  :where
                  [?b :block/string ?s]
                  [?b :block/uid ?uid]
                  [?b :block/page ?p]
                  [?p :node/title ?page-title]
                  [?b :create/time ?time]
                  [{tag_condition}]]"""
        
        tag_results = execute_query(query)
        
        # Method 2: Check for dedicated page with the clean tag name
        # This would be blocks directly on the memories page
        query = f"""[:find ?uid ?s ?time
                  :where
                  [?p :node/title "{clean_tag}"]
                  [?b :block/page ?p]
                  [?b :block/string ?s]
                  [?b :block/uid ?uid]
                  [?b :create/time ?time]]"""
        
        page_results = execute_query(query)
        
        # Process results
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
        
        # Filter by tag if specified
        if filter_tag:
            clean_filter = filter_tag.replace('#', '').replace('[[', '').replace(']]', '')
            
            if " " in clean_filter:
                filter_variants = [f"#{clean_filter}", f"#[[{clean_filter}]]", f"[[{clean_filter}]]"]
            else:
                filter_variants = [f"#{clean_filter}", f"#[[{clean_filter}]]", f"[[{clean_filter}]]"]
            
            filtered_memories = []
            for memory in memories:
                for variant in filter_variants:
                    if variant in memory["content"]:
                        filtered_memories.append(memory)
                        break
            
            memories = filtered_memories
        
        # Clean up content - remove the MEMORIES_TAG
        for memory in memories:
            content = memory["content"]
            memory["content"] = content.replace(MEMORIES_TAG, "").strip()
        
        # Remove duplicates
        seen_contents = set()
        unique_memories = []
        
        for memory in memories:
            content = memory["content"]
            if content not in seen_contents:
                seen_contents.add(content)
                unique_memories.append(memory)
        
        return {
            "success": True,
            "memories": [m["content"] for m in unique_memories],
            "message": f"Found {len(unique_memories)} memories"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }