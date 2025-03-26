"""Memory system operations for the Roam MCP server."""

from typing import Dict, List, Any, Optional, Union
from datetime import datetime
import logging

# Use client from api module
from roam_mcp.api import (
    get_client, # Import function to get client instance
    get_daily_page,
    add_block_to_page,
    get_memories_tag, # Get tag via function
    ValidationError,
    PageNotFoundError,
    QueryError
)
# Import utils needed here
from roam_mcp.utils import (
    format_roam_date,
    resolve_block_references as resolve_block_references_util # Rename to avoid conflict
)

# Set up logging
logger = logging.getLogger("roam-mcp.memory")


def remember(memory: str, categories: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Store a memory with the specified MEMORIES_TAG using the RoamClient.
    
    Args:
        memory: The memory text to store.
        categories: Optional list of category strings to add as tags.
        
    Returns:
        Dictionary with success status, block UID, and final content.
    """
    if not memory:
        return {"success": False, "error": "Memory cannot be empty"}
    
    client = get_client() # Get client instance
    memories_tag_val = get_memories_tag() # Get the configured tag

    try:
        # Validate and normalize categories
        normalized_categories = []
        if categories:
            if not all(isinstance(cat, str) for cat in categories):
                raise ValidationError("All categories must be strings.", "categories")
            
            for category in categories:
                clean_category = category.replace('#', '').replace('[[', '').replace(']]', '').strip()
                if clean_category:
                    normalized_categories.append(clean_category)
        
        # Get today's daily page UID (uses client internally)
        daily_page_uid = get_daily_page()
        
        # Format memory content with tags
        # Start with the main memory tag
        formatted_memory = memories_tag_val
        # Add the memory text itself
        formatted_memory += f" {memory.strip()}"
        # Add category tags
        for category in normalized_categories:
            # Format as Roam tag (#[[Multi Word]] or #tag)
            tag = f"#[[{category}]]" if (" " in category or "/" in category) else f"#{category}"
            formatted_memory += f" {tag}"
        
        # Create the memory block (uses client internally)
        block_uid = add_block_to_page(daily_page_uid, formatted_memory.strip())
        
        logger.info(f"Stored memory with UID: {block_uid}")
        return {
            "success": True,
            "block_uid": block_uid,
            "content": formatted_memory.strip()
        }
        
    except (ValidationError, PageNotFoundError, QueryError) as e: # Catch relevant errors from helpers
        logger.error(f"Error storing memory: {e}", exc_info=True)
        return {"success": False, "error": str(e)}
    except Exception as e: # Catch unexpected errors
        logger.error(f"Unexpected error storing memory: {e}", exc_info=True)
        return {"success": False, "error": f"Unexpected error: {e}"}


def recall(sort_by: str = "newest", filter_tag: Optional[str] = None) -> Dict[str, Any]:
    """
    Recall stored memories from MEMORIES_TAG blocks or page. Uses RoamClient.
    
    Args:
        sort_by: Sort order ('newest' or 'oldest').
        filter_tag: Optional tag string to filter memories by.
        
    Returns:
        Dictionary with success status and list of recalled memory contents.
    """
    if sort_by not in ["newest", "oldest"]:
        return {"success": False, "error": "sort_by must be 'newest' or 'oldest'"}
    
    client = get_client() # Get client instance
    memories_tag_val = get_memories_tag() # Get the configured tag
    # Clean the main tag for use in queries and content stripping
    clean_memories_tag = memories_tag_val.replace('#', '').replace('[[', '').replace(']]', '').strip()

    # Prepare filter tag condition (Datalog fragment)
    filter_condition_str = ""
    clean_filter_tag = ""
    if filter_tag:
        clean_filter_tag = filter_tag.replace('#', '').replace('[[', '').replace(']]', '').strip()
        if clean_filter_tag:
             # Create variants #tag, #[[tag]], [[tag]] for matching in block string
             filter_variants = [f"#{clean_filter_tag}", f"#[[{clean_filter_tag}]]", f"[[{clean_filter_tag}]]"]
             conditions = [f'(clojure.string/includes? ?s "{variant}")' for variant in filter_variants]
             filter_condition_str = f"(or {' '.join(conditions)})"
             logger.debug(f"Filtering recall by tag: '{clean_filter_tag}' using condition: {filter_condition_str}")
        else:
             logger.warning("Filter tag provided but was empty after cleaning.")

    # Prepare MEMORIES_TAG condition (Datalog fragment)
    memories_tag_variants = [f"#{clean_memories_tag}", f"#[[{clean_memories_tag}]]", f"[[{clean_memories_tag}]]"]
    memories_tag_conditions = [f'(clojure.string/includes? ?s "{variant}")' for variant in memories_tag_variants]
    memories_tag_condition_str = f"(or {' '.join(memories_tag_conditions)})"

    # Combine main tag condition and optional filter condition
    combined_condition = memories_tag_condition_str
    if filter_condition_str:
        combined_condition = f"(and {memories_tag_condition_str} {filter_condition_str})"

    try:
        logger.info(f"Recalling memories (Sort: {sort_by}, Filter: {filter_tag or 'None'})")
        
        # 1. Query for blocks across the graph containing the MEMORIES_TAG (and filter tag if specified)
        logger.debug("Querying tagged blocks globally...")
        global_query = f"""[:find ?uid ?s ?time ?page-title
                           :where [?b :block/string ?s] [?b :block/uid ?uid]
                                  [?b :create/time ?time] [?b :block/page ?p] [?p :node/title ?page-title]
                                  [{combined_condition}]]"""
        global_results = client.query(global_query) or []

        # 2. Query for blocks on a potential dedicated page named after the clean MEMORIES_TAG
        logger.debug(f"Querying blocks on dedicated page '{clean_memories_tag}'...")
        page_query_condition = filter_condition_str if filter_condition_str else "[?b :block/string ?s]" # Use filter if present, else match any block string
        page_query = f"""[:find ?uid ?s ?time
                         :in $ ?page_title
                         :where [?p :node/title ?page_title] [?b :block/page ?p]
                                [?b :block/string ?s] [?b :block/uid ?uid] [?b :create/time ?time]
                                [{page_query_condition}]]"""
        page_results = client.query(page_query, inputs=[clean_memories_tag]) or []

        # --- Process and combine results ---
        all_memories = []
        
        # Process global tag results
        for uid, content, time, page_title in global_results:
             # Resolve references needed before cleaning
             resolved_content = resolve_block_references_util(client, content)
             all_memories.append({"uid": uid, "content": resolved_content, "time": time, "source": "global_tag"})
             
        # Process dedicated page results
        for uid, content, time in page_results:
             resolved_content = resolve_block_references_util(client, content)
             all_memories.append({"uid": uid, "content": resolved_content, "time": time, "source": "page_block"})
             
        logger.debug(f"Found {len(global_results)} global matches and {len(page_results)} page matches initially.")

        # Sort combined list by time
        all_memories.sort(key=lambda x: x["time"], reverse=(sort_by == "newest"))
        
        # Deduplicate based on resolved content and clean the content
        unique_cleaned_memories = []
        seen_cleaned_content = set()
        
        for memory_data in all_memories:
            cleaned_content = memory_data["content"]
            # Remove all variants of the MEMORIES_TAG from the content
            for variant in memories_tag_variants:
                 cleaned_content = cleaned_content.replace(variant, "")
            cleaned_content = cleaned_content.strip() # Remove leading/trailing whitespace

            if cleaned_content and cleaned_content not in seen_cleaned_content:
                seen_cleaned_content.add(cleaned_content)
                unique_cleaned_memories.append(cleaned_content)
                
        logger.info(f"Recalled {len(unique_cleaned_memories)} unique memories.")
        return {
            "success": True,
            "memories": unique_cleaned_memories,
            "message": f"Found {len(unique_cleaned_memories)} unique memories"
        }
        
    except QueryError as e:
        logger.error(f"Error recalling memories: {e}", exc_info=True)
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error(f"Unexpected error recalling memories: {e}", exc_info=True)
        return {"success": False, "error": f"Unexpected error: {e}"}