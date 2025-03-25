"""Utility functions for the Roam MCP server."""

import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Set, Match, Tuple

# Date formatting
def format_roam_date(date: Optional[datetime] = None) -> str:
    """
    Format a date in Roam's preferred format (e.g., "March 25th, 2025").
    
    Args:
        date: The date to format, defaults to today's date
        
    Returns:
        A string in Roam's date format
    """
    if date is None:
        date = datetime.now()
    
    day = date.day
    if 11 <= day <= 13:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    
    return date.strftime(f"%B %-d{suffix}, %Y")


# Markdown conversion utilities
def convert_to_roam_markdown(text: str) -> str:
    """
    Convert standard markdown to Roam-compatible format.
    
    Args:
        text: Standard markdown text
        
    Returns:
        Roam-formatted markdown text
    """
    # Handle double asterisks/underscores (bold)
    text = re.sub(r'\*\*(.+?)\*\*', r'**\1**', text)  # Preserve double asterisks
    
    # Handle single asterisks/underscores (italic)
    text = re.sub(r'(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)', r'__\1__', text)  # Single asterisk to double underscore
    text = re.sub(r'(?<!_)_(?!_)(.+?)(?<!_)_(?!_)', r'__\1__', text)        # Single underscore to double underscore
    
    # Handle highlights
    text = re.sub(r'==(.+?)==', r'^^\\1^^', text)
    
    # Convert tasks
    text = re.sub(r'- \[ \]', r'- {{[[TODO]]}}', text)
    text = re.sub(r'- \[x\]', r'- {{[[DONE]]}}', text)
    
    # Convert links
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'[\1](\2)', text)
    
    return text


def parse_markdown_list(markdown: str) -> List[Dict[str, Any]]:
    """
    Parse a markdown list into a hierarchical structure.
    
    Args:
        markdown: Markdown text with nested lists
        
    Returns:
        List of dictionaries with 'text', 'level', and 'children' keys
    """
    lines = markdown.strip().split('\n')
    result = []
    stack = [{'level': 0, 'children': result}]
    
    for line in lines:
        if not line.strip():
            continue
            
        # Calculate indentation level
        match = re.match(r'^(\s*)[-*+]\s(.+)$', line)
        if not match:
            continue
            
        indent, content = match.groups()
        level = len(indent) // 2 + 1
        
        # Create node
        node = {'text': content, 'level': level, 'children': []}
        
        # Find parent in stack
        while stack[-1]['level'] >= level:
            stack.pop()
            
        # Add to parent's children
        stack[-1]['children'].append(node)
        stack.append(node)
    
    return result


def convert_roam_dates(text: str) -> str:
    """
    Convert date references to Roam date format.
    
    Args:
        text: Text with potential date references
        
    Returns:
        Text with dates in Roam format
    """
    # Convert ISO dates (YYYY-MM-DD)
    def replace_date(match: Match) -> str:
        date_str = match.group(0)
        try:
            date = datetime.strptime(date_str, "%Y-%m-%d")
            return format_roam_date(date)
        except ValueError:
            return date_str
    
    return re.sub(r'\b\d{4}-\d{2}-\d{2}\b', replace_date, text)


def extract_youtube_video_id(url: str) -> Optional[str]:
    """
    Extract the video ID from a YouTube URL.
    
    Args:
        url: YouTube URL
        
    Returns:
        Video ID or None if not found
    """
    patterns = [
        r"(?:youtube\.com\/watch\?v=|youtu\.be\/)([a-zA-Z0-9_-]{11})",
        r"youtube\.com\/embed\/([a-zA-Z0-9_-]{11})",
        r"youtube\.com\/v\/([a-zA-Z0-9_-]{11})",
        r"youtube\.com\/user\/[^\/]+\/\?v=([a-zA-Z0-9_-]{11})"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None


def process_nested_content(content: List[Dict], parent_uid: str, session, headers, graph_name: str) -> List[str]:
    """
    Recursively process nested content structure and create blocks.
    
    Args:
        content: List of content items with potential children
        parent_uid: UID of the parent block
        session: Active session for API requests
        headers: Request headers with authentication
        graph_name: Roam graph name
        
    Returns:
        List of created block UIDs
    """
    created_uids = []
    
    for i, block in enumerate(content):
        block_data = {
            "action": "create-block",
            "location": {
                "parent-uid": parent_uid,
                "order": i
            },
            "block": {
                "string": block["text"],
                **({"heading": block.get("heading_level", 0)} if block.get("heading_level") else {})
            }
        }
        
        response = session.post(
            f'https://api.roamresearch.com/api/graph/{graph_name}/write',
            headers=headers,
            json=block_data
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to create block: {response.text}")
        
        # Get the UID of the newly created block
        new_block_uid = find_block_uid(session, headers, graph_name, block["text"])
        created_uids.append(new_block_uid)
        
        # Process children if present
        if block.get("children"):
            child_uids = process_nested_content(
                block["children"],
                new_block_uid,
                session, 
                headers, 
                graph_name
            )
            created_uids.extend(child_uids)
    
    return created_uids


def find_block_uid(session, headers, graph_name: str, block_content: str) -> str:
    """
    Search for a block by its content to find its UID.
    
    Args:
        session: Active session for API requests
        headers: Request headers with authentication
        graph_name: Roam graph name
        block_content: Content to search for
        
    Returns:
        Block UID
    """
    # Escape quotes in content
    escaped_content = block_content.replace('"', '\\"')
    
    search_query = f'''[:find (pull ?e [:block/uid])
                      :where [?e :block/string "{escaped_content}"]]'''
    
    search_response = session.post(
        f'https://api.roamresearch.com/api/graph/{graph_name}/q',
        headers=headers,
        json={"query": search_query}
    )
    
    if search_response.status_code == 200 and search_response.json().get('result'):
        block_uid = search_response.json()['result'][0][0][':block/uid']
        return block_uid
    else:
        raise Exception("Failed to find the block UID.")


def find_page_by_title(session, headers, graph_name: str, title: str) -> Optional[str]:
    """
    Find a page by title, with case-insensitive matching.
    
    Args:
        session: Active session for API requests
        headers: Request headers with authentication
        graph_name: Roam graph name
        title: Page title to search for
        
    Returns:
        Page UID or None if not found
    """
    # Check exact match first
    query = f'''[:find ?uid .
                :where [?e :node/title "{title}"]
                        [?e :block/uid ?uid]]'''
    
    response = session.post(
        f'https://api.roamresearch.com/api/graph/{graph_name}/q',
        headers=headers,
        json={"query": query}
    )
    
    if response.status_code == 200 and response.json().get('result'):
        return response.json()['result']
    
    # Try case-insensitive match
    query = f'''[:find ?title ?uid
                :where [?e :node/title ?title]
                        [?e :block/uid ?uid]]'''
    
    response = session.post(
        f'https://api.roamresearch.com/api/graph/{graph_name}/q',
        headers=headers,
        json={"query": query}
    )
    
    if response.status_code == 200 and response.json().get('result'):
        for result_title, uid in response.json()['result']:
            if result_title.lower() == title.lower():
                return uid
    
    return None


def resolve_block_references(session, headers, graph_name: str, content: str, max_depth: int = 3, current_depth: int = 0) -> str:
    """
    Resolve block references in content recursively.
    
    Args:
        session: Active session for API requests
        headers: Request headers with authentication
        graph_name: Roam graph name
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
    
    # Get content for each referenced block
    for ref in refs:
        query = f'''[:find ?string .
                    :where [?b :block/uid "{ref}"]
                            [?b :block/string ?string]]'''
        
        response = session.post(
            f'https://api.roamresearch.com/api/graph/{graph_name}/q',
            headers=headers,
            json={"query": query}
        )
        
        if response.status_code == 200 and response.json().get('result'):
            ref_content = response.json()['result']
            # Recursively resolve references in the referenced content
            resolved_ref = resolve_block_references(
                session, headers, graph_name, 
                ref_content, max_depth, current_depth + 1
            )
            # Replace reference with content
            content = content.replace(f"(({ref}))", resolved_ref)
    
    return content