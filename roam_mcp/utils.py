"""Utility functions for the Roam MCP server."""

import re
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Set, Match, Tuple, Union
import json

# Set up logging
logger = logging.getLogger("roam-mcp.utils")

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


# Regular expressions for markdown elements
MD_BOLD_PATTERN = r'\*\*(.+?)\*\*'
MD_ITALIC_PATTERN = r'(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)'
MD_ITALIC_UNDERSCORE_PATTERN = r'(?<!_)_(?!_)(.+?)(?<!_)_(?!_)'
MD_HIGHLIGHT_PATTERN = r'==(.+?)=='
MD_LINK_PATTERN = r'\[([^\]]+)\]\(([^)]+)\)'
MD_CODE_BLOCK_PATTERN = r'```([a-zA-Z0-9]*)\s*\n([\s\S]*?)```'
MD_INLINE_CODE_PATTERN = r'`([^`]+)`'

# Table regex patterns
MD_TABLE_PATTERN = r'(?:\|(.+)\|\s*\n\|(?::?-+:?\|)+\s*\n(?:\|(?:.+)\|\s*\n*)+)'
MD_TABLE_ROW_PATTERN = r'\|(.*)\|'
MD_TABLE_HEADER_PATTERN = r'\|(\s*:?-+:?\s*)\|'
MD_TABLE_ALIGNMENT_PATTERN = r'^(:?)-+(:?)$'  # For detecting alignment in table headers

# Headings pattern
MD_HEADING_PATTERN = r'^(#{1,6})\s+(.+)$'


# Markdown conversion utilities
def convert_to_roam_markdown(text: str) -> str:
    """
    Convert standard markdown to Roam-compatible format.
    
    Args:
        text: Standard markdown text
        
    Returns:
        Roam-formatted markdown text
    """
    # Convert tables first (they may contain other markdown elements)
    text = convert_tables(text)
    
    # Handle code blocks (must be done before other inline elements)
    text = convert_code_blocks(text)
    
    # Handle double asterisks/underscores (bold)
    text = re.sub(MD_BOLD_PATTERN, r'**\1**', text)
    
    # Handle single asterisks/underscores (italic)
    text = re.sub(MD_ITALIC_PATTERN, r'__\1__', text)  # Single asterisk to double underscore
    text = re.sub(MD_ITALIC_UNDERSCORE_PATTERN, r'__\1__', text)  # Single underscore to double underscore
    
    # Handle highlights
    text = re.sub(MD_HIGHLIGHT_PATTERN, r'^^\\1^^', text)
    
    # Convert tasks
    text = re.sub(r'- \[ \]', r'- {{[[TODO]]}}', text)
    text = re.sub(r'- \[x\]', r'- {{[[DONE]]}}', text)
    
    # Convert links
    text = re.sub(MD_LINK_PATTERN, r'[\1](\2)', text)
    
    # Handle headings (convert to Roam's heading format)
    text = convert_headings(text)
    
    # Handle inline code
    text = re.sub(MD_INLINE_CODE_PATTERN, r'`\1`', text)
    
    return text


def convert_headings(text: str) -> str:
    """
    Convert markdown headings to Roam's heading format.
    
    Args:
        text: Markdown text with potential headings
        
    Returns:
        Text with headings converted to Roam format
    """
    def heading_replacer(match: Match) -> str:
        level = len(match.group(1))  # Number of # characters
        content = match.group(2).strip()
        
        # For text format, we'll just keep the heading text and let block attributes 
        # handle the actual heading level in Roam
        return content
    
    # Process line by line to avoid matching # in code blocks
    lines = text.split('\n')
    for i, line in enumerate(lines):
        heading_match = re.match(MD_HEADING_PATTERN, line)
        if heading_match:
            lines[i] = heading_replacer(heading_match)
    
    return '\n'.join(lines)


def convert_code_blocks(text: str) -> str:
    """
    Convert markdown code blocks while preserving language and indentation.
    
    Args:
        text: Markdown text with potential code blocks
        
    Returns:
        Text with code blocks properly formatted
    """
    def code_block_replacer(match: Match) -> str:
        language = match.group(1).strip()
        code_content = match.group(2)
        
        # Preserve language info
        language_tag = f"{language}\n" if language else "\n"
        
        # Clean up indentation
        lines = code_content.split('\n')
        # Find the common indentation level
        non_empty_lines = [line for line in lines if line.strip()]
        if non_empty_lines:
            common_indent = min(len(line) - len(line.lstrip()) for line in non_empty_lines)
            # Remove common indentation
            code_content = '\n'.join(line[common_indent:] if line.strip() else line for line in lines)
        
        return f"```{language_tag}{code_content}```"
    
    return re.sub(MD_CODE_BLOCK_PATTERN, code_block_replacer, text)


def convert_tables(text: str) -> str:
    """
    Convert markdown tables to Roam format.
    
    Args:
        text: Markdown text with potential tables
        
    Returns:
        Text with tables converted to Roam format
    """
    def table_replacer(match: Match) -> str:
        table_text = match.group(0)
        
        # Find all rows
        rows = re.findall(MD_TABLE_ROW_PATTERN, table_text)
        if len(rows) < 2:  # Need at least header and separator
            return table_text
        
        # First row is header, second is separator, rest are data
        header_cells = [cell.strip() for cell in rows[0].split('|') if cell.strip()]
        separator_cells = [cell.strip() for cell in rows[1].split('|') if cell.strip()]
        
        # Determine column alignments from separator row
        alignments = []
        for sep in separator_cells:
            alignment_match = re.match(MD_TABLE_ALIGNMENT_PATTERN, sep)
            if alignment_match:
                left_colon = bool(alignment_match.group(1))
                right_colon = bool(alignment_match.group(2))
                
                if left_colon and right_colon:
                    alignments.append("center")
                elif right_colon:
                    alignments.append("right")
                else:
                    alignments.append("left")
            else:
                alignments.append("left")  # Default alignment
        
        # Generate Roam table format
        roam_table = "{{table}}\n"
        
        # Add header row
        for i, header in enumerate(header_cells):
            indent = "  " * (i + 1)
            roam_table += f"{indent}- {header}\n"
        
        # Add data rows - start from index 2 to skip header and separator
        for row_idx in range(2, len(rows)):
            data_cells = [cell.strip() for cell in rows[row_idx].split('|') if cell.strip()]
            
            for i, cell in enumerate(data_cells):
                if i < len(header_cells):  # Only process cells that have a corresponding header
                    indent = "  " * (i + 1)
                    roam_table += f"{indent}- {cell}\n"
        
        return roam_table
    
    return re.sub(MD_TABLE_PATTERN, table_replacer, text)


class MarkdownNode:
    """Class representing a node in the markdown parsing tree."""
    def __init__(self, content: str, level: int = 0, heading_level: int = 0):
        self.content = content
        self.level = level
        self.heading_level = heading_level
        self.children = []
    
    def add_child(self, node: 'MarkdownNode') -> None:
        """Add a child node to this node."""
        self.children.append(node)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert node to dictionary representation."""
        result = {
            "text": self.content,
            "level": self.level
        }
        
        if self.heading_level:
            result["heading_level"] = self.heading_level
            
        if self.children:
            result["children"] = [child.to_dict() for child in self.children]
            
        return result
        

def parse_markdown_list(markdown: str) -> List[Dict[str, Any]]:
    """
    Parse a markdown list into a hierarchical structure.
    
    Args:
        markdown: Markdown text with nested lists
        
    Returns:
        List of dictionaries with 'text', 'level', and 'children' keys
    """
    # Convert markdown syntax first
    markdown = convert_to_roam_markdown(markdown)
    
    lines = markdown.split('\n')
    root = MarkdownNode("ROOT", -1)  # Root node to hold all top-level items
    node_stack = [root]
    current_level = -1
    in_code_block = False
    code_block_content = []
    code_block_indent = 0
    
    for line_idx, line in enumerate(lines):
        if not line.strip() and not in_code_block:
            continue
            
        # Handle code blocks
        if "```" in line and not in_code_block:
            # Start of code block
            in_code_block = True
            code_block_content = [line]
            # Store the indentation level
            code_block_indent = len(line) - len(line.lstrip())
            continue
        elif "```" in line and in_code_block:
            # End of code block - process the entire block
            code_block_content.append(line)
            
            # Calculate the level based on indentation
            level = code_block_indent // 2
            
            # Join the content with proper line breaks
            content = "\n".join(code_block_content)
            
            # Create a node for the code block
            node = MarkdownNode(content, level)
            
            # Find the right parent for this node
            while node_stack[-1].level >= level:
                node_stack.pop()
                
            # Add to parent
            node_stack[-1].add_child(node)
            
            # Update stack and level
            node_stack.append(node)
            current_level = level
            
            # Reset code block state
            in_code_block = False
            code_block_content = []
            continue
        elif in_code_block:
            # In a code block - just collect the line
            code_block_content.append(line)
            continue
            
        # Check for heading
        heading_match = re.match(MD_HEADING_PATTERN, line)
        if heading_match:
            level = 0  # Headings are top-level
            heading_text = heading_match.group(2).strip()
            heading_level = len(heading_match.group(1))  # Number of # characters
            
            # Reset stack for headings
            while len(node_stack) > 1:
                node_stack.pop()
                
            # Create heading node
            node = MarkdownNode(heading_text, level, heading_level)
            node_stack[-1].add_child(node)
            node_stack.append(node)
            current_level = level
            continue
            
        # Regular list items
        match = re.match(r'^(\s*)[-*+]\s+(.+)$', line)
        if match:
            indent, content = match.groups()
            level = len(indent) // 2 + 1  # Convert indentation to level, starting with 1
            
            # Check for TODO/DONE
            if "{{[[TODO]]}}" in content or "{{[[DONE]]}}" in content:
                level_to_append = level
            else:
                level_to_append = level
            
            # Pop stack until we find parent level
            while node_stack[-1].level >= level:
                node_stack.pop()
                
            # Create new node
            node = MarkdownNode(content, level_to_append)
            node_stack[-1].add_child(node)
            node_stack.append(node)
            current_level = level
        else:
            # Non-list line - treat as continuation of previous item or as top-level text
            content = line.strip()
            if content and current_level >= 0:
                # Add to the current node's content
                node_stack[-1].content += "\n" + content
            elif content:
                # Create as top-level text
                node = MarkdownNode(content, 0)
                node_stack[0].add_child(node)
                node_stack = [root, node]
                current_level = 0
    
    # Convert the tree to the expected dictionary format
    result = []
    for node in root.children:
        def process_node(node, result_list):
            node_dict = node.to_dict()
            
            # Handle children recursively
            children = node_dict.pop("children", [])
            result_list.append(node_dict)
            
            # Process children
            for child in children:
                process_node(MarkdownNode(
                    child["text"], 
                    child.get("level", 0),
                    child.get("heading_level", 0)
                ), result_list)
            
        process_node(node, result)
    
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


def create_block_action(parent_uid: str, content: str, order: Union[int, str] = "last", 
                        uid: Optional[str] = None, heading: Optional[int] = None) -> Dict[str, Any]:
    """
    Create a block action for batch operations.
    
    Args:
        parent_uid: UID of the parent block/page
        content: Block content
        order: Position of the block
        uid: Optional UID for the block
        heading: Optional heading level (1-3)
        
    Returns:
        Block action dictionary
    """
    block_data = {
        "string": content
    }
    
    if uid:
        block_data["uid"] = uid
        
    if heading and heading > 0 and heading <= 3:
        block_data["heading"] = heading
    
    return {
        "action": "create-block",
        "location": {
            "parent-uid": parent_uid,
            "order": order
        },
        "block": block_data
    }


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
    from roam_mcp.api import execute_batch_actions  # Import here to avoid circular imports
    
    created_uids = []
    batch_actions = []
    
    # First pass: create actions for all blocks in the hierarchy
    def build_actions(items, parent_uid, index_start=0):
        action_map = {}  # Maps item index to action index in batch_actions
        
        for i, block in enumerate(items, start=index_start):
            # Extract heading level if present
            heading_level = block.get("heading_level", 0)
            
            action = create_block_action(
                parent_uid=parent_uid,
                content=block["text"],
                order=i,
                heading=heading_level
            )
            
            batch_actions.append(action)
            action_map[i] = len(batch_actions) - 1
            
            # If the block has children, process them
            children = block.get("children", [])
            if children:
                # Children will be processed after we get UIDs for parents
                action_map.update(build_actions(children, f"TEMP_PARENT_{i}", len(action_map)))
                
        return action_map
    
    # Build initial actions
    action_map = build_actions(content, parent_uid)
    
    # If there are no actions, return empty list
    if not batch_actions:
        return []
    
    # Execute actions in batches of 50
    BATCH_SIZE = 50
    for i in range(0, len(batch_actions), BATCH_SIZE):
        chunk = batch_actions[i:i + BATCH_SIZE]
        
        # Execute batch
        response = session.post(
            f'https://api.roamresearch.com/api/graph/{graph_name}/write',
            headers=headers,
            json={"action": "batch-actions", "actions": chunk}
        )
        
        if response.status_code != 200:
            logger.error(f"Failed to create batch: {response.text}")
            raise Exception(f"Failed to create blocks: {response.text}")
        
        # Get UIDs of created blocks
        result = response.json()
        if "created_uids" in result:
            created_uids.extend(result["created_uids"])
    
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
        try:
            block_uid = search_response.json()['result'][0][0][':block/uid']
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
            
            time_response = session.post(
                f'https://api.roamresearch.com/api/graph/{graph_name}/q',
                headers=headers,
                json={"query": time_query}
            )
            
            if time_response.status_code == 200 and time_response.json().get('result'):
                # Check if any of these recent blocks match our content
                clean_content = block_content.strip()
                for uid, content, time in time_response.json()['result']:
                    if content.strip() == clean_content:
                        return uid
            
            logger.error("Could not find block UID with relaxed search")
            raise Exception("Failed to find the block UID even with relaxed search")
        except Exception as e:
            logger.error(f"Error in relaxed block search: {str(e)}")
            raise Exception(f"Failed to find the block UID: {str(e)}")


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
    # Clean up the title
    title = title.strip()
    
    # First try direct page lookup (more reliable than case-insensitive queries in Roam)
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
    
    # If not found, try checking if it's a UID
    if len(title) == 9 and re.match(r'^[a-zA-Z0-9_-]{9}$', title):
        # This looks like a UID, check if it's a valid page UID
        uid_query = f'''[:find ?title .
                        :where [?e :block/uid "{title}"]
                                [?e :node/title ?title]]'''
        
        uid_response = session.post(
            f'https://api.roamresearch.com/api/graph/{graph_name}/q',
            headers=headers,
            json={"query": uid_query}
        )
        
        if uid_response.status_code == 200 and uid_response.json().get('result'):
            return title
    
    # If still not found, try case-insensitive match by getting all pages
    all_pages_query = f'''[:find ?title ?uid
                         :where [?e :node/title ?title]
                                 [?e :block/uid ?uid]]'''
    
    all_pages_response = session.post(
        f'https://api.roamresearch.com/api/graph/{graph_name}/q',
        headers=headers,
        json={"query": all_pages_query}
    )
    
    if all_pages_response.status_code == 200 and all_pages_response.json().get('result'):
        for page_title, uid in all_pages_response.json()['result']:
            if page_title.lower() == title.lower():
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
    
    # For each reference, get its content
    for ref in refs:
        try:
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
                
                # Recursively resolve nested references
                resolved_ref = resolve_block_references(
                    session, headers, graph_name, 
                    ref_content, max_depth, current_depth + 1
                )
                
                # Replace reference with content
                content = content.replace(f"(({ref}))", resolved_ref)
        except Exception as e:
            logger.warning(f"Failed to resolve reference (({ref})): {str(e)}")
    
    return content