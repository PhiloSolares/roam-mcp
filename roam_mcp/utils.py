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
MD_TABLE_PATTERN = r'(?:\|(.+)\|\s*\n\|(?::?-+:?\|)+\s*\n(?:\|(?:.+)\|\s*\n)+)'
MD_TABLE_ROW_PATTERN = r'\|(.*)\|'
MD_TABLE_HEADER_PATTERN = r'\|(\s*:?-+:?\s*)\|'
MD_TABLE_ALIGNMENT_PATTERN = r'^(:?)-+(:?)$'  # For detecting alignment in table headers

# Headings pattern
MD_HEADING_PATTERN = r'^(#{1,6})\s+(.+)$'


class MarkdownNode:
    """Enhanced node representation for markdown parsing."""
    
    def __init__(self, content: str, level: int = 0, heading_level: int = 0):
        """
        Initialize a markdown node with improved properties.
        
        Args:
            content: The text content of the node
            level: Indentation level (0 for root)
            heading_level: Optional heading level (1-3)
        """
        self.content = content
        self.level = level
        self.heading_level = heading_level
        self.children = []
        self.attrs = {}  # For additional attributes like alignment, code language
        self.node_type = "normal"  # Can be "normal", "code", "table", "heading"
    
    def add_child(self, node: 'MarkdownNode') -> None:
        """
        Add a child node with proper linking.
        
        Args:
            node: Child node to add
        """
        self.children.append(node)
        
    def to_roam_action(self, parent_uid: str, order: Union[str, int] = "last") -> Dict[str, Any]:
        """
        Convert node to a Roam API action.
        
        Args:
            parent_uid: UID of the parent block
            order: Position in parent's children
            
        Returns:
            Action dictionary for Roam API
        """
        block_data = {
            "string": self.content
        }
        
        if self.heading_level > 0 and self.heading_level <= 3:
            block_data["heading"] = self.heading_level
            
        return {
            "action": "create-block",
            "location": {
                "parent-uid": parent_uid,
                "order": order
            },
            "block": block_data
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert node to dictionary representation.
        
        Returns:
            Dictionary with node properties
        """
        result = {
            "text": self.content,
            "level": self.level
        }
        
        if self.heading_level:
            result["heading_level"] = self.heading_level
            
        if self.attrs:
            result["attrs"] = self.attrs
            
        if self.node_type != "normal":
            result["node_type"] = self.node_type
            
        return result


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


def parse_markdown_content(markdown: str) -> List[MarkdownNode]:
    """
    Parse a markdown string into a hierarchical structure of MarkdownNode objects.
    
    Args:
        markdown: Markdown text to parse
        
    Returns:
        List of top-level MarkdownNode objects with their children
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
    code_block_language = ""
    
    for line_idx, line in enumerate(lines):
        if not line.strip() and not in_code_block:
            continue
            
        # Handle code blocks
        if "```" in line and not in_code_block:
            # Start of code block
            in_code_block = True
            # Extract language if specified
            language_match = re.match(r'^(\s*)```(.*)$', line)
            if language_match:
                code_block_indent = len(language_match.group(1))
                code_block_language = language_match.group(2).strip()
            else:
                code_block_indent = len(line) - len(line.lstrip())
                code_block_language = ""
                
            code_block_content = [line]
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
            node.node_type = "code"
            if code_block_language:
                node.attrs["language"] = code_block_language
            
            # Find the right parent for this node
            while len(node_stack) > 1 and node_stack[-1].level >= level:
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
            node.node_type = "heading"
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
            todo_match = re.search(r'^\s*\{\{\[\[(TODO|DONE)\]\]\}\}\s*(.*)$', content)
            if todo_match:
                status = todo_match.group(1)
                text = todo_match.group(2).strip()
                
                node = MarkdownNode(content, level)
                node.attrs["status"] = status
            else:
                node = MarkdownNode(content, level)
            
            # Pop stack until we find parent level
            while len(node_stack) > 1 and node_stack[-1].level >= level:
                node_stack.pop()
                
            # Add to parent
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
    
    # Return the children of the root node
    return root.children


def parse_markdown_list(markdown: str) -> List[Dict[str, Any]]:
    """
    Parse a markdown list into a hierarchical structure suitable for Roam.
    
    Args:
        markdown: Markdown text with nested lists
        
    Returns:
        List of dictionaries with 'text', 'level', and 'heading_level' keys
    """
    nodes = parse_markdown_content(markdown)
    result = []
    
    def process_node(node: MarkdownNode):
        node_dict = node.to_dict()
        result.append(node_dict)
        
        for child in node.children:
            process_node(child)
    
    for node in nodes:
        process_node(node)
        
    return result


def nodes_to_roam_actions(nodes: List[MarkdownNode], parent_uid: str, order: Union[str, int] = "last") -> List[Dict[str, Any]]:
    """
    Convert MarkdownNode objects to Roam API actions.
    
    Args:
        nodes: List of MarkdownNode objects
        parent_uid: UID of the parent block/page
        order: Position for the blocks
        
    Returns:
        List of Roam API actions
    """
    if not nodes:
        return []
    
    actions = []
    
    # Process top-level nodes first
    temp_uid_map = {}  # Maps node to temporary UID for parent reference
    
    for i, node in enumerate(nodes):
        # Create action for this node
        node_order = order if i == 0 else "last"
        action = node.to_roam_action(parent_uid, node_order)
        actions.append(action)
        
        # Save temporary UID for children to reference
        temp_uid = f"temp_{i}"
        temp_uid_map[node] = temp_uid
        
        # Process children recursively
        if node.children:
            child_actions = process_children(node.children, temp_uid)
            actions.extend(child_actions)
    
    return actions

def process_children(nodes: List[MarkdownNode], parent_temp_uid: str) -> List[Dict[str, Any]]:
    """
    Process child nodes recursively for conversion to Roam actions.
    
    Args:
        nodes: List of child MarkdownNode objects
        parent_temp_uid: Temporary UID of the parent
        
    Returns:
        List of Roam API actions for children
    """
    if not nodes:
        return []
    
    actions = []
    temp_uid_map = {}  # Maps node to temporary UID
    
    for i, node in enumerate(nodes):
        # Create action for this node
        action = node.to_roam_action(parent_temp_uid, "last")
        actions.append(action)
        
        # Save temporary UID for children to reference
        temp_uid = f"{parent_temp_uid}_{i}"
        temp_uid_map[node] = temp_uid
        
        # Process children recursively
        if node.children:
            child_actions = process_children(node.children, temp_uid)
            actions.extend(child_actions)
    
    return actions


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


def prepare_batch_actions(actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Prepare batch actions by optimizing order and resolving dependencies.
    
    Args:
        actions: List of actions to prepare
        
    Returns:
        Optimized list of actions
    """
    if not actions:
        return []
    
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
    
    # Create pages first, then blocks, then updates
    optimized = []
    optimized.extend(action_groups["create-page"])
    
    # Organize create-block actions by dependency
    create_blocks = action_groups["create-block"]
    if create_blocks:
        # Build dependency graph
        dependency_map = {}
        for i, action in enumerate(create_blocks):
            parent_uid = action["location"]["parent-uid"]
            # Track dependency if parent is a temporary UID
            if isinstance(parent_uid, str) and parent_uid.startswith("temp_"):
                if parent_uid not in dependency_map:
                    dependency_map[parent_uid] = []
                dependency_map[parent_uid].append(i)
        
        # Sort create-block actions to respect dependencies
        sorted_blocks = []
        processed = set()
        
        def process_action(idx):
            if idx in processed:
                return
            action = create_blocks[idx]
            parent_uid = action["location"]["parent-uid"]
            # Process parent dependencies first
            if parent_uid.startswith("temp_") and parent_uid in dependency_map:
                for dep_idx in dependency_map[parent_uid]:
                    if dep_idx != idx:  # Avoid circular dependencies
                        process_action(dep_idx)
            sorted_blocks.append(action)
            processed.add(idx)
        
        # Process all actions
        for i in range(len(create_blocks)):
            process_action(i)
            
        optimized.extend(sorted_blocks)
    
    # Add remaining action types
    optimized.extend(action_groups["update-block"])
    optimized.extend(action_groups["delete-block"])
    optimized.extend(action_groups["other"])
    
    return optimized


def process_nested_content(content: List[Dict], parent_uid: str) -> List[str]:
    """
    Recursively process nested content structure and convert to Roam actions.
    
    Args:
        content: List of content items with potential children
        parent_uid: UID of the parent block
        
    Returns:
        List of created block UIDs
    """
    from roam_mcp.api import client
    
    # Convert content dictionaries to MarkdownNode objects
    nodes = []
    for item in content:
        node = MarkdownNode(
            content=item.get("text", ""),
            level=item.get("level", 0),
            heading_level=item.get("heading_level", 0)
        )
        nodes.append(node)
    
    # Generate actions from nodes
    actions = nodes_to_roam_actions(nodes, parent_uid)
    
    # Optimize actions for best performance
    optimized_actions = prepare_batch_actions(actions)
    
    # Execute batch actions
    result = client.execute_batch_actions(optimized_actions)
    
    return result.get("created_uids", [])


def topological_sort(actions: List[Dict[str, Any]], dependency_map: Dict[str, List[int]]) -> List[Dict[str, Any]]:
    """
    Sort actions topologically to respect dependencies.
    
    Args:
        actions: List of actions to sort
        dependency_map: Map of temporary UIDs to action indices that depend on them
        
    Returns:
        Sorted list of actions
    """
    sorted_actions = []
    visited = set()
    temp_visited = set()
    
    def visit(idx):
        if idx in visited:
            return
        if idx in temp_visited:
            # Circular dependency detected
            logger.warning(f"Circular dependency detected in action {idx}")
            return
        
        temp_visited.add(idx)
        
        # Visit dependencies
        action = actions[idx]
        if action["action"] == "create-block":
            parent_uid = action["location"]["parent-uid"]
            if parent_uid.startswith("temp_") and parent_uid in dependency_map:
                for dep_idx in dependency_map[parent_uid]:
                    if dep_idx != idx:  # Avoid self-dependencies
                        visit(dep_idx)
        
        temp_visited.remove(idx)
        visited.add(idx)
        sorted_actions.append(action)
    
    for i in range(len(actions)):
        if i not in visited:
            visit(i)
    
    return sorted_actions


def update_parent_uids(actions: List[Dict[str, Any]], uid_mapping: Dict[str, str]) -> None:
    """
    Update parent UIDs in actions based on mapping.
    
    Args:
        actions: List of actions to update
        uid_mapping: Map of temporary UIDs to real UIDs
    """
    for action in actions:
        if action["action"] == "create-block":
            parent_uid = action["location"]["parent-uid"]
            if parent_uid in uid_mapping:
                action["location"]["parent-uid"] = uid_mapping[parent_uid]


def update_uid_mapping(uid_mapping: Dict[str, str], actions: List[Dict[str, Any]], 
                       created_uids: List[str]) -> None:
    """
    Update UID mapping with newly created UIDs.
    
    Args:
        uid_mapping: Map of temporary UIDs to real UIDs
        actions: List of actions that were executed
        created_uids: List of UIDs that were created
    """
    # Map temporary UIDs to real UIDs
    for i, uid in enumerate(created_uids):
        if i < len(actions):
            action = actions[i]
            if action["action"] == "create-block":
                # Generate temp_uid from action index
                temp_uid = f"temp_{i}"
                uid_mapping[temp_uid] = uid