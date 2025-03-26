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
    
    # Use '-' to remove leading zero on day for compatibility
    return date.strftime(f"%B %-d{suffix}, %Y")


# Regular expressions for markdown elements
MD_BOLD_PATTERN = r'\*\*(.+?)\*\*'
MD_ITALIC_PATTERN = r'(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)'
MD_ITALIC_UNDERSCORE_PATTERN = r'(?<!_)_(?!_)(.+?)(?<!_)_(?!_)'
MD_HIGHLIGHT_PATTERN = r'==(.+?)=='
MD_LINK_PATTERN = r'\[([^\]]+)\]\(([^)]+)\)'
# Match ``` optionally followed by language, then content, then ```
MD_CODE_BLOCK_PATTERN = r'(^|\n)(\s*)```(\w*)\s*\n([\s\S]*?)\n\s*```'
MD_INLINE_CODE_PATTERN = r'`([^`]+)`'

# Table regex patterns - Updated to handle potential whitespace around pipes
MD_TABLE_PATTERN = r'((?:^[ \t]*\|(?:.*\|)+[ \t]*\n)+(?:^[ \t]*\|(?::?-+:?\|)+[ \t]*\n)((?:^[ \t]*\|(?:.*\|)+[ \t]*\n)+))'
MD_TABLE_ROW_PATTERN = r'^[ \t]*\|(.*)\|[ \t]*$'
MD_TABLE_SEPARATOR_PATTERN = r'^[ \t]*\|((?:\s*:?-+:?\s*\|)+)[ \t]*$'
MD_TABLE_ALIGNMENT_PATTERN = r'^\s*(:?)-+(:?)\s*$'  # For detecting alignment in table headers

# Headings pattern
MD_HEADING_PATTERN = r'^(#{1,6})\s+(.+)$'


# --- Enhanced Markdown Conversion Utilities ---

def convert_to_roam_markdown(text: str) -> str:
    """
    Convert standard markdown to Roam-compatible format.
    Includes improved table and code block handling.
    
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
    text = re.sub(MD_HIGHLIGHT_PATTERN, r'^^\1^^', text) # Corrected replacement
    
    # Convert tasks
    text = re.sub(r'- \[ \]', r'- {{[[TODO]]}}', text)
    text = re.sub(r'- \[x\]', r'- {{[[DONE]]}}', text)
    
    # Convert links ([alias](url)) - Roam uses [alias]([[url]]) or similar
    # Basic conversion for now, Roam's linking is complex
    text = re.sub(MD_LINK_PATTERN, r'[\1](\2)', text)
    
    # Handle headings - Roam uses block attributes, so we remove # but store level later
    text = convert_headings(text)
    
    # Handle inline code
    text = re.sub(MD_INLINE_CODE_PATTERN, r'`\1`', text)
    
    return text


def convert_headings(text: str) -> str:
    """
    Remove markdown heading syntax (#) as Roam handles headings via block attributes.
    The heading level is captured during parsing in parse_markdown_list.
    
    Args:
        text: Markdown text with potential headings
        
    Returns:
        Text with heading syntax removed
    """
    lines = text.split('\n')
    processed_lines = []
    for line in lines:
        match = re.match(MD_HEADING_PATTERN, line)
        if match:
            # Keep only the content part
            processed_lines.append(match.group(2).strip())
        else:
            processed_lines.append(line)
    return '\n'.join(processed_lines)


def convert_code_blocks(text: str) -> str:
    """
    Convert markdown code blocks, preserving language and relative indentation.
    
    Args:
        text: Markdown text with potential code blocks
        
    Returns:
        Text with code blocks properly formatted for Roam.
    """
    def code_block_replacer(match: Match) -> str:
        leading_whitespace = match.group(2) or ""
        language = match.group(3).strip()
        code_content = match.group(4)
        
        # Preserve language info
        language_tag = f"{language}" if language else ""
        
        # Clean up indentation within the code block
        lines = code_content.split('\n')
        
        # Find the minimum indentation of non-empty lines
        min_indent = None
        for line in lines:
            if line.strip():
                indent = len(line) - len(line.lstrip(' '))
                if min_indent is None or indent < min_indent:
                    min_indent = indent
        
        if min_indent is None: # Empty code block or only whitespace lines
             min_indent = 0
             
        # Remove the common minimum indentation
        processed_lines = []
        for line in lines:
             if line.strip():
                 processed_lines.append(line[min_indent:])
             else:
                 # Preserve empty lines relative to the block's structure
                 processed_lines.append(line.strip()) # Keep empty lines empty

        cleaned_code_content = '\n'.join(processed_lines)
        
        # Format for Roam: ```language\ncontent```
        # The Roam block containing this should have the outer indentation `leading_whitespace`
        return f"{leading_whitespace}```{language_tag}\n{cleaned_code_content}\n```"

    return re.sub(MD_CODE_BLOCK_PATTERN, code_block_replacer, text)


def convert_tables(text: str) -> str:
    """
    Convert markdown tables to Roam's {{table}} format, respecting alignment.
    
    Args:
        text: Markdown text with potential tables
        
    Returns:
        Text with tables converted to Roam format
    """
    def table_replacer(match: Match) -> str:
        table_block = match.group(1) # The full matched table block
        lines = table_block.strip().split('\n')
        
        # Extract rows that contain pipes
        pipe_lines = [line for line in lines if '|' in line]
        
        if len(pipe_lines) < 2:  # Need at least header and separator
            return table_block # Return original if not a valid table structure

        # Parse header
        header_match = re.match(MD_TABLE_ROW_PATTERN, pipe_lines[0])
        if not header_match: return table_block
        header_cells = [cell.strip() for cell in header_match.group(1).split('|')]
        # Adjust for leading/trailing pipes
        if not header_cells[0]: header_cells.pop(0)
        if not header_cells[-1]: header_cells.pop(-1)

        # Parse separator and detect alignment
        separator_match = re.match(MD_TABLE_SEPARATOR_PATTERN, pipe_lines[1])
        if not separator_match: return table_block
        separator_cells = [cell.strip() for cell in separator_match.group(1).split('|')]
        # Adjust for leading/trailing pipes
        if not separator_cells[0]: separator_cells.pop(0)
        if not separator_cells[-1]: separator_cells.pop(-1)

        alignments = []
        for sep in separator_cells:
            align_match = re.match(MD_TABLE_ALIGNMENT_PATTERN, sep)
            if align_match:
                left_colon = bool(align_match.group(1))
                right_colon = bool(align_match.group(2))
                if left_colon and right_colon: alignments.append("center")
                elif right_colon: alignments.append("right")
                else: alignments.append("left") # Includes :--- and ---
            else:
                alignments.append("left") # Default

        # Ensure alignment list matches header count
        while len(alignments) < len(header_cells): alignments.append("left")
        while len(alignments) > len(header_cells): alignments.pop()

        # Generate Roam table format
        roam_table_lines = ["{{table}}"]
        
        # Process data rows (start from index 2)
        data_rows_cells = []
        for row_line in pipe_lines[2:]:
            row_match = re.match(MD_TABLE_ROW_PATTERN, row_line)
            if row_match:
                cells = [cell.strip() for cell in row_match.group(1).split('|')]
                # Adjust for leading/trailing pipes
                if not cells[0]: cells.pop(0)
                if not cells[-1]: cells.pop(-1)
                # Pad row if it has fewer cells than header
                while len(cells) < len(header_cells): cells.append("")
                data_rows_cells.append(cells[:len(header_cells)]) # Truncate if too many cells

        # Build Roam table string column by column
        for col_idx, header in enumerate(header_cells):
            # Add header block
            roam_table_lines.append(f"  - {header}") # Indent level 1 for header
            # Add data blocks for this column
            for row_cells in data_rows_cells:
                 cell_content = row_cells[col_idx] if col_idx < len(row_cells) else ""
                 # Apply alignment if needed (Roam doesn't directly support alignment this way,
                 # but we could potentially add markers or styles if desired in future)
                 # For now, just add the content.
                 roam_table_lines.append(f"    - {cell_content}") # Indent level 2 for data

        return "\n".join(roam_table_lines)

    # Use re.MULTILINE to match ^ at the start of each line
    return re.sub(MD_TABLE_PATTERN, table_replacer, text, flags=re.MULTILINE)


class MarkdownNode:
    """Class representing a node in the markdown parsing tree."""
    def __init__(self, content: str, level: int = 0, heading_level: int = 0):
        self.content = content
        self.level = level # Represents the intended Roam nesting level
        self.heading_level = heading_level # 1-6 if it was a markdown heading, 0 otherwise
        self.children: List['MarkdownNode'] = [] # Explicit type hint
    
    def add_child(self, node: 'MarkdownNode') -> None:
        """Add a child node to this node."""
        self.children.append(node)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert node to dictionary representation for processing."""
        result = {
            "text": self.content,
            "level": self.level, # Roam nesting level
            "heading_level": self.heading_level
        }
        if self.children:
            result["children"] = [child.to_dict() for child in self.children]
        return result


def parse_markdown_list(markdown: str) -> List[Dict[str, Any]]:
    """
    Parse markdown text (lists, headings, code blocks) into a hierarchical structure suitable for Roam conversion.

    Args:
        markdown: Markdown text.

    Returns:
        List of dictionaries representing the root nodes of the parsed structure.
    """
    # Convert standard markdown elements to Roam equivalents where applicable
    # Note: Headings (# syntax) are handled below by capturing level, not converting text
    processed_markdown = convert_to_roam_markdown(markdown)
    
    lines = processed_markdown.split('\n')
    root_nodes: List[MarkdownNode] = []
    node_stack: List[MarkdownNode] = [] # Stores the parent node at each indentation level
    
    in_code_block = False
    code_block_content = []
    code_block_start_indent = 0 # Indentation level where the code block started

    for line_idx, line in enumerate(lines):
        stripped_line = line.lstrip()
        indentation = len(line) - len(stripped_line)

        # --- Code Block Handling ---
        if stripped_line.startswith("```"):
            if not in_code_block:
                # Start of code block
                in_code_block = True
                code_block_content = [line.strip()] # Store the opening ``` line itself
                code_block_start_indent = indentation
                continue # Move to next line
            else:
                # End of code block
                in_code_block = False
                code_block_content.append(line.strip()) # Store the closing ``` line

                # Calculate Roam level based on the starting indentation
                # Roam uses 2 spaces per level, but block content itself isn't indented in Roam
                roam_level = code_block_start_indent // 2

                # Create node for the entire code block
                full_code_content = "\n".join(code_block_content)
                node = MarkdownNode(full_code_content, level=roam_level)

                # Find the correct parent in the stack based on the Roam level
                while node_stack and node_stack[-1].level >= roam_level:
                    node_stack.pop()
                
                if not node_stack: # Should not happen if root is handled, but safeguard
                    root_nodes.append(node)
                    node_stack = [node] # Reset stack if it became empty
                else:
                    node_stack[-1].add_child(node)
                    node_stack.append(node) # Add code block node itself to stack

                code_block_content = [] # Reset for next block
                continue # Move to next line

        if in_code_block:
            code_block_content.append(line) # Collect lines inside the block
            continue

        # Skip empty lines when not in a code block
        if not stripped_line:
            continue

        # --- Heading Handling ---
        heading_match = re.match(MD_HEADING_PATTERN, stripped_line)
        if heading_match:
            heading_level = len(heading_match.group(1))
            content = heading_match.group(2).strip()
            # Roam headings are typically top-level blocks (level 0) with a heading attribute
            roam_level = 0

            node = MarkdownNode(content, level=roam_level, heading_level=heading_level)
            
            # Reset stack to root for top-level headings
            node_stack = []
            root_nodes.append(node)
            node_stack.append(node) # Add heading to stack
            continue

        # --- List Item Handling ---
        list_match = re.match(r'^(\s*)(?:[-*+]|[0-9]+\.)\s+(.*)', line) # Matches bullets and numbered lists
        if list_match:
            list_indent_str, content = list_match.groups()
            list_indent = len(list_indent_str)
            # Calculate Roam level based on list indentation (usually 2 spaces per level)
            roam_level = list_indent // 2

            node = MarkdownNode(content, level=roam_level)

            # Find the correct parent node in the stack
            while node_stack and node_stack[-1].level >= roam_level:
                node_stack.pop()

            if not node_stack: # If stack empty, add as root
                root_nodes.append(node)
            else:
                node_stack[-1].add_child(node)
            
            node_stack.append(node) # Add current node to stack for potential children
            continue

        # --- Plain Text Handling (Continuation or New Top-Level) ---
        # If it's not a heading, list item, or code block line, treat as continuation or new root
        content = stripped_line
        if node_stack: # If there's a previous node
             # Check if indentation suggests continuation
             current_parent_level = node_stack[-1].level
             # Arbitrary threshold: if indent is greater than parent level, maybe continuation?
             # Roam typically handles continuation by just appending to the previous block string.
             # This parser creates separate blocks. Let's append to the last block if indentation matches.
             roam_level = indentation // 2
             if roam_level == current_parent_level and not root_nodes: # Append if same level and not a root node itself
                 node_stack[-1].content += "\n" + content
             elif roam_level > current_parent_level: # Assume continuation of the last item
                  node_stack[-1].content += "\n" + line.lstrip() # Keep relative indent
             else: # Treat as a new top-level block (level 0)
                 node = MarkdownNode(content, level=0)
                 root_nodes.append(node)
                 node_stack = [node] # Reset stack to this new root
        else: # No previous node, must be a root node
            node = MarkdownNode(content, level=0)
            root_nodes.append(node)
            node_stack.append(node)

    # Convert the parsed node tree to the desired dictionary list format
    return [node.to_dict() for node in root_nodes]


# --- Other Utilities ---

def convert_roam_dates(text: str) -> str:
    """
    Convert date references (YYYY-MM-DD) to Roam date format ([[Month DaySuffix, Year]]).
    
    Args:
        text: Text with potential date references
        
    Returns:
        Text with dates linked in Roam format
    """
    def replace_date(match: Match) -> str:
        date_str = match.group(0)
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            roam_date_str = format_roam_date(date_obj)
            # Create Roam page link
            return f"[[{roam_date_str}]]"
        except ValueError:
            return date_str # Return original if parsing fails
    
    # Use word boundaries to avoid matching parts of other numbers/strings
    return re.sub(r'\b(\d{4}-\d{2}-\d{2})\b', replace_date, text)


def extract_youtube_video_id(url: str) -> Optional[str]:
    """
    Extract the video ID from various YouTube URL formats.
    
    Args:
        url: YouTube URL
        
    Returns:
        Video ID string or None if not found
    """
    patterns = [
        r"(?:v=|/|embed/|youtu\.be/|/v/|/e/|watch\?v=|\?v=|\&v=)([^#\&\?]{11})", # Combined pattern
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    logger.warning(f"Could not extract YouTube video ID from URL: {url}")
    return None


def create_block_action(parent_uid: str, content: str, order: Union[int, str] = "last", 
                        uid: Optional[str] = None, heading: Optional[int] = None) -> Dict[str, Any]:
    """
    Helper function to create a dictionary representing a 'create-block' action for the Roam API.

    Args:
        parent_uid: The UID of the parent block or page where the new block should be added.
        content: The string content of the new block.
        order: The position of the new block relative to siblings ('first', 'last', or an integer index). Defaults to 'last'.
        uid: Optional specific UID to assign to the new block. If None, Roam generates one.
        heading: Optional heading level (1, 2, or 3) for the block.

    Returns:
        A dictionary formatted for the Roam 'create-block' action.
    """
    block_data: Dict[str, Any] = {
        "string": content
    }
    
    if uid:
        block_data["uid"] = uid
        
    # Roam uses heading levels 1, 2, 3
    if heading and 1 <= heading <= 3:
        block_data["heading"] = heading
    
    action = {
        "action": "create-block",
        "location": {
            "parent-uid": parent_uid,
            "order": order
        },
        "block": block_data
    }
    return action


def process_nested_content(content: List[Dict], parent_uid: str, client) -> List[str]:
    """
    Convert a nested structure (from parse_markdown_list) into Roam blocks using batch actions.

    Args:
        content: List of dictionaries representing the hierarchical content.
        parent_uid: The UID of the Roam block/page to add the content under.
        client: An instance of RoamClient used for API calls.

    Returns:
        A list of UIDs of the top-level blocks created. Returns empty list on failure or no content.
    
    Raises:
        TransactionError: If the batch action fails.
    """
    if not content:
        return []

    batch_actions: List[Dict[str, Any]] = []
    # Use a list to store temporary ID mappings: [(temp_id, action_index)]
    # Map parent temp_id -> list of child action indices
    dependency_map: Dict[str, List[int]] = {}
    # Map action_index -> temp_id
    action_to_temp_id: Dict[int, str] = {}
    temp_id_counter = 0

    def build_actions_recursive(items: List[Dict], current_parent_uid: str):
        nonlocal temp_id_counter
        for i, block_dict in enumerate(items):
            action_index = len(batch_actions)
            
            # Assign a temporary UID for potential children
            temp_id = f"temp_{temp_id_counter}"
            temp_id_counter += 1
            action_to_temp_id[action_index] = temp_id

            action = create_block_action(
                parent_uid=current_parent_uid,
                content=block_dict.get("text", ""),
                # Use 'last' for simplicity within recursion, actual order from list
                order="last",
                # Use temporary UID in the action itself, Roam ignores it but helps tracking? No, Roam needs real UID for parent.
                # uid=temp_id # Roam generates UIDs, cannot pre-assign temp ones this way.
                heading=block_dict.get("heading_level")
            )
            batch_actions.append(action)

            # Record dependency if parent is temporary
            if isinstance(current_parent_uid, str) and current_parent_uid.startswith("temp_"):
                if current_parent_uid not in dependency_map:
                    dependency_map[current_parent_uid] = []
                dependency_map[current_parent_uid].append(action_index)

            # Recursively process children, passing the *temporary* ID of the current block
            if "children" in block_dict:
                build_actions_recursive(block_dict["children"], temp_id)

    # Start building actions from the root items
    build_actions_recursive(content, parent_uid)

    if not batch_actions:
        return []

    # --- Execute batch actions with chunking and TEMP UID resolution ---
    all_created_uids = []
    # Map temp_id -> real_uid
    temp_to_real_uid: Dict[str, str] = {}
    # Map action_index -> real_uid
    action_index_to_real_uid: Dict[int, str] = {}

    chunk_size = 50 # Roam API limit or practical limit
    processed_action_indices = set()

    while len(processed_action_indices) < len(batch_actions):
        chunk_to_send: List[Dict[str, Any]] = []
        chunk_indices: List[int] = [] # Track indices relative to original batch_actions

        # Find actions ready to be processed in this chunk
        for idx, action in enumerate(batch_actions):
            if idx in processed_action_indices:
                continue

            parent_uid = action["location"]["parent-uid"]
            # Check if parent dependency is met (either real UID or resolved temp UID)
            parent_ready = not (isinstance(parent_uid, str) and parent_uid.startswith("temp_") and parent_uid not in temp_to_real_uid)
            
            if parent_ready:
                 # If parent was a temp_id, replace with real UID before sending
                 if isinstance(parent_uid, str) and parent_uid.startswith("temp_"):
                      action["location"]["parent-uid"] = temp_to_real_uid[parent_uid]

                 chunk_to_send.append(action)
                 chunk_indices.append(idx)
                 if len(chunk_to_send) >= chunk_size:
                      break # Process this chunk

        if not chunk_to_send:
            # This indicates a dependency cycle or an error
            logger.error(f"Could not resolve dependencies for remaining actions. Processed: {len(processed_action_indices)}/{len(batch_actions)}")
            raise TransactionError("Batch dependency resolution failed.", "batch-create", {"unresolved_actions": len(batch_actions) - len(processed_action_indices)})

        logger.debug(f"Executing batch chunk with {len(chunk_to_send)} actions.")
        try:
            # Use the client's batch execution method
            # Assuming execute_batch_actions returns {"success": bool, "created_uids": list}
            # Need to modify execute_batch_actions or the client method to handle this call
            # For now, simulate calling the underlying write action
            # result = client.write(chunk_to_send) # Correct way using client
            
            # --- Simplified call for this refactor step ---
            from roam_mcp.api import execute_write_action # Temporary import
            result = execute_write_action(chunk_to_send)
            # --- End simplified call ---

            if not result or not isinstance(result.get("created_uids"), list):
                 raise TransactionError("Batch chunk failed to return valid created_uids.", "batch-create", {"chunk_size": len(chunk_to_send)})

            created_uids_in_chunk = result["created_uids"]
            if len(created_uids_in_chunk) != len(chunk_to_send):
                 logger.warning(f"Mismatch between actions sent ({len(chunk_to_send)}) and UIDs received ({len(created_uids_in_chunk)})")
                 # Attempt to map based on order, might be inaccurate if Roam reorders/fails partially
                 # A more robust solution would need Roam to return mapping info

            # Map temp UIDs to real UIDs based on order
            for i, real_uid in enumerate(created_uids_in_chunk):
                original_action_index = chunk_indices[i]
                temp_id = action_to_temp_id.get(original_action_index)
                if temp_id:
                    temp_to_real_uid[temp_id] = real_uid
                action_index_to_real_uid[original_action_index] = real_uid # Store real UID by index too

            all_created_uids.extend(created_uids_in_chunk)
            processed_action_indices.update(chunk_indices)

        except Exception as e:
            logger.error(f"Batch chunk execution failed: {str(e)}")
            # Add context about which actions were in the failed chunk
            failed_action_details = [{"index": idx, "content": batch_actions[idx]['block']['string'][:50]+"..."} for idx in chunk_indices]
            raise TransactionError(f"Batch chunk failed: {str(e)}", "batch-create", {"failed_chunk_indices": chunk_indices, "details": failed_action_details}) from e

    # Return only the UIDs of the top-level items originally passed in 'content'
    top_level_indices = [i for i, item in enumerate(batch_actions) if item['location']['parent-uid'] == parent_uid]
    top_level_uids = [action_index_to_real_uid[idx] for idx in top_level_indices if idx in action_index_to_real_uid]
    
    return top_level_uids


# --- Block/Page Finding Utilities (Moved from api.py logic) ---

# Note: These still need the 'client' object passed or accessible
# They are placed here temporarily during refactoring. Ideally, they belong in api.py or a dedicated finder module.

def find_block_uid(client, block_content: str) -> str:
    """
    Search for a block by its content to find its UID using the RoamClient.

    Args:
        client: An instance of RoamClient.
        block_content: Content to search for.

    Returns:
        Block UID.

    Raises:
        BlockNotFoundError: If the block cannot be found.
        QueryError: If the query fails.
    """
    # Escape quotes in content for Datalog query
    escaped_content = block_content.replace('"', '\\"')
    
    # Query to pull the UID for a block with the exact string
    search_query = f'''[:find ?uid .
                      :where [?e :block/string "{escaped_content}"]
                             [?e :block/uid ?uid]]'''
    
    try:
        result = client.query(search_query)
        
        if result:
            # Result should be the UID string directly due to `. ` in :find
            if isinstance(result, str) and len(result) == 9:
                 return result
            else:
                 logger.warning(f"Unexpected format from block UID query result: {result}")
                 # Fallback if format is different, e.g., [[{':block/uid': 'UID'}]]
                 if isinstance(result, list) and result and isinstance(result[0], list) and result[0] and isinstance(result[0][0], dict):
                      uid = result[0][0].get(':block/uid')
                      if uid: return uid

        # If exact match fails, try relaxed search (e.g., find recent blocks and compare content)
        logger.debug(f"Exact block match for '{block_content[:50]}...' not found, trying relaxed search.")
        # Query for the 5 most recently created blocks
        time_query = '''[:find ?uid ?string ?time
                        :where [?b :block/string ?string]
                               [?b :block/uid ?uid]
                               [?b :create/time ?time]
                        :order (desc ?time)
                        :limit 5]'''
        recent_blocks = client.query(time_query)

        if recent_blocks and isinstance(recent_blocks, list):
            clean_target_content = block_content.strip()
            for uid, content, _time in recent_blocks:
                if content.strip() == clean_target_content:
                    logger.debug(f"Found block UID via relaxed search: {uid}")
                    return uid

        # If still not found after relaxed search
        raise BlockNotFoundError(f"Content: {block_content[:50]}...")

    except (QueryError, BlockNotFoundError) as e:
        raise # Re-raise specific errors
    except Exception as e:
        logger.error(f"Unexpected error finding block UID for '{block_content[:50]}...': {e}", exc_info=True)
        raise QueryError(f"Failed to find block UID: {e}", search_query) from e


def find_page_by_title(client, title: str) -> Optional[str]:
    """
    Find a page by title using the RoamClient, with case-insensitive fallback.

    Args:
        client: An instance of RoamClient.
        title: Page title to search for.

    Returns:
        Page UID string or None if not found.
        
    Raises:
        QueryError: If the query fails.
    """
    title = title.strip()
    if not title:
        return None

    # 1. Try direct, case-sensitive lookup (most reliable)
    logger.debug(f"Attempting direct lookup for page title: '{title}'")
    direct_query = f'''[:find ?uid .
                       :where [?e :node/title "{title}"]
                              [?e :block/uid ?uid]]'''
    try:
        direct_result = client.query(direct_query)
        if direct_result and isinstance(direct_result, str) and len(direct_result) == 9:
            logger.debug(f"Direct lookup found UID: {direct_result}")
            return direct_result
    except QueryError as e:
         logger.warning(f"Direct page lookup query failed (might be okay if page doesn't exist): {e}")
    except Exception as e:
         logger.error(f"Unexpected error during direct page lookup: {e}", exc_info=True)
         # Don't raise yet, try other methods

    # 2. Check if the input string itself is a valid page UID
    if len(title) == 9 and re.match(r'^[a-zA-Z0-9_-]{9}$', title):
        logger.debug(f"Input '{title}' looks like a UID, verifying if it's a page.")
        uid_verify_query = f'''[:find ?title .
                                :where [?e :block/uid "{title}"]
                                       [?e :node/title ?title]]'''
        try:
            verify_result = client.query(uid_verify_query)
            # If the query returns the title, it's a valid page UID
            if verify_result:
                 logger.debug(f"Input '{title}' confirmed as a page UID.")
                 return title
        except QueryError as e:
             logger.warning(f"Page UID verification query failed: {e}")
        except Exception as e:
             logger.error(f"Unexpected error during page UID verification: {e}", exc_info=True)

    # 3. Case-insensitive fallback: Get all pages and compare titles
    logger.debug(f"Direct/UID lookup failed for '{title}', trying case-insensitive comparison.")
    all_pages_query = '''[:find ?title ?uid
                         :where [?p :node/title ?title]
                                [?p :block/uid ?uid]]'''
    try:
        all_pages_result = client.query(all_pages_query)
        if all_pages_result and isinstance(all_pages_result, list):
            title_lower = title.lower()
            for page_title, uid in all_pages_result:
                if isinstance(page_title, str) and page_title.lower() == title_lower:
                    logger.debug(f"Case-insensitive match found: '{page_title}' with UID {uid}")
                    return uid
    except QueryError as e:
        logger.error(f"Failed to retrieve all pages for case-insensitive check: {e}")
        raise # If we can't get all pages, we can't do the fallback
    except Exception as e:
         logger.error(f"Unexpected error during case-insensitive page check: {e}", exc_info=True)
         raise QueryError("Failed during case-insensitive page lookup", all_pages_query) from e


    logger.debug(f"Page title '{title}' not found by any method.")
    return None


# --- Block Reference Resolution ---

# Keep resolve_block_references here as it depends on find_page_by_title logic (potentially)
# Or move both find_page_by_title and resolve_block_references to api.py/client later.

def resolve_block_references(client, content: str, max_depth: int = 3, current_depth: int = 0) -> str:
    """
    Resolve block references `((uid))` in content recursively using RoamClient.

    Args:
        client: An instance of RoamClient.
        content: Content string with potential block references.
        max_depth: Maximum recursion depth to prevent infinite loops.
        current_depth: Current recursion depth (internal use).

    Returns:
        Content string with block references replaced by their content.
    """
    if current_depth >= max_depth:
        logger.warning(f"Max recursion depth ({max_depth}) reached during block reference resolution.")
        return content # Stop recursion

    ref_pattern = r'\(\(([a-zA-Z0-9_-]{9})\)\)'
    refs_found = re.findall(ref_pattern, content)

    if not refs_found:
        return content # No references to resolve at this level

    resolved_content = content
    processed_refs = set() # Avoid processing the same ref multiple times if it appears repeatedly

    for ref_uid in refs_found:
        if ref_uid in processed_refs:
            continue
        
        logger.debug(f"Resolving block reference: (({ref_uid})) at depth {current_depth}")
        try:
            # Query for the referenced block's string content
            query = f'''[:find ?string .
                        :in $ ?uid
                        :where [?b :block/uid ?uid]
                               [?b :block/string ?string]]'''
            ref_block_content = client.query(query, inputs=[ref_uid])

            if ref_block_content is not None and isinstance(ref_block_content, str):
                # Recursively resolve references within the fetched content
                nested_resolved_content = resolve_block_references(
                    client, ref_block_content, max_depth, current_depth + 1
                )
                
                # Replace all occurrences of this specific ref_uid in the current content block
                placeholder = f"(({ref_uid}))"
                # Use re.escape on placeholder if UIDs could contain regex special chars (unlikely but safe)
                resolved_content = resolved_content.replace(placeholder, nested_resolved_content)
                processed_refs.add(ref_uid)
            else:
                logger.warning(f"Content for block reference (({ref_uid})) not found or invalid.")
                # Optionally replace with a placeholder like "[Content Not Found]"
                # resolved_content = resolved_content.replace(f"(({ref_uid}))", "[Content Not Found]")
                processed_refs.add(ref_uid) # Mark as processed even if not found

        except QueryError as e:
            logger.error(f"Query error resolving block reference (({ref_uid}))): {e}")
            processed_refs.add(ref_uid) # Mark as processed to avoid retrying in this pass
        except Exception as e:
            logger.error(f"Unexpected error resolving block reference (({ref_uid}))): {e}", exc_info=True)
            processed_refs.add(ref_uid) # Mark as processed

    # Safety check: If replacements significantly increased length, log warning (potential runaway recursion?)
    if len(resolved_content) > len(content) * 10 and len(resolved_content) > 1000: # Arbitrary thresholds
         logger.warning(f"Content length increased significantly after resolving refs (depth {current_depth}). Original: {len(content)}, New: {len(resolved_content)}")

    return resolved_content