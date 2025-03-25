import type { 
  RoamCreateBlock,
  RoamCreatePage,
  RoamUpdateBlock,
  RoamDeleteBlock,
  RoamDeletePage,
  RoamMoveBlock
} from '@roam-research/roam-api-sdk';

export type BatchAction = 
  | RoamCreateBlock 
  | RoamCreatePage 
  | RoamUpdateBlock 
  | RoamDeleteBlock 
  | RoamDeletePage 
  | RoamMoveBlock;

interface MarkdownNode {
  content: string;
  level: number;
  heading_level?: number;  // Optional heading level (1-3) for heading nodes
  children: MarkdownNode[];
}

/**
 * Check if text has a traditional markdown table
 */
function hasMarkdownTable(text: string): boolean {
  return /^\|([^|]+\|)+\s*$\n\|(\s*:?-+:?\s*\|)+\s*$\n(\|([^|]+\|)+\s*$\n*)+$/.test(text);
}

/**
 * Converts a markdown table to Roam format
 */
function convertTableToRoamFormat(text: string) {
  const lines = text.split('\n')
    .map(line => line.trim())
    .filter(line => line.length > 0);

  const tableRegex = /^\|([^|]+\|)+\s*$\n\|(\s*:?-+:?\s*\|)+\s*$\n(\|([^|]+\|)+\s*$\n*)+/m;

  if (!tableRegex.test(text)) {
    return text;
  }

  const rows = lines
    .filter((_, index) => index !== 1)
    .map(line => 
      line.trim()
        .replace(/^\||\|$/g, '')
        .split('|')
        .map(cell => cell.trim())
    );

  let roamTable = '{{table}}\n';
  
  // First row becomes column headers
  const headers = rows[0];
  for (let i = 0; i < headers.length; i++) {
    roamTable += `${'  '.repeat(i + 1)}- ${headers[i]}\n`;
  }
  
  // Remaining rows become nested under each column
  for (let rowIndex = 1; rowIndex < rows.length; rowIndex++) {
    const row = rows[rowIndex];
    for (let colIndex = 0; colIndex < row.length; colIndex++) {
      roamTable += `${'  '.repeat(colIndex + 1)}- ${row[colIndex]}\n`;
    }
  }

  return roamTable.trim();
}

function convertAllTables(text: string) {
  return text.replaceAll(
    /(^\|([^|]+\|)+\s*$\n\|(\s*:?-+:?\s*\|)+\s*$\n(\|([^|]+\|)+\s*$\n*)+)/gm,
          (match) => {
      return '\n' + convertTableToRoamFormat(match) + '\n';
          }
        );
      }

/**
 * Parse markdown heading syntax (e.g. "### Heading") and return the heading level (1-3) and content.
 * Heading level is determined by the number of # characters (e.g. # = h1, ## = h2, ### = h3).
 * Returns heading_level: 0 for non-heading content.
 */
function parseMarkdownHeadingLevel(text: string): { heading_level: number; content: string } {
  const match = text.match(/^(#{1,3})\s+(.+)$/);
  if (match) {
    return {
      heading_level: match[1].length,  // Number of # characters determines heading level
      content: match[2].trim()
    };
  }
  return {
    heading_level: 0,  // Not a heading
    content: text.trim()
  };
}

function convertToRoamMarkdown(text: string): string {
  // Handle double asterisks/underscores (bold)
  text = text.replace(/\*\*(.+?)\*\*/g, '**$1**');  // Preserve double asterisks
  
  // Handle single asterisks/underscores (italic)
  text = text.replace(/(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)/g, '__$1__');  // Single asterisk to double underscore
  text = text.replace(/(?<!_)_(?!_)(.+?)(?<!_)_(?!_)/g, '__$1__');        // Single underscore to double underscore
  
  // Handle highlights
  text = text.replace(/==(.+?)==/g, '^^$1^^');
  
  // Convert tasks
  text = text.replace(/- \[ \]/g, '- {{[[TODO]]}}');
  text = text.replace(/- \[x\]/g, '- {{[[DONE]]}}');
  
  // Convert tables
  text = convertAllTables(text);
  
  return text;
}

function parseMarkdown(markdown: string): MarkdownNode[] {
  // Convert markdown syntax first
  markdown = convertToRoamMarkdown(markdown);
  
  const lines = markdown.split('\n');
  const rootNodes: MarkdownNode[] = [];
  const stack: MarkdownNode[] = [];
  let inCodeBlock = false;
  let codeBlockContent = '';
  let codeBlockIndentation = 0;
  let codeBlockParentLevel = 0;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmedLine = line.trimEnd();
    
    // Handle code blocks
    if (trimmedLine.match(/^(\s*)```/)) {
      if (!inCodeBlock) {
        // Start of code block
        inCodeBlock = true;
        // Store the opening backticks without indentation
        codeBlockContent = trimmedLine.trimStart() + '\n';
        codeBlockIndentation = line.match(/^\s*/)?.[0].length ?? 0;
        // Save current parent level
        codeBlockParentLevel = stack.length;
      } else {
        // End of code block
        inCodeBlock = false;
        // Add closing backticks without indentation
        codeBlockContent += trimmedLine.trimStart();
        
        // Process the code block content to fix indentation
        const lines = codeBlockContent.split('\n');
        
        // Find the first non-empty code line to determine base indentation
        let baseIndentation = '';
        let codeStartIndex = -1;
        for (let i = 1; i < lines.length - 1; i++) {
          const line = lines[i];
          if (line.trim().length > 0) {
            const indentMatch = line.match(/^[\t ]*/);
            if (indentMatch) {
              baseIndentation = indentMatch[0];
              codeStartIndex = i;
              break;
            }
          }
        }

        // Process lines maintaining relative indentation from the first code line
        const processedLines = lines.map((line, index) => {
          // Keep backticks as is
          if (index === 0 || index === lines.length - 1) return line.trimStart();
          
          // Empty lines should be completely trimmed
          if (line.trim().length === 0) return '';
          
          // For code lines, remove only the base indentation
          if (line.startsWith(baseIndentation)) {
            return line.slice(baseIndentation.length);
          }
          // If line has less indentation than base, trim all leading whitespace
          return line.trimStart();
        });
        
        // Create node for the entire code block
        const level = Math.floor(codeBlockIndentation / 2);
        const node: MarkdownNode = {
          content: processedLines.join('\n'),
          level,
          children: []
        };

        // Restore to code block's parent level
        while (stack.length > codeBlockParentLevel) {
          stack.pop();
        }
        if (level === 0) {
          rootNodes.push(node);
          stack[0] = node;
        } else {
          while (stack.length > level) {
            stack.pop();
          }
          if (stack[level - 1]) {
            stack[level - 1].children.push(node);
          } else {
            rootNodes.push(node);
          }
          stack[level] = node;
        }
        
        codeBlockContent = '';
      }
      continue;
    }

    if (inCodeBlock) {
      codeBlockContent += line + '\n';
      continue;
    }

    // Skip truly empty lines (no spaces)
    if (trimmedLine === '') {
      continue;
    }

    // Calculate indentation level (2 spaces = 1 level)
    const indentation = line.match(/^\s*/)?.[0].length ?? 0;
    let level = Math.floor(indentation / 2);

    // First check for headings
    const { heading_level, content: headingContent } = parseMarkdownHeadingLevel(trimmedLine);
    
    // Then handle bullet points if not a heading
    let content: string;
    if (heading_level > 0) {
      content = headingContent;  // Use clean heading content without # marks
      level = 0;  // Headings start at root level
      stack.length = 1;  // Reset stack but keep heading as parent
      // Create heading node
      const node: MarkdownNode = {
        content,
        level,
        heading_level,  // Store heading level in node
        children: []
      };
      rootNodes.push(node);
      stack[0] = node;
      continue;  // Skip to next line
    }

    // Handle non-heading content
    const bulletMatch = trimmedLine.match(/^(\s*)[-*+]\s+/);
    if (bulletMatch) {
      // For bullet points, use the bullet's indentation for level
      content = trimmedLine.substring(bulletMatch[0].length);
      level = Math.floor(bulletMatch[1].length / 2);
    } else {
      content = trimmedLine;
    }
    
    // Create regular node
    const node: MarkdownNode = {
      content,
      level,
      children: []
    };

    // Pop stack until we find the parent level
    while (stack.length > level) {
      stack.pop();
    }
    
    // Add to appropriate parent
    if (level === 0 || !stack[level - 1]) {
      rootNodes.push(node);
      stack[0] = node;
    } else {
      stack[level - 1].children.push(node);
    }
    stack[level] = node;
  }

  return rootNodes;
}

function parseTableRows(lines: string[]): MarkdownNode[] {
  const tableNodes: MarkdownNode[] = [];
  let currentLevel = -1;

  for (const line of lines) {
    const trimmedLine = line.trimEnd();
    if (!trimmedLine) continue;

    // Calculate indentation level
    const indentation = line.match(/^\s*/)?.[0].length ?? 0;
    const level = Math.floor(indentation / 2);

    // Extract content after bullet point
    const content = trimmedLine.replace(/^\s*[-*+]\s*/, '');

    // Create node for this cell
    const node: MarkdownNode = {
      content,
      level,
      children: []
    };

    // Track the first level we see to maintain relative nesting
    if (currentLevel === -1) {
      currentLevel = level;
    }

    // Add node to appropriate parent based on level
    if (level === currentLevel) {
      tableNodes.push(node);
    } else {
      // Find parent by walking back through nodes
      let parent = tableNodes[tableNodes.length - 1];
      while (parent && parent.level < level - 1) {
        parent = parent.children[parent.children.length - 1];
      }
      if (parent) {
        parent.children.push(node);
      }
    }
  }

  return tableNodes;
}

function generateBlockUid(): string {
  // Generate a random string of 9 characters (Roam's format)
  const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_';
  let uid = '';
  for (let i = 0; i < 9; i++) {
    uid += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return uid;
}

interface BlockInfo {
  uid: string;
  content: string;
  heading_level?: number;  // Optional heading level (1-3) for heading nodes
  children: BlockInfo[];
}

function convertNodesToBlocks(nodes: MarkdownNode[]): BlockInfo[] {
  return nodes.map(node => ({
    uid: generateBlockUid(),
    content: node.content,
    ...(node.heading_level && { heading_level: node.heading_level }),  // Preserve heading level if present
    children: convertNodesToBlocks(node.children)
  }));
}

function convertToRoamActions(
  nodes: MarkdownNode[], 
  parentUid: string,
  order: 'first' | 'last' | number = 'last'
): BatchAction[] {
  // First convert nodes to blocks with UIDs, reversing to maintain original order
  const blocks = convertNodesToBlocks([...nodes].reverse());
  const actions: BatchAction[] = [];

  // Helper function to recursively create actions
  function createBlockActions(blocks: BlockInfo[], parentUid: string, order: 'first' | 'last' | number): void {
    for (const block of blocks) {
      // Create the current block
      const action: RoamCreateBlock = {
        action: 'create-block',
        location: {
          'parent-uid': parentUid,
          order
        },
        block: {
          uid: block.uid,
          string: block.content,
          ...(block.heading_level && { heading: block.heading_level })
        }
      };
      
      actions.push(action);

      // Create child blocks if any
      if (block.children.length > 0) {
        createBlockActions(block.children, block.uid, 'last');
      }
    }
  }

  // Create all block actions
  createBlockActions(blocks, parentUid, order);
  
  return actions;
}

// Export public functions and types
export {
  parseMarkdown,
  convertToRoamActions,
  hasMarkdownTable,
  convertAllTables,
  convertToRoamMarkdown,
  parseMarkdownHeadingLevel
};
