import { Graph, q, createPage, createBlock, batchActions } from '@roam-research/roam-api-sdk';
import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';
import { formatRoamDate } from '../../utils/helpers.js';
import { capitalizeWords } from '../helpers/text.js';
import { 
  parseMarkdown, 
  convertToRoamActions,
  convertToRoamMarkdown,
  hasMarkdownTable,
  type BatchAction 
} from '../../markdown-utils.js';
import type { OutlineItem } from '../types/index.js';

export class OutlineOperations {
  constructor(private graph: Graph) {}

  async createOutline(
    outline: Array<OutlineItem>,
    page_title_uid?: string,
    block_text_uid?: string
  ) {
    // Validate input
    if (!Array.isArray(outline) || outline.length === 0) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        'outline must be a non-empty array'
      );
    }

    // Filter out items with undefined text
    const validOutline = outline.filter(item => item.text !== undefined);
    if (validOutline.length === 0) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        'outline must contain at least one item with text'
      );
    }

    // Validate outline structure
    const invalidItems = validOutline.filter(item => 
      typeof item.level !== 'number' || 
      item.level < 1 || 
      item.level > 10 ||
      typeof item.text !== 'string' ||
      item.text.trim().length === 0
    );
    
    if (invalidItems.length > 0) {
      throw new McpError(
        ErrorCode.InvalidRequest,
        'outline contains invalid items - each item must have a level (1-10) and non-empty text'
      );
    }

    // Helper function to find or create page with retries
    const findOrCreatePage = async (titleOrUid: string, maxRetries = 3, delayMs = 500): Promise<string> => {
      // First try to find by title
      const titleQuery = `[:find ?uid :in $ ?title :where [?e :node/title ?title] [?e :block/uid ?uid]]`;
      const variations = [
        titleOrUid, // Original
        capitalizeWords(titleOrUid), // Each word capitalized
        titleOrUid.toLowerCase() // All lowercase
      ];

      for (let retry = 0; retry < maxRetries; retry++) {
        // Try each case variation
        for (const variation of variations) {
          const findResults = await q(this.graph, titleQuery, [variation]) as [string][];
          if (findResults && findResults.length > 0) {
            return findResults[0][0];
          }
        }

        // If not found as title, try as UID
        const uidQuery = `[:find ?uid
                          :where [?e :block/uid "${titleOrUid}"]
                                 [?e :block/uid ?uid]]`;
        const uidResult = await q(this.graph, uidQuery, []);
        if (uidResult && uidResult.length > 0) {
          return uidResult[0][0];
        }

        // If still not found and this is the first retry, try to create the page
        if (retry === 0) {
          const success = await createPage(this.graph, {
            action: 'create-page',
            page: { title: titleOrUid }
          });

          // Even if createPage returns false, the page might still have been created
          // Wait a bit and continue to next retry
          await new Promise(resolve => setTimeout(resolve, delayMs));
          continue;
        }

        if (retry < maxRetries - 1) {
          await new Promise(resolve => setTimeout(resolve, delayMs));
        }
      }

      throw new McpError(
        ErrorCode.InvalidRequest,
        `Failed to find or create page "${titleOrUid}" after multiple attempts`
      );
    };

    // Get or create the target page
    const targetPageUid = await findOrCreatePage(
      page_title_uid || formatRoamDate(new Date())
    );

    // Helper function to find block with improved relationship checks
    const findBlockWithRetry = async (pageUid: string, blockString: string, maxRetries = 5, initialDelay = 1000): Promise<string> => {
      // Try multiple query strategies
      const queries = [
        // Strategy 1: Direct page and string match
        `[:find ?b-uid ?order
          :where [?p :block/uid "${pageUid}"]
                 [?b :block/page ?p]
                 [?b :block/string "${blockString}"]
                 [?b :block/order ?order]
                 [?b :block/uid ?b-uid]]`,
        
        // Strategy 2: Parent-child relationship
        `[:find ?b-uid ?order
          :where [?p :block/uid "${pageUid}"]
                 [?b :block/parents ?p]
                 [?b :block/string "${blockString}"]
                 [?b :block/order ?order]
                 [?b :block/uid ?b-uid]]`,
        
        // Strategy 3: Broader page relationship
        `[:find ?b-uid ?order
          :where [?p :block/uid "${pageUid}"]
                 [?b :block/page ?page]
                 [?p :block/page ?page]
                 [?b :block/string "${blockString}"]
                 [?b :block/order ?order]
                 [?b :block/uid ?b-uid]]`
      ];

      for (let retry = 0; retry < maxRetries; retry++) {
        // Try each query strategy
        for (const queryStr of queries) {
          const blockResults = await q(this.graph, queryStr, []) as [string, number][];
          if (blockResults && blockResults.length > 0) {
            // Use the most recently created block
            const sorted = blockResults.sort((a, b) => b[1] - a[1]);
            return sorted[0][0];
          }
        }

        // Exponential backoff
        const delay = initialDelay * Math.pow(2, retry);
        await new Promise(resolve => setTimeout(resolve, delay));
        
        console.log(`Retry ${retry + 1}/${maxRetries} finding block "${blockString}" under "${pageUid}"`);
      }

      throw new McpError(
        ErrorCode.InternalError,
        `Failed to find block "${blockString}" under page "${pageUid}" after trying multiple strategies`
      );
    };

    // Helper function to create and verify block with improved error handling
    const createAndVerifyBlock = async (
      content: string,
      parentUid: string,
      maxRetries = 5,
      initialDelay = 1000,
      isRetry = false
    ): Promise<string> => {
      try {
        // Initial delay before any operations
        if (!isRetry) {
          await new Promise(resolve => setTimeout(resolve, initialDelay));
        }

        for (let retry = 0; retry < maxRetries; retry++) {
          console.log(`Attempt ${retry + 1}/${maxRetries} to create block "${content}" under "${parentUid}"`);

          // Create block
          const success = await createBlock(this.graph, {
            action: 'create-block',
            location: {
              'parent-uid': parentUid,
              order: 'last'
            },
            block: { string: content }
          });

          // Wait with exponential backoff
          const delay = initialDelay * Math.pow(2, retry);
          await new Promise(resolve => setTimeout(resolve, delay));

          try {
            // Try to find the block using our improved findBlockWithRetry
            return await findBlockWithRetry(parentUid, content);
          } catch (error: any) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            console.log(`Failed to find block on attempt ${retry + 1}: ${errorMessage}`);
            if (retry === maxRetries - 1) throw error;
          }
        }

        throw new McpError(
          ErrorCode.InternalError,
          `Failed to create and verify block "${content}" after ${maxRetries} attempts`
        );
      } catch (error) {
        // If this is already a retry, throw the error
        if (isRetry) throw error;

        // Otherwise, try one more time with a clean slate
        console.log(`Retrying block creation for "${content}" with fresh attempt`);
        await new Promise(resolve => setTimeout(resolve, initialDelay * 2));
        return createAndVerifyBlock(content, parentUid, maxRetries, initialDelay, true);
      }
    };

    // Helper function to check if string is a valid Roam UID (9 characters)
    const isValidUid = (str: string): boolean => {
      return typeof str === 'string' && str.length === 9;
    };

    // Get or create the parent block
    let targetParentUid: string;
    if (!block_text_uid) {
      targetParentUid = targetPageUid;
    } else {
      try {
        if (isValidUid(block_text_uid)) {
          // First try to find block by UID
          const uidQuery = `[:find ?uid
                           :where [?e :block/uid "${block_text_uid}"]
                                  [?e :block/uid ?uid]]`;
          const uidResult = await q(this.graph, uidQuery, []) as [string][];
          
          if (uidResult && uidResult.length > 0) {
            // Use existing block if found
            targetParentUid = uidResult[0][0];
          } else {
            throw new McpError(
              ErrorCode.InvalidRequest,
              `Block with UID "${block_text_uid}" not found`
            );
          }
        } else {
          // Create header block and get its UID if not a valid UID
          targetParentUid = await createAndVerifyBlock(block_text_uid, targetPageUid);
        }
      } catch (error: any) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        throw new McpError(
          ErrorCode.InternalError,
          `Failed to ${isValidUid(block_text_uid) ? 'find' : 'create'} block "${block_text_uid}": ${errorMessage}`
        );
      }
    }

    // Initialize result variable
    let result;

    try {
      // Validate level sequence
      let prevLevel = 0;
      for (const item of validOutline) {
        // Level should not increase by more than 1 at a time
        if (item.level > prevLevel + 1) {
          throw new McpError(
            ErrorCode.InvalidRequest,
            `Invalid outline structure - level ${item.level} follows level ${prevLevel}`
          );
        }
        prevLevel = item.level;
      }

      // Convert outline items to markdown-like structure
      const markdownContent = validOutline
        .map(item => {
          const indent = '  '.repeat(item.level - 1);
          return `${indent}- ${item.text?.trim()}`;
        })
        .join('\n');

      // Convert to Roam markdown format
      const convertedContent = convertToRoamMarkdown(markdownContent);

      // Parse markdown into hierarchical structure
      const nodes = parseMarkdown(convertedContent);

      // Convert nodes to batch actions
      const actions = convertToRoamActions(nodes, targetParentUid, 'first');

      if (actions.length === 0) {
        throw new McpError(
          ErrorCode.InvalidRequest,
          'No valid actions generated from outline'
        );
      }

      // Execute batch actions to create the outline
      result = await batchActions(this.graph, {
        action: 'batch-actions',
        actions
      }).catch(error => {
        throw new McpError(
          ErrorCode.InternalError,
          `Failed to create outline blocks: ${error.message}`
        );
      });

      if (!result) {
        throw new McpError(
          ErrorCode.InternalError,
          'Failed to create outline blocks - no result returned'
        );
      }
    } catch (error: any) {
      if (error instanceof McpError) throw error;
      throw new McpError(
        ErrorCode.InternalError,
        `Failed to create outline: ${error.message}`
      );
    }

    // Get the created block UIDs
    const createdUids = result?.created_uids || [];
    
    return {
      success: true,
      page_uid: targetPageUid,
      parent_uid: targetParentUid,
      created_uids: createdUids
    };
  }

  async importMarkdown(
    content: string,
    page_uid?: string,
    page_title?: string,
    parent_uid?: string,
    parent_string?: string,
    order: 'first' | 'last' = 'first'
  ): Promise<{ success: boolean; page_uid: string; parent_uid: string; created_uids?: string[] }> {
    // First get the page UID
    let targetPageUid = page_uid;
    
    if (!targetPageUid && page_title) {
      const findQuery = `[:find ?uid :in $ ?title :where [?e :node/title ?title] [?e :block/uid ?uid]]`;
      const findResults = await q(this.graph, findQuery, [page_title]) as [string][];
      
      if (findResults && findResults.length > 0) {
        targetPageUid = findResults[0][0];
      } else {
        throw new McpError(
          ErrorCode.InvalidRequest,
          `Page with title "${page_title}" not found`
        );
      }
    }

    // If no page specified, use today's date page
    if (!targetPageUid) {
      const today = new Date();
      const dateStr = formatRoamDate(today);
      
      const findQuery = `[:find ?uid :in $ ?title :where [?e :node/title ?title] [?e :block/uid ?uid]]`;
      const findResults = await q(this.graph, findQuery, [dateStr]) as [string][];
      
      if (findResults && findResults.length > 0) {
        targetPageUid = findResults[0][0];
      } else {
        // Create today's page
        try {
          await createPage(this.graph, {
            action: 'create-page',
            page: { title: dateStr }
          });

          const results = await q(this.graph, findQuery, [dateStr]) as [string][];
          if (!results || results.length === 0) {
            throw new McpError(
              ErrorCode.InternalError,
              'Could not find created today\'s page'
            );
          }
          targetPageUid = results[0][0];
        } catch (error) {
          throw new McpError(
            ErrorCode.InternalError,
            `Failed to create today's page: ${error instanceof Error ? error.message : String(error)}`
          );
        }
      }
    }

    // Now get the parent block UID
    let targetParentUid = parent_uid;

    if (!targetParentUid && parent_string) {
      if (!targetPageUid) {
        throw new McpError(
          ErrorCode.InvalidRequest,
          'Must provide either page_uid or page_title when using parent_string'
        );
      }

      // Find block by exact string match within the page
      const findBlockQuery = `[:find ?uid
                             :where [?p :block/uid "${targetPageUid}"]
                                    [?b :block/page ?p]
                                    [?b :block/string "${parent_string}"]]`;
      const blockResults = await q(this.graph, findBlockQuery, []) as [string][];
      
      if (!blockResults || blockResults.length === 0) {
        throw new McpError(
          ErrorCode.InvalidRequest,
          `Block with content "${parent_string}" not found on specified page`
        );
      }
      
      targetParentUid = blockResults[0][0];
    }

    // If no parent specified, use page as parent
    if (!targetParentUid) {
      targetParentUid = targetPageUid;
    }

    // Always use parseMarkdown for content with multiple lines or any markdown formatting
    const isMultilined = content.includes('\n');
    
    if (isMultilined) {
      // Parse markdown into hierarchical structure
      const convertedContent = convertToRoamMarkdown(content);
      const nodes = parseMarkdown(convertedContent);

      // Convert markdown nodes to batch actions
      const actions = convertToRoamActions(nodes, targetParentUid, order);

      // Execute batch actions to add content
      const result = await batchActions(this.graph, {
        action: 'batch-actions',
        actions
      });

      if (!result) {
        throw new McpError(
          ErrorCode.InternalError,
          'Failed to import nested markdown content'
        );
      }

      // Get the created block UIDs
      const createdUids = result.created_uids || [];
      
      return { 
        success: true,
        page_uid: targetPageUid,
        parent_uid: targetParentUid,
        created_uids: createdUids
      };
    } else {
      // Create a simple block for non-nested content
      try {
        await createBlock(this.graph, {
          action: 'create-block',
          location: { 
            "parent-uid": targetParentUid,
            order
          },
          block: { string: content }
        });
      } catch (error) {
        throw new McpError(
          ErrorCode.InternalError,
          'Failed to create content block'
        );
      }

      return { 
        success: true,
        page_uid: targetPageUid,
        parent_uid: targetParentUid
      };
    }
  }
}
