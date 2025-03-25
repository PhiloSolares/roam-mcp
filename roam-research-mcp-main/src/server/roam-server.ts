import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ErrorCode,
  ListToolsRequestSchema,
  McpError,
} from '@modelcontextprotocol/sdk/types.js';
import { initializeGraph, type Graph } from '@roam-research/roam-api-sdk';
import { API_TOKEN, GRAPH_NAME } from '../config/environment.js';
import { toolSchemas } from '../tools/schemas.js';
import { ToolHandlers } from '../tools/tool-handlers.js';

export class RoamServer {
  private server: Server;
  private toolHandlers: ToolHandlers;
  private graph: Graph;

  constructor() {
    this.graph = initializeGraph({
      token: API_TOKEN,
      graph: GRAPH_NAME,
    });

    this.toolHandlers = new ToolHandlers(this.graph);
    
    this.server = new Server(
      {
        name: 'roam-research',
        version: '0.24.6',
      },
      {
          capabilities: {
            tools: {
              roam_remember: {},
              roam_recall: {},
              roam_add_todo: {},
              roam_fetch_page_by_title: {},
              roam_create_page: {},
              roam_create_block: {},
              roam_import_markdown: {},
              roam_create_outline: {},
              roam_search_for_tag: {},
              roam_search_by_status: {},
              roam_search_block_refs: {},
              roam_search_hierarchy: {},
              roam_find_pages_modified_today: {},
              roam_search_by_text: {},
              roam_update_block: {},
              roam_update_multiple_blocks: {},
              roam_search_by_date: {},
              roam_datomic_query: {}
            },
          },
      }
    );

    this.setupRequestHandlers();
    
    // Error handling
    this.server.onerror = (error) => { /* handle error silently */ };
    process.on('SIGINT', async () => {
      await this.server.close();
      process.exit(0);
    });
  }

  private setupRequestHandlers() {
    // List available tools
    this.server.setRequestHandler(ListToolsRequestSchema, async () => ({
      tools: Object.values(toolSchemas),
    }));

    // Handle tool calls
    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      try {
        switch (request.params.name) {
          case 'roam_remember': {
            const { memory, categories } = request.params.arguments as {
              memory: string;
              categories?: string[];
            };
            const result = await this.toolHandlers.remember(memory, categories);
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_fetch_page_by_title': {
            const { title } = request.params.arguments as { title: string };
            const content = await this.toolHandlers.fetchPageByTitle(title);
            return {
              content: [{ type: 'text', text: content }],
            };
          }

          case 'roam_create_page': {
            const { title, content } = request.params.arguments as { 
              title: string; 
              content?: Array<{
                text: string;
                level: number;
              }>;
            };
            const result = await this.toolHandlers.createPage(title, content);
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_create_block': {
            const { content, page_uid, title } = request.params.arguments as {
              content: string;
              page_uid?: string;
              title?: string;
            };
            const result = await this.toolHandlers.createBlock(content, page_uid, title);
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_import_markdown': {
            const { 
              content,
              page_uid,
              page_title,
              parent_uid,
              parent_string,
              order = 'first'
            } = request.params.arguments as {
              content: string;
              page_uid?: string;
              page_title?: string;
              parent_uid?: string;
              parent_string?: string;
              order?: 'first' | 'last';
            };
            const result = await this.toolHandlers.importMarkdown(
              content,
              page_uid,
              page_title,
              parent_uid,
              parent_string,
              order
            );
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_add_todo': {
            const { todos } = request.params.arguments as { todos: string[] };
            const result = await this.toolHandlers.addTodos(todos);
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_create_outline': {
            const { outline, page_title_uid, block_text_uid } = request.params.arguments as {
              outline: Array<{text: string | undefined; level: number}>;
              page_title_uid?: string;
              block_text_uid?: string;
            };
            const result = await this.toolHandlers.createOutline(
              outline,
              page_title_uid,
              block_text_uid
            );
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_search_for_tag': {
            const { primary_tag, page_title_uid, near_tag } = request.params.arguments as {
              primary_tag: string;
              page_title_uid?: string;
              near_tag?: string;
            };
            const result = await this.toolHandlers.searchForTag(primary_tag, page_title_uid, near_tag);
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_search_by_status': {
            const { status, page_title_uid, include, exclude } = request.params.arguments as {
              status: 'TODO' | 'DONE';
              page_title_uid?: string;
              include?: string;
              exclude?: string;
            };
            const result = await this.toolHandlers.searchByStatus(status, page_title_uid, include, exclude);
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_search_block_refs': {
            const params = request.params.arguments as {
              block_uid?: string;
              page_title_uid?: string;
            };
            const result = await this.toolHandlers.searchBlockRefs(params);
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_search_hierarchy': {
            const params = request.params.arguments as {
              parent_uid?: string;
              child_uid?: string;
              page_title_uid?: string;
              max_depth?: number;
            };
            const result = await this.toolHandlers.searchHierarchy(params);
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_find_pages_modified_today': {
            const { max_num_pages } = request.params.arguments as {
              max_num_pages?: number;
            };
            const result = await this.toolHandlers.findPagesModifiedToday(max_num_pages || 50);
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_search_by_text': {
            const params = request.params.arguments as {
              text: string;
              page_title_uid?: string;
            };
            const result = await this.toolHandlers.searchByText(params);
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_search_by_date': {
            const params = request.params.arguments as {
              start_date: string;
              end_date?: string;
              type: 'created' | 'modified' | 'both';
              scope: 'blocks' | 'pages' | 'both';
              include_content: boolean;
            };
            const result = await this.toolHandlers.searchByDate(params);
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_update_block': {
            const { block_uid, content, transform_pattern } = request.params.arguments as {
              block_uid: string;
              content?: string;
              transform_pattern?: {
                find: string;
                replace: string;
                global?: boolean;
              };
            };

            let result;
            if (content) {
              result = await this.toolHandlers.updateBlock(block_uid, content);
            } else if (transform_pattern) {
              result = await this.toolHandlers.updateBlock(
                block_uid,
                undefined,
                (currentContent: string) => {
                  const regex = new RegExp(transform_pattern.find, transform_pattern.global !== false ? 'g' : '');
                  return currentContent.replace(regex, transform_pattern.replace);
                }
              );
            }
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_recall': {
            const { sort_by = 'newest', filter_tag } = request.params.arguments as {
              sort_by?: 'newest' | 'oldest';
              filter_tag?: string;
            };
            const result = await this.toolHandlers.recall(sort_by, filter_tag);
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_update_multiple_blocks': {
            const { updates } = request.params.arguments as {
              updates: Array<{
                block_uid: string;
                content?: string;
                transform?: {
                  find: string;
                  replace: string;
                  global?: boolean;
                };
              }>;
            };
            
            const result = await this.toolHandlers.updateBlocks(updates);
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          case 'roam_datomic_query': {
            const { query, inputs } = request.params.arguments as {
              query: string;
              inputs?: unknown[];
            };
            const result = await this.toolHandlers.executeDatomicQuery({ query, inputs });
            return {
              content: [{ type: 'text', text: JSON.stringify(result, null, 2) }],
            };
          }

          default:
            throw new McpError(
              ErrorCode.MethodNotFound,
              `Unknown tool: ${request.params.name}`
            );
        }
      } catch (error: unknown) {
        if (error instanceof McpError) {
          throw error;
        }
        const errorMessage = error instanceof Error ? error.message : String(error);
        throw new McpError(
          ErrorCode.InternalError,
          `Roam API error: ${errorMessage}`
        );
      }
    });
  }

  async run() {
    const transport = new StdioServerTransport();
    await this.server.connect(transport);
  }
}
