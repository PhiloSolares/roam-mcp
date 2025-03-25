import { Graph } from '@roam-research/roam-api-sdk';
import { PageOperations } from './operations/pages.js';
import { BlockOperations } from './operations/blocks.js';
import { SearchOperations } from './operations/search/index.js';
import { MemoryOperations } from './operations/memory.js';
import { TodoOperations } from './operations/todos.js';
import { OutlineOperations } from './operations/outline.js';
import { DatomicSearchHandlerImpl } from './operations/search/handlers.js';

export class ToolHandlers {
  private pageOps: PageOperations;
  private blockOps: BlockOperations;
  private searchOps: SearchOperations;
  private memoryOps: MemoryOperations;
  private todoOps: TodoOperations;
  private outlineOps: OutlineOperations;

  constructor(private graph: Graph) {
    this.pageOps = new PageOperations(graph);
    this.blockOps = new BlockOperations(graph);
    this.searchOps = new SearchOperations(graph);
    this.memoryOps = new MemoryOperations(graph);
    this.todoOps = new TodoOperations(graph);
    this.outlineOps = new OutlineOperations(graph);
  }

  // Page Operations
  async findPagesModifiedToday(max_num_pages: number = 50) {
    return this.pageOps.findPagesModifiedToday(max_num_pages);
  }

  async createPage(title: string, content?: Array<{text: string; level: number}>) {
    return this.pageOps.createPage(title, content);
  }

  async fetchPageByTitle(title: string) {
    return this.pageOps.fetchPageByTitle(title);
  }

  // Block Operations
  async createBlock(content: string, page_uid?: string, title?: string) {
    return this.blockOps.createBlock(content, page_uid, title);
  }

  async updateBlock(block_uid: string, content?: string, transform?: (currentContent: string) => string) {
    return this.blockOps.updateBlock(block_uid, content, transform);
  }

  async updateBlocks(updates: Array<{
    block_uid: string;
    content?: string;
    transform?: { find: string; replace: string; global?: boolean };
  }>) {
    return this.blockOps.updateBlocks(updates);
  }

  // Search Operations
  async searchByStatus(
    status: 'TODO' | 'DONE',
    page_title_uid?: string,
    include?: string,
    exclude?: string
  ) {
    return this.searchOps.searchByStatus(status, page_title_uid, include, exclude);
  }

  async searchForTag(
    primary_tag: string,
    page_title_uid?: string,
    near_tag?: string
  ) {
    return this.searchOps.searchForTag(primary_tag, page_title_uid, near_tag);
  }

  async searchBlockRefs(params: { block_uid?: string; page_title_uid?: string }) {
    return this.searchOps.searchBlockRefs(params);
  }

  async searchHierarchy(params: { 
    parent_uid?: string;
    child_uid?: string;
    page_title_uid?: string;
    max_depth?: number;
  }) {
    return this.searchOps.searchHierarchy(params);
  }

  async searchByText(params: {
    text: string;
    page_title_uid?: string;
  }) {
    return this.searchOps.searchByText(params);
  }

  async searchByDate(params: {
    start_date: string;
    end_date?: string;
    type: 'created' | 'modified' | 'both';
    scope: 'blocks' | 'pages' | 'both';
    include_content: boolean;
  }) {
    return this.searchOps.searchByDate(params);
  }

  // Datomic query
  async executeDatomicQuery(params: { query: string; inputs?: unknown[] }) {
    const handler = new DatomicSearchHandlerImpl(this.graph, params);
    return handler.execute();
  }

  // Memory Operations
  async remember(memory: string, categories?: string[]) {
    return this.memoryOps.remember(memory, categories);
  }

  async recall(sort_by: 'newest' | 'oldest' = 'newest', filter_tag?: string) {
    return this.memoryOps.recall(sort_by, filter_tag);
  }

  // Todo Operations
  async addTodos(todos: string[]) {
    return this.todoOps.addTodos(todos);
  }

  // Outline Operations
  async createOutline(outline: Array<{text: string | undefined; level: number}>, page_title_uid?: string, block_text_uid?: string) {
    return this.outlineOps.createOutline(outline, page_title_uid, block_text_uid);
  }

  async importMarkdown(
    content: string,
    page_uid?: string,
    page_title?: string,
    parent_uid?: string,
    parent_string?: string,
    order: 'first' | 'last' = 'first'
  ) {
    return this.outlineOps.importMarkdown(content, page_uid, page_title, parent_uid, parent_string, order);
  }
}
