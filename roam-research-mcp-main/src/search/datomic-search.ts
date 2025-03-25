import { q } from '@roam-research/roam-api-sdk';
import type { Graph } from '@roam-research/roam-api-sdk';
import { BaseSearchHandler, SearchResult } from './types.js';
// import { resolveRefs } from '../helpers/refs.js';

export interface DatomicSearchParams {
  query: string;
  inputs?: unknown[];
}

export class DatomicSearchHandler extends BaseSearchHandler {
  constructor(
    graph: Graph,
    private params: DatomicSearchParams
  ) {
    super(graph);
  }

  async execute(): Promise<SearchResult> {
    try {
      // Execute the datomic query using the Roam API
      const results = await q(this.graph, this.params.query, this.params.inputs || []) as unknown[];

      return {
        success: true,
        matches: results.map(result => ({
          content: JSON.stringify(result),
          block_uid: '', // Datomic queries may not always return block UIDs
          page_title: '' // Datomic queries may not always return page titles
        })),
        message: `Query executed successfully. Found ${results.length} results.`
      };
    } catch (error) {
      return {
        success: false,
        matches: [],
        message: `Failed to execute query: ${error instanceof Error ? error.message : String(error)}`
      };
    }
  }
}
