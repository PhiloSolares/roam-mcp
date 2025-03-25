import { Graph } from '@roam-research/roam-api-sdk';
import type { RoamBlock } from '../../types/roam.js';

export interface ToolHandlerDependencies {
  graph: Graph;
}

export interface SearchResult {
  block_uid: string;
  content: string;
  page_title?: string;
}

export interface BlockUpdateResult {
  block_uid: string;
  content: string;
  success: boolean;
  error?: string;
}

export interface BlockUpdate {
  block_uid: string;
  content?: string;
  transform?: { 
    find: string;
    replace: string;
    global?: boolean;
  };
}

export interface OutlineItem {
  text: string | undefined;
  level: number;
}

export { RoamBlock };
