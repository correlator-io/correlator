import type { Producer, ParentJob } from "./types";

/**
 * A single level in the orchestration chain, from root to leaf.
 */
export interface ChainLevel {
  name: string;
  namespace?: string;
  runId: string;
  producer?: Producer;
  status: string;
  isCurrent: boolean;
}

/**
 * Build the orchestration chain from job hierarchy data.
 *
 * Returns levels ordered root → ... → leaf.
 * Returns empty array if no parent exists (no chain to show).
 * Deduplicates when rootParent.runId === parent.runId.
 */
export function buildOrchestrationChain(job: {
  name: string;
  namespace: string;
  runId: string;
  producer: Producer;
  status: string;
  parent?: ParentJob;
  rootParent?: ParentJob;
}): ChainLevel[] {
  if (!job.parent) return [];

  const levels: ChainLevel[] = [];

  // Add root if present and different from parent
  if (job.rootParent && job.rootParent.runId !== job.parent.runId) {
    levels.push({
      name: job.rootParent.name,
      namespace: job.rootParent.namespace,
      runId: job.rootParent.runId,
      producer: job.rootParent.producer,
      status: job.rootParent.status,
      isCurrent: false,
    });
  }

  // Add parent
  levels.push({
    name: job.parent.name,
    namespace: job.parent.namespace,
    runId: job.parent.runId,
    producer: job.parent.producer,
    status: job.parent.status,
    isCurrent: false,
  });

  // Add leaf (current producing job)
  levels.push({
    name: job.name,
    namespace: job.namespace,
    runId: job.runId,
    producer: job.producer,
    status: job.status,
    isCurrent: true,
  });

  return levels;
}
