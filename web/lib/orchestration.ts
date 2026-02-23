import type { Producer, OrchestrationNode } from "./types";

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
 * Uses the `orchestration` array (root → immediate parent) returned by the API,
 * then appends the current producing job as the leaf.
 *
 * Falls back to `parent` when `orchestration` is absent (backward compat).
 * Returns empty array if no parent exists (no chain to show).
 */
export function buildOrchestrationChain(job: {
  name: string;
  namespace: string;
  runId: string;
  producer: Producer;
  status: string;
  parent?: { name: string; namespace?: string; runId: string; producer: Producer; status: string };
  orchestration?: OrchestrationNode[];
}): ChainLevel[] {
  if (!job.parent && (!job.orchestration || job.orchestration.length === 0)) {
    return [];
  }

  const levels: ChainLevel[] = [];

  if (job.orchestration && job.orchestration.length > 0) {
    for (const node of job.orchestration) {
      levels.push({
        name: node.name,
        namespace: node.namespace,
        runId: node.runId,
        producer: node.producer,
        status: node.status,
        isCurrent: false,
      });
    }
  } else if (job.parent) {
    levels.push({
      name: job.parent.name,
      namespace: job.parent.namespace,
      runId: job.parent.runId,
      producer: job.parent.producer,
      status: job.parent.status,
      isCurrent: false,
    });
  }

  // Append leaf (current producing job)
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
