// Core types for Correlator UI

export type TestStatus = "failed" | "passed" | "warning" | "unknown";
export type Producer = "dbt" | "airflow" | "great_expectations" | "unknown";
export type CorrelationStatus = "correlated" | "orphan" | "unknown";

export interface Incident {
  id: string;
  testName: string;
  testType: string;
  testStatus: TestStatus;
  datasetUrn: string;
  datasetName: string;
  producer: Producer;
  jobName: string;
  jobRunId: string;
  downstreamCount: number;
  hasCorrelationIssue: boolean;
  executedAt: string; // ISO 8601
}

export interface IncidentDetail {
  id: string;
  test: {
    name: string;
    type: string;
    status: TestStatus;
    message: string;
    executedAt: string; // ISO 8601
    durationMs: number;
    producer: Producer;
  };
  dataset: {
    urn: string;
    name: string;
    namespace: string;
  };
  job: {
    name: string;
    namespace: string;
    runId: string;
    producer: Producer;
    status: string;
    startedAt: string; // ISO 8601
    completedAt: string | null; // ISO 8601, null if still running
    parent?: ParentJob;
    orchestration?: OrchestrationNode[];
  } | null;
  upstream: UpstreamDataset[];
  downstream: DownstreamDataset[];
  correlationStatus: CorrelationStatus;
}

export interface ParentJob {
  name: string;
  namespace?: string;
  runId: string;
  producer: Producer;
  status: string;
  completedAt: string | null;
}

export interface OrchestrationNode {
  name: string;
  namespace: string;
  runId: string;
  producer: Producer;
  status: string;
}

export interface DownstreamDataset {
  urn: string;
  name: string;
  depth: number;
  parentUrn: string; // For building lineage tree
  producer?: Producer;
}

export interface UpstreamDataset {
  urn: string;
  name: string;
  depth: number;
  childUrn: string; // What this dataset feeds into
  producer?: Producer;
}

// ============================================================
// Dataset Pattern Aliasing Types
// ============================================================

/**
 * A likely match for an orphan dataset based on structural matching.
 */
export interface DatasetMatch {
  datasetUrn: string;
  confidence: number; // 0-1, where 1 = exact match
  matchReason: "exact_table_name" | "fuzzy_match" | "no_match";
}

/**
 * A dataset with test results that can't be matched to a producing job.
 */
export interface OrphanDataset {
  datasetUrn: string;
  testCount: number;
  lastSeen: string; // ISO 8601
  likelyMatch: DatasetMatch | null;
}

/**
 * A suggested pattern transformation to resolve orphan datasets.
 */
export interface SuggestedPattern {
  pattern: string; // e.g., "demo_postgres/{name}"
  canonical: string; // e.g., "postgresql://demo/marts.{name}"
  resolvesCount: number;
  orphansResolved: string[]; // URNs of orphan datasets this pattern fixes
}

export interface CorrelationHealth {
  correlationRate: number; // 0-1
  totalDatasets: number;
  producedDatasets: number; // Datasets with producers (outputs only)
  correlatedDatasets: number; // Produced datasets with test correlation
  orphanDatasets: OrphanDataset[];
  suggestedPatterns: SuggestedPattern[];
}

// API response types
export interface IncidentListResponse {
  incidents: Incident[];
  total: number;
  limit: number;
  offset: number;
  orphanCount: number; // Datasets with test failures but no producer correlation
}
