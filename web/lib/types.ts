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
    completedAt: string; // ISO 8601
  } | null;
  downstream: DownstreamDataset[];
  correlationStatus: CorrelationStatus;
}

export interface DownstreamDataset {
  urn: string;
  name: string;
  depth: number;
  parentUrn: string; // For building lineage tree
}

export interface OrphanNamespace {
  namespace: string;
  producer: Producer;
  lastSeen: string; // ISO 8601
  eventCount: number;
  suggestedAlias: string | null;
}

export interface CorrelationHealth {
  correlationRate: number; // 0-1
  totalDatasets: number;
  orphanNamespaces: OrphanNamespace[];
}

// API response types
export interface IncidentListResponse {
  incidents: Incident[];
  total: number;
  limit: number;
  offset: number;
}

export type IncidentDetailResponse = IncidentDetail;

export type CorrelationHealthResponse = CorrelationHealth;
