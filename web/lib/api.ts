// API client for Correlator backend
// Handles snake_case (API) → camelCase (frontend) transformation

import type {
  Incident,
  IncidentDetail,
  IncidentListResponse,
  CorrelationHealth,
  OrphanDataset,
  DatasetMatch,
  SuggestedPattern,
  Producer,
  TestStatus,
  CorrelationStatus,
  UpstreamDataset,
  DownstreamDataset,
} from "./types";

// API base URL - defaults to localhost:8080 for development
const API_BASE_URL = process.env.NEXT_PUBLIC_CORRELATOR_URL || "http://localhost:8080";

// API key for authentication (required by backend)
const API_KEY = process.env.NEXT_PUBLIC_CORRELATOR_API_KEY || "";

// Generic fetch wrapper with error handling
async function apiFetch<T>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  const url = `${API_BASE_URL}${endpoint}`;

  const headers: HeadersInit = {
    "Content-Type": "application/json",
    ...(API_KEY && { "X-Api-Key": API_KEY }),
    ...options.headers,
  };

  const response = await fetch(url, {
    ...options,
    headers,
  });

  if (!response.ok) {
    // Try to parse RFC 7807 error response
    let errorDetail = `HTTP ${response.status}`;
    try {
      const errorBody = await response.json();
      errorDetail = errorBody.detail || errorBody.title || errorDetail;
    } catch {
      // Ignore JSON parse errors
    }
    throw new ApiError(errorDetail, response.status);
  }

  return response.json();
}

// Custom error class for API errors
export class ApiError extends Error {
  constructor(
    message: string,
    public status: number
  ) {
    super(message);
    this.name = "ApiError";
  }
}

// ============================================================
// Case transformation utilities
// ============================================================

// Note: We use explicit transformation functions instead of generic key transformation
// for better type safety and clearer code

// ============================================================
// API Response Types (snake_case from backend)
// ============================================================

export interface ApiIncidentSummary {
  id: string;
  test_name: string;
  test_type: string;
  test_status: string;
  dataset_urn: string;
  dataset_name: string;
  producer: string;
  job_name: string;
  job_run_id: string;
  downstream_count: number;
  has_correlation_issue: boolean;
  executed_at: string;
}

interface ApiIncidentListResponse {
  incidents: ApiIncidentSummary[];
  total: number;
  limit: number;
  offset: number;
  orphan_count?: number; // Optional for backward compatibility
}

interface ApiTestDetail {
  name: string;
  type: string;
  status: string;
  message: string;
  executed_at: string;
  duration_ms: number;
  producer: string;
}

interface ApiDatasetDetail {
  urn: string;
  name: string;
  namespace: string;
}

interface ApiJobDetail {
  name: string;
  namespace: string;
  run_id: string;
  producer: string;
  status: string;
  started_at: string;
  completed_at: string;
}

interface ApiDownstreamDataset {
  urn: string;
  name: string;
  depth: number;
  parentUrn: string; // Already camelCase in API
  producer?: string;
}

interface ApiUpstreamDataset {
  urn: string;
  name: string;
  depth: number;
  childUrn: string; // Already camelCase in API
  producer?: string;
}

export interface ApiIncidentDetailResponse {
  id: string;
  test: ApiTestDetail;
  dataset: ApiDatasetDetail;
  job: ApiJobDetail | null;
  upstream: ApiUpstreamDataset[];
  downstream: ApiDownstreamDataset[];
  correlation_status: string;
}

/**
 * @deprecated Use ApiOrphanDataset instead
 */
export interface ApiOrphanNamespace {
  namespace: string;
  producer: string;
  last_seen: string;
  event_count: number;
  suggested_alias: string | null;
}

// ============================================================
// Dataset Pattern Aliasing API Types
// ============================================================

export interface ApiDatasetMatch {
  dataset_urn: string;
  confidence: number;
  match_reason: string;
}

export interface ApiOrphanDataset {
  dataset_urn: string;
  test_count: number;
  last_seen: string;
  likely_match: ApiDatasetMatch | null;
}

export interface ApiSuggestedPattern {
  pattern: string;
  canonical: string;
  resolves_count: number;
  orphans_resolved: string[];
}

export interface ApiCorrelationHealthResponse {
  correlation_rate: number;
  total_datasets: number;
  produced_datasets: number;
  correlated_datasets: number;
  orphan_datasets: ApiOrphanDataset[];
  suggested_patterns: ApiSuggestedPattern[];
}

// ============================================================
// Transform functions (API → Frontend types)
// ============================================================

// TODO: For production, consider using Zod for runtime validation of API responses.
// Current `as` casts assume the API contract is correct. If the backend returns
// unexpected enum values (e.g., a new status), TypeScript won't catch it at runtime.

/**
 * Normalize producer field from API format to frontend format.
 * API returns "correlator-dbt", "correlator-airflow", etc.
 * Frontend expects "dbt", "airflow", "great_expectations", "unknown".
 */
function normalizeProducer(apiProducer: string): Producer {
  // Strip "correlator-" prefix if present
  const normalized = apiProducer.replace(/^correlator-/, "");

  // Map to known producer types
  const producerMap: Record<string, Producer> = {
    dbt: "dbt",
    airflow: "airflow",
    great_expectations: "great_expectations",
    ge: "great_expectations", // alias
  };

  return producerMap[normalized] ?? "unknown";
}

function transformIncident(api: ApiIncidentSummary): Incident {
  return {
    id: api.id,
    testName: api.test_name,
    testType: api.test_type,
    testStatus: api.test_status as TestStatus,
    datasetUrn: api.dataset_urn,
    datasetName: api.dataset_name,
    producer: normalizeProducer(api.producer),
    jobName: api.job_name,
    jobRunId: api.job_run_id,
    downstreamCount: api.downstream_count,
    hasCorrelationIssue: api.has_correlation_issue,
    executedAt: api.executed_at,
  };
}

function transformIncidentDetail(api: ApiIncidentDetailResponse): IncidentDetail {
  return {
    id: api.id,
    test: {
      name: api.test.name,
      type: api.test.type,
      status: api.test.status as TestStatus,
      message: api.test.message,
      executedAt: api.test.executed_at,
      durationMs: api.test.duration_ms,
      producer: normalizeProducer(api.test.producer),
    },
    dataset: {
      urn: api.dataset.urn,
      name: api.dataset.name,
      namespace: api.dataset.namespace,
    },
    job: api.job
      ? {
          name: api.job.name,
          namespace: api.job.namespace,
          runId: api.job.run_id,
          producer: normalizeProducer(api.job.producer),
          status: api.job.status,
          startedAt: api.job.started_at,
          completedAt: api.job.completed_at,
        }
      : null,
    upstream: (api.upstream || []).map((u): UpstreamDataset => ({
      urn: u.urn,
      name: u.name,
      depth: u.depth,
      childUrn: u.childUrn, // Already camelCase
      producer: u.producer ? normalizeProducer(u.producer) : undefined,
    })),
    downstream: (api.downstream || []).map((d): DownstreamDataset => ({
      urn: d.urn,
      name: d.name,
      depth: d.depth,
      parentUrn: d.parentUrn, // Already camelCase
      producer: d.producer ? normalizeProducer(d.producer) : undefined,
    })),
    correlationStatus: api.correlation_status as CorrelationStatus,
  };
}

function transformDatasetMatch(api: ApiDatasetMatch): DatasetMatch {
  return {
    datasetUrn: api.dataset_urn,
    confidence: api.confidence,
    matchReason: api.match_reason as DatasetMatch["matchReason"],
  };
}

function transformOrphanDataset(api: ApiOrphanDataset): OrphanDataset {
  return {
    datasetUrn: api.dataset_urn,
    testCount: api.test_count,
    lastSeen: api.last_seen,
    likelyMatch: api.likely_match ? transformDatasetMatch(api.likely_match) : null,
  };
}

function transformSuggestedPattern(api: ApiSuggestedPattern): SuggestedPattern {
  return {
    pattern: api.pattern,
    canonical: api.canonical,
    resolvesCount: api.resolves_count,
    orphansResolved: api.orphans_resolved,
  };
}

function transformCorrelationHealth(
  api: ApiCorrelationHealthResponse
): CorrelationHealth {
  return {
    correlationRate: api.correlation_rate,
    totalDatasets: api.total_datasets,
    producedDatasets: api.produced_datasets,
    correlatedDatasets: api.correlated_datasets,
    orphanDatasets: api.orphan_datasets.map(transformOrphanDataset),
    suggestedPatterns: api.suggested_patterns.map(transformSuggestedPattern),
  };
}

// ============================================================
// Public API Functions
// ============================================================

export interface FetchIncidentsParams {
  limit?: number;
  offset?: number;
  since?: string;
}

export async function fetchIncidents(
  params: FetchIncidentsParams = {}
): Promise<IncidentListResponse> {
  const searchParams = new URLSearchParams();

  if (params.limit !== undefined) {
    searchParams.set("limit", String(params.limit));
  }
  if (params.offset !== undefined) {
    searchParams.set("offset", String(params.offset));
  }
  if (params.since) {
    searchParams.set("since", params.since);
  }

  const query = searchParams.toString();
  const endpoint = `/api/v1/incidents${query ? `?${query}` : ""}`;

  const response = await apiFetch<ApiIncidentListResponse>(endpoint);

  return {
    incidents: response.incidents.map(transformIncident),
    total: response.total,
    limit: response.limit,
    offset: response.offset,
    orphanCount: response.orphan_count ?? 0,
  };
}

export async function fetchIncidentDetail(id: string): Promise<IncidentDetail> {
  const response = await apiFetch<ApiIncidentDetailResponse>(
    `/api/v1/incidents/${id}`
  );

  return transformIncidentDetail(response);
}

export async function fetchCorrelationHealth(): Promise<CorrelationHealth> {
  const response = await apiFetch<ApiCorrelationHealthResponse>(
    "/api/v1/health/correlation"
  );

  return transformCorrelationHealth(response);
}

// ============================================================
// Exports for testing
// ============================================================

export const __testing__ = {
  transformIncident,
  transformIncidentDetail,
  transformDatasetMatch,
  transformOrphanDataset,
  transformSuggestedPattern,
  transformCorrelationHealth,
};
