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
  ResolutionStatus,
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
    ...(API_KEY && { Authorization: `Bearer ${API_KEY}` }),
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
  resolution_status?: string;
  resolved_by?: string;
  resolved_at?: string | null;
  mute_expires_at?: string | null;
  retry_context?: {
    total_attempts: number;
    current_attempt: number;
    all_failed: boolean;
    root_run_id: string;
  } | null;
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

interface ApiParentJob {
  name: string;
  namespace?: string;
  run_id: string;
  producer: string;
  status: string;
  completed_at: string | null;
}

interface ApiOrchestrationNode {
  name: string;
  namespace: string;
  run_id: string;
  producer: string;
  status: string;
}

interface ApiJobDetail {
  name: string;
  namespace: string;
  run_id: string;
  producer: string;
  status: string;
  started_at: string;
  completed_at: string;
  parent?: ApiParentJob;
  orchestration?: ApiOrchestrationNode[];
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
  resolution_status?: string;
  resolved_by?: string;
  resolution_reason?: string;
  resolved_at?: string | null;
  mute_expires_at?: string | null;
  retry_context?: {
    total_attempts: number;
    current_attempt: number;
    all_failed: boolean;
    root_run_id: string;
    other_attempts?: {
      incident_id: string;
      attempt: number;
      test_status: string;
      executed_at: string;
      job_run_id: string;
      resolution_status: string;
    }[];
  } | null;
}

// ============================================================
// Dataset Pattern Aliasing API Types
// ============================================================

export interface ApiDatasetMatch {
  dataset_urn: string;
  confidence: number;
  match_reason: string;
  producer?: string;
}

export interface ApiOrphanDataset {
  dataset_urn: string;
  test_count: number;
  last_seen: string;
  producer?: string;
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
 * Normalize Go zero-value timestamps to null.
 * Go's time.Time zero value serializes as "0001-01-01T00:00:00Z".
 * These represent unset timestamps (e.g., completedAt for a RUNNING job).
 */
function normalizeTimestamp(ts: string | null | undefined): string | null {
  if (!ts || ts.startsWith("0001-01-01")) return null;
  return ts;
}

/**
 * Normalize producer field from API format to frontend format.
 * API returns standard OpenLineage producer names: "dbt", "airflow", "great_expectations".
 * Frontend expects "dbt", "airflow", "great_expectations", "unknown".
 */
function normalizeProducer(apiProducer: string): Producer {
  const producerMap: Record<string, Producer> = {
    dbt: "dbt",
    airflow: "airflow",
    great_expectations: "great_expectations",
    ge: "great_expectations", // alias
  };

  return producerMap[apiProducer] ?? "unknown";
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
    resolutionStatus: (api.resolution_status as ResolutionStatus) ?? "open",
    retryContext: api.retry_context
      ? {
          totalAttempts: api.retry_context.total_attempts,
          currentAttempt: api.retry_context.current_attempt,
          allFailed: api.retry_context.all_failed,
          rootRunId: api.retry_context.root_run_id,
        }
      : null,
    resolvedBy: api.resolved_by,
    muteExpiresAt: api.mute_expires_at ?? undefined,
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
          completedAt: normalizeTimestamp(api.job.completed_at),
          parent: api.job.parent
            ? {
                name: api.job.parent.name,
                namespace: api.job.parent.namespace,
                runId: api.job.parent.run_id,
                producer: normalizeProducer(api.job.parent.producer),
                status: api.job.parent.status,
                completedAt: normalizeTimestamp(api.job.parent.completed_at),
              }
            : undefined,
          orchestration: api.job.orchestration?.map((n) => ({
            name: n.name,
            namespace: n.namespace,
            runId: n.run_id,
            producer: normalizeProducer(n.producer),
            status: n.status,
          })),
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
    resolutionStatus: (api.resolution_status as ResolutionStatus) ?? "open",
    resolvedBy: api.resolved_by,
    resolutionReason: api.resolution_reason,
    resolvedAt: api.resolved_at ?? null,
    muteExpiresAt: api.mute_expires_at ?? null,
    retryContext: api.retry_context
      ? {
          totalAttempts: api.retry_context.total_attempts,
          currentAttempt: api.retry_context.current_attempt,
          allFailed: api.retry_context.all_failed,
          rootRunId: api.retry_context.root_run_id,
          otherAttempts: (api.retry_context.other_attempts ?? []).map((a) => ({
            incidentId: a.incident_id,
            attempt: a.attempt,
            status: a.test_status as TestStatus,
            executedAt: a.executed_at,
            jobRunId: a.job_run_id,
            resolutionStatus: a.resolution_status as ResolutionStatus,
          })),
        }
      : null,
  };
}

function transformDatasetMatch(api: ApiDatasetMatch): DatasetMatch {
  return {
    datasetUrn: api.dataset_urn,
    confidence: api.confidence,
    matchReason: api.match_reason as DatasetMatch["matchReason"],
    producer: normalizeProducer(api.producer ?? ""),
  };
}

function transformOrphanDataset(api: ApiOrphanDataset): OrphanDataset {
  return {
    datasetUrn: api.dataset_urn,
    testCount: api.test_count,
    lastSeen: api.last_seen,
    producer: normalizeProducer(api.producer ?? ""),
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

export type StatusFilter = "active" | "resolved" | "muted" | "all";

export interface FetchIncidentsParams {
  status?: StatusFilter;
  window?: number;
  limit?: number;
  offset?: number;
  since?: string;
}

export async function fetchIncidents(
  params: FetchIncidentsParams = {}
): Promise<IncidentListResponse> {
  const searchParams = new URLSearchParams();

  if (params.status) {
    searchParams.set("status", params.status);
  }
  if (params.window !== undefined) {
    searchParams.set("window", String(params.window));
  }
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

export interface IncidentCounts {
  active: number;
  resolved: number;
  muted: number;
}

export async function fetchIncidentCounts(): Promise<IncidentCounts> {
  return apiFetch<IncidentCounts>("/api/v1/incidents/counts");
}

export interface UpdateStatusParams {
  status: "acknowledged" | "resolved" | "muted";
  reason?: string;
  note?: string;
  mute_days?: number;
}

export interface UpdateStatusResponse {
  id: string;
  resolution_status: string;
  resolved_by: string;
  resolved_at: string;
  resolution_reason: string | null;
  mute_expires_at: string | null;
}

export async function updateIncidentStatus(
  id: string,
  params: UpdateStatusParams
): Promise<UpdateStatusResponse> {
  return apiFetch<UpdateStatusResponse>(`/api/v1/incidents/${id}/status`, {
    method: "PATCH",
    body: JSON.stringify(params),
  });
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
  normalizeTimestamp,
  transformIncident,
  transformIncidentDetail,
  transformDatasetMatch,
  transformOrphanDataset,
  transformSuggestedPattern,
  transformCorrelationHealth,
};
