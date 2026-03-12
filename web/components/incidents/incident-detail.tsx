"use client";

import { useState } from "react";
import { IncidentHeader } from "./incident-header";
import { ImpactSummary } from "./impact-summary";
import { TestDetailsCard } from "./test-details-card";
import { JobDetailsCard } from "./job-details-card";
import { CorrelationWarning } from "./correlation-warning";
import { ResolutionActionBar } from "./resolution-action-bar";
import { RetryTimeline } from "./retry-timeline";
import { LineageGraph } from "@/components/lineage/lineage-graph";
import { useUpdateIncidentStatus } from "@/hooks/use-incidents";
import type { IncidentDetail as IncidentDetailType, ResolutionStatus } from "@/lib/types";

interface IncidentDetailProps {
  incident: IncidentDetailType;
}

export function IncidentDetail({ incident }: IncidentDetailProps) {
  const {
    id,
    test,
    dataset,
    job,
    upstream,
    downstream,
    correlationStatus,
    resolutionStatus: initialResolutionStatus,
    resolvedBy,
    resolutionReason,
    resolvedAt,
    muteExpiresAt,
    retryContext,
  } = incident;
  const isOrphan = correlationStatus === "orphan";
  const hasLineage = upstream.length > 0 || downstream.length > 0;

  const [resolutionStatus, setResolutionStatus] =
    useState<ResolutionStatus>(initialResolutionStatus);

  // Lineage graph expanded state - collapsed by default on mobile
  const [isLineageExpanded, setIsLineageExpanded] = useState(() => {
    if (typeof window !== "undefined") {
      return window.innerWidth >= 768;
    }
    return true;
  });

  const mutation = useUpdateIncidentStatus();

  const handleStatusChange = (
    newStatus: ResolutionStatus,
    options?: { reason?: string; muteDays?: number }
  ) => {
    setResolutionStatus(newStatus);
    mutation.mutate({
      id,
      status: newStatus as "acknowledged" | "resolved" | "muted",
      reason: options?.reason,
      mute_days: options?.muteDays,
    });
  };

  return (
    <div className="space-y-6">
      {/* Header with back button, title, and copy link */}
      <IncidentHeader
        id={id}
        testName={test.name}
        testStatus={test.status}
        executedAt={test.executedAt}
      />

      {/* Resolution action bar — status + actions */}
      <ResolutionActionBar
        resolutionStatus={resolutionStatus}
        resolvedBy={resolvedBy}
        resolutionReason={resolutionReason}
        resolvedAt={resolvedAt}
        muteExpiresAt={muteExpiresAt}
        onStatusChange={handleStatusChange}
      />

      {/* Retry timeline — only when retries exist */}
      {retryContext && (
        <RetryTimeline
          retryContext={retryContext}
          currentIncidentId={id}
          currentResolutionStatus={resolutionStatus}
        />
      )}

      {/* Impact Summary - always visible first */}
      <ImpactSummary
        upstream={upstream}
        downstream={downstream}
        isOrphan={isOrphan}
        isExpanded={isLineageExpanded}
        onToggleExpand={() => setIsLineageExpanded(!isLineageExpanded)}
      />

      {/* Lineage Graph - collapsible, shown right after impact summary */}
      {isLineageExpanded && hasLineage && !isOrphan && (
        <div className="rounded-lg border">
          <LineageGraph
            currentDataset={dataset}
            upstream={upstream}
            downstream={downstream}
            className="h-[400px] w-full"
          />
        </div>
      )}

      {/* Main content grid: Test details and Job details */}
      <div className="grid gap-6 lg:grid-cols-2">
        {/* Left column: Test details */}
        <TestDetailsCard test={test} dataset={dataset} />

        {/* Right column: Job details or correlation warning */}
        {job ? (
          <JobDetailsCard job={job} />
        ) : (
          <CorrelationWarning
            namespace={dataset.namespace}
            producer={incident.test.type.includes("expect") ? "Great Expectations" : "Unknown"}
          />
        )}
      </div>
    </div>
  );
}
