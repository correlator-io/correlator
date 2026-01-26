"use client";

import Link from "next/link";
import { ArrowLeft, GitBranch } from "lucide-react";
import { Button } from "@/components/ui/button";
import { StatusBadge } from "./status-badge";
import { TestDetailsCard } from "./test-details-card";
import { JobDetailsCard } from "./job-details-card";
import { CorrelationWarning } from "./correlation-warning";
import { NoDownstreamImpact } from "./no-downstream-impact";
import { DownstreamGraph } from "@/components/lineage/downstream-graph";
import type { IncidentDetail as IncidentDetailType } from "@/lib/types";

interface IncidentDetailProps {
  incident: IncidentDetailType;
}

export function IncidentDetail({ incident }: IncidentDetailProps) {
  const { test, dataset, job, downstream, correlationStatus } = incident;
  const hasDownstream = downstream.length > 0;
  const isOrphan = correlationStatus === "orphan";

  return (
    <div className="space-y-6">
      {/* Back navigation and header */}
      <div className="flex items-start gap-4">
        <Button variant="ghost" size="icon" asChild className="flex-shrink-0 mt-0.5">
          <Link href="/incidents">
            <ArrowLeft className="h-5 w-5" />
            <span className="sr-only">Back to incidents</span>
          </Link>
        </Button>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-3 flex-wrap">
            <h2 className="text-lg font-semibold truncate">{test.name}</h2>
            <StatusBadge status={test.status} />
          </div>
          <p className="text-sm text-muted-foreground mt-1 font-mono truncate">
            {dataset.urn}
          </p>
        </div>
      </div>

      {/* Main content grid */}
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

      {/* Downstream impact section */}
      <div className="space-y-4">
        <div className="flex items-center gap-2">
          <GitBranch className="h-5 w-5 text-muted-foreground" />
          <h3 className="font-medium">Downstream Impact</h3>
          {hasDownstream && (
            <span className="text-sm text-muted-foreground">
              ({downstream.length} dataset{downstream.length !== 1 ? "s" : ""})
            </span>
          )}
        </div>

        {isOrphan ? (
          <div className="rounded-lg border border-dashed border-border p-6 text-center text-muted-foreground">
            <p className="text-sm">
              Cannot determine downstream impact without correlation.
            </p>
            <p className="text-xs mt-1">
              Fix the namespace mismatch to see affected datasets.
            </p>
          </div>
        ) : hasDownstream ? (
          <div className="rounded-lg border">
            <DownstreamGraph
              sourceDataset={dataset}
              downstream={downstream}
              className="h-[350px] w-full"
            />
          </div>
        ) : (
          <NoDownstreamImpact datasetName={dataset.name} />
        )}
      </div>
    </div>
  );
}
