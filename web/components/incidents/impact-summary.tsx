"use client";

import { AlertTriangle, ChevronDown, ChevronRight, CheckCircle } from "lucide-react";
import { cn } from "@/lib/utils";
import type { DownstreamDataset, UpstreamDataset } from "@/lib/types";

interface ImpactSummaryProps {
  upstream: UpstreamDataset[];
  downstream: DownstreamDataset[];
  isOrphan: boolean;
  isExpanded: boolean;
  onToggleExpand: () => void;
}

export function ImpactSummary({
  upstream,
  downstream,
  isOrphan,
  isExpanded,
  onToggleExpand,
}: ImpactSummaryProps) {
  // Dedupe by URN, keeping the entry with MINIMUM depth
  // This ensures datasets are classified by their closest relationship
  const downstreamMap = new Map<string, DownstreamDataset>();
  downstream.forEach((d) => {
    const existing = downstreamMap.get(d.urn);
    if (!existing || d.depth < existing.depth) {
      downstreamMap.set(d.urn, d);
    }
  });
  const uniqueDownstream = [...downstreamMap.values()];

  const upstreamMap = new Map<string, UpstreamDataset>();
  upstream.forEach((u) => {
    const existing = upstreamMap.get(u.urn);
    if (!existing || u.depth < existing.depth) {
      upstreamMap.set(u.urn, u);
    }
  });
  const uniqueUpstream = [...upstreamMap.values()];

  const downstreamCount = uniqueDownstream.length;
  const upstreamCount = uniqueUpstream.length;

  // Separate direct (depth=1) and indirect (depth>1)
  const directDownstream = uniqueDownstream.filter((d) => d.depth === 1);
  const indirectDownstream = uniqueDownstream.filter((d) => d.depth > 1);

  const hasImpact = downstreamCount > 0;
  const hasLineage = upstreamCount > 0 || downstreamCount > 0;

  if (isOrphan) {
    return (
      <div className="rounded-lg border border-dashed border-status-warning bg-status-warning/5 p-4">
        <div className="flex items-start gap-3">
          <AlertTriangle className="h-5 w-5 text-status-warning flex-shrink-0 mt-0.5" />
          <div>
            <p className="font-medium text-status-warning">Cannot Determine Impact</p>
            <p className="text-sm text-muted-foreground mt-1">
              This test couldn&apos;t be linked to a producing job. Fix the namespace mismatch
              on the{" "}
              <a href="/health" className="text-primary hover:underline">
                Correlation Health
              </a>{" "}
              page to see downstream impact.
            </p>
          </div>
        </div>
      </div>
    );
  }

  if (!hasImpact) {
    return (
      <div className="rounded-lg border border-status-passed/30 bg-status-passed/5 p-4">
        <div className="flex items-start gap-3">
          <CheckCircle className="h-5 w-5 text-status-passed flex-shrink-0 mt-0.5" />
          <div>
            <p className="font-medium text-status-passed">No Downstream Impact</p>
            <p className="text-sm text-muted-foreground mt-1">
              This dataset has no known consumers. The failure is isolated.
            </p>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-status-failed/30 bg-status-failed/5 p-4">
      <div className="flex items-start gap-3">
        <AlertTriangle className="h-5 w-5 text-status-failed flex-shrink-0 mt-0.5" />
        <div className="flex-1 min-w-0">
          <div className="flex items-center justify-between gap-4">
            <p className="font-medium text-status-failed">
              {downstreamCount} downstream dataset{downstreamCount !== 1 ? "s" : ""} affected
            </p>
          </div>

          {/* Breakdown by direct/indirect */}
          <div className="mt-2 space-y-1">
            {directDownstream.length > 0 && (
              <div className="text-sm">
                <span className="text-muted-foreground">Direct: </span>
                <span className="text-foreground">
                  {directDownstream.map((d) => d.name).join(", ")}
                </span>
              </div>
            )}
            {indirectDownstream.length > 0 && (
              <div className="text-sm">
                <span className="text-muted-foreground">Indirect: </span>
                <span className="text-foreground">
                  {indirectDownstream.map((d) => d.name).join(", ")}
                </span>
              </div>
            )}
          </div>

          {/* Toggle for lineage graph */}
          {hasLineage && (
            <button
              onClick={onToggleExpand}
              className={cn(
                "mt-3 flex items-center gap-1 text-sm font-medium",
                "text-primary hover:text-primary/80 transition-colors"
              )}
            >
              {isExpanded ? (
                <>
                  <ChevronDown className="h-4 w-4" />
                  Hide Lineage Graph
                </>
              ) : (
                <>
                  <ChevronRight className="h-4 w-4" />
                  Show Lineage Graph
                  {upstreamCount > 0 && (
                    <span className="text-muted-foreground font-normal ml-1">
                      ({upstreamCount} upstream)
                    </span>
                  )}
                </>
              )}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
