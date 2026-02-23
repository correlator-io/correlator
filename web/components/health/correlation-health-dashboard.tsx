"use client";

import { HealthStatusHeader } from "./health-status-header";
import { SuggestedPatternCard } from "./suggested-pattern-card";
import { OrphanDatasetList } from "./orphan-dataset-list";
import { HealthyState } from "./healthy-state";
import type { CorrelationHealth } from "@/lib/types";

interface CorrelationHealthDashboardProps {
  health: CorrelationHealth;
}

export function CorrelationHealthDashboard({ health }: CorrelationHealthDashboardProps) {
  const {
    correlationRate,
    totalDatasets,
    correlatedDatasets,
    orphanDatasets,
    suggestedPatterns,
  } = health;

  const hasOrphans = orphanDatasets.length > 0;

  return (
    <div className="space-y-6">
      {/* Status header - always visible */}
      <HealthStatusHeader
        orphanCount={orphanDatasets.length}
        correlationRate={correlationRate}
        totalDatasets={totalDatasets}
        correlatedDatasets={correlatedDatasets}
      />

      {hasOrphans ? (
        <>
          {/* Suggested fix - above the fold */}
          {suggestedPatterns.length > 0 && (
            <SuggestedPatternCard patterns={suggestedPatterns} />
          )}

          {/* Affected datasets - show 5, collapse rest */}
          <OrphanDatasetList datasets={orphanDatasets} />
        </>
      ) : (
        <HealthyState />
      )}
    </div>
  );
}
