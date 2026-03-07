"use client";

import { useState } from "react";
import { ChevronDown, ChevronUp, ArrowRight, ArrowDown } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ProducerIcon } from "@/components/icons/producer-icon";
import { OrphanDatasetNoMatch } from "./orphan-dataset-no-match";
import { formatRelativeTime, formatMatchReason } from "@/lib/utils";
import type { OrphanDataset } from "@/lib/types";

interface OrphanDatasetListProps {
  datasets: OrphanDataset[];
}

const INITIAL_DISPLAY_COUNT = 5;

function MatchedOrphansTable({ datasets }: { datasets: OrphanDataset[] }) {
  return (
    <div className="rounded-lg border bg-card overflow-hidden hidden md:block">
      <table className="w-full">
        <thead>
          <tr className="border-b bg-muted/50">
            <th className="text-left text-xs font-medium text-muted-foreground px-4 py-2.5">
              Orphan datasets
            </th>
            <th className="w-8" aria-hidden="true" />
            <th className="text-left text-xs font-medium text-muted-foreground px-4 py-2.5">
              Likely matches
            </th>
          </tr>
        </thead>
        <tbody>
          {datasets.map((dataset) => (
              <tr key={dataset.datasetUrn} className="border-b last:border-b-0">
                <td className="px-4 py-3 align-top">
                  <div className="flex items-center gap-2">
                    <ProducerIcon producer={dataset.producer} size={16} />
                    <span className="font-mono text-sm break-all">{dataset.datasetUrn}</span>
                  </div>
                  <p className="text-xs text-muted-foreground mt-1 ml-6">
                    {dataset.testCount} test{dataset.testCount !== 1 ? "s" : ""} · Last seen: {formatRelativeTime(dataset.lastSeen)}
                  </p>
                </td>
                <td className="px-1 align-top pt-3.5">
                  <ArrowRight className="h-4 w-4 text-muted-foreground flex-shrink-0" aria-hidden="true" />
                </td>
                <td className="px-4 py-3 align-top">
                  <div className="flex items-center gap-2">
                    <ProducerIcon producer={dataset.likelyMatch!.producer} size={16} />
                    <span className="font-mono text-sm break-all text-green-600 dark:text-green-400">
                      {dataset.likelyMatch!.datasetUrn}
                    </span>
                  </div>
                  <p className="text-xs text-muted-foreground mt-1 ml-6">
                    {formatMatchReason(dataset.likelyMatch!.matchReason, dataset.likelyMatch!.datasetUrn)}
                  </p>
                </td>
              </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function MatchedOrphansMobile({ datasets }: { datasets: OrphanDataset[] }) {
  return (
    <div className="space-y-3 md:hidden">
      {datasets.map((dataset) => (
          <div key={dataset.datasetUrn} className="rounded-lg border bg-card p-4 space-y-3">
            {/* Orphan */}
            <div>
              <div className="flex items-center gap-2">
                <ProducerIcon producer={dataset.producer} size={16} />
                <span className="text-xs font-medium text-muted-foreground">Orphan</span>
              </div>
              <p className="font-mono text-sm break-all mt-1">{dataset.datasetUrn}</p>
              <p className="text-xs text-muted-foreground mt-1">
                {dataset.testCount} test{dataset.testCount !== 1 ? "s" : ""} · Last seen: {formatRelativeTime(dataset.lastSeen)}
              </p>
            </div>

            {/* Down arrow */}
            <div className="flex justify-center">
              <ArrowDown className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
            </div>

            {/* Match */}
            <div>
              <div className="flex items-center gap-2">
                <ProducerIcon producer={dataset.likelyMatch!.producer} size={16} />
                <span className="text-xs font-medium text-muted-foreground">Likely match</span>
              </div>
              <p className="font-mono text-sm break-all text-green-600 dark:text-green-400 mt-1">
                {dataset.likelyMatch!.datasetUrn}
              </p>
              <p className="text-xs text-muted-foreground mt-1">
                {formatMatchReason(dataset.likelyMatch!.matchReason, dataset.likelyMatch!.datasetUrn)}
              </p>
            </div>
          </div>
      ))}
    </div>
  );
}

export function OrphanDatasetList({ datasets }: OrphanDatasetListProps) {
  const [expanded, setExpanded] = useState(false);

  if (datasets.length === 0) {
    return null;
  }

  const hasMore = datasets.length > INITIAL_DISPLAY_COUNT;
  const displayedDatasets = expanded
    ? datasets
    : datasets.slice(0, INITIAL_DISPLAY_COUNT);
  const hiddenCount = datasets.length - INITIAL_DISPLAY_COUNT;

  const matched = displayedDatasets.filter((d) => d.likelyMatch !== null);
  const unmatched = displayedDatasets.filter((d) => d.likelyMatch === null);

  return (
    <div className="space-y-4">
      <h2 className="text-sm font-medium text-muted-foreground">
        Affected Datasets ({datasets.length} total)
      </h2>

      {/* Matched orphans — table on desktop, stacked cards on mobile */}
      {matched.length > 0 && (
        <>
          <MatchedOrphansTable datasets={matched} />
          <MatchedOrphansMobile datasets={matched} />
        </>
      )}

      {/* Unmatched orphans — separate section */}
      {unmatched.length > 0 && (
        <div className="space-y-3">
          {unmatched.map((dataset) => (
            <OrphanDatasetNoMatch key={dataset.datasetUrn} dataset={dataset} />
          ))}
        </div>
      )}

      {hasMore && (
        <Button
          variant="ghost"
          size="sm"
          onClick={() => setExpanded(!expanded)}
          className="w-full text-muted-foreground hover:text-foreground"
        >
          {expanded ? (
            <>
              <ChevronUp className="h-4 w-4 mr-2" aria-hidden="true" />
              Show less
            </>
          ) : (
            <>
              <ChevronDown className="h-4 w-4 mr-2" aria-hidden="true" />
              Show {hiddenCount} more affected dataset{hiddenCount !== 1 ? "s" : ""}
            </>
          )}
        </Button>
      )}
    </div>
  );
}
