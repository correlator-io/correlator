"use client";

import { useState } from "react";
import { ChevronDown, ChevronUp } from "lucide-react";
import { Button } from "@/components/ui/button";
import { OrphanDatasetCard } from "./orphan-dataset-card";
import type { OrphanDataset } from "@/lib/types";

interface OrphanDatasetListProps {
  datasets: OrphanDataset[];
}

const INITIAL_DISPLAY_COUNT = 5;

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

  return (
    <div className="space-y-4">
      <h2 className="text-sm font-medium text-muted-foreground">
        Affected Datasets ({datasets.length} total)
      </h2>

      <div className="space-y-3">
        {displayedDatasets.map((dataset) => (
          <OrphanDatasetCard key={dataset.datasetUrn} dataset={dataset} />
        ))}
      </div>

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
