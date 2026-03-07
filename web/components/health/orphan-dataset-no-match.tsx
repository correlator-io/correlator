"use client";

import { useState } from "react";
import { AlertTriangle, Copy, Check } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ProducerIcon } from "@/components/icons/producer-icon";
import { cn } from "@/lib/utils";
import type { OrphanDataset } from "@/lib/types";

function generateManualTemplate(datasetUrn: string): string {
  return `dataset_patterns:
  - pattern: "${datasetUrn}"
    canonical: "YOUR_CANONICAL_URN_HERE"`;
}

interface OrphanDatasetNoMatchProps {
  dataset: OrphanDataset;
}

export function OrphanDatasetNoMatch({ dataset }: OrphanDatasetNoMatchProps) {
  const [copied, setCopied] = useState(false);
  const { datasetUrn } = dataset;
  const orphanProducer = dataset.producer;

  const handleCopyTemplate = async () => {
    try {
      await navigator.clipboard.writeText(generateManualTemplate(datasetUrn));
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      console.error("Failed to copy to clipboard");
    }
  };

  return (
    <div className={cn(
      "rounded-lg border p-4 space-y-2",
      "bg-yellow-50 border-yellow-200 dark:bg-yellow-950/30 dark:border-yellow-800"
    )}>
      <div className="flex items-center gap-2">
        <ProducerIcon producer={orphanProducer} size={16} />
        <span className="font-mono text-sm break-all">{datasetUrn}</span>
      </div>
      <div className="flex items-center gap-2 text-sm text-yellow-800 dark:text-yellow-200">
        <AlertTriangle className="h-4 w-4 flex-shrink-0" aria-hidden="true" />
        <span className="font-medium">No matching producer found</span>
      </div>
      <p className="text-xs text-yellow-700 dark:text-yellow-300">
        Manual configuration required. Copy the template below and replace{" "}
        <code className="font-mono bg-yellow-100 dark:bg-yellow-900 px-1 rounded">YOUR_CANONICAL_URN_HERE</code>{" "}
        with the correct URN.
      </p>
      <div className="flex items-center justify-between pt-1">
        <code className="text-xs font-mono text-yellow-800 dark:text-yellow-200 truncate max-w-[70%]">
          {datasetUrn}
        </code>
        <Button
          variant="outline"
          size="sm"
          onClick={handleCopyTemplate}
          className="h-7 text-xs border-yellow-300 dark:border-yellow-700"
        >
          {copied ? (
            <>
              <Check className="h-3 w-3 mr-1" />
              Copied
            </>
          ) : (
            <>
              <Copy className="h-3 w-3 mr-1" />
              Copy Template
            </>
          )}
        </Button>
      </div>
    </div>
  );
}
