"use client";

import { useState } from "react";
import { ArrowRight, AlertTriangle, Copy, Check } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import type { OrphanDataset } from "@/lib/types";

interface OrphanDatasetCardProps {
  dataset: OrphanDataset;
}

/**
 * Format match reason for display.
 */
function formatMatchReason(reason: string, canonicalUrn: string): string {
  switch (reason) {
    case "exact_table_name": {
      // Extract table name from canonical URN (last segment after / or .)
      const tableName = canonicalUrn.split(/[/.]/).pop() || "table";
      return `Same table name "${tableName}"`;
    }
    case "fuzzy_match":
      return "Similar name pattern";
    default:
      return "Structural match";
  }
}

/**
 * Format relative time from ISO timestamp.
 */
function formatRelativeTime(isoTimestamp: string): string {
  const date = new Date(isoTimestamp);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMins / 60);
  const diffDays = Math.floor(diffHours / 24);

  if (diffMins < 1) return "Just now";
  if (diffMins < 60) return `${diffMins} min ago`;
  if (diffHours < 24) return `${diffHours} hour${diffHours > 1 ? "s" : ""} ago`;
  return `${diffDays} day${diffDays > 1 ? "s" : ""} ago`;
}

/**
 * Generate manual config template for no-match case.
 */
function generateManualTemplate(datasetUrn: string): string {
  return `dataset_patterns:
  - pattern: "${datasetUrn}"
    canonical: "YOUR_CANONICAL_URN_HERE"`;
}

export function OrphanDatasetCard({ dataset }: OrphanDatasetCardProps) {
  const [copied, setCopied] = useState(false);
  const { datasetUrn, testCount, lastSeen, likelyMatch } = dataset;
  const hasMatch = likelyMatch !== null;

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
    <div className="rounded-lg border bg-card p-4 space-y-3">
      {/* Dataset URN and metadata */}
      <div>
        <p className="font-mono text-sm break-all">{datasetUrn}</p>
        <p className="text-xs text-muted-foreground mt-1">
          {testCount} test{testCount !== 1 ? "s" : ""} · Last seen: {formatRelativeTime(lastSeen)}
        </p>
      </div>

      {/* Match info or no-match warning */}
      {hasMatch ? (
        <div className="flex items-start gap-2 text-sm">
          <ArrowRight className="h-4 w-4 mt-0.5 flex-shrink-0 text-muted-foreground" aria-hidden="true" />
          <div className="min-w-0">
            <p className="font-mono text-sm break-all text-green-600 dark:text-green-400">
              {likelyMatch.datasetUrn}
            </p>
            <p className="text-xs text-muted-foreground mt-0.5">
              Match: {formatMatchReason(likelyMatch.matchReason, likelyMatch.datasetUrn)}
            </p>
          </div>
        </div>
      ) : (
        <div className={cn(
          "rounded-md border p-3 space-y-2",
          "bg-yellow-50 border-yellow-200 dark:bg-yellow-950/30 dark:border-yellow-800"
        )}>
          <div className="flex items-center gap-2 text-sm text-yellow-800 dark:text-yellow-200">
            <AlertTriangle className="h-4 w-4 flex-shrink-0" aria-hidden="true" />
            <span className="font-medium">No match found</span>
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
      )}
    </div>
  );
}
