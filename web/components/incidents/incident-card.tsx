"use client";

import Link from "next/link";
import { AlertTriangle, ArrowRight, GitBranch, RotateCcw } from "lucide-react";
import { StatusDot } from "./status-badge";
import { ResolutionBadge } from "./resolution-badge";
import { ResolutionActions } from "./resolution-actions";
import { ProducerIcon } from "@/components/icons/producer-icon";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import {
  cn,
  formatRelativeTime,
  formatAbsoluteTime,
  extractDatasetName,
} from "@/lib/utils";
import type { Incident, ResolutionStatus } from "@/lib/types";

interface IncidentCardProps {
  incident: Incident;
  className?: string;
  onStatusChange?: (
    id: string,
    status: ResolutionStatus,
    options?: { reason?: string; muteDays?: number }
  ) => void;
}

export function IncidentCard({ incident, className, onStatusChange }: IncidentCardProps) {
  const {
    id,
    testName,
    testType,
    testStatus,
    datasetUrn,
    producer,
    jobName,
    downstreamCount,
    hasCorrelationIssue,
    executedAt,
    resolutionStatus,
    retryContext,
    resolvedBy,
    muteExpiresAt,
  } = incident;

  const datasetName = extractDatasetName(datasetUrn);
  const isResolved = resolutionStatus === "resolved" || resolutionStatus === "muted";

  return (
    <TooltipProvider>
      <Link href={`/incidents/${id}`} className="block group">
        <Card
          className={cn(
            "transition-colors hover:bg-muted/50 cursor-pointer",
            isResolved && "opacity-60",
            className
          )}
        >
          {/* Header: INC-{id} · Human-readable title + resolution badge + actions */}
          <CardHeader className="pb-1 pt-4 px-4">
            <div className="flex items-center gap-2">
              <StatusDot status={testStatus} className="h-2.5 w-2.5 flex-shrink-0" />
              <CardTitle className="text-sm font-medium leading-tight flex-1 min-w-0">
                <span className="text-muted-foreground">INC-{id}</span>
                <span className="mx-1.5 text-muted-foreground/50">·</span>
                <span className="group-hover:text-primary">{testName}</span>
              </CardTitle>

              {/* Resolution badge — visible on all states except open */}
              {resolutionStatus !== "open" && (
                <ResolutionBadge
                  status={resolutionStatus}
                  resolvedBy={resolvedBy}
                  muteExpiresAt={muteExpiresAt}
                  className="flex-shrink-0"
                />
              )}

              {/* Retry badge — compact indicator when retries exist */}
              {retryContext && (
                <Tooltip>
                  <TooltipTrigger asChild>
                    <span
                      className={cn(
                        "inline-flex items-center gap-1 rounded-full border px-1.5 py-0.5 text-xs font-medium flex-shrink-0",
                        retryContext.allFailed
                          ? "border-status-failed/30 text-status-failed bg-status-failed/10"
                          : resolutionStatus === "resolved"
                            ? "border-status-passed/30 text-status-passed bg-status-passed/10"
                            : "border-status-warning/30 text-status-warning bg-status-warning/10"
                      )}
                    >
                      <RotateCcw className="h-3 w-3" />
                      {retryContext.totalAttempts}
                    </span>
                  </TooltipTrigger>
                  <TooltipContent>
                    <p>
                      Attempt {retryContext.currentAttempt} of{" "}
                      {retryContext.totalAttempts}
                      {retryContext.allFailed
                        ? " — all attempts failed"
                        : " — passed on retry"}
                    </p>
                  </TooltipContent>
                </Tooltip>
              )}

              {/* Action buttons — only for active incidents */}
              {onStatusChange && (
                <ResolutionActions
                  incidentId={id}
                  currentStatus={resolutionStatus}
                  onStatusChange={onStatusChange}
                />
              )}

              <ArrowRight className="h-4 w-4 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0" />
            </div>
          </CardHeader>

          <CardContent className="px-4 pb-4 pt-0 space-y-3">
            {/* Type and Dataset info */}
            <div className="text-sm space-y-1 pl-[18px]">
              <p>
                <span className="text-muted-foreground">Type:</span>{" "}
                <span>{testType}</span>
              </p>
              <p className="truncate">
                <span className="text-muted-foreground">Dataset:</span>{" "}
                <span className="font-medium">{datasetName}</span>
              </p>
            </div>

            {/* Job and metadata row */}
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground pt-2 border-t pl-[18px]">
              {/* Job with producer */}
              <span className="inline-flex items-center gap-1.5 min-w-0">
                <span className="text-muted-foreground/70">Job:</span>
                <ProducerIcon producer={producer} size={14} className="flex-shrink-0" />
                <span className="truncate max-w-[200px]">{jobName}</span>
              </span>

              {/* Downstream count - amber color */}
              {(testStatus === "failed" || testStatus === "warning") &&
                !hasCorrelationIssue &&
                downstreamCount > 0 && (
                  <span className="inline-flex items-center gap-1 text-status-warning">
                    <GitBranch className="h-3.5 w-3.5" />
                    <span>{downstreamCount} downstream</span>
                  </span>
                )}

              {/* Correlation warning */}
              {hasCorrelationIssue && (
                <span className="inline-flex items-center gap-1 text-status-warning">
                  <AlertTriangle className="h-3.5 w-3.5" />
                  <span>No correlation</span>
                </span>
              )}

              {/* Timestamp - pushed to right */}
              <Tooltip>
                <TooltipTrigger asChild>
                  <span className="ml-auto flex-shrink-0">
                    {formatRelativeTime(executedAt)}
                  </span>
                </TooltipTrigger>
                <TooltipContent>
                  <p>{formatAbsoluteTime(executedAt)}</p>
                </TooltipContent>
              </Tooltip>
            </div>
          </CardContent>
        </Card>
      </Link>
    </TooltipProvider>
  );
}
