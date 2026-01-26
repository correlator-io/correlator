"use client";

import Link from "next/link";
import { AlertTriangle, ArrowRight, GitBranch } from "lucide-react";
import { StatusDot } from "./status-badge";
import { ProducerIcon } from "@/components/icons/producer-icon";
import { Card } from "@/components/ui/card";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { cn, formatRelativeTime, formatAbsoluteTime } from "@/lib/utils";
import type { Incident } from "@/lib/types";

interface IncidentCardProps {
  incident: Incident;
  className?: string;
}

export function IncidentCard({ incident, className }: IncidentCardProps) {
  const {
    id,
    testName,
    testStatus,
    datasetUrn,
    datasetName,
    producer,
    jobName,
    downstreamCount,
    hasCorrelationIssue,
    executedAt,
  } = incident;

  return (
    <TooltipProvider>
      <Link href={`/incidents/${id}`} className="block group">
        <Card
          className={cn(
            "p-4 transition-colors hover:bg-muted/50 cursor-pointer",
            className
          )}
        >
          <div className="flex items-start gap-3">
            {/* Status indicator */}
            <div className="pt-1.5">
              <StatusDot status={testStatus} className="h-2.5 w-2.5" />
            </div>

            {/* Main content */}
            <div className="flex-1 min-w-0 space-y-1.5">
              {/* Test name row */}
              <div className="flex items-start justify-between gap-2">
                <h3 className="font-medium text-sm leading-tight truncate group-hover:text-primary">
                  {testName}
                </h3>
                <span className="flex-shrink-0">
                  <ArrowRight className="h-4 w-4 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity" />
                </span>
              </div>

              {/* Dataset URN */}
              <Tooltip>
                <TooltipTrigger asChild>
                  <p className="font-mono text-xs text-muted-foreground truncate">
                    {datasetUrn}
                  </p>
                </TooltipTrigger>
                <TooltipContent>
                  <p className="font-mono text-xs">{datasetUrn}</p>
                </TooltipContent>
              </Tooltip>

              {/* Info row */}
              <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
                {/* Producer + Job */}
                <span className="inline-flex items-center gap-1.5">
                  <ProducerIcon producer={producer} size={14} />
                  <span className="truncate max-w-[150px]">{jobName}</span>
                </span>

                {/* Downstream count (only show for failed/warning with correlation) */}
                {(testStatus === "failed" || testStatus === "warning") &&
                  !hasCorrelationIssue &&
                  downstreamCount > 0 && (
                    <span className="inline-flex items-center gap-1 text-destructive">
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

                {/* Timestamp */}
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
            </div>
          </div>
        </Card>
      </Link>
    </TooltipProvider>
  );
}
