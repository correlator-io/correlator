"use client";

import Link from "next/link";
import { RotateCcw, ExternalLink } from "lucide-react";
import { StatusDot } from "./status-badge";
import { ResolutionBadge } from "./resolution-badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatRelativeTime, formatAbsoluteTime } from "@/lib/utils";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import type { RetryContextDetail, ResolutionStatus } from "@/lib/types";

interface RetryTimelineProps {
  retryContext: RetryContextDetail;
  currentIncidentId: string;
  currentResolutionStatus: ResolutionStatus;
}

export function RetryTimeline({
  retryContext,
  currentIncidentId,
  currentResolutionStatus,
}: RetryTimelineProps) {
  const allAttempts = [
    ...retryContext.otherAttempts.map((a) => ({
      incidentId: a.incidentId,
      attempt: a.attempt,
      status: a.status,
      executedAt: a.executedAt,
      resolutionStatus: a.resolutionStatus,
      isCurrent: false,
    })),
    {
      incidentId: currentIncidentId,
      attempt: retryContext.currentAttempt,
      status: "failed" as const,
      executedAt: "", // current, shown differently
      resolutionStatus: currentResolutionStatus,
      isCurrent: true,
    },
  ].sort((a, b) => a.attempt - b.attempt);

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm font-medium flex items-center gap-2">
          <RotateCcw className="h-4 w-4 text-muted-foreground" />
          Retry History
          <span className="text-xs text-muted-foreground font-normal">
            {retryContext.totalAttempts} attempt
            {retryContext.totalAttempts !== 1 ? "s" : ""}
            {retryContext.allFailed
              ? " — all failed"
              : " — passed on retry"}
          </span>
        </CardTitle>
      </CardHeader>
      <CardContent className="pt-0">
        <TooltipProvider>
          <div className="relative">
            {/* Timeline line */}
            <div className="absolute left-[9px] top-3 bottom-3 w-px bg-border" />

            {/* Attempts */}
            <div className="space-y-3">
              {allAttempts.map((attempt) => (
                <div
                  key={attempt.attempt}
                  className="relative flex items-start gap-3 pl-0"
                >
                  {/* Timeline dot */}
                  <div className="relative z-10 flex-shrink-0 mt-0.5">
                    <StatusDot
                      status={attempt.status}
                      className="h-[18px] w-[18px]"
                    />
                  </div>

                  {/* Attempt content */}
                  <div
                    className={cn(
                      "flex-1 min-w-0 flex items-center gap-2 flex-wrap",
                      attempt.isCurrent && "font-medium"
                    )}
                  >
                    <span className="text-sm">
                      Attempt {attempt.attempt}
                    </span>

                    <ResolutionBadge
                      status={attempt.resolutionStatus}
                      className="text-[10px] px-1.5 py-0"
                    />

                    {attempt.isCurrent ? (
                      <span className="text-xs text-muted-foreground">
                        (this incident)
                      </span>
                    ) : (
                      <>
                        {attempt.executedAt && (
                          <Tooltip>
                            <TooltipTrigger asChild>
                              <span className="text-xs text-muted-foreground">
                                {formatRelativeTime(attempt.executedAt)}
                              </span>
                            </TooltipTrigger>
                            <TooltipContent>
                              <p>{formatAbsoluteTime(attempt.executedAt)}</p>
                            </TooltipContent>
                          </Tooltip>
                        )}
                        <Link
                          href={`/incidents/${attempt.incidentId}`}
                          className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
                        >
                          INC-{attempt.incidentId}
                          <ExternalLink className="h-3 w-3" />
                        </Link>
                      </>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </TooltipProvider>
      </CardContent>
    </Card>
  );
}
