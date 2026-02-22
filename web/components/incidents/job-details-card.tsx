import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { ProducerIcon } from "@/components/icons/producer-icon";
import { JobOrchestrationChain } from "./job-orchestration-chain";
import { buildOrchestrationChain } from "@/lib/orchestration";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { formatRelativeTime, formatAbsoluteTime } from "@/lib/utils";
import { PlayCircle, CheckCircle } from "lucide-react";
import type { Producer, ParentJob } from "@/lib/types";

interface JobDetailsCardProps {
  job: {
    name: string;
    namespace: string;
    runId: string;
    producer: Producer;
    status: string;
    startedAt: string;
    completedAt: string;
    parent?: ParentJob;
    rootParent?: ParentJob;
  };
}

export function JobDetailsCard({ job }: JobDetailsCardProps) {
  const chain = buildOrchestrationChain(job);

  // Use root > parent > job status fallback for the header badge
  const displayStatus = job.rootParent?.status || job.parent?.status || job.status;
  const displayCompletedAt = job.rootParent?.completedAt || job.parent?.completedAt || job.completedAt;

  const statusVariant = displayStatus === "COMPLETE" ? "default" : "secondary";

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between gap-4">
          <CardTitle className="text-base font-medium">Producing Job</CardTitle>
          <Badge variant={statusVariant}>{displayStatus}</Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Orchestration chain (only renders when parent exists) */}
        {chain.length > 0 && (
          <>
            <JobOrchestrationChain levels={chain} />
            <Separator />
          </>
        )}

        {/* Job name and namespace */}
        <div className="space-y-1">
          <p className="text-sm font-medium">{job.name}</p>
          <p className="text-xs text-muted-foreground">Namespace: {job.namespace}</p>
        </div>

        {/* Producer */}
        <div className="flex items-center gap-2">
          <ProducerIcon producer={job.producer} showLabel />
        </div>

        {/* Run ID */}
        <div className="space-y-1">
          <p className="text-xs text-muted-foreground">Run ID</p>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <p className="font-mono text-xs truncate cursor-help bg-muted px-2 py-1 rounded">
                  {job.runId}
                </p>
              </TooltipTrigger>
              <TooltipContent>
                <p className="font-mono text-xs">{job.runId}</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>

        {/* Timestamps */}
        <div className="flex flex-wrap gap-4 pt-2 text-xs text-muted-foreground border-t">
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="flex items-center gap-1.5 cursor-help">
                  <PlayCircle className="h-3.5 w-3.5" />
                  Started {formatRelativeTime(job.startedAt)}
                </span>
              </TooltipTrigger>
              <TooltipContent>
                <p>{formatAbsoluteTime(job.startedAt)}</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>

          {displayCompletedAt && (
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <span className="flex items-center gap-1.5 cursor-help">
                    <CheckCircle className="h-3.5 w-3.5" />
                    Completed {formatRelativeTime(displayCompletedAt)}
                  </span>
                </TooltipTrigger>
                <TooltipContent>
                  <p>{formatAbsoluteTime(displayCompletedAt)}</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
