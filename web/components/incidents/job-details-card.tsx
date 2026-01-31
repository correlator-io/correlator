import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ProducerIcon } from "@/components/icons/producer-icon";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { formatRelativeTime, formatAbsoluteTime } from "@/lib/utils";
import { PlayCircle, CheckCircle } from "lucide-react";
import type { Producer } from "@/lib/types";

interface JobDetailsCardProps {
  job: {
    name: string;
    namespace: string;
    runId: string;
    producer: Producer;
    status: string;
    startedAt: string;
    completedAt: string;
  };
}

export function JobDetailsCard({ job }: JobDetailsCardProps) {
  const statusVariant = job.status === "COMPLETE" ? "default" : "secondary";

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between gap-4">
          <CardTitle className="text-base font-medium">Producing Job</CardTitle>
          <Badge variant={statusVariant}>{job.status}</Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
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

          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="flex items-center gap-1.5 cursor-help">
                  <CheckCircle className="h-3.5 w-3.5" />
                  Completed {formatRelativeTime(job.completedAt)}
                </span>
              </TooltipTrigger>
              <TooltipContent>
                <p>{formatAbsoluteTime(job.completedAt)}</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
      </CardContent>
    </Card>
  );
}
