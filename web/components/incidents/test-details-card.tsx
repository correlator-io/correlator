import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { StatusBadge } from "./status-badge";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { formatRelativeTime, formatAbsoluteTime, formatDuration } from "@/lib/utils";
import { Clock, Timer, FileText } from "lucide-react";
import type { TestStatus } from "@/lib/types";

interface TestDetailsCardProps {
  test: {
    name: string;
    type: string;
    status: TestStatus;
    message: string;
    executedAt: string;
    durationMs: number;
  };
  dataset: {
    urn: string;
    name: string;
    namespace: string;
  };
}

export function TestDetailsCard({ test, dataset }: TestDetailsCardProps) {
  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between gap-4">
          <CardTitle className="text-base font-medium">Test Details</CardTitle>
          <StatusBadge status={test.status} />
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Test name and type */}
        <div className="space-y-1">
          <p className="text-sm font-medium">{test.name}</p>
          <p className="text-xs text-muted-foreground">Type: {test.type}</p>
        </div>

        {/* Dataset */}
        <div className="space-y-1">
          <p className="text-xs text-muted-foreground">Dataset</p>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <p className="font-mono text-sm truncate cursor-help">{dataset.urn}</p>
              </TooltipTrigger>
              <TooltipContent>
                <p className="font-mono text-xs">{dataset.urn}</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>

        {/* Error message (if any) */}
        {test.message && (
          <div className="space-y-1">
            <p className="text-xs text-muted-foreground flex items-center gap-1">
              <FileText className="h-3 w-3" />
              Message
            </p>
            <div className="rounded-md bg-muted p-3 max-h-32 overflow-y-auto">
              <pre className="text-xs whitespace-pre-wrap font-mono">{test.message}</pre>
            </div>
          </div>
        )}

        {/* Metadata row */}
        <div className="flex flex-wrap gap-4 pt-2 text-xs text-muted-foreground border-t">
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="flex items-center gap-1.5 cursor-help">
                  <Clock className="h-3.5 w-3.5" />
                  {formatRelativeTime(test.executedAt)}
                </span>
              </TooltipTrigger>
              <TooltipContent>
                <p>{formatAbsoluteTime(test.executedAt)}</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>

          <span className="flex items-center gap-1.5">
            <Timer className="h-3.5 w-3.5" />
            {formatDuration(test.durationMs)}
          </span>
        </div>
      </CardContent>
    </Card>
  );
}
