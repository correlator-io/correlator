"use client";

import { useState } from "react";
import { Eye, CheckCircle2, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { MutePopover } from "./mute-popover";
import type { ResolutionStatus } from "@/lib/types";

interface ResolutionActionsProps {
  incidentId: string;
  currentStatus: ResolutionStatus;
  onStatusChange: (
    id: string,
    status: ResolutionStatus,
    options?: { reason?: string; muteDays?: number }
  ) => void;
}

export function ResolutionActions({
  incidentId,
  currentStatus,
  onStatusChange,
}: ResolutionActionsProps) {
  const [pending, setPending] = useState<ResolutionStatus | null>(null);

  const handleAction = (
    newStatus: ResolutionStatus,
    options?: { reason?: string; muteDays?: number }
  ) => {
    setPending(newStatus);
    onStatusChange(incidentId, newStatus, options);
    setTimeout(() => setPending(null), 600);
  };

  if (currentStatus === "resolved" || currentStatus === "muted") {
    return null;
  }

  return (
    <div
      className="flex items-center gap-1"
      onClick={(e) => {
        e.preventDefault();
        e.stopPropagation();
      }}
    >
      {/* Acknowledge — 1 click, no confirmation */}
      {currentStatus === "open" && (
        <Tooltip>
          <TooltipTrigger asChild>
            <Button
              variant="ghost"
              size="icon"
              className="h-7 w-7 text-muted-foreground hover:text-chart-1 hover:bg-chart-1/10"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                handleAction("acknowledged");
              }}
              disabled={pending !== null}
            >
              {pending === "acknowledged" ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <Eye className="h-3.5 w-3.5" />
              )}
              <span className="sr-only">Acknowledge</span>
            </Button>
          </TooltipTrigger>
          <TooltipContent side="bottom">
            <p>Acknowledge — I&apos;m looking at this</p>
          </TooltipContent>
        </Tooltip>
      )}

      {/* Resolve — 1 click */}
      <Tooltip>
        <TooltipTrigger asChild>
          <Button
            variant="ghost"
            size="icon"
            className="h-7 w-7 text-muted-foreground hover:text-status-passed hover:bg-status-passed/10"
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              handleAction("resolved", { reason: "manual" });
            }}
            disabled={pending !== null}
          >
            {pending === "resolved" ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <CheckCircle2 className="h-3.5 w-3.5" />
            )}
            <span className="sr-only">Resolve</span>
          </Button>
        </TooltipTrigger>
        <TooltipContent side="bottom">
          <p>Resolve — this is fixed</p>
        </TooltipContent>
      </Tooltip>

      {/* Mute — click opens duration popover (2 clicks total) */}
      <MutePopover
        onMute={(days) =>
          handleAction("muted", { reason: "false_positive", muteDays: days })
        }
        disabled={pending !== null}
        isPending={pending === "muted"}
      />
    </div>
  );
}
