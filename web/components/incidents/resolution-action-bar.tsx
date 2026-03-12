"use client";

import { useState } from "react";
import { Eye, CheckCircle2, Loader2, Bot, User, Clock } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ResolutionBadge } from "./resolution-badge";
import { MutePopover } from "./mute-popover";
import { formatRelativeTime, formatAbsoluteTime } from "@/lib/utils";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type { ResolutionStatus } from "@/lib/types";

interface ResolutionActionBarProps {
  resolutionStatus: ResolutionStatus;
  resolvedBy?: string;
  resolutionReason?: string;
  resolvedAt?: string | null;
  muteExpiresAt?: string | null;
  onStatusChange?: (
    status: ResolutionStatus,
    options?: { reason?: string; muteDays?: number }
  ) => void;
}

export function ResolutionActionBar({
  resolutionStatus,
  resolvedBy,
  resolvedAt,
  muteExpiresAt,
  onStatusChange,
}: ResolutionActionBarProps) {
  const [pending, setPending] = useState<ResolutionStatus | null>(null);

  const handleAction = (
    newStatus: ResolutionStatus,
    options?: { reason?: string; muteDays?: number }
  ) => {
    setPending(newStatus);
    onStatusChange?.(newStatus, options);
    setTimeout(() => setPending(null), 600);
  };

  const isActive =
    resolutionStatus === "open" || resolutionStatus === "acknowledged";

  return (
    <TooltipProvider>
      <div className="flex flex-col sm:flex-row sm:items-center gap-3 rounded-lg border p-3">
        {/* Current status + attribution */}
        <div className="flex items-center gap-2 flex-1 min-w-0 flex-wrap">
          <ResolutionBadge
            status={resolutionStatus}
            resolvedBy={resolvedBy}
            muteExpiresAt={muteExpiresAt ?? undefined}
          />

          {/* Timestamp for when status changed */}
          {resolvedAt && (
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="text-xs text-muted-foreground">
                  {formatRelativeTime(resolvedAt)}
                </span>
              </TooltipTrigger>
              <TooltipContent>
                <p>{formatAbsoluteTime(resolvedAt)}</p>
              </TooltipContent>
            </Tooltip>
          )}

          {/* Acknowledged attribution */}
          {resolutionStatus === "acknowledged" && (
            <span className="inline-flex items-center gap-1 text-xs text-muted-foreground">
              <User className="h-3 w-3" />
              <span>Acknowledged by user</span>
            </span>
          )}

          {/* Resolved attribution */}
          {resolutionStatus === "resolved" && resolvedBy && (
            <span className="inline-flex items-center gap-1 text-xs text-muted-foreground">
              {resolvedBy === "auto" ? (
                <>
                  <Bot className="h-3 w-3" />
                  <span>Auto-resolved on re-pass</span>
                </>
              ) : (
                <>
                  <User className="h-3 w-3" />
                  <span>Manually resolved</span>
                </>
              )}
            </span>
          )}

          {/* Muted attribution + expiry */}
          {resolutionStatus === "muted" && (
            <span className="inline-flex items-center gap-1 text-xs text-muted-foreground">
              <span>False positive / accepted risk</span>
              {muteExpiresAt && (
                <>
                  <span className="text-muted-foreground/50 mx-0.5">·</span>
                  <Clock className="h-3 w-3" />
                  <span>{formatMuteExpiry(muteExpiresAt)}</span>
                </>
              )}
            </span>
          )}
        </div>

        {/* Action buttons — only for active incidents */}
        {isActive && onStatusChange && (
          <div className="flex items-center gap-2 flex-shrink-0">
            {resolutionStatus === "open" && (
              <Button
                variant="outline"
                size="sm"
                className="h-8 gap-1.5"
                onClick={() => handleAction("acknowledged")}
                disabled={pending !== null}
              >
                {pending === "acknowledged" ? (
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                ) : (
                  <Eye className="h-3.5 w-3.5" />
                )}
                Acknowledge
              </Button>
            )}

            <Button
              variant="outline"
              size="sm"
              className="h-8 gap-1.5 text-status-passed border-status-passed/30 hover:bg-status-passed/10"
              onClick={() => handleAction("resolved", { reason: "manual" })}
              disabled={pending !== null}
            >
              {pending === "resolved" ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <CheckCircle2 className="h-3.5 w-3.5" />
              )}
              Resolve
            </Button>

            <MutePopover
              onMute={(days) =>
                handleAction("muted", {
                  reason: "false_positive",
                  muteDays: days,
                })
              }
              disabled={pending !== null}
              isPending={pending === "muted"}
            />
          </div>
        )}
      </div>
    </TooltipProvider>
  );
}

function formatMuteExpiry(expiresAt: string): string {
  const now = new Date();
  const expiry = new Date(expiresAt);
  const diffDays = Math.ceil((expiry.getTime() - now.getTime()) / 86_400_000);
  if (diffDays <= 0) return "Mute expired";
  if (diffDays === 1) return "Expires tomorrow";
  return `Expires in ${diffDays} days`;
}
