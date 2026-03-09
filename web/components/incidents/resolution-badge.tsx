"use client";

import { cn, formatRelativeTime } from "@/lib/utils";
import { Eye, CheckCircle2, VolumeX } from "lucide-react";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import type { ResolutionStatus } from "@/lib/types";

interface ResolutionBadgeProps {
  status: ResolutionStatus;
  resolvedBy?: string;
  muteExpiresAt?: string;
  className?: string;
}

const resolutionConfig: Record<
  ResolutionStatus,
  { label: string; icon: typeof Eye; className: string }
> = {
  open: {
    label: "Open",
    icon: Eye,
    className: "bg-status-failed/15 text-status-failed border-status-failed/30",
  },
  acknowledged: {
    label: "Acknowledged",
    icon: Eye,
    className: "bg-chart-1/15 text-chart-1 border-chart-1/30",
  },
  resolved: {
    label: "Resolved",
    icon: CheckCircle2,
    className: "bg-status-passed/15 text-status-passed border-status-passed/30",
  },
  muted: {
    label: "Muted",
    icon: VolumeX,
    className: "bg-muted text-muted-foreground border-border",
  },
};

function getMuteExpiryText(muteExpiresAt: string): string {
  const now = new Date();
  const expiry = new Date(muteExpiresAt);
  const diffDays = Math.ceil((expiry.getTime() - now.getTime()) / 86_400_000);
  if (diffDays <= 0) return "Mute expired";
  if (diffDays === 1) return "Expires tomorrow";
  return `Expires in ${diffDays} days`;
}

export function ResolutionBadge({
  status,
  resolvedBy,
  muteExpiresAt,
  className,
}: ResolutionBadgeProps) {
  const config = resolutionConfig[status];
  const Icon = config.icon;

  const label =
    status === "resolved" && resolvedBy === "auto"
      ? "Auto-resolved"
      : config.label;

  const tooltipText =
    status === "muted" && muteExpiresAt
      ? getMuteExpiryText(muteExpiresAt)
      : null;

  const badge = (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs font-medium",
        config.className,
        className
      )}
    >
      <Icon className="h-3 w-3" />
      {label}
    </span>
  );

  if (tooltipText) {
    return (
      <Tooltip>
        <TooltipTrigger asChild>{badge}</TooltipTrigger>
        <TooltipContent>
          <p>{tooltipText}</p>
        </TooltipContent>
      </Tooltip>
    );
  }

  return badge;
}
