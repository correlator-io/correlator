import { cn } from "@/lib/utils";
import type { TestStatus } from "@/lib/types";

interface StatusBadgeProps {
  status: TestStatus;
  className?: string;
  showLabel?: boolean;
}

const statusConfig: Record<
  TestStatus,
  { label: string; className: string; dotClassName: string }
> = {
  failed: {
    label: "Failed",
    className: "bg-status-failed text-status-failed-foreground",
    dotClassName: "bg-status-failed",
  },
  warning: {
    label: "Warning",
    className: "bg-status-warning text-status-warning-foreground",
    dotClassName: "bg-status-warning",
  },
  passed: {
    label: "Passed",
    className: "bg-status-passed text-status-passed-foreground",
    dotClassName: "bg-status-passed",
  },
  unknown: {
    label: "Unknown",
    className: "bg-status-unknown text-status-unknown-foreground",
    dotClassName: "bg-status-unknown",
  },
};

export function StatusBadge({
  status,
  className,
  showLabel = true,
}: StatusBadgeProps) {
  const config = statusConfig[status];

  if (showLabel) {
    return (
      <span
        className={cn(
          "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium",
          config.className,
          className
        )}
      >
        {config.label}
      </span>
    );
  }

  // Dot-only variant
  return (
    <span
      role="img"
      className={cn("inline-block h-2 w-2 rounded-full", config.dotClassName, className)}
      title={config.label}
      aria-label={config.label}
    />
  );
}

export function StatusDot({
  status,
  className,
}: {
  status: TestStatus;
  className?: string;
}) {
  return <StatusBadge status={status} showLabel={false} className={className} />;
}
