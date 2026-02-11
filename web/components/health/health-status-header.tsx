"use client";

import { AlertTriangle, CheckCircle2, XCircle } from "lucide-react";
import { cn } from "@/lib/utils";

interface HealthStatusHeaderProps {
  orphanCount: number;
  correlationRate: number;
  totalDatasets: number;
  correlatedDatasets: number;
}

type HealthStatus = "healthy" | "warning" | "critical";

function getHealthStatus(correlationRate: number): HealthStatus {
  if (correlationRate >= 1) return "healthy";
  if (correlationRate >= 0.5) return "warning";
  return "critical";
}

const statusConfig: Record<
  HealthStatus,
  {
    icon: typeof CheckCircle2;
    containerClass: string;
    iconClass: string;
    textClass: string;
  }
> = {
  healthy: {
    icon: CheckCircle2,
    containerClass: "bg-green-50 border-green-200 dark:bg-green-950/30 dark:border-green-900",
    iconClass: "text-green-600 dark:text-green-400",
    textClass: "text-green-800 dark:text-green-200",
  },
  warning: {
    icon: AlertTriangle,
    containerClass: "bg-yellow-50 border-yellow-200 dark:bg-yellow-950/30 dark:border-yellow-900",
    iconClass: "text-yellow-600 dark:text-yellow-400",
    textClass: "text-yellow-800 dark:text-yellow-200",
  },
  critical: {
    icon: XCircle,
    containerClass: "bg-red-50 border-red-200 dark:bg-red-950/30 dark:border-red-900",
    iconClass: "text-red-600 dark:text-red-400",
    textClass: "text-red-800 dark:text-red-200",
  },
};

export function HealthStatusHeader({
  orphanCount,
  correlationRate,
  totalDatasets,
  correlatedDatasets,
}: HealthStatusHeaderProps) {
  const status = getHealthStatus(correlationRate);
  const config = statusConfig[status];
  const Icon = config.icon;
  const percentage = Math.round(correlationRate * 100);

  const statusMessage =
    orphanCount === 0
      ? "All datasets correlated"
      : orphanCount === 1
        ? "1 dataset needs attention"
        : `${orphanCount} datasets need attention`;

  return (
    <div
      className={cn(
        "flex items-center justify-between rounded-lg border p-4",
        config.containerClass
      )}
      role="status"
      aria-live="polite"
    >
      <div className="flex items-center gap-3">
        <Icon className={cn("h-6 w-6 flex-shrink-0", config.iconClass)} aria-hidden="true" />
        <span className={cn("font-medium", config.textClass)}>{statusMessage}</span>
      </div>
      <div className="text-sm text-muted-foreground">
        {percentage}% correlated · {correlatedDatasets} of {totalDatasets} datasets
      </div>
    </div>
  );
}
