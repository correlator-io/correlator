"use client";

import { useCorrelationHealth } from "@/hooks";
import { CorrelationHealthDashboard } from "./correlation-health-dashboard";
import { HealthSkeleton } from "./health-skeleton";
import { HealthError } from "./health-error";

export function HealthPageContent() {
  const { data, isLoading, error, refetch } = useCorrelationHealth();

  if (isLoading) {
    return <HealthSkeleton />;
  }

  if (error) {
    return (
      <HealthError
        message={error instanceof Error ? error.message : "Unknown error"}
        onRetry={() => refetch()}
      />
    );
  }

  if (!data) {
    return <HealthSkeleton />;
  }

  return <CorrelationHealthDashboard health={data} />;
}
