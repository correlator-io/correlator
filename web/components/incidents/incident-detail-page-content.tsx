"use client";

import { useIncidentDetail } from "@/hooks";
import { IncidentDetail } from "./incident-detail";
import { IncidentDetailSkeleton } from "./incident-detail-skeleton";
import { IncidentError } from "./incident-error";
import { ApiError } from "@/lib/api";

interface IncidentDetailPageContentProps {
  id: string;
}

export function IncidentDetailPageContent({ id }: IncidentDetailPageContentProps) {
  const { data, isLoading, error, refetch } = useIncidentDetail(id);

  if (isLoading) {
    return <IncidentDetailSkeleton />;
  }

  if (error) {
    // Handle 404 specifically
    if (error instanceof ApiError && error.status === 404) {
      return (
        <IncidentError
          message="Incident not found. It may have been resolved or the ID is invalid."
          onRetry={() => refetch()}
        />
      );
    }

    return (
      <IncidentError
        message={error instanceof Error ? error.message : "Unknown error"}
        onRetry={() => refetch()}
      />
    );
  }

  if (!data) {
    return <IncidentDetailSkeleton />;
  }

  return <IncidentDetail incident={data} />;
}
