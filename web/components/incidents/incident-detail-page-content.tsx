"use client";

import { IncidentDetail } from "./incident-detail";
import { IncidentDetailSkeleton } from "./incident-detail-skeleton";
import { IncidentError } from "./incident-error";
import { useIncidentDetail } from "@/hooks/use-incidents";

interface IncidentDetailPageContentProps {
  id: string;
}

export function IncidentDetailPageContent({ id }: IncidentDetailPageContentProps) {
  const { data, isLoading, isError, error } = useIncidentDetail(id);

  if (isLoading) return <IncidentDetailSkeleton />;
  if (isError) return <IncidentError message={error?.message ?? "Failed to load incident"} />;
  if (!data) return <IncidentDetailSkeleton />;

  return <IncidentDetail incident={data} />;
}
