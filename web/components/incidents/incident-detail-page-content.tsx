"use client";

import { IncidentDetail } from "./incident-detail";
import { IncidentDetailSkeleton } from "./incident-detail-skeleton";
import { IncidentError } from "./incident-error";
import {
  MOCK_INCIDENT_DETAIL,
  MOCK_ACKNOWLEDGED_DETAIL,
  MOCK_RESOLVED_DETAIL,
  MOCK_MANUALLY_RESOLVED_DETAIL,
  MOCK_MUTED_DETAIL,
} from "@/lib/mock-data";
import type { IncidentDetail as IncidentDetailType } from "@/lib/types";

const MOCK_DETAILS: Record<string, IncidentDetailType> = {
  "32": MOCK_INCIDENT_DETAIL,
  "28": MOCK_ACKNOWLEDGED_DETAIL,
  "18": MOCK_RESOLVED_DETAIL,
  "12": MOCK_MANUALLY_RESOLVED_DETAIL,
  "9": MOCK_MUTED_DETAIL,
};

interface IncidentDetailPageContentProps {
  id: string;
}

export function IncidentDetailPageContent({ id }: IncidentDetailPageContentProps) {
  const data = MOCK_DETAILS[id] ?? { ...MOCK_INCIDENT_DETAIL, id };

  if (!data) {
    return <IncidentDetailSkeleton />;
  }

  return <IncidentDetail incident={data} />;
}
