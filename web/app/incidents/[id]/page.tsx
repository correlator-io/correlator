"use client";

import { use } from "react";
import { AppShell } from "@/components/layout/app-shell";
import { IncidentDetailPageContent } from "@/components/incidents/incident-detail-page-content";

interface IncidentDetailPageProps {
  params: Promise<{ id: string }>;
}

export default function IncidentDetailPage({ params }: IncidentDetailPageProps) {
  const { id } = use(params);

  return (
    <AppShell title="Incident Detail">
      <IncidentDetailPageContent id={id} />
    </AppShell>
  );
}
