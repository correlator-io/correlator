import { notFound } from "next/navigation";
import { AppShell } from "@/components/layout/app-shell";
import { IncidentDetail } from "@/components/incidents/incident-detail";
import { getIncidentDetail } from "@/lib/mock-data";

interface IncidentDetailPageProps {
  params: Promise<{ id: string }>;
}

export default async function IncidentDetailPage({
  params,
}: IncidentDetailPageProps) {
  const { id } = await params;
  const incident = getIncidentDetail(id);

  if (!incident) {
    notFound();
  }

  return (
    <AppShell title="Incident Detail">
      <IncidentDetail incident={incident} />
    </AppShell>
  );
}
