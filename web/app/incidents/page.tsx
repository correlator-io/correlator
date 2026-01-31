import { Suspense } from "react";
import { AppShell } from "@/components/layout/app-shell";
import { IncidentsPageContent } from "@/components/incidents/incidents-page-content";
import { IncidentListSkeleton } from "@/components/incidents/incident-list-skeleton";

export default function IncidentsPage() {
  return (
    <AppShell title="Incidents">
      <Suspense fallback={<IncidentListSkeleton />}>
        <IncidentsPageContent />
      </Suspense>
    </AppShell>
  );
}
