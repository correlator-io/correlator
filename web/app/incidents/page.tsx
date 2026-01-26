import { AppShell } from "@/components/layout/app-shell";
import { IncidentList } from "@/components/incidents/incident-list";
import { MOCK_INCIDENTS } from "@/lib/mock-data";

export default function IncidentsPage() {
  return (
    <AppShell title="Incidents">
      <IncidentList incidents={MOCK_INCIDENTS} />
    </AppShell>
  );
}
