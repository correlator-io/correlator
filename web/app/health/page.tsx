import { AppShell } from "@/components/layout/app-shell";
import { HealthPageContent } from "@/components/health/health-page-content";

export default function HealthPage() {
  return (
    <AppShell title="Correlation Health">
      <HealthPageContent />
    </AppShell>
  );
}
