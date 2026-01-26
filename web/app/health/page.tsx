import { AppShell } from "@/components/layout/app-shell";
import { CorrelationHealthDashboard } from "@/components/health/correlation-health-dashboard";
import { MOCK_CORRELATION_HEALTH } from "@/lib/mock-data";

export default function HealthPage() {
  return (
    <AppShell title="Correlation Health">
      <CorrelationHealthDashboard health={MOCK_CORRELATION_HEALTH} />
    </AppShell>
  );
}
