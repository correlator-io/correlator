import { Card, CardContent } from "@/components/ui/card";
import { TrendingUp, AlertTriangle, Database } from "lucide-react";
import { cn } from "@/lib/utils";

interface MetricsCardsProps {
  correlationRate: number;
  orphanCount: number;
  totalDatasets: number;
}

export function MetricsCards({
  correlationRate,
  orphanCount,
  totalDatasets,
}: MetricsCardsProps) {
  const ratePercent = Math.round(correlationRate * 100);
  const isHealthy = ratePercent >= 95;
  const isWarning = ratePercent >= 80 && ratePercent < 95;
  const isCritical = ratePercent < 80;

  return (
    <div className="grid gap-4 sm:grid-cols-3">
      {/* Correlation Rate */}
      <Card
        className={cn(
          "border-l-4",
          isHealthy && "border-l-status-passed",
          isWarning && "border-l-status-warning",
          isCritical && "border-l-status-failed"
        )}
      >
        <CardContent className="pt-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-muted-foreground">Correlation Rate</p>
              <p
                className={cn(
                  "text-3xl font-bold",
                  isHealthy && "text-status-passed",
                  isWarning && "text-status-warning",
                  isCritical && "text-status-failed"
                )}
              >
                {ratePercent}%
              </p>
            </div>
            <TrendingUp
              className={cn(
                "h-8 w-8 opacity-20",
                isHealthy && "text-status-passed",
                isWarning && "text-status-warning",
                isCritical && "text-status-failed"
              )}
            />
          </div>
          <p className="mt-2 text-xs text-muted-foreground">
            {isHealthy
              ? "All namespaces aligned"
              : isWarning
                ? "Some namespaces need attention"
                : "Critical namespace issues"}
          </p>
        </CardContent>
      </Card>

      {/* Orphan Namespaces */}
      <Card
        className={cn(
          "border-l-4",
          orphanCount === 0 ? "border-l-status-passed" : "border-l-status-warning"
        )}
      >
        <CardContent className="pt-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-muted-foreground">Orphan Namespaces</p>
              <p
                className={cn(
                  "text-3xl font-bold",
                  orphanCount === 0 ? "text-status-passed" : "text-status-warning"
                )}
              >
                {orphanCount}
              </p>
            </div>
            <AlertTriangle
              className={cn(
                "h-8 w-8 opacity-20",
                orphanCount === 0 ? "text-status-passed" : "text-status-warning"
              )}
            />
          </div>
          <p className="mt-2 text-xs text-muted-foreground">
            {orphanCount === 0
              ? "No unmatched namespaces"
              : `${orphanCount} namespace${orphanCount !== 1 ? "s" : ""} need aliasing`}
          </p>
        </CardContent>
      </Card>

      {/* Total Datasets */}
      <Card className="border-l-4 border-l-primary">
        <CardContent className="pt-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm text-muted-foreground">Total Datasets</p>
              <p className="text-3xl font-bold">{totalDatasets}</p>
            </div>
            <Database className="h-8 w-8 opacity-20 text-primary" />
          </div>
          <p className="mt-2 text-xs text-muted-foreground">
            Tracked across all producers
          </p>
        </CardContent>
      </Card>
    </div>
  );
}
