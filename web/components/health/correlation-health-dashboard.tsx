"use client";

import { MetricsCards } from "./metrics-cards";
import { OrphanNamespacesTable } from "./orphan-namespaces-table";
import { YamlConfigBlock } from "./yaml-config-block";
import { HealthyState } from "./healthy-state";
import { AlertTriangle, Wrench } from "lucide-react";
import type { CorrelationHealth } from "@/lib/types";
import { generateYamlConfig } from "@/lib/mock-data";

interface CorrelationHealthDashboardProps {
  health: CorrelationHealth;
}

export function CorrelationHealthDashboard({ health }: CorrelationHealthDashboardProps) {
  const { correlationRate, totalDatasets, orphanNamespaces } = health;
  const hasOrphans = orphanNamespaces.length > 0;
  const yamlConfig = generateYamlConfig(orphanNamespaces);

  return (
    <div className="space-y-8">
      {/* Metrics overview */}
      <MetricsCards
        correlationRate={correlationRate}
        orphanCount={orphanNamespaces.length}
        totalDatasets={totalDatasets}
      />

      {/* Orphan namespaces section */}
      {hasOrphans ? (
        <>
          <div className="space-y-4">
            <div className="flex items-center gap-2">
              <AlertTriangle className="h-5 w-5 text-status-warning" />
              <h2 className="font-medium text-lg">Orphan Namespaces</h2>
            </div>
            <p className="text-sm text-muted-foreground">
              These namespaces couldn&apos;t be matched to any known data producer.
              Test results from these namespaces cannot be correlated to their producing jobs.
            </p>
            <OrphanNamespacesTable orphanNamespaces={orphanNamespaces} />
          </div>

          {/* How to fix section */}
          <div className="space-y-4">
            <div className="flex items-center gap-2">
              <Wrench className="h-5 w-5 text-muted-foreground" />
              <h2 className="font-medium text-lg">How to Fix</h2>
            </div>
            <p className="text-sm text-muted-foreground">
              Add namespace aliases to your <code className="font-mono text-xs bg-muted px-1.5 py-0.5 rounded">correlator.yaml</code> configuration file.
              This maps the orphan namespaces to their canonical equivalents.
            </p>
            <YamlConfigBlock yaml={yamlConfig} />
            <p className="text-xs text-muted-foreground">
              After updating the config, restart the Correlator server for changes to take effect.
            </p>
          </div>
        </>
      ) : (
        <HealthyState />
      )}
    </div>
  );
}
