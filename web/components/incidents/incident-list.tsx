"use client";

import { useState } from "react";
import { IncidentCard } from "./incident-card";
import { IncidentFilter, type FilterValue } from "./incident-filter";
import { Button } from "@/components/ui/button";
import { filterIncidents } from "@/lib/filters";
import type { Incident } from "@/lib/types";
import { CheckCircle2 } from "lucide-react";

interface IncidentListProps {
  incidents: Incident[];
}

export function IncidentList({ incidents }: IncidentListProps) {
  const [filter, setFilter] = useState<FilterValue>("all");
  const filteredIncidents = filterIncidents(incidents, filter);

  return (
    <div className="space-y-4">
      {/* Header with filter */}
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          {filteredIncidents.length} incident{filteredIncidents.length !== 1 ? "s" : ""}
          {filter !== "all" && " (filtered)"}
        </p>
        <IncidentFilter value={filter} onValueChange={setFilter} />
      </div>

      {/* Incident cards */}
      {filteredIncidents.length > 0 ? (
        <div className="space-y-2">
          {filteredIncidents.map((incident) => (
            <IncidentCard key={incident.id} incident={incident} />
          ))}

          {/* Load more button placeholder */}
          {incidents.length > 5 && (
            <div className="pt-4 text-center">
              <Button variant="outline" disabled>
                Load more
              </Button>
              <p className="mt-2 text-xs text-muted-foreground">
                Pagination will be wired in API integration phase
              </p>
            </div>
          )}
        </div>
      ) : (
        <EmptyState filter={filter} />
      )}
    </div>
  );
}

function EmptyState({ filter }: { filter: FilterValue }) {
  if (filter === "all") {
    return (
      <div className="rounded-lg border border-dashed border-border p-8 text-center">
        <CheckCircle2 className="mx-auto h-12 w-12 text-status-passed" />
        <h3 className="mt-4 font-medium">No incidents</h3>
        <p className="mt-1 text-sm text-muted-foreground">
          All tests are passing. Your data pipeline is healthy.
        </p>
      </div>
    );
  }

  const filterLabels: Record<FilterValue, string> = {
    all: "incidents",
    failed: "failed tests",
    correlation_issues: "correlation issues",
  };

  return (
    <div className="rounded-lg border border-dashed border-border p-8 text-center">
      <p className="text-muted-foreground">
        No {filterLabels[filter]} found.
      </p>
    </div>
  );
}
