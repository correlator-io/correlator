"use client";

import { useState, useCallback } from "react";
import { useIncidents } from "@/hooks";
import { IncidentCard } from "./incident-card";
import { OrphanCalloutBanner } from "./orphan-callout-banner";
import { IncidentListSkeleton } from "./incident-list-skeleton";
import { IncidentError } from "./incident-error";
import { Button } from "@/components/ui/button";
import { CheckCircle2, Loader2 } from "lucide-react";

const PAGE_SIZE = 20;

export function IncidentsPageContent() {
  // Pagination state
  const [limit, setLimit] = useState(PAGE_SIZE);

  const { data, isLoading, isFetching, error, refetch } = useIncidents({ limit });

  // Load more handler
  const handleLoadMore = useCallback(() => {
    setLimit((prev) => prev + PAGE_SIZE);
  }, []);

  if (isLoading) {
    return <IncidentListSkeleton />;
  }

  if (error) {
    return (
      <IncidentError
        message={error instanceof Error ? error.message : "Unknown error"}
        onRetry={() => refetch()}
      />
    );
  }

  if (!data) {
    return <IncidentListSkeleton />;
  }

  const { incidents, total, orphanCount } = data;
  const hasMore = total > incidents.length;

  return (
    <div className="space-y-4">
      {/* Orphan callout banner */}
      <OrphanCalloutBanner orphanCount={orphanCount} />

      {/* Header */}
      <p className="text-sm text-muted-foreground">
        {incidents.length} incident{incidents.length !== 1 ? "s" : ""}
        {total > incidents.length && (
          <span className="text-muted-foreground/70"> of {total}</span>
        )}
      </p>

      {/* Incident cards */}
      {incidents.length > 0 ? (
        <div className="space-y-2">
          {incidents.map((incident) => (
            <IncidentCard key={incident.id} incident={incident} />
          ))}

          {/* Load more button */}
          {hasMore && (
            <div className="pt-4 text-center">
              <Button
                variant="outline"
                onClick={handleLoadMore}
                disabled={isFetching}
              >
                {isFetching ? (
                  <>
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                    Loading...
                  </>
                ) : (
                  `Load more (${total - incidents.length} remaining)`
                )}
              </Button>
            </div>
          )}
        </div>
      ) : (
        <EmptyState />
      )}
    </div>
  );
}

function EmptyState() {
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
