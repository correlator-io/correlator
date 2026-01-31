"use client";

import { useState, useCallback } from "react";
import { useSearchParams, useRouter, usePathname } from "next/navigation";
import { useIncidents } from "@/hooks";
import { IncidentCard } from "./incident-card";
import { IncidentFilter, type FilterValue } from "./incident-filter";
import { IncidentListSkeleton } from "./incident-list-skeleton";
import { IncidentError } from "./incident-error";
import { Button } from "@/components/ui/button";
import { CheckCircle2, Loader2 } from "lucide-react";
import { filterIncidents } from "@/lib/filters";

const PAGE_SIZE = 20;

function isValidFilter(value: string | null): value is FilterValue {
  return value === "all" || value === "failed" || value === "correlation_issues";
}

export function IncidentsPageContent() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  // Read filter from URL, default to "all"
  const filterParam = searchParams.get("filter");
  const filter: FilterValue = isValidFilter(filterParam) ? filterParam : "all";

  // Pagination state
  const [limit, setLimit] = useState(PAGE_SIZE);

  const { data, isLoading, isFetching, error, refetch } = useIncidents({ limit });

  // Update URL when filter changes
  const handleFilterChange = useCallback(
    (newFilter: FilterValue) => {
      const params = new URLSearchParams(searchParams.toString());
      if (newFilter === "all") {
        params.delete("filter");
      } else {
        params.set("filter", newFilter);
      }
      const query = params.toString();
      router.push(`${pathname}${query ? `?${query}` : ""}`, { scroll: false });
    },
    [router, pathname, searchParams]
  );

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

  // Apply client-side filter (API returns all failed/error, we filter further)
  const filteredIncidents = filterIncidents(data.incidents, filter);
  const hasMore = data.total > data.incidents.length;

  return (
    <div className="space-y-4">
      {/* Header with filter */}
      <div className="flex items-center justify-between">
        <p className="text-sm text-muted-foreground">
          {filteredIncidents.length} incident{filteredIncidents.length !== 1 ? "s" : ""}
          {filter !== "all" && " (filtered)"}
          {data.total > 0 && filteredIncidents.length < data.total && (
            <span className="text-muted-foreground/70"> of {data.total}</span>
          )}
        </p>
        <IncidentFilter value={filter} onValueChange={handleFilterChange} />
      </div>

      {/* Incident cards */}
      {filteredIncidents.length > 0 ? (
        <div className="space-y-2">
          {filteredIncidents.map((incident) => (
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
                  `Load more (${data.total - data.incidents.length} remaining)`
                )}
              </Button>
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
