"use client";

import { useState, useCallback } from "react";
import { IncidentCard } from "./incident-card";
import { OrphanCalloutBanner } from "./orphan-callout-banner";
import { IncidentListSkeleton } from "./incident-list-skeleton";
import { IncidentError } from "./incident-error";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { CheckCircle2, Clock } from "lucide-react";
import { cn } from "@/lib/utils";
import { useIncidents, useIncidentCounts, useUpdateIncidentStatus } from "@/hooks/use-incidents";
import type { StatusFilter } from "@/lib/api";
import type { ResolutionStatus } from "@/lib/types";

type TimeWindow = "7" | "14" | "30" | "90";

export function IncidentsPageContent() {
  const [statusFilter, setStatusFilter] = useState<StatusFilter>("active");
  const [timeWindow, setTimeWindow] = useState<TimeWindow>("7");

  const needsWindow = statusFilter === "resolved" || statusFilter === "muted";
  const listParams = {
    status: statusFilter,
    ...(needsWindow ? { window: Number(timeWindow) } : {}),
  };

  const { data, isLoading, isError, error } = useIncidents(listParams);
  const { data: counts } = useIncidentCounts();
  const mutation = useUpdateIncidentStatus();

  const handleStatusChange = useCallback(
    (id: string, newStatus: ResolutionStatus, options?: { reason?: string; muteDays?: number }) => {
      mutation.mutate({
        id,
        status: newStatus as "acknowledged" | "resolved" | "muted",
        reason: options?.reason,
        mute_days: options?.muteDays,
      });
    },
    [mutation]
  );

  if (isLoading) return <IncidentListSkeleton />;
  if (isError) return <IncidentError message={error?.message ?? "Failed to load incidents"} />;

  const incidents = data?.incidents ?? [];
  const orphanCount = data?.orphanCount ?? 0;

  const tabs: { value: StatusFilter; label: string; count?: number }[] = [
    { value: "active", label: "Active", count: counts?.active },
    { value: "resolved", label: "Resolved", count: counts?.resolved },
    { value: "muted", label: "Muted", count: counts?.muted },
    { value: "all", label: "All" },
  ];

  return (
    <div className="space-y-4">
      <OrphanCalloutBanner orphanCount={orphanCount} />

      <div className="flex flex-col sm:flex-row sm:items-center gap-3">
        {/* Tab bar */}
        <div className="flex items-center gap-1 rounded-lg border bg-muted/50 p-1">
          {tabs.map((tab) => {
            const isActive = statusFilter === tab.value;
            return (
              <button
                key={tab.value}
                onClick={() => setStatusFilter(tab.value)}
                className={cn(
                  "relative flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                  isActive
                    ? "bg-background text-foreground shadow-sm"
                    : "text-muted-foreground hover:text-foreground"
                )}
              >
                {tab.label}
                {tab.count !== undefined && tab.count > 0 && (
                  <span
                    className={cn(
                      "inline-flex h-5 min-w-5 items-center justify-center rounded-full px-1 text-xs font-medium",
                      isActive
                        ? tab.value === "active"
                          ? "bg-status-failed/15 text-status-failed"
                          : "bg-muted text-muted-foreground"
                        : "bg-transparent text-muted-foreground"
                    )}
                  >
                    {tab.count}
                  </span>
                )}
              </button>
            );
          })}
        </div>

        {/* Time window — for resolved/muted tabs only */}
        {needsWindow && (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Clock className="h-3.5 w-3.5" />
            <span>Last</span>
            <Select
              value={timeWindow}
              onValueChange={(v) => setTimeWindow(v as TimeWindow)}
            >
              <SelectTrigger className="h-8 w-[100px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="7">7 days</SelectItem>
                <SelectItem value="14">14 days</SelectItem>
                <SelectItem value="30">30 days</SelectItem>
                <SelectItem value="90">90 days</SelectItem>
              </SelectContent>
            </Select>
          </div>
        )}

        {/* Count */}
        <p className="text-sm text-muted-foreground sm:ml-auto">
          {data?.total ?? 0} incident{(data?.total ?? 0) !== 1 ? "s" : ""}
        </p>
      </div>

      {/* Incident cards */}
      {incidents.length > 0 ? (
        <div className="space-y-2">
          {incidents.map((incident) => {
            const isMutable =
              incident.resolutionStatus === "open" ||
              incident.resolutionStatus === "acknowledged";

            return (
              <IncidentCard
                key={incident.id}
                incident={incident}
                onStatusChange={isMutable ? handleStatusChange : undefined}
              />
            );
          })}
        </div>
      ) : (
        <EmptyState filter={statusFilter} />
      )}
    </div>
  );
}

function EmptyState({ filter }: { filter: StatusFilter }) {
  if (filter === "active") {
    return (
      <div className="rounded-lg border border-dashed border-border p-8 text-center">
        <CheckCircle2 className="mx-auto h-12 w-12 text-status-passed" />
        <h3 className="mt-4 font-medium">All clear</h3>
        <p className="mt-1 text-sm text-muted-foreground">
          No active incidents. Your data pipeline is healthy.
        </p>
      </div>
    );
  }

  const labels: Record<StatusFilter, string> = {
    active: "active incidents",
    resolved: "resolved incidents in this time window",
    muted: "muted incidents",
    all: "incidents",
  };

  return (
    <div className="rounded-lg border border-dashed border-border p-8 text-center">
      <p className="text-muted-foreground">No {labels[filter]} found.</p>
    </div>
  );
}
