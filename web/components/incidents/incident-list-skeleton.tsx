import { Skeleton } from "@/components/ui/skeleton";

export function IncidentListSkeleton() {
  return (
    <div className="space-y-4">
      {/* Header skeleton */}
      <div className="flex items-center justify-between">
        <Skeleton className="h-5 w-24" />
        <Skeleton className="h-9 w-32" />
      </div>

      {/* Incident card skeletons */}
      <div className="space-y-2">
        {[1, 2, 3, 4, 5].map((i) => (
          <IncidentCardSkeleton key={i} />
        ))}
      </div>
    </div>
  );
}

function IncidentCardSkeleton() {
  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="flex items-start gap-3">
        {/* Status dot */}
        <Skeleton className="h-3 w-3 rounded-full mt-1.5" />

        <div className="flex-1 min-w-0 space-y-2">
          {/* Test name */}
          <Skeleton className="h-5 w-3/4" />

          {/* Dataset URN */}
          <Skeleton className="h-4 w-full max-w-md" />

          {/* Meta row: producer, job, downstream, time */}
          <div className="flex items-center gap-4 flex-wrap">
            <Skeleton className="h-4 w-16" />
            <Skeleton className="h-4 w-32" />
            <Skeleton className="h-4 w-24" />
          </div>
        </div>
      </div>
    </div>
  );
}
