import { Skeleton } from "@/components/ui/skeleton";
import { Card, CardContent, CardHeader } from "@/components/ui/card";

export function HealthSkeleton() {
  return (
    <div className="space-y-6">
      {/* Status header skeleton */}
      <Skeleton className="h-16 w-full rounded-lg" />

      {/* Suggested fix card skeleton */}
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center gap-2">
            <Skeleton className="h-5 w-5" />
            <Skeleton className="h-5 w-28" />
          </div>
        </CardHeader>
        <CardContent className="space-y-4">
          <Skeleton className="h-4 w-64" />
          <Skeleton className="h-32 w-full rounded-lg" />
          <Skeleton className="h-3 w-80" />
        </CardContent>
      </Card>

      {/* Affected datasets skeleton */}
      <div className="space-y-4">
        <Skeleton className="h-4 w-40" />
        <div className="space-y-3">
          <OrphanDatasetCardSkeleton />
          <OrphanDatasetCardSkeleton />
        </div>
      </div>
    </div>
  );
}

function OrphanDatasetCardSkeleton() {
  return (
    <div className="rounded-lg border bg-card p-4 space-y-3">
      <div>
        <Skeleton className="h-4 w-48" />
        <Skeleton className="h-3 w-32 mt-2" />
      </div>
      <div className="flex items-start gap-2">
        <Skeleton className="h-4 w-4 mt-0.5" />
        <div className="space-y-1">
          <Skeleton className="h-4 w-56" />
          <Skeleton className="h-3 w-40" />
        </div>
      </div>
    </div>
  );
}
