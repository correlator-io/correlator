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

      {/* Affected datasets table skeleton */}
      <div className="space-y-4">
        <Skeleton className="h-4 w-40" />
        <div className="rounded-lg border bg-card overflow-hidden">
          <div className="border-b bg-muted/50 px-4 py-2.5 flex gap-8">
            <Skeleton className="h-3 w-28" />
            <Skeleton className="h-3 w-24" />
          </div>
          <OrphanDatasetRowSkeleton />
          <OrphanDatasetRowSkeleton />
        </div>
      </div>
    </div>
  );
}

function OrphanDatasetRowSkeleton() {
  return (
    <div className="flex items-center gap-4 px-4 py-3 border-b last:border-b-0">
      <div className="flex-1 space-y-1">
        <Skeleton className="h-4 w-48" />
        <Skeleton className="h-3 w-32" />
      </div>
      <Skeleton className="h-4 w-4" />
      <div className="flex-1 space-y-1">
        <Skeleton className="h-4 w-56" />
        <Skeleton className="h-3 w-40" />
      </div>
    </div>
  );
}
