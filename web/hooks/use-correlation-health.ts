// React Query hooks for correlation health data

import { useQuery } from "@tanstack/react-query";
import { fetchCorrelationHealth } from "@/lib/api";

// Query key factory
export const healthKeys = {
  all: ["health"] as const,
  correlation: () => [...healthKeys.all, "correlation"] as const,
};

// Fetch correlation health metrics
export function useCorrelationHealth() {
  return useQuery({
    queryKey: healthKeys.correlation(),
    queryFn: fetchCorrelationHealth,
    // Health data doesn't change frequently
    staleTime: 5 * 60_000, // 5 minutes
    // Don't refetch on window focus for health page
    refetchOnWindowFocus: false,
  });
}
