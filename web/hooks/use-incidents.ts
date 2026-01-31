// React Query hooks for incident data

import { useQuery, keepPreviousData } from "@tanstack/react-query";
import {
  fetchIncidents,
  fetchIncidentDetail,
  type FetchIncidentsParams,
} from "@/lib/api";

// Query key factory for consistent cache management
export const incidentKeys = {
  all: ["incidents"] as const,
  lists: () => [...incidentKeys.all, "list"] as const,
  list: (params: FetchIncidentsParams) =>
    [...incidentKeys.lists(), params] as const,
  details: () => [...incidentKeys.all, "detail"] as const,
  detail: (id: string) => [...incidentKeys.details(), id] as const,
};

// Fetch incident list with pagination
export function useIncidents(params: FetchIncidentsParams = {}) {
  return useQuery({
    queryKey: incidentKeys.list(params),
    queryFn: () => fetchIncidents(params),
    // Keep previous data while fetching new page (smooth pagination)
    placeholderData: keepPreviousData,
    // Refetch every 30 seconds to catch new incidents
    refetchInterval: 30_000,
    // Refetch when user returns to see latest incidents
    refetchOnWindowFocus: true,
  });
}

// Fetch single incident detail
export function useIncidentDetail(id: string | undefined) {
  return useQuery({
    queryKey: incidentKeys.detail(id ?? ""),
    queryFn: () => fetchIncidentDetail(id!),
    // Only fetch if we have an ID
    enabled: !!id,
    // Incident details change less frequently
    staleTime: 60_000,
    // Don't refetch on window focus during active investigation
    refetchOnWindowFocus: false,
  });
}
