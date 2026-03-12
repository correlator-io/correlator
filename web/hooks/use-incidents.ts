import { useQuery, useMutation, useQueryClient, keepPreviousData } from "@tanstack/react-query";
import {
  fetchIncidents,
  fetchIncidentDetail,
  fetchIncidentCounts,
  updateIncidentStatus,
  type FetchIncidentsParams,
  type UpdateStatusParams,
} from "@/lib/api";

export const incidentKeys = {
  all: ["incidents"] as const,
  lists: () => [...incidentKeys.all, "list"] as const,
  list: (params: FetchIncidentsParams) =>
    [...incidentKeys.lists(), params] as const,
  details: () => [...incidentKeys.all, "detail"] as const,
  detail: (id: string) => [...incidentKeys.details(), id] as const,
  counts: () => [...incidentKeys.all, "counts"] as const,
};

export function useIncidents(params: FetchIncidentsParams = {}) {
  return useQuery({
    queryKey: incidentKeys.list(params),
    queryFn: () => fetchIncidents(params),
    placeholderData: keepPreviousData,
    refetchInterval: 30_000,
    refetchOnWindowFocus: true,
  });
}

export function useIncidentDetail(id: string | undefined) {
  return useQuery({
    queryKey: incidentKeys.detail(id ?? ""),
    queryFn: () => fetchIncidentDetail(id!),
    enabled: !!id,
    staleTime: 60_000,
    refetchOnWindowFocus: false,
  });
}

export function useIncidentCounts() {
  return useQuery({
    queryKey: incidentKeys.counts(),
    queryFn: fetchIncidentCounts,
    staleTime: 15_000,
    refetchInterval: 30_000,
    refetchOnWindowFocus: true,
  });
}

export function useUpdateIncidentStatus() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ id, ...params }: UpdateStatusParams & { id: string }) =>
      updateIncidentStatus(id, params),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: incidentKeys.lists() });
      queryClient.invalidateQueries({ queryKey: incidentKeys.counts() });
    },
  });
}
