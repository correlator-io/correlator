import type { Incident } from "./types";

export type IncidentFilter = "all" | "failed" | "passed" | "correlation_issues";

/**
 * Filter incidents by status
 */
export function filterIncidents(
  incidents: Incident[],
  filter: IncidentFilter
): Incident[] {
  switch (filter) {
    case "failed":
      return incidents.filter((i) => i.testStatus === "failed");
    case "passed":
      return incidents.filter((i) => i.testStatus === "passed");
    case "correlation_issues":
      return incidents.filter((i) => i.hasCorrelationIssue);
    default:
      return incidents;
  }
}
