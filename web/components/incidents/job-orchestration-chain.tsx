import { ProducerIcon } from "@/components/icons/producer-icon";
import { cn } from "@/lib/utils";
import type { ChainLevel } from "@/lib/orchestration";

interface JobOrchestrationChainProps {
  levels: ChainLevel[];
}

function statusColor(status: string): string {
  switch (status) {
    case "COMPLETE":
      return "text-green-600 dark:text-green-400";
    case "FAIL":
      return "text-red-600 dark:text-red-400";
    case "RUNNING":
      return "text-amber-600 dark:text-amber-400";
    default:
      return "text-muted-foreground";
  }
}

function statusLabel(status: string): string {
  switch (status) {
    case "COMPLETE":
      return "Complete";
    case "FAIL":
      return "Failed";
    case "RUNNING":
      return "Running";
    default:
      return status;
  }
}

export function JobOrchestrationChain({ levels }: JobOrchestrationChainProps) {
  if (levels.length === 0) return null;

  return (
    <div>
      <p className="text-xs font-medium text-muted-foreground mb-2">
        Orchestration
      </p>
      <div className="relative">
        {levels.map((level, index) => {
          const isLast = index === levels.length - 1;

          return (
            <div key={level.runId} className="flex items-start gap-2.5">
              {/* Vertical connector */}
              <div className="flex flex-col items-center w-3 shrink-0">
                <div
                  className={cn(
                    "h-2.5 w-2.5 rounded-full mt-1 shrink-0",
                    level.isCurrent
                      ? "bg-primary ring-2 ring-primary/20"
                      : "bg-muted-foreground/40"
                  )}
                />
                {!isLast && <div className="w-px h-5 bg-border" />}
              </div>

              {/* Level content */}
              <div
                className={cn(
                  "flex items-center gap-1.5 min-w-0 text-xs",
                  isLast ? "pb-0" : "pb-1.5",
                  level.isCurrent ? "font-medium" : "text-muted-foreground"
                )}
              >
                {level.producer && (
                  <ProducerIcon producer={level.producer} size={14} />
                )}
                <span className="truncate">{level.name}</span>
                <span
                  className={cn(
                    "shrink-0 ml-auto text-[11px]",
                    statusColor(level.status)
                  )}
                >
                  {statusLabel(level.status)}
                </span>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
