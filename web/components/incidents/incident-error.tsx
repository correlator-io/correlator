import { AlertCircle, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";

interface IncidentErrorProps {
  message: string;
  onRetry?: () => void;
}

export function IncidentError({ message, onRetry }: IncidentErrorProps) {
  return (
    <div className="rounded-lg border border-destructive/50 bg-destructive/10 p-6 text-center">
      <AlertCircle className="mx-auto h-10 w-10 text-destructive" />
      <h3 className="mt-4 font-medium text-destructive">Failed to load data</h3>
      <p className="mt-1 text-sm text-muted-foreground">{message}</p>
      {onRetry && (
        <Button variant="outline" size="sm" className="mt-4" onClick={onRetry}>
          <RefreshCw className="mr-2 h-4 w-4" />
          Try again
        </Button>
      )}
    </div>
  );
}
