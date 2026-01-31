"use client";

import { useEffect } from "react";
import { AlertTriangle, RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";

interface ErrorProps {
  error: Error & { digest?: string };
  reset: () => void;
}

export default function Error({ error, reset }: ErrorProps) {
  useEffect(() => {
    // Log the error to console in development
    console.error("Page error:", error);
  }, [error]);

  return (
    <div className="flex min-h-[60vh] items-center justify-center p-6">
      <div className="max-w-md text-center">
        <div className="mx-auto mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-destructive/10">
          <AlertTriangle className="h-8 w-8 text-destructive" />
        </div>
        <h2 className="mb-2 text-lg font-semibold">Something went wrong</h2>
        <p className="mb-4 text-sm text-muted-foreground">
          An unexpected error occurred while loading this page.
        </p>
        {error.message && (
          <p className="mb-4 rounded bg-muted p-2 font-mono text-xs text-muted-foreground break-all">
            {error.message}
          </p>
        )}
        <div className="flex justify-center gap-2">
          <Button variant="outline" onClick={reset}>
            <RefreshCw className="mr-2 h-4 w-4" />
            Try again
          </Button>
          <Button variant="default" onClick={() => window.location.reload()}>
            Refresh page
          </Button>
        </div>
      </div>
    </div>
  );
}
