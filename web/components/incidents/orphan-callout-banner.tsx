"use client";

import { useState } from "react";
import Link from "next/link";
import { AlertTriangle, X } from "lucide-react";
import { Alert, AlertTitle, AlertDescription } from "@/components/ui/alert";

const STORAGE_KEY = "correlator-orphan-banner-dismissed";

function getInitialDismissedState(): boolean {
  // Check if we're in the browser before accessing sessionStorage
  if (typeof window === "undefined") {
    return false;
  }
  return sessionStorage.getItem(STORAGE_KEY) === "true";
}

interface OrphanCalloutBannerProps {
  orphanCount: number;
}

export function OrphanCalloutBanner({ orphanCount }: OrphanCalloutBannerProps) {
  const [isDismissed, setIsDismissed] = useState(getInitialDismissedState);

  const handleDismiss = () => {
    sessionStorage.setItem(STORAGE_KEY, "true");
    setIsDismissed(true);
  };

  if (orphanCount === 0 || isDismissed) {
    return null;
  }

  return (
    <Alert variant="warning" className="relative">
      <AlertTriangle className="h-4 w-4" />
      <AlertTitle>
        {orphanCount} dataset{orphanCount !== 1 ? "s" : ""} have test failures
        but no producer correlation
      </AlertTitle>
      <AlertDescription>
        Configure patterns to see all incidents.{" "}
        <Link href="/health" className="underline font-medium hover:no-underline">
          View Health
        </Link>
      </AlertDescription>
      <button
        type="button"
        className="absolute right-3 top-3 inline-flex h-7 w-7 items-center justify-center rounded-md !p-0 transition-colors hover:bg-status-warning focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring group"
        onClick={handleDismiss}
        aria-label="Dismiss"
      >
        <X className="h-4 w-4 stroke-status-warning group-hover:stroke-black" />
      </button>
    </Alert>
  );
}
