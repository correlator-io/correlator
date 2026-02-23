"use client";

import Link from "next/link";
import { ArrowLeft, Link2, Check } from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { StatusBadge } from "./status-badge";
import { formatIncidentId } from "@/lib/utils";
import type { TestStatus } from "@/lib/types";

interface IncidentHeaderProps {
  id: string;
  testName: string;
  testStatus: TestStatus;
  executedAt: string;
}

export function IncidentHeader({
  id,
  testName,
  testStatus,
  executedAt,
}: IncidentHeaderProps) {
  const [copied, setCopied] = useState(false);

  const handleCopyLink = async () => {
    const url = window.location.href;

    try {
      await navigator.clipboard.writeText(url);
      setCopied(true);
      toast.success("Link copied to clipboard");
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Fallback: show URL in prompt for manual copy
      window.prompt("Copy this link:", url);
    }
  };

  const title = formatIncidentId(id, testName);

  // Format relative time
  const relativeTime = formatRelativeTime(executedAt);

  return (
    <div className="flex items-start gap-4">
      <Button variant="ghost" size="icon" asChild className="flex-shrink-0 mt-0.5">
        <Link href="/incidents">
          <ArrowLeft className="h-5 w-5" />
          <span className="sr-only">Back to incidents</span>
        </Link>
      </Button>

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-3 flex-wrap">
          <h2 className="text-lg font-semibold truncate">{title}</h2>
          <StatusBadge status={testStatus} />
          <Button
            variant="ghost"
            size="sm"
            className="h-7 gap-1.5 text-muted-foreground hover:text-foreground"
            onClick={handleCopyLink}
          >
            {copied ? (
              <>
                <Check className="h-3.5 w-3.5 text-status-passed" />
                <span className="text-status-passed">Copied</span>
              </>
            ) : (
              <>
                <Link2 className="h-3.5 w-3.5" />
                <span className="hidden sm:inline">Copy Link</span>
              </>
            )}
          </Button>
        </div>
        <p className="text-sm text-muted-foreground mt-1">
          {testStatus === "failed" ? "Failed" : testStatus === "passed" ? "Passed" : "Unknown"} · {relativeTime}
        </p>
      </div>
    </div>
  );
}

function formatRelativeTime(isoDate: string): string {
  const date = new Date(isoDate);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);

  if (diffMins < 1) return "just now";
  if (diffMins < 60) return `${diffMins}m ago`;

  const diffHours = Math.floor(diffMins / 60);
  if (diffHours < 24) return `${diffHours}h ago`;

  const diffDays = Math.floor(diffHours / 24);
  if (diffDays < 7) return `${diffDays}d ago`;

  return date.toLocaleDateString();
}
