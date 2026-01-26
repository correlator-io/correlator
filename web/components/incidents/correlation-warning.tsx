import Link from "next/link";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { AlertTriangle, ArrowRight, HelpCircle } from "lucide-react";

interface CorrelationWarningProps {
  namespace: string;
  producer: string;
}

export function CorrelationWarning({ namespace, producer }: CorrelationWarningProps) {
  return (
    <Card className="border-status-warning/50 bg-status-warning/5">
      <CardHeader className="pb-3">
        <div className="flex items-center gap-2">
          <AlertTriangle className="h-5 w-5 text-status-warning" />
          <CardTitle className="text-base font-medium">No Correlation Found</CardTitle>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-sm text-muted-foreground">
          This test result could not be linked to a producing job. This means we cannot determine
          downstream impact or trace the data lineage.
        </p>

        <div className="rounded-md bg-muted p-3 space-y-2">
          <p className="text-xs text-muted-foreground flex items-center gap-1.5">
            <HelpCircle className="h-3.5 w-3.5" />
            Why did this happen?
          </p>
          <p className="text-sm">
            The namespace <code className="font-mono text-xs bg-background px-1.5 py-0.5 rounded">{namespace}</code>{" "}
            from {producer} doesn&apos;t match any known data producer namespaces.
          </p>
        </div>

        <div className="pt-2">
          <p className="text-xs text-muted-foreground mb-2">To fix this:</p>
          <ol className="text-sm space-y-1 list-decimal list-inside text-muted-foreground">
            <li>Check the Correlation Health page for namespace mismatches</li>
            <li>Add a namespace alias in your <code className="font-mono text-xs">correlator.yaml</code> config</li>
          </ol>
        </div>

        <Button asChild variant="outline" size="sm" className="mt-2">
          <Link href="/health">
            View Correlation Health
            <ArrowRight className="ml-2 h-4 w-4" />
          </Link>
        </Button>
      </CardContent>
    </Card>
  );
}
