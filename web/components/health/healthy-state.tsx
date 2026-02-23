import { Card, CardContent } from "@/components/ui/card";
import { CheckCircle2 } from "lucide-react";

export function HealthyState() {
  return (
    <Card className="border-green-200 bg-green-50 dark:border-green-900 dark:bg-green-950/30">
      <CardContent className="py-8 text-center">
        <CheckCircle2 className="h-12 w-12 text-green-600 dark:text-green-400 mx-auto mb-3" />
        <p className="text-sm text-green-800 dark:text-green-200">
          Cross-tool correlation is working properly.
          <br />
          All test results are linked to their producing jobs.
        </p>
      </CardContent>
    </Card>
  );
}
