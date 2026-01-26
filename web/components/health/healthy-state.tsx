import { Card, CardContent } from "@/components/ui/card";
import { CheckCircle2, Sparkles } from "lucide-react";

export function HealthyState() {
  return (
    <Card className="border-status-passed/30 bg-status-passed/5">
      <CardContent className="py-12 text-center">
        <div className="flex justify-center mb-4">
          <div className="relative">
            <CheckCircle2 className="h-16 w-16 text-status-passed" />
            <Sparkles className="h-6 w-6 text-status-passed absolute -top-1 -right-2" />
          </div>
        </div>
        <h3 className="font-semibold text-xl text-status-passed">
          Perfect Correlation Health
        </h3>
        <p className="text-sm text-muted-foreground mt-2 max-w-md mx-auto">
          All namespaces are properly aligned. Every test result can be correlated
          to its producing job and downstream impact is fully traceable.
        </p>
      </CardContent>
    </Card>
  );
}
