import { DbtIcon } from "./dbt-icon";
import { AirflowIcon } from "./airflow-icon";
import { GreatExpectationsIcon } from "./great-expectations-icon";
import { HelpCircle } from "lucide-react";
import type { Producer } from "@/lib/types";
import { cn } from "@/lib/utils";

/**
 * ProducerIcon uses a `size` prop (number) rather than className for sizing.
 *
 * Convention note: Lucide icons use `className="h-4 w-4"` throughout this codebase,
 * but producer icons use `size={16}` because:
 * 1. These are custom SVGs with viewBox that scale cleanly with width/height attributes
 * 2. The size prop is more ergonomic: `<DbtIcon size={14} />` vs `<DbtIcon className="h-3.5 w-3.5" />`
 * 3. Numeric sizing avoids Tailwind class specificity issues with inline SVGs
 *
 * Both conventions work - this documents the intentional difference.
 */
interface ProducerIconProps {
  producer: Producer;
  size?: number;
  className?: string;
  showLabel?: boolean;
}

const producerLabels: Record<Producer, string> = {
  dbt: "dbt",
  airflow: "Airflow",
  great_expectations: "Great Expectations",
  unknown: "Unknown",
};

export function ProducerIcon({
  producer,
  size = 16,
  className,
  showLabel = false,
}: ProducerIconProps) {
  const Icon = () => {
    switch (producer) {
      case "dbt":
        return <DbtIcon size={size} className={className} />;
      case "airflow":
        return <AirflowIcon size={size} className={className} />;
      case "great_expectations":
        return <GreatExpectationsIcon size={size} className={className} />;
      default:
        return (
          <HelpCircle
            size={size}
            className={cn("text-muted-foreground", className)}
          />
        );
    }
  };

  if (showLabel) {
    return (
      <span className="inline-flex items-center gap-1.5">
        <Icon />
        <span className="text-sm">{producerLabels[producer]}</span>
      </span>
    );
  }

  return <Icon />;
}

export { producerLabels };
