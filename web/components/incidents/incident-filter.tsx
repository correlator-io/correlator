"use client";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Filter } from "lucide-react";

export type FilterValue = "all" | "failed" | "passed" | "correlation_issues";

interface IncidentFilterProps {
  value: FilterValue;
  onValueChange: (value: FilterValue) => void;
}

const filterOptions: { value: FilterValue; label: string }[] = [
  { value: "all", label: "All incidents" },
  { value: "failed", label: "Failed" },
  { value: "passed", label: "Passed" },
  { value: "correlation_issues", label: "Correlation issues" },
];

export function IncidentFilter({ value, onValueChange }: IncidentFilterProps) {
  return (
    <Select value={value} onValueChange={onValueChange}>
      <SelectTrigger className="w-[180px]">
        <Filter className="mr-2 h-4 w-4" />
        <SelectValue placeholder="Filter incidents" />
      </SelectTrigger>
      <SelectContent>
        {filterOptions.map((option) => (
          <SelectItem key={option.value} value={option.value}>
            {option.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
