"use client";

import { useState } from "react";
import { VolumeX, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface MutePopoverProps {
  onMute: (days: number) => void;
  disabled?: boolean;
  isPending?: boolean;
}

const MUTE_DURATIONS = [
  { days: 7, label: "7 days" },
  { days: 30, label: "30 days" },
  { days: 90, label: "90 days" },
];

export function MutePopover({ onMute, disabled, isPending }: MutePopoverProps) {
  const [isOpen, setIsOpen] = useState(false);

  return (
    <div className="relative">
      <Tooltip>
        <TooltipTrigger asChild>
          <Button
            variant="ghost"
            size="icon"
            className="h-7 w-7 text-muted-foreground hover:text-foreground hover:bg-muted"
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              setIsOpen(!isOpen);
            }}
            disabled={disabled}
          >
            {isPending ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <VolumeX className="h-3.5 w-3.5" />
            )}
            <span className="sr-only">Mute</span>
          </Button>
        </TooltipTrigger>
        {!isOpen && (
          <TooltipContent side="bottom">
            <p>Mute — false positive or accepted risk</p>
          </TooltipContent>
        )}
      </Tooltip>

      {isOpen && (
        <>
          {/* Backdrop */}
          <div
            className="fixed inset-0 z-40"
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              setIsOpen(false);
            }}
          />

          {/* Popover */}
          <div className="absolute right-0 top-full z-50 mt-1 w-44 rounded-lg border bg-popover p-1.5 shadow-lg animate-in fade-in-0 zoom-in-95">
            <p className="px-2 py-1 text-xs font-medium text-muted-foreground">
              Mute for...
            </p>
            {MUTE_DURATIONS.map(({ days, label }) => (
              <button
                key={days}
                className="flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-sm hover:bg-accent transition-colors"
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  onMute(days);
                  setIsOpen(false);
                }}
              >
                <VolumeX className="h-3.5 w-3.5 text-muted-foreground" />
                {label}
              </button>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
