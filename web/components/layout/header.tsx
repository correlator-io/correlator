"use client";

import { ThemeToggle } from "@/components/theme/theme-toggle";
import { MobileNav } from "./mobile-nav";

interface HeaderProps {
  title: string;
}

export function Header({ title }: HeaderProps) {
  return (
    <header className="sticky top-0 z-40 flex h-14 items-center gap-4 border-b border-border bg-background px-4 lg:px-6">
      <MobileNav />
      <h1 className="flex-1 text-lg font-semibold lg:text-xl">{title}</h1>
      <ThemeToggle />
    </header>
  );
}
