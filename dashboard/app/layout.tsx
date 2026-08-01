import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "LogScope - Operations Console",
  description: "Search logs, investigate alerts, and understand service health in real time.",
  icons: { icon: "/favicon.svg", shortcut: "/favicon.svg" },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="en"><body>{children}</body></html>;
}
