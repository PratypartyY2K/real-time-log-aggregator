import type { AlertEntry, LogEntry } from "./page";

export const demoLogs: LogEntry[] = [
  { timestamp: new Date(Date.now() - 32000).toISOString(), service: "checkout", environment: "prod", level: "error", message: "Payment gateway timed out after 3 attempts", trace_id: "tr_8fd14b20", fields: { region: "us-west-2", host: "checkout-7c9d" } },
  { timestamp: new Date(Date.now() - 68000).toISOString(), service: "orders", environment: "prod", level: "warn", message: "Inventory reservation latency exceeded threshold", trace_id: "tr_4a91cc73", fields: { latency_ms: "842", sku: "sku-9012" } },
  { timestamp: new Date(Date.now() - 91000).toISOString(), service: "api-gateway", environment: "prod", level: "info", message: "Request completed", trace_id: "tr_5ec20a11", fields: { status: "200", duration_ms: "48" } },
  { timestamp: new Date(Date.now() - 145000).toISOString(), service: "payments", environment: "prod", level: "error", message: "Circuit breaker opened for stripe-primary", trace_id: "tr_8fd14b20", fields: { failures: "12", window: "60s" } },
  { timestamp: new Date(Date.now() - 201000).toISOString(), service: "auth", environment: "prod", level: "info", message: "Session token refreshed", trace_id: "tr_b12f04e8", fields: { provider: "internal" } },
  { timestamp: new Date(Date.now() - 265000).toISOString(), service: "catalog", environment: "prod", level: "warn", message: "Search index is 3 revisions behind", trace_id: "tr_21d3e409", fields: { index: "products-v4" } },
];

export const demoAlerts: AlertEntry[] = [
  { id: 1, rule_name: "Checkout error rate", service: "checkout", status: "active", latest_firing_at: new Date(Date.now() - 48000).toISOString(), current_value: 8.4 },
  { id: 2, rule_name: "Payment p95 latency", service: "payments", status: "active", latest_firing_at: new Date(Date.now() - 190000).toISOString(), current_value: 1240 },
  { id: 3, rule_name: "Inventory failures", service: "inventory", status: "resolved", latest_firing_at: new Date(Date.now() - 4200000).toISOString(), current_value: 0.3 },
];

export const volume = [24, 27, 23, 31, 28, 34, 37, 32, 46, 43, 50, 47, 58, 54, 61, 56, 68, 64, 72, 67, 77, 73, 82, 79, 86, 80, 88, 84, 91, 87, 94, 89, 97, 92];
export const errors = [2, 1, 2, 2, 1, 2, 3, 2, 3, 2, 4, 3, 3, 4, 4, 3, 5, 4, 5, 5, 6, 5, 7, 6, 6, 5, 7, 6, 8, 7, 9, 7, 10, 8];

export const services = [
  { name: "api-gateway", requests: "18.4k", error: "0.8%", latency: "42 ms", health: "healthy" },
  { name: "checkout", requests: "8.9k", error: "8.4%", latency: "286 ms", health: "critical" },
  { name: "orders", requests: "12.2k", error: "1.7%", latency: "118 ms", health: "warning" },
  { name: "payments", requests: "7.1k", error: "4.2%", latency: "1.24 s", health: "critical" },
  { name: "catalog", requests: "21.6k", error: "0.3%", latency: "64 ms", health: "healthy" },
];
