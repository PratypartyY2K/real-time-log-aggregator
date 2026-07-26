"use client";

import { useEffect, useMemo, useState } from "react";

type Tab = "Overview" | "Logs" | "Alerts" | "Services";
type LogEntry = {
  timestamp: string;
  service: string;
  environment: string;
  level: string;
  message: string;
  trace_id: string;
  fields?: Record<string, string>;
};
type AlertEntry = {
  id?: number;
  rule_name?: string;
  name?: string;
  service?: string;
  status: string;
  latest_firing_at?: string;
  first_firing_at?: string;
  current_value?: number;
};

const demoLogs: LogEntry[] = [
  { timestamp: new Date(Date.now() - 32000).toISOString(), service: "checkout", environment: "prod", level: "error", message: "Payment gateway timed out after 3 attempts", trace_id: "tr_8fd14b20", fields: { region: "us-west-2", host: "checkout-7c9d" } },
  { timestamp: new Date(Date.now() - 68000).toISOString(), service: "orders", environment: "prod", level: "warn", message: "Inventory reservation latency exceeded threshold", trace_id: "tr_4a91cc73", fields: { latency_ms: "842", sku: "sku-9012" } },
  { timestamp: new Date(Date.now() - 91000).toISOString(), service: "api-gateway", environment: "prod", level: "info", message: "Request completed", trace_id: "tr_5ec20a11", fields: { status: "200", duration_ms: "48" } },
  { timestamp: new Date(Date.now() - 145000).toISOString(), service: "payments", environment: "prod", level: "error", message: "Circuit breaker opened for stripe-primary", trace_id: "tr_8fd14b20", fields: { failures: "12", window: "60s" } },
  { timestamp: new Date(Date.now() - 201000).toISOString(), service: "auth", environment: "prod", level: "info", message: "Session token refreshed", trace_id: "tr_b12f04e8", fields: { provider: "internal" } },
  { timestamp: new Date(Date.now() - 265000).toISOString(), service: "catalog", environment: "prod", level: "warn", message: "Search index is 3 revisions behind", trace_id: "tr_21d3e409", fields: { index: "products-v4" } },
];

const demoAlerts: AlertEntry[] = [
  { id: 1, rule_name: "Checkout error rate", service: "checkout", status: "active", latest_firing_at: new Date(Date.now() - 48000).toISOString(), current_value: 8.4 },
  { id: 2, rule_name: "Payment p95 latency", service: "payments", status: "active", latest_firing_at: new Date(Date.now() - 190000).toISOString(), current_value: 1240 },
  { id: 3, rule_name: "Inventory failures", service: "inventory", status: "resolved", latest_firing_at: new Date(Date.now() - 4200000).toISOString(), current_value: 0.3 },
];

const volume = [24, 27, 23, 31, 28, 34, 37, 32, 46, 43, 50, 47, 58, 54, 61, 56, 68, 64, 72, 67, 77, 73, 82, 79, 86, 80, 88, 84, 91, 87, 94, 89, 97, 92];
const errors = [2, 1, 2, 2, 1, 2, 3, 2, 3, 2, 4, 3, 3, 4, 4, 3, 5, 4, 5, 5, 6, 5, 7, 6, 6, 5, 7, 6, 8, 7, 9, 7, 10, 8];

const services = [
  { name: "api-gateway", requests: "18.4k", error: "0.8%", latency: "42 ms", health: "healthy" },
  { name: "checkout", requests: "8.9k", error: "8.4%", latency: "286 ms", health: "critical" },
  { name: "orders", requests: "12.2k", error: "1.7%", latency: "118 ms", health: "warning" },
  { name: "payments", requests: "7.1k", error: "4.2%", latency: "1.24 s", health: "critical" },
  { name: "catalog", requests: "21.6k", error: "0.3%", latency: "64 ms", health: "healthy" },
];

function timeAgo(value?: string) {
  if (!value) return "—";
  const seconds = Math.max(1, Math.round((Date.now() - new Date(value).getTime()) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m ago`;
  return `${Math.round(seconds / 3600)}h ago`;
}

function Sparkline({ data, tone = "cyan" }: { data: number[]; tone?: "cyan" | "red" }) {
  const max = Math.max(...data);
  const points = data.map((n, i) => `${(i / (data.length - 1)) * 100},${42 - (n / max) * 38}`).join(" ");
  return (
    <svg className={`spark ${tone}`} viewBox="0 0 100 46" preserveAspectRatio="none" aria-hidden="true">
      <polyline points={points} fill="none" stroke="currentColor" strokeWidth="1.8" vectorEffect="non-scaling-stroke" />
    </svg>
  );
}

export default function Dashboard() {
  const [tab, setTab] = useState<Tab>("Overview");
  const [logs, setLogs] = useState<LogEntry[]>(demoLogs);
  const [alerts, setAlerts] = useState<AlertEntry[]>(demoAlerts);
  const [query, setQuery] = useState("");
  const [level, setLevel] = useState("all");
  const [range, setRange] = useState("1h");
  const [connected, setConnected] = useState(false);
  const [expanded, setExpanded] = useState<string | null>(null);

  useEffect(() => {
    const end = new Date();
    const start = new Date(end.getTime() - 60 * 60 * 1000);
    const params = new URLSearchParams({ resource: "logs", start: start.toISOString(), end: end.toISOString(), limit: "100" });
    Promise.all([
      fetch(`/api/logagg?${params}`).then((r) => r.ok ? r.json() : Promise.reject()),
      fetch(`/api/logagg?${new URLSearchParams({ resource: "alerts", start: new Date(end.getTime() - 86400000).toISOString(), end: end.toISOString(), limit: "20" })}`).then((r) => r.ok ? r.json() : Promise.reject()),
    ]).then(([logData, alertData]) => {
      if (Array.isArray(logData.logs)) setLogs(logData.logs);
      if (Array.isArray(alertData.alerts)) setAlerts(alertData.alerts);
      setConnected(true);
    }).catch(() => setConnected(false));
  }, []);

  const filtered = useMemo(() => logs.filter((log) => {
    const matchesLevel = level === "all" || log.level === level;
    const haystack = `${log.service} ${log.message} ${log.trace_id}`.toLowerCase();
    return matchesLevel && haystack.includes(query.toLowerCase());
  }), [logs, level, query]);

  return (
    <main className="shell">
      <aside className="sidebar">
        <div className="brand"><span className="brand-mark">L</span><span>Log<span className="brand-accent">Scope</span></span></div>
        <nav aria-label="Dashboard">
          {(["Overview", "Logs", "Alerts", "Services"] as Tab[]).map((item) => (
            <button key={item} className={tab === item ? "nav-item active" : "nav-item"} onClick={() => setTab(item)}>
              <span className="nav-icon">{item === "Overview" ? "⌁" : item === "Logs" ? "≡" : item === "Alerts" ? "△" : "◇"}</span>{item}
              {item === "Alerts" && <span className="badge">{alerts.filter((a) => a.status === "active").length}</span>}
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <a href="http://localhost:3000" target="_blank" rel="noreferrer" className="nav-item"><span className="nav-icon">↗</span>Grafana</a>
          <div className="workspace"><span className="avatar">PK</span><div><strong>Production</strong><small>Default workspace</small></div><span>•••</span></div>
        </div>
      </aside>

      <section className="workspace-main">
        <header className="topbar">
          <div>
            <p className="eyebrow">OPERATIONS / {tab.toUpperCase()}</p>
            <h1>{tab === "Overview" ? "System overview" : tab === "Logs" ? "Log explorer" : tab === "Alerts" ? "Alert center" : "Service health"}</h1>
          </div>
          <div className="top-actions">
            <span className={connected ? "status connected" : "status demo"}><i />{connected ? "Live API" : "Demo data"}</span>
            <select value={range} onChange={(e) => setRange(e.target.value)} aria-label="Time range">
              <option value="15m">Last 15 minutes</option><option value="1h">Last 1 hour</option><option value="24h">Last 24 hours</option>
            </select>
            <button className="icon-button" aria-label="Refresh" onClick={() => location.reload()}>↻</button>
          </div>
        </header>

        {tab === "Overview" && <>
          <section className="stats">
            <article><p>LOGS INGESTED</p><strong>48.2k <small>/ min</small></strong><span className="trend up">↑ 12.4%</span></article>
            <article><p>ERROR RATE</p><strong>2.84<small>%</small></strong><span className="trend down">↑ 0.6%</span></article>
            <article><p>QUERY P95</p><strong>184 <small>ms</small></strong><span className="trend up">↓ 18 ms</span></article>
            <article><p>ACTIVE ALERTS</p><strong>{alerts.filter((a) => a.status === "active").length}</strong><span className="trend neutral">2 critical</span></article>
          </section>
          <section className="overview-grid">
            <article className="panel chart-panel">
              <div className="panel-head"><div><p className="panel-kicker">TRAFFIC</p><h2>Log volume</h2></div><div className="legend"><span className="dot cyan" />All logs <span className="dot red" />Errors</div></div>
              <div className="chart-wrap"><div className="axis"><span>60k</span><span>40k</span><span>20k</span><span>0</span></div><div className="grid-lines" /><Sparkline data={volume} /><Sparkline data={errors.map((n) => n * 4.2)} tone="red" /></div>
              <div className="x-axis"><span>11:00</span><span>11:15</span><span>11:30</span><span>11:45</span><span>Now</span></div>
            </article>
            <article className="panel alert-panel">
              <div className="panel-head"><div><p className="panel-kicker">ATTENTION</p><h2>Active alerts</h2></div><button onClick={() => setTab("Alerts")}>View all →</button></div>
              <div className="alert-list">{alerts.filter((a) => a.status === "active").map((alert) => <div className="alert-row" key={alert.id || alert.rule_name}><span className="severity">!</span><div><strong>{alert.rule_name || alert.name}</strong><p>{alert.service || "All services"} · {timeAgo(alert.latest_firing_at || alert.first_firing_at)}</p></div><span className="alert-value">{alert.current_value}{(alert.rule_name || "").includes("rate") ? "%" : " ms"}</span></div>)}</div>
            </article>
            <article className="panel service-panel">
              <div className="panel-head"><div><p className="panel-kicker">RUNTIME</p><h2>Service health</h2></div><button onClick={() => setTab("Services")}>Explore map →</button></div>
              <div className="service-table">{services.map((service) => <div className="service-row" key={service.name}><span className={`health ${service.health}`} /><strong>{service.name}</strong><span>{service.requests} req</span><span className={service.health === "critical" ? "danger" : ""}>{service.error} errors</span><span>{service.latency}</span></div>)}</div>
            </article>
            <article className="panel recent-panel">
              <div className="panel-head"><div><p className="panel-kicker">LIVE FEED</p><h2>Recent errors</h2></div><button onClick={() => setTab("Logs")}>Open explorer →</button></div>
              {logs.filter((l) => l.level === "error").slice(0, 3).map((log) => <div className="mini-log" key={log.timestamp}><time>{timeAgo(log.timestamp)}</time><span>{log.service}</span><p>{log.message}</p></div>)}
            </article>
          </section>
        </>}

        {tab === "Logs" && <section className="panel full-panel">
          <div className="filterbar"><div className="search"><span>⌕</span><input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search messages, services, trace IDs…" aria-label="Search logs" /></div><select value={level} onChange={(e) => setLevel(e.target.value)}><option value="all">All levels</option><option value="error">Error</option><option value="warn">Warning</option><option value="info">Info</option></select><span className="result-count">{filtered.length} events</span></div>
          <div className="log-head"><span>TIME</span><span>LEVEL</span><span>SERVICE</span><span>MESSAGE</span><span>TRACE</span></div>
          <div className="logs">{filtered.map((log) => <button className="log-entry" key={`${log.timestamp}-${log.message}`} onClick={() => setExpanded(expanded === log.timestamp ? null : log.timestamp)}><span>{new Date(log.timestamp).toLocaleTimeString([], { hour12: false })}</span><span><i className={`level ${log.level}`}>{log.level}</i></span><strong>{log.service}</strong><span className="log-message">{log.message}</span><code>{log.trace_id}</code>{expanded === log.timestamp && <pre>{JSON.stringify(log.fields || {}, null, 2)}</pre>}</button>)}</div>
        </section>}

        {tab === "Alerts" && <section className="panel full-panel">
          <div className="section-intro"><div><p className="panel-kicker">INCIDENT RESPONSE</p><h2>Alert activity</h2><p>Current and recently resolved conditions across your services.</p></div><button className="primary">+ New rule</button></div>
          <div className="alert-cards">{alerts.map((alert) => <article key={alert.id || alert.rule_name}><span className={`alert-state ${alert.status}`}>{alert.status}</span><h3>{alert.rule_name || alert.name}</h3><p>{alert.service || "All services"}</p><div><span>Last fired</span><strong>{timeAgo(alert.latest_firing_at || alert.first_firing_at)}</strong></div><div><span>Current value</span><strong>{alert.current_value ?? "—"}</strong></div><button onClick={() => setTab("Logs")}>Investigate logs →</button></article>)}</div>
        </section>}

        {tab === "Services" && <section className="panel full-panel">
          <div className="section-intro"><div><p className="panel-kicker">DEPENDENCIES</p><h2>Production service map</h2><p>Health, traffic, and error propagation across correlated flows.</p></div><span className="status connected"><i />5 services online</span></div>
          <div className="service-map" aria-label="Service dependency map">
            <div className="map-line l1" /><div className="map-line l2" /><div className="map-line l3" /><div className="map-line l4" />
            <button className="node n1 healthy"><span>API</span><strong>api-gateway</strong><small>0.8% errors</small></button>
            <button className="node n2 critical"><span>CH</span><strong>checkout</strong><small>8.4% errors</small></button>
            <button className="node n3 warning"><span>OR</span><strong>orders</strong><small>1.7% errors</small></button>
            <button className="node n4 critical"><span>PY</span><strong>payments</strong><small>4.2% errors</small></button>
            <button className="node n5 healthy"><span>CA</span><strong>catalog</strong><small>0.3% errors</small></button>
          </div>
        </section>}
      </section>
    </main>
  );
}
