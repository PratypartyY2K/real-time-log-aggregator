import { NextRequest, NextResponse } from "next/server";

const paths: Record<string, string> = {
  logs: "/v1/logs",
  alerts: "/v1/alerts/history",
  deliveries: "/v1/alerts/deliveries",
  analytics: "/v1/analytics",
  graph: "/v1/graph",
};

export async function GET(request: NextRequest) {
  const resource = request.nextUrl.searchParams.get("resource") || "logs";
  const path = paths[resource];
  if (!path) return NextResponse.json({ error: "Unknown resource" }, { status: 400 });

  const baseURL = process.env.LOGAGG_QUERY_API_URL || "http://localhost:8081";
  const apiKey = process.env.LOGAGG_API_KEY;
  if (!apiKey) return NextResponse.json({ error: "LOGAGG_API_KEY is not configured" }, { status: 503 });

  const params = new URLSearchParams(request.nextUrl.searchParams);
  params.delete("resource");

  try {
    const response = await fetch(`${baseURL}${path}?${params}`, {
      headers: { "X-API-Key": apiKey },
      cache: "no-store",
    });
    const body = await response.text();
    return new NextResponse(body, {
      status: response.status,
      headers: { "content-type": response.headers.get("content-type") || "application/json" },
    });
  } catch {
    return NextResponse.json({ error: "Query API is unavailable" }, { status: 502 });
  }
}
