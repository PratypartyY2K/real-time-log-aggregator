# LogScope dashboard

Next.js operations console for the real-time log aggregator.

## Run locally

```bash
cp .env.example .env.local
npm install
npm run dev
```

The dashboard runs at `http://localhost:3000` and proxies authenticated requests
to `LOGAGG_QUERY_API_URL`. If the query API is unavailable or no API key is
configured, the interface uses built-in demonstration data.
