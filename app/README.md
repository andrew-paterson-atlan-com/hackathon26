# Penny Console — FastAPI app (`app/`)

The ask-first console from the approved mock, live: chat pane (primary surface)
+ precision-funnel rail over SSE. One Python service — it can run the **real
Penny agent in-process** (same repo, `agent/sdk_loop.py`), no sidecar needed.

## Run locally

```bash
pip install -r requirements.txt -r app/requirements.txt
uvicorn app.main:app --reload            # http://localhost:8000
```

Zero config needed: defaults to the **simulated backend** (`PENNY_BACKEND=sim`)
with the rail simulator on — the full demo loop works offline.

## Switch on the real agent

```bash
set -a && . ./.env && set +a             # MCCTX_MCP_URL, MCP_AUTH_TOKEN (+ Claude auth)
PENNY_BACKEND=agent uvicorn app.main:app
```

`PENNY_BACKEND=agent` gives every browser session a live multi-turn Penny
session (the `agent/chat.py` pattern, served): `run_sql`/`read_skill` calls
stream into the chat as trace lines, captured `submit_*` verdicts become
flagged/cleared cases in the rail via the app-owned routing map, and
scope-fence violations surface as ⛔ system messages.

## Environment

| var | default | meaning |
|---|---|---|
| `PENNY_BACKEND` | `sim` | `sim` (scripted) · `agent` (real Penny, in-process) |
| `SIMULATOR` | `1` | `0` disables the rail ticker/case simulator |
| `APP_DB` | `penny_console.db` | SQLite path (cases, turns, audit, stats) |
| `MCCTX_MCP_URL` / `MCP_AUTH_TOKEN` | — | required for `agent` backend |
| `WORLD_PG_URI` | — | read-only world DB; presence enables the **real ticker feed** |
| `FEED` | `auto` | `stream` (force) · `sim` (force off) · `auto` (on if `WORLD_PG_URI`) |
| `FEED_SPEED` | `60` | finance_stream time compression (1 real s ≈ 1 sim min) |
| `FEED_RATE` / `FEED_PO_RATE` | `8` / `4` | txns per store per sim-hour / POs per sim-hour |
| `FEED_LEAK_RATE` | `0.05` | injected-leak probability (ground truth → `FEED_LEAK_LOG`) |

## Deploy on Railway

`railway.toml` at the repo root configures build + start + healthcheck:

```bash
railway login
railway init          # or link the GitHub repo in the dashboard
railway up
railway variables set PENNY_BACKEND=sim SIMULATOR=1
```

Notes:
- SQLite state is per-deploy (ephemeral FS) — fine for the demo; attach a
  Railway volume or move to Railway Postgres if state must survive redeploys.
- One uvicorn worker (the default) — the SSE bus is in-process.
- The world DB is already on Railway; when the agent backend goes live, put
  the console in the same project/region.

## HTTP surface

| route | what |
|---|---|
| `GET /` | console UI |
| `GET /events` | SSE rail: `snapshot`, `ticker`, `case.flagged/updated/cleared`, `kpis` |
| `POST /turn` | `{session_id, text}` → NDJSON stream: `trace`, `sys`, `verdict`, `done` |
| `POST /cases/{id}/open` | case payload for the chat widget |
| `POST /cases/{id}/confirm` | app-side: route + audit (`open → routed`) |
| `POST /cases/{id}/dismiss` | app-side: ledger + audit (`open → dismissed`) |
| `POST /cases/{id}/why` | NDJSON explanation (real agent turn when `PENNY_BACKEND=agent`) |
| `POST /inject` | force the simulator to raise the next case |

## Where things live

`main.py` routes · `bus.py` SSE fanout · `store.py` SQLite · `seeds.py` demo
cases/answers · `backends.py` sim + in-process agent · `cases.py` routing map +
case factory (agent verdict → Case) · `simulator.py` rail feed ·
`static/` UI (console.css tokens from mccontext.com, console.js).
