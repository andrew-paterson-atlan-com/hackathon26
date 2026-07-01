"""Penny Console — FastAPI app.

Routes:
  GET  /            console UI
  GET  /events      SSE rail stream (snapshot first, then live events)
  POST /turn        chat turn → NDJSON stream (sim or real agent backend)
  POST /cases/{id}/open|confirm|dismiss   case processing (app-owned)
  POST /cases/{id}/why                    ask the backend to explain → NDJSON
  GET  /healthz

Run:  uvicorn app.main:app --reload      (Railway: see railway.toml)
"""
import asyncio
import json
import os
import pathlib

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import backends, bus, cases, feed, simulator, store

STATIC = pathlib.Path(__file__).parent / "static"
app = FastAPI(title="Penny Console")
app.mount("/static", StaticFiles(directory=STATIC), name="static")

_inject = asyncio.Event()


@app.on_event("startup")
async def _startup():
    store.conn()  # create tables
    live_feed = feed.enabled()
    if live_feed:
        asyncio.create_task(feed.run())          # real txns own the ticker
    if os.environ.get("SIMULATOR", "1") != "0":
        # cases/candidates still simulated until the real scan loop lands
        asyncio.create_task(simulator.run(_inject, ticker=not live_feed))


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


@app.get("/healthz")
async def healthz():
    return {"ok": True, "backend": os.environ.get("PENNY_BACKEND", "sim"),
            "feed": "stream" if feed.enabled() else "sim"}


def _sse(event: dict) -> str:
    return f"data: {json.dumps(event, default=str)}\n\n"


@app.get("/events")
async def events(request: Request):
    async def gen():
        q = bus.subscribe()
        try:
            yield _sse({"type": "snapshot", "stats": store.snapshot(),
                        "flags": store.open_flags(), "clears": store.cleared()})
            while True:
                if await request.is_disconnected():
                    break
                try:
                    ev = await asyncio.wait_for(q.get(), timeout=15)
                    yield _sse(ev)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            bus.unsubscribe(q)
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _ndjson(agen):
    async def gen():
        async for ev in agen:
            yield json.dumps(ev, default=str) + "\n"
    return StreamingResponse(gen(), media_type="application/x-ndjson")


@app.post("/turn")
async def turn(payload: dict):
    session_id = payload.get("session_id") or "anon"
    text = (payload.get("text") or "").strip()
    if not text:
        raise HTTPException(400, "text required")
    return _ndjson(backends.turn(session_id, text))


@app.post("/cases/{case_id}/open")
async def open_case(case_id: int):
    kase = store.get_case(case_id)
    if not kase:
        raise HTTPException(404)
    return JSONResponse(kase)


@app.post("/cases/{case_id}/confirm")
async def confirm_case(case_id: int):
    kase = cases.act(case_id, "confirm")
    if not kase:
        raise HTTPException(409, "case not open")
    return JSONResponse(kase)


@app.post("/cases/{case_id}/dismiss")
async def dismiss_case(case_id: int):
    kase = cases.act(case_id, "dismiss")
    if not kase:
        raise HTTPException(409, "case not open")
    return JSONResponse(kase)


@app.post("/cases/{case_id}/why")
async def why_case(case_id: int, payload: dict):
    kase = store.get_case(case_id)
    if not kase:
        raise HTTPException(404)
    session_id = payload.get("session_id") or "anon"
    if os.environ.get("PENNY_BACKEND", "sim") == "agent":
        q = f"Why did you flag this case: {kase['title']} ({kase['duty']}, {kase['entity_id']})? Summarise the evidence."
        return _ndjson(backends.turn(session_id, q))
    return _ndjson(backends.sim_why(session_id, kase))


@app.post("/inject")
async def inject():
    _inject.set()
    return {"ok": True}
