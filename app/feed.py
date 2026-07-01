"""Real transaction feed for the rail ticker.

Tails `tools/finance_stream` (run as a subprocess, JSONL on stdout) and maps
selected rows into ticker events + the scanned counter. This is *ambient
context only* — cases/KPIs about judgment still come from agent verdicts.

Enabled when WORLD_PG_URI is set (FEED=stream|sim|auto overrides). Falls back
to the simulator's fake ticker otherwise, so local dev needs zero config.

The generator runs time-compressed (FEED_SPEED, default 60 → 1 real second
≈ 1 sim minute) starting at today 09:00 sim time, so the ticker is lively at
any real-world hour and end-of-day events roll by every ~12 real minutes.
--inject-leak is on with a leak log, ready for the scan-loop workstream to
score against.
"""
import asyncio
import datetime as dt
import json
import os
import pathlib
import sys

from . import bus, seeds, store

TOOLS_DIR = pathlib.Path(__file__).resolve().parent.parent / "tools"
LEAK_LOG = os.path.abspath(os.environ.get("FEED_LEAK_LOG", "feed_leaks.jsonl"))


def enabled() -> bool:
    mode = os.environ.get("FEED", "auto")
    if mode == "stream":
        return True
    if mode == "sim":
        return False
    return bool(os.environ.get("WORLD_PG_URI"))


def _epoch(ts: str | None) -> float:
    try:
        return dt.datetime.fromisoformat(ts).timestamp()
    except (TypeError, ValueError):
        return dt.datetime.now(dt.timezone.utc).timestamp()


def _event(table: str, row: dict) -> dict | None:
    """Map a finance_stream row to a ticker event (or None to skip)."""
    if table == "fin_register_txns":
        return {"type": "ticker", "ts": _epoch(row.get("ts")),
                "branch": seeds.BRANCHES.get(row.get("store_id"), row.get("store_id")),
                "store_id": row.get("store_id"),
                "txn": row.get("txn_type"), "amount_cents": row.get("amount_cents") or 0}
    if table == "fin_invoices":
        return {"type": "ticker", "ts": _epoch(row.get("invoiced_at")),
                "branch": seeds.SUPPLIERS.get(row.get("supplier_id"), row.get("supplier_id")),
                "txn": f"invoice · {row.get('status')}", "amount_cents": row.get("total_cents") or 0}
    if table == "fin_payments_out":
        return {"type": "ticker", "ts": _epoch(row.get("paid_at")),
                "branch": seeds.SUPPLIERS.get(row.get("supplier_id"), row.get("supplier_id")),
                "txn": "payment · ach", "amount_cents": row.get("amount_cents") or 0}
    if table == "fin_bank_settlements":
        return {"type": "ticker", "ts": dt.datetime.now(dt.timezone.utc).timestamp(),
                "branch": seeds.BRANCHES.get(row.get("store_id"), row.get("store_id")),
                "txn": "bank deposit", "amount_cents": row.get("net_deposit_cents") or 0}
    return None  # rollups/lines/receipts: too chatty for a 7-row ticker


def _cmd() -> list[str]:
    sim_start = dt.date.today().isoformat() + "T09:00:00"
    return [sys.executable, "-m", "finance_stream",
            "--sink", "stdout",
            "--speed", os.environ.get("FEED_SPEED", "60"),
            "--rate", os.environ.get("FEED_RATE", "8"),
            "--po-rate", os.environ.get("FEED_PO_RATE", "4"),
            "--sim-start", sim_start,
            "--inject-leak", "all",
            "--leak-rate", os.environ.get("FEED_LEAK_RATE", "0.05"),
            "--leak-log", LEAK_LOG]


async def run() -> None:
    """Supervised: spawn the generator, tail stdout, respawn on exit."""
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    while True:
        try:
            proc = await asyncio.create_subprocess_exec(
                *_cmd(), stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL, env=env, cwd=TOOLS_DIR)
            bus.publish({"type": "feed", "mode": "stream"})
            assert proc.stdout is not None
            async for raw in proc.stdout:
                line = raw.decode().strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ev = _event(msg.get("table", ""), msg.get("row") or {})
                if ev:
                    bus.publish(ev)
                    if msg.get("table") == "fin_register_txns":
                        store.bump(scanned=1)
                        bus.publish({"type": "kpis", "stats": store.snapshot()})
            await proc.wait()
        except Exception:
            pass
        await asyncio.sleep(5)  # generator died (bad DSN, network) — retry
