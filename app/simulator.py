"""Background rail feed: ticker transactions + seeded cases through the funnel.
Runs as an asyncio task inside the web process (SIMULATOR=0 disables — e.g.
when the real scan loop takes over)."""
import asyncio
import random
import time

from . import bus, cases, seeds, store

TX_TYPES = [("sale", 60), ("card sale", 22), ("cash sale", 10), ("refund", 4), ("void", 3), ("no_sale", 1)]
STORES = list(seeds.BRANCHES)


def _pick_type() -> str:
    r, acc = random.random() * 100, 0
    for t, w in TX_TYPES:
        acc += w
        if r < acc:
            return t
    return "sale"


async def run(inject_event: asyncio.Event, ticker: bool = True) -> None:
    """Case/candidate cycle for the funnel. `ticker=False` when the real
    finance_stream feed (app/feed.py) owns the transaction ticker."""
    lap, idx, since = 0, 0, 0
    while True:
        await asyncio.sleep(1.1)
        if ticker:
            sid = random.choice(STORES)
            bus.publish({"type": "ticker", "ts": time.time(), "branch": seeds.BRANCHES[sid],
                         "store_id": sid, "txn": _pick_type(),
                         "amount_cents": random.randint(800, 3500)})
            store.bump(scanned=1)
        since += 1
        if inject_event.is_set() or since >= random.randint(5, 8):
            inject_event.clear()
            since = 0
            seed = seeds.CASES[idx % len(seeds.CASES)]
            idx += 1
            lap = idx // len(seeds.CASES)
            bus.publish({"type": "ticker", "ts": time.time(), "cand": True,
                         "label": f"candidate: {seed['duty']} @ {seeds.name_of(seed['entity_id'])}"})
            store.bump(investigating=1)
            bus.publish({"type": "kpis", "stats": store.snapshot()})
            asyncio.get_event_loop().call_later(
                2.2, lambda s=seed, l=lap: _resolve(s, l))
        else:
            bus.publish({"type": "kpis", "stats": store.snapshot()})


def _resolve(seed: dict, lap: int) -> None:
    store.bump(investigating=-1)
    cases.from_seed(seed, lap=int(time.time() * 1000))
