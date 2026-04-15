"""
polymarket_resolver.py

BTC 5m resolver via Polymarket WebSocket (market channel).

Public interface (compatibility):
    get_result_with_fallback(block_end_ts, binance_fn, timeout)
    is_resolver_running()
    start_resolver()
"""

import asyncio
import contextlib
import json
import logging
import threading
import time
from typing import Optional

import requests

log = logging.getLogger("polymarket_resolver")

# Shared state
_lock = threading.Lock()
_results = {}  # slug -> {"result": "Up"/"Down", "ts": float, "source": str}
_token_map = {}  # token_id -> {"outcome": "Up"/"Down", "slug": str}
_zero_price_first_seen = {}  # token_id -> timestamp

_ws_running = False
_ws_thread = None
_loop = None

ZERO_PRICE_THRESHOLD = 0.005
ZERO_PRICE_CONFIRM_SECONDS = 2.0
OFFICIAL_SOURCES = {"market_resolved"}

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_URL = "https://gamma-api.polymarket.com/events"


def _slug_for_ts(block_end_ts: int) -> str:
    # BTC-only by design (as requested for now).
    return f"btc-updown-5m-{block_end_ts - 300}"


def _parse_list_field(raw_value):
    if isinstance(raw_value, list):
        return raw_value
    if isinstance(raw_value, str):
        text = raw_value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            return []
    return []


def _opposite_outcome(outcome: str) -> Optional[str]:
    side = str(outcome or "").strip().lower()
    if side == "up":
        return "Down"
    if side == "down":
        return "Up"
    return None


def _normalize_outcome(value: str) -> Optional[str]:
    side = str(value or "").strip().lower()
    if side in ("up", "yes"):
        return "Up"
    if side in ("down", "no"):
        return "Down"
    return None


def _store_result(slug: str, result: str, source: str, detail: str = "") -> None:
    slug_text = str(slug or "").strip()
    res_text = _normalize_outcome(result)
    if not slug_text or res_text is None:
        return

    now = time.time()
    with _lock:
        if slug_text in _results:
            return
        _results[slug_text] = {"result": res_text, "ts": now, "source": str(source or "unknown")}

    if detail:
        log.info(f"[resolver] RESOLUTION ({source}): {slug_text} -> {res_text} | {detail}")
    else:
        log.info(f"[resolver] RESOLUTION ({source}): {slug_text} -> {res_text}")


def _fetch_tokens_for_slug(slug: str) -> dict:
    """
    Fetch Up/Down token ids for a given slug.
    Returns {"token_up": str, "token_down": str} or {}.
    """
    try:
        r = requests.get(GAMMA_URL, params={"slug": slug}, timeout=5)
        r.raise_for_status()
        data = r.json()
        if not data or not isinstance(data, list):
            return {}

        markets = (data[0] or {}).get("markets") or []
        if not markets:
            return {}
        market = markets[0]

        outcomes = _parse_list_field(market.get("outcomes"))
        tokens = _parse_list_field(market.get("clobTokenIds"))
        result = {}
        for outcome, token in zip(outcomes, tokens):
            outcome_text = str(outcome).strip().upper()
            token_text = str(token).strip()
            if not token_text:
                continue
            if outcome_text == "UP":
                result["token_up"] = token_text
            elif outcome_text == "DOWN":
                result["token_down"] = token_text
        return result
    except Exception as e:
        log.error(f"[resolver] error fetching tokens for {slug}: {e}")
        return {}


def _register_block_tokens(block_end_ts: int) -> list:
    """
    Register tokens for the given block and next block.
    Returns token ids to subscribe.
    """
    tokens_to_subscribe = []
    for ts in (block_end_ts, block_end_ts + 300):
        slug = _slug_for_ts(ts)
        if ensure_slug_registered(slug):
            with _lock:
                sl_tokens = [tid for tid, info in _token_map.items() if info.get("slug") == slug]
            tokens_to_subscribe.extend(sl_tokens)
        else:
            log.warning(f"[resolver] tokens missing for {slug}")
    return tokens_to_subscribe


def _all_token_ids() -> list:
    with _lock:
        return list(set(_token_map.keys()))


def ensure_slug_registered(slug: str) -> bool:
    """
    Ensure token mapping for a specific slug is loaded.
    Returns True when both UP and DOWN token ids are mapped.
    """
    token_data = _fetch_tokens_for_slug(slug)
    up = str(token_data.get("token_up") or "").strip()
    down = str(token_data.get("token_down") or "").strip()
    if not up or not down:
        return False
    with _lock:
        _token_map[up] = {"outcome": "Up", "slug": slug}
        _token_map[down] = {"outcome": "Down", "slug": slug}
    log.info(f"[resolver] tokens registered: {slug}")
    return True


def peek_result_for_slug(slug: str) -> Optional[str]:
    """
    Non-blocking read of cached OFFICIAL result for a slug.
    (Heuristic results are ignored here.)
    """
    with _lock:
        entry = _results.get(str(slug or "").strip())
    if not entry:
        return None
    source = str(entry.get("source") or "").strip()
    if source not in OFFICIAL_SOURCES:
        return None
    return str(entry.get("result") or "").strip() or None


async def _send_subscription(ws, token_ids: list) -> None:
    if not token_ids:
        return
    await ws.send(
        json.dumps(
            {
                "assets_ids": token_ids,
                "type": "Market",
                "custom_feature_enabled": True,
            }
        )
    )


async def _token_refresh_loop(ws):
    """Refresh tokens each minute and resubscribe with full token list."""
    while True:
        await asyncio.sleep(60)
        try:
            now = int(time.time())
            bucket = now - (now % 300)

            found_new = False
            for block_end in (bucket, bucket + 300, bucket + 600):
                slug = _slug_for_ts(block_end)
                with _lock:
                    known = any(info["slug"] == slug for info in _token_map.values())
                if known:
                    continue
                new_tokens = _register_block_tokens(block_end)
                if new_tokens:
                    found_new = True

            if found_new:
                token_ids = _all_token_ids()
                await _send_subscription(ws, token_ids)
                log.info(f"[resolver] tokens refreshed: total={len(token_ids)}")
        except Exception as e:
            log.warning(f"[resolver] token refresh error: {e}")


def _clear_zero_trackers_for_slug(slug: str) -> None:
    with _lock:
        to_remove = [tid for tid, info in _token_map.items() if info.get("slug") == slug]
        for tid in to_remove:
            _zero_price_first_seen.pop(tid, None)


def _process_event(event: dict):
    """
    Resolution logic:
    - official: market_resolved event (preferred).
    - asset at ~0.00 for 2s => this outcome likely LOST, opposite wins.
    - backup: asset at >=0.98 => this outcome wins.
    """
    event_type = str(event.get("event_type") or "").strip().lower()

    # Preferred official path from Polymarket market channel.
    if event_type == "market_resolved":
        try:
            slug = str(event.get("slug") or "").strip()
            winning_outcome = _normalize_outcome(str(event.get("winning_outcome") or ""))
            winning_asset_id = str(event.get("winning_asset_id") or "").strip()

            # Fallback: infer via token map when winning_outcome is absent/unknown.
            if winning_outcome is None and winning_asset_id:
                with _lock:
                    token_info = _token_map.get(winning_asset_id)
                if token_info:
                    winning_outcome = _normalize_outcome(token_info.get("outcome"))
                    if not slug:
                        slug = str(token_info.get("slug") or "").strip()

            # Last fallback: infer slug from listed assets_ids.
            if not slug:
                for asset_id in event.get("assets_ids") or []:
                    aid = str(asset_id or "").strip()
                    if not aid:
                        continue
                    with _lock:
                        token_info = _token_map.get(aid)
                    if token_info and token_info.get("slug"):
                        slug = str(token_info.get("slug")).strip()
                        break

            if winning_outcome and slug:
                _store_result(
                    slug=slug,
                    result=winning_outcome,
                    source="market_resolved",
                    detail=f"winning_outcome={winning_outcome}",
                )
                _clear_zero_trackers_for_slug(slug)
            return
        except Exception as e:
            log.debug(f"[resolver] market_resolved parse error: {e}")
            return

    if event_type != "last_trade_price":
        return

    try:
        price = float(event.get("price", 0.5))
        asset_id = str(event.get("asset_id", ""))
        if not asset_id:
            return

        with _lock:
            token_info = _token_map.get(asset_id)
        if not token_info:
            return

        outcome = token_info["outcome"]  # Up / Down
        slug = token_info["slug"]
        now = time.time()

        with _lock:
            if slug in _results:
                return

        if price <= ZERO_PRICE_THRESHOLD:
            with _lock:
                first_seen = _zero_price_first_seen.get(asset_id)
                if first_seen is None:
                    _zero_price_first_seen[asset_id] = now
                    log.info(
                        f"[resolver] {slug} token {outcome} near $0.00; "
                        f"confirming for {ZERO_PRICE_CONFIRM_SECONDS:.1f}s"
                    )
                    return

                if (now - first_seen) >= ZERO_PRICE_CONFIRM_SECONDS:
                    result = _opposite_outcome(outcome)
                    if result is None:
                        return
                    _store_result(
                        slug=slug,
                        result=result,
                        source="price_heuristic_zero",
                        detail=f"token {outcome} near $0.00 for {now - first_seen:.1f}s",
                    )
            _clear_zero_trackers_for_slug(slug)
            return

        # Price recovered: clear zero tracker for this token
        with _lock:
            _zero_price_first_seen.pop(asset_id, None)

        # Backup resolution path
        if price >= 0.98:
            _store_result(
                slug=slug,
                result=outcome,
                source="price_heuristic_ge98",
                detail=f"price={price:.3f}",
            )
            _clear_zero_trackers_for_slug(slug)
    except Exception as e:
        log.debug(f"[resolver] event processing error: {e}")


async def _ws_loop():
    global _ws_running
    import websockets

    log.info("[resolver] websocket loop started")
    _ws_running = False

    now = int(time.time())
    bucket = now - (now % 300)
    tokens = _register_block_tokens(bucket + 300)
    if not tokens:
        log.error("[resolver] no initial tokens to subscribe")
        return

    reconnect_delay = 2
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=10, ping_timeout=20) as ws:
                token_ids = _all_token_ids() or tokens
                await _send_subscription(ws, token_ids)
                _ws_running = True
                reconnect_delay = 2
                log.info(f"[resolver] connected; subscribed tokens={len(token_ids)}")

                refresh_task = asyncio.create_task(_token_refresh_loop(ws))
                try:
                    async for raw in ws:
                        try:
                            events = json.loads(raw)
                            if not isinstance(events, list):
                                events = [events]
                            for event in events:
                                _process_event(event)
                        except Exception as e:
                            log.debug(f"[resolver] message parse error: {e}")
                finally:
                    refresh_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await refresh_task
                _ws_running = False
        except Exception as e:
            _ws_running = False
            log.warning(f"[resolver] websocket disconnected: {e}; reconnecting in {reconnect_delay}s")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30)

            now = int(time.time())
            bucket = now - (now % 300)
            tokens = _register_block_tokens(bucket + 300)
            if not tokens:
                log.error("[resolver] no tokens during reconnect")
                break

    _ws_running = False


def _run_ws_in_thread():
    global _loop, _ws_running
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    try:
        _loop.run_until_complete(_ws_loop())
    except Exception as e:
        log.error(f"[resolver] websocket thread stopped: {e}")
    finally:
        _ws_running = False


def start_resolver() -> bool:
    """Start websocket resolver in daemon thread."""
    global _ws_thread

    if _ws_thread and _ws_thread.is_alive():
        log.info("[resolver] already running")
        return True

    log.info("[resolver] starting websocket resolver")
    _ws_thread = threading.Thread(target=_run_ws_in_thread, daemon=True, name="polymarket-resolver")
    _ws_thread.start()

    for _ in range(10):
        time.sleep(0.5)
        if _ws_running and _ws_thread.is_alive():
            log.info("[resolver] websocket resolver active")
            return True

    log.warning("[resolver] websocket resolver did not become active in time")
    return False


def is_resolver_running() -> bool:
    return bool(_ws_running and _ws_thread is not None and _ws_thread.is_alive())


def get_result_from_ws(
    block_end_timestamp: int, timeout: float = 15.0, include_heuristics: bool = False
) -> Optional[str]:
    """
    Wait for result for a block_end timestamp.
    Returns "Up", "Down" or None on timeout.
    By default, returns only OFFICIAL websocket outcomes (market_resolved).
    """
    slug = _slug_for_ts(block_end_timestamp)
    deadline = time.time() + timeout

    with _lock:
        tokens_known = any(info["slug"] == slug for info in _token_map.values())

    if not tokens_known:
        log.info(f"[resolver] registering tokens for {slug}")
        ensure_slug_registered(slug)

    log.info(f"[resolver] waiting result for {slug} timeout={timeout:.1f}s")
    while time.time() < deadline:
        with _lock:
            entry = _results.get(slug)
        if entry:
            source = str(entry.get("source") or "").strip()
            if include_heuristics or source in OFFICIAL_SOURCES:
                return entry["result"]
        time.sleep(0.1)

    log.warning(f"[resolver] timeout waiting for {slug}")
    return None


def get_result_with_fallback(block_end_timestamp: int, binance_fn, timeout: float = 15.0):
    """
    Uses ONLY Polymarket WebSocket (no Binance fallback).
    Returns (result, "polymarket_ws") or (None, "timeout"/"offline").
    """
    _ = binance_fn  # kept for compatibility
    if is_resolver_running():
        result = get_result_from_ws(block_end_timestamp, timeout=timeout)
        if result:
            return result, "polymarket_ws"
        log.warning("[resolver] ws timeout; block skipped")
        return None, "timeout"
    log.warning("[resolver] resolver offline")
    return None, "offline"


def cleanup_old_results(max_age: float = 1800.0):
    """Cleanup old in-memory entries."""
    cutoff = time.time() - max_age
    with _lock:
        stale_slugs = {slug for slug, data in _results.items() if data["ts"] < cutoff}
        for slug in list(stale_slugs):
            _results.pop(slug, None)
        for token_id, info in list(_token_map.items()):
            if info.get("slug") in stale_slugs:
                _token_map.pop(token_id, None)
        for token_id, ts in list(_zero_price_first_seen.items()):
            if (time.time() - ts) > 600:
                _zero_price_first_seen.pop(token_id, None)
