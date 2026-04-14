import argparse
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests


GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
CHAINLINK_STREAM_PAGE = "https://data.chain.link/streams/{stream_slug}"
CHAINLINK_HISTORICAL_URL = "https://data.chain.link/api/historical-data-engine-stream-data"


def iso_to_epoch(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).timestamp()
    except Exception:
        return None


def epoch_to_utc_text(ts: Optional[float]) -> str:
    if ts is None:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def parse_list_field(raw_value) -> List:
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
        except json.JSONDecodeError:
            return []
    return []


def normalize_outcome_name(name: Optional[str]) -> Optional[str]:
    if name is None:
        return None
    text = str(name).strip().lower()
    if text == "up":
        return "UP"
    if text == "down":
        return "DOWN"
    return str(name).strip()


def infer_official_outcome(outcomes_raw, outcome_prices_raw) -> Optional[str]:
    outcomes = [str(x) for x in parse_list_field(outcomes_raw)]
    prices = parse_list_field(outcome_prices_raw)
    parsed_prices: List[float] = []
    for p in prices:
        try:
            parsed_prices.append(float(p))
        except Exception:
            parsed_prices.append(0.0)

    if not outcomes or not parsed_prices:
        return None

    winner_idx = None
    for idx, price in enumerate(parsed_prices):
        if price >= 0.999999:
            winner_idx = idx
            break

    if winner_idx is None:
        return None
    if winner_idx >= len(outcomes):
        return None
    return normalize_outcome_name(outcomes[winner_idx])


def parse_next_data_payload(html: str) -> dict:
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        flags=re.S,
    )
    if not match:
        raise RuntimeError("Nao foi possivel localizar __NEXT_DATA__ na pagina da Chainlink.")
    return json.loads(match.group(1))


def get_primary_abi_index(schema_name: str) -> int:
    schema = str(schema_name or "").strip()
    mapping = {
        "Premium": 0,
        "v7": 0,
        "v8": 1,
        "v9": 0,
        "v10": 1,
        "v11": 0,
    }
    return mapping.get(schema, 0)


def resolve_chainlink_feed(session: requests.Session, stream_slug: str, timeout: float) -> Tuple[str, int, str]:
    url = CHAINLINK_STREAM_PAGE.format(stream_slug=stream_slug)
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    payload = parse_next_data_payload(resp.text)

    stream_data = (((payload.get("props") or {}).get("pageProps") or {}).get("streamData") or {})
    meta = stream_data.get("streamMetadata") or {}
    docs = meta.get("docs") or {}
    schema = docs.get("schema") or "Premium"
    abi_index = get_primary_abi_index(schema)

    extra_cfg = stream_data.get("extraConfig") or {}
    feed_id = extra_cfg.get("feedId") or meta.get("feedId")
    if not feed_id:
        raise RuntimeError("Nao foi possivel obter feedId da Chainlink.")

    return str(feed_id), int(abi_index), str(schema)


def parse_candlestick_field(candlestick: str, field_name: str) -> Tuple[Optional[float], Optional[float]]:
    text = str(candlestick or "")
    pattern = rf'{re.escape(field_name)}:\(ts:"([^"]+)",val:([-\d.]+)\)'
    match = re.search(pattern, text)
    if not match:
        return None, None

    ts_raw = match.group(1).strip()
    ts_norm = ts_raw.replace(" ", "T", 1)
    if re.search(r"[+-]\d{2}$", ts_norm):
        ts_norm = ts_norm + ":00"

    ts_epoch = iso_to_epoch(ts_norm)
    try:
        value = float(match.group(2))
    except Exception:
        value = None
    return ts_epoch, value


def fetch_chainlink_open_minute_map(
    session: requests.Session,
    feed_id: str,
    abi_index: int,
    timeout: float,
) -> Dict[int, float]:
    resp = session.get(
        CHAINLINK_HISTORICAL_URL,
        params={"feedId": feed_id, "abiIndex": str(abi_index), "timeRange": "1D"},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()

    nodes = (((data.get("data") or {}).get("allStreamValuesGeneric1Minutes") or {}).get("nodes") or [])
    by_minute: Dict[int, float] = {}
    for item in nodes:
        attr = str(item.get("attributeName") or "").strip().lower()
        if attr != "benchmark":
            continue
        bucket_epoch = iso_to_epoch(item.get("bucket"))
        if bucket_epoch is None:
            continue
        _, open_value = parse_candlestick_field(item.get("candlestick"), "open")
        if open_value is None:
            continue
        minute_key = int(bucket_epoch) - (int(bucket_epoch) % 60)
        by_minute[minute_key] = open_value
    return by_minute


def fetch_series_events(
    session: requests.Session,
    series_id: str,
    page_size: int,
    timeout: float,
    max_pages: int,
    closed_only: bool,
) -> List[dict]:
    page_size = min(max(1, page_size), 500)
    all_events: List[dict] = []
    offset = 0
    page = 0

    while True:
        if max_pages > 0 and page >= max_pages:
            break

        resp = session.get(
            GAMMA_EVENTS_URL,
            params={
                "series_id": str(series_id),
                "closed": "true" if closed_only else "false",
                "limit": page_size,
                "offset": offset,
            },
            timeout=timeout,
        )
        resp.raise_for_status()
        chunk = resp.json()
        if not isinstance(chunk, list) or not chunk:
            break

        all_events.extend(chunk)
        page += 1
        offset += page_size
        print(f"[PAGINA] {page} | recebidos={len(chunk)} | total={len(all_events)}")

        if len(chunk) < page_size:
            break

    return all_events


def event_to_row(event: dict, chainlink_minute_open: Dict[int, float]) -> Optional[dict]:
    markets = event.get("markets") or []
    if not markets:
        return None
    market = markets[0]

    slug = str(market.get("slug") or event.get("slug") or "")
    start_iso = market.get("eventStartTime") or event.get("startTime")
    end_iso = market.get("endDate") or event.get("endDate")
    start_ts = iso_to_epoch(start_iso)
    end_ts = iso_to_epoch(end_iso)

    if start_ts is not None:
        start_minute = int(start_ts) - (int(start_ts) % 60)
    else:
        start_minute = None
    if end_ts is not None:
        end_minute = int(end_ts) - (int(end_ts) % 60)
    else:
        end_minute = None

    target_price = chainlink_minute_open.get(start_minute) if start_minute is not None else None
    close_price = chainlink_minute_open.get(end_minute) if end_minute is not None else None

    inferred_chainlink = None
    if target_price is not None and close_price is not None:
        inferred_chainlink = "UP" if close_price >= target_price else "DOWN"

    official_outcome = infer_official_outcome(market.get("outcomes"), market.get("outcomePrices"))
    match_chainlink = None
    if official_outcome and inferred_chainlink:
        match_chainlink = "1" if official_outcome == inferred_chainlink else "0"

    row = {
        "slug": slug,
        "question": market.get("question") or event.get("title") or "",
        "start_time_utc": epoch_to_utc_text(start_ts),
        "end_time_utc": epoch_to_utc_text(end_ts),
        "closed": str(bool(market.get("closed"))).lower(),
        "closed_time": str(market.get("closedTime") or ""),
        "uma_resolution_status": str(market.get("umaResolutionStatus") or ""),
        "official_outcome": official_outcome or "",
        "target_chainlink": f"{target_price:.10f}" if target_price is not None else "",
        "close_chainlink": f"{close_price:.10f}" if close_price is not None else "",
        "inferred_chainlink": inferred_chainlink or "",
        "match_chainlink_official": match_chainlink or "",
        "resolution_source": str(market.get("resolutionSource") or event.get("resolutionSource") or ""),
        "series_slug": str(event.get("seriesSlug") or ""),
        "event_id": str(event.get("id") or ""),
        "market_id": str(market.get("id") or ""),
        "outcomes_raw": json.dumps(parse_list_field(market.get("outcomes")), ensure_ascii=True),
        "outcome_prices_raw": json.dumps(parse_list_field(market.get("outcomePrices")), ensure_ascii=True),
    }
    return row


def write_csv(rows: List[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "slug",
        "question",
        "start_time_utc",
        "end_time_utc",
        "closed",
        "closed_time",
        "uma_resolution_status",
        "official_outcome",
        "target_chainlink",
        "close_chainlink",
        "inferred_chainlink",
        "match_chainlink_official",
        "resolution_source",
        "series_slug",
        "event_id",
        "market_id",
        "outcomes_raw",
        "outcome_prices_raw",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def summarize(rows: List[dict]) -> None:
    total = len(rows)
    official = sum(1 for r in rows if r["official_outcome"])
    with_target = sum(1 for r in rows if r["target_chainlink"])
    with_target_close = sum(1 for r in rows if r["target_chainlink"] and r["close_chainlink"])
    with_inferred = sum(1 for r in rows if r["inferred_chainlink"])
    with_match = sum(1 for r in rows if r["match_chainlink_official"] == "1")

    print("[RESUMO]")
    print(f"  total_rows={total}")
    print(f"  with_official_outcome={official}")
    print(f"  with_chainlink_target={with_target}")
    print(f"  with_chainlink_target_and_close={with_target_close}")
    print(f"  with_chainlink_inference={with_inferred}")
    print(f"  chainlink_match_official={with_match}")
    if with_inferred > 0:
        print(f"  chainlink_match_rate={with_match / with_inferred:.4f}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exporta base historica da serie BTC Up/Down 5m da Polymarket."
    )
    parser.add_argument(
        "--series-id",
        default="10684",
        help="Series ID da Polymarket para BTC Up/Down 5m (padrao: 10684).",
    )
    parser.add_argument(
        "--stream-slug",
        default="btc-usd",
        help="Slug do stream da Chainlink (padrao: btc-usd).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=500,
        help="Tamanho de pagina da Gamma API (maximo 500).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Limite de paginas para debug (0 = sem limite).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Timeout HTTP por request em segundos.",
    )
    parser.add_argument(
        "--output",
        default=str(Path("Target") / "btc_updown_5m_history.csv"),
        help="Arquivo CSV de saida.",
    )
    parser.add_argument(
        "--include-open",
        action="store_true",
        help="Inclui mercados nao fechados na exportacao (padrao: apenas fechados).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = requests.Session()

    print("[INIT] Resolvendo feed da Chainlink...")
    feed_id, abi_index, schema = resolve_chainlink_feed(session, args.stream_slug, args.timeout)
    print(f"[INIT] feed_id={feed_id} | abi_index={abi_index} | schema={schema}")

    print("[INIT] Baixando candles 1m publicos da Chainlink (janela recente ~24h)...")
    chainlink_open_by_minute = fetch_chainlink_open_minute_map(
        session=session,
        feed_id=feed_id,
        abi_index=abi_index,
        timeout=args.timeout,
    )
    print(f"[INIT] candles_1m_carregados={len(chainlink_open_by_minute)}")

    print("[INIT] Baixando eventos da serie da Polymarket...")
    events = fetch_series_events(
        session=session,
        series_id=args.series_id,
        page_size=args.page_size,
        timeout=args.timeout,
        max_pages=args.max_pages,
        closed_only=not args.include_open,
    )
    print(f"[INIT] eventos_recebidos={len(events)}")

    rows: List[dict] = []
    for ev in events:
        row = event_to_row(ev, chainlink_open_by_minute)
        if row is None:
            continue
        rows.append(row)

    rows.sort(key=lambda r: r["start_time_utc"])
    output_path = Path(args.output)
    write_csv(rows, output_path)
    print(f"[OK] CSV salvo em: {output_path}")
    summarize(rows)


if __name__ == "__main__":
    main()
