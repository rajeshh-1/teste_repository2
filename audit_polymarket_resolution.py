import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests


GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
SCRIPT_DIR = Path(__file__).resolve().parent
ASSET_5M_CONFIG = {
    "btc": {"folder_name": "btc 5min", "file_prefix": "btc_updown"},
    "eth": {"folder_name": "eth 5min", "file_prefix": "eth_updown"},
    "sol": {"folder_name": "sol 5min", "file_prefix": "sol_updown"},
}


def normalize_asset_name(value: Optional[str]) -> str:
    return str(value or "").strip().lower()


def resolve_asset_config(asset_name: str) -> Dict[str, str]:
    asset = normalize_asset_name(asset_name)
    cfg = ASSET_5M_CONFIG.get(asset)
    if cfg is None:
        supported = ", ".join(sorted(ASSET_5M_CONFIG.keys()))
        raise ValueError(f"Ativo invalido: '{asset_name}'. Use um de: {supported}.")
    return {
        "asset": asset,
        "folder_name": str(cfg["folder_name"]),
        "file_prefix": str(cfg["file_prefix"]),
    }


def resolve_paths(
    asset_cfg: Dict[str, str],
    asset_dir: Optional[str],
    paper_csv: Optional[str],
    capture_csv: Optional[str],
    output_csv: Optional[str],
) -> Tuple[Path, Path, Path]:
    base_dir = Path(asset_dir) if asset_dir else (SCRIPT_DIR / asset_cfg["folder_name"])
    file_prefix = asset_cfg["file_prefix"]
    resolved_paper = Path(paper_csv) if paper_csv else (base_dir / f"{file_prefix}_paper_trade.csv")
    resolved_capture = Path(capture_csv) if capture_csv else (base_dir / f"{file_prefix}_fast_capture.csv")
    resolved_output = Path(output_csv) if output_csv else (base_dir / f"{file_prefix}_resolution_audit.csv")
    return resolved_paper, resolved_capture, resolved_output


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
    text = str(name).strip().upper()
    if text == "UP":
        return "UP"
    if text == "DOWN":
        return "DOWN"
    return str(name).strip()


def infer_official_outcome(outcomes_raw, outcome_prices_raw) -> Optional[str]:
    outcomes = [str(item) for item in parse_list_field(outcomes_raw)]
    prices_raw = parse_list_field(outcome_prices_raw)
    prices: List[float] = []
    for item in prices_raw:
        try:
            prices.append(float(item))
        except Exception:
            prices.append(0.0)

    if not outcomes or not prices:
        return None

    winner_idx = None
    for idx, price in enumerate(prices):
        if price >= 0.999999:
            winner_idx = idx
            break

    if winner_idx is None:
        return None
    if winner_idx >= len(outcomes):
        return None

    return normalize_outcome_name(outcomes[winner_idx])


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def first_non_empty(rows: List[Dict[str, str]], key: str) -> str:
    for row in rows:
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def unique_non_empty_upper(rows: List[Dict[str, str]], key: str) -> List[str]:
    values = sorted(
        {
            str(row.get(key) or "").strip().upper()
            for row in rows
            if str(row.get(key) or "").strip()
        }
    )
    return values


def fetch_market_by_slug(session: requests.Session, slug: str, timeout: float) -> Optional[dict]:
    resp = session.get(GAMMA_EVENTS_URL, params={"slug": slug}, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list) or not data:
        return None
    markets = (data[0] or {}).get("markets") or []
    if not markets:
        return None
    return markets[0]


def build_audit_rows(
    paper_rows: List[Dict[str, str]],
    capture_rows: List[Dict[str, str]],
    timeout: float,
) -> List[Dict[str, str]]:
    grouped_paper: Dict[str, List[Dict[str, str]]] = {}
    grouped_capture: Dict[str, List[Dict[str, str]]] = {}

    for row in paper_rows:
        slug = str(row.get("slug") or "").strip()
        if slug:
            grouped_paper.setdefault(slug, []).append(row)

    for row in capture_rows:
        slug = str(row.get("slug") or "").strip()
        if slug:
            grouped_capture.setdefault(slug, []).append(row)

    all_slugs = sorted(set(grouped_paper.keys()) | set(grouped_capture.keys()))
    session = requests.Session()

    out_rows: List[Dict[str, str]] = []
    for slug in all_slugs:
        paper_group = grouped_paper.get(slug, [])
        capture_group = grouped_capture.get(slug, [])

        paper_resolved_values = unique_non_empty_upper(paper_group, "resolved_result")
        capture_inferred_values = unique_non_empty_upper(capture_group, "inferred_result")
        side_values = unique_non_empty_upper(paper_group, "side")

        market = None
        official = ""
        closed = ""
        uma_status = ""
        resolution_source = ""
        status = "ok"
        try:
            market = fetch_market_by_slug(session, slug, timeout=timeout)
        except Exception as e:
            status = f"error:{type(e).__name__}:{e}"

        if market:
            official = infer_official_outcome(market.get("outcomes"), market.get("outcomePrices")) or ""
            closed = str(bool(market.get("closed"))).lower()
            uma_status = str(market.get("umaResolutionStatus") or "")
            resolution_source = str(market.get("resolutionSource") or "")
        elif status == "ok":
            status = "not_found"

        paper_resolved = paper_resolved_values[0] if paper_resolved_values else ""
        capture_inferred = capture_inferred_values[0] if capture_inferred_values else ""
        side_value = side_values[0] if side_values else ""

        match_paper = ""
        if paper_resolved and official:
            match_paper = "1" if paper_resolved == official else "0"

        match_capture = ""
        if capture_inferred and official:
            match_capture = "1" if capture_inferred == official else "0"

        won_expected = ""
        if side_value and official:
            won_expected = "1" if side_value == official else "0"

        out_rows.append(
            {
                "slug": slug,
                "question": first_non_empty(paper_group + capture_group, "question"),
                "start_time_utc": first_non_empty(paper_group + capture_group, "start_time_utc"),
                "end_time_utc": first_non_empty(paper_group + capture_group, "end_time_utc"),
                "paper_side": side_value,
                "paper_resolved_result": paper_resolved,
                "capture_inferred_result": capture_inferred,
                "official_outcome": official,
                "match_paper_official": match_paper,
                "match_capture_official": match_capture,
                "expected_won_by_official": won_expected,
                "closed": closed,
                "uma_resolution_status": uma_status,
                "resolution_source": resolution_source,
                "status": status,
            }
        )

    return out_rows


def write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "slug",
        "question",
        "start_time_utc",
        "end_time_utc",
        "paper_side",
        "paper_resolved_result",
        "capture_inferred_result",
        "official_outcome",
        "match_paper_official",
        "match_capture_official",
        "expected_won_by_official",
        "closed",
        "uma_resolution_status",
        "resolution_source",
        "status",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def summarize(rows: List[Dict[str, str]]) -> None:
    total = len(rows)
    with_official = sum(1 for r in rows if r.get("official_outcome"))
    paper_rows = [r for r in rows if r.get("paper_resolved_result")]
    capture_rows = [r for r in rows if r.get("capture_inferred_result")]
    paper_match = sum(1 for r in paper_rows if r.get("match_paper_official") == "1")
    capture_match = sum(1 for r in capture_rows if r.get("match_capture_official") == "1")

    print("[AUDIT]")
    print(f"  total_slugs={total}")
    print(f"  with_official={with_official}")
    print(f"  paper_rows={len(paper_rows)} | paper_match={paper_match}")
    print(f"  capture_rows={len(capture_rows)} | capture_match={capture_match}")
    if paper_rows:
        print(f"  paper_match_rate={paper_match / len(paper_rows):.4f}")
    if capture_rows:
        print(f"  capture_match_rate={capture_match / len(capture_rows):.4f}")

    mismatches = [r for r in rows if r.get("match_paper_official") == "0" or r.get("match_capture_official") == "0"]
    if mismatches:
        print("  mismatches:")
        for row in mismatches:
            print(
                f"    {row['slug']} | paper={row['paper_resolved_result'] or '-'} | "
                f"cap={row['capture_inferred_result'] or '-'} | official={row['official_outcome'] or '-'} | "
                f"closed={row['closed']} | uma={row['uma_resolution_status']}"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audita resultados locais vs resultado oficial da Polymarket para mercados 5m (BTC/ETH/SOL)."
    )
    parser.add_argument(
        "--asset",
        default="btc",
        help="Ativo base (btc, eth, sol). Define defaults de pasta/arquivo.",
    )
    parser.add_argument(
        "--asset-dir",
        default=None,
        help="Pasta base dos arquivos do ativo (default: Target/<asset> 5min).",
    )
    parser.add_argument(
        "--paper-csv",
        default=None,
        help="CSV do paper trade (default depende de --asset e --asset-dir).",
    )
    parser.add_argument(
        "--capture-csv",
        default=None,
        help="CSV de capturas/inferencias (default depende de --asset e --asset-dir).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="CSV de auditoria de saida (default depende de --asset e --asset-dir).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=12.0,
        help="Timeout HTTP por request.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asset_cfg = resolve_asset_config(args.asset)
    paper_path, capture_path, output_path = resolve_paths(
        asset_cfg=asset_cfg,
        asset_dir=args.asset_dir,
        paper_csv=args.paper_csv,
        capture_csv=args.capture_csv,
        output_csv=args.output,
    )
    print(
        f"[CONFIG] asset={asset_cfg['asset']} | paper_csv={paper_path} | "
        f"capture_csv={capture_path} | output={output_path}"
    )

    paper_rows = read_csv_rows(paper_path)
    capture_rows = read_csv_rows(capture_path)
    rows = build_audit_rows(paper_rows, capture_rows, timeout=max(2.0, float(args.timeout)))
    write_csv(output_path, rows)
    print(f"[OK] CSV de auditoria salvo em: {output_path}")
    summarize(rows)


if __name__ == "__main__":
    main()
