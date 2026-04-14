"""
ZigzagPro — Polymarket Order Executor
Integração com a CLOB API da Polymarket para apostas BTC 5m

Dependências:
    pip install py-clob-client python-dotenv

Slug format: btc-updown-5m-{unix_timestamp}
Onde unix_timestamp = início da janela de 5 minutos (UTC, múltiplo de 300)
"""
import os, time, math, logging, requests
from datetime import datetime, timezone
from zlog import blog
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger("executor")

CLOB_HOST      = "https://clob.polymarket.com"
_raw_key       = os.getenv("POLY_PRIVATE_KEY", "")
PRIVATE_KEY    = ("0x" + _raw_key) if _raw_key and not _raw_key.startswith("0x") else _raw_key
PROXY_WALLET   = os.getenv("POLY_PROXY_WALLET", "0xdd24d543f8006a2ecc1e3c0d035307831420b54c")
DAILY_SL       = float(os.getenv("DAILY_STOP_LOSS", "200.0"))

# Cache do client autenticado (recriado se expirar)
_client_cache = {"client": None, "ts": 0}
_CLIENT_TTL   = 3600  # renovar creds a cada 1h


def _get_client():
    """Retorna client CLOB autenticado (com cache de 1h)."""
    now = time.time()
    if _client_cache["client"] and now - _client_cache["ts"] < _CLIENT_TTL:
        return _client_cache["client"]

    if not PRIVATE_KEY or PRIVATE_KEY.startswith("0xSUA_"):
        raise RuntimeError(
            "POLY_PRIVATE_KEY não configurada. "
            "Copie .env.example → .env e insira sua chave privada."
        )

    from py_clob_client.client import ClobClient
    from py_clob_client.constants import POLYGON

    log.info("Criando client CLOB autenticado...")
    client = ClobClient(
        CLOB_HOST,
        key=PRIVATE_KEY,
        chain_id=POLYGON,
        signature_type=2,   # EIP-712 para proxy wallets
        funder=PROXY_WALLET,
    )
    client.set_api_creds(client.create_or_derive_api_creds())
    _client_cache["client"] = client
    _client_cache["ts"]     = now
    log.info("Client CLOB OK (chain=137 Polygon)")
    return client


# ── MARKET DISCOVERY ──────────────────────────────────────────────────────────

GAMMA_API = "https://gamma-api.polymarket.com"


def current_period_ts() -> int:
    """Timestamp UTC do início da janela de 5 min atual (múltiplo de 300)."""
    return math.floor(time.time() / 300) * 300


def next_period_ts() -> int:
    """Timestamp UTC do início da próxima janela de 5 min."""
    return current_period_ts() + 300


def _fetch_event(ts: int) -> dict | None:
    """
    Busca evento BTC 5m na Gamma API pelo slug.
    Retorna o primeiro evento encontrado ou None.
    """
    slug = f"btc-updown-5m-{ts}"
    url  = f"{GAMMA_API}/events?slug={slug}"
    try:
        r = requests.get(url, timeout=8)
        if r.status_code == 200:
            data = r.json()
            return data[0] if data else None
        return None
    except Exception as e:
        log.warning(f"Erro buscando evento {slug}: {e}")
        return None


def find_bettable_market() -> dict | None:
    """
    Encontra o mercado BTC 5m que aceita ordens.
    Tenta offsets 0, +300 e -300 do período atual.

    Retorna dict com campos extras injetados:
        period_ts, up_token, down_token
      onde cada token tem: token_id, outcome, price
    """
    base_ts = current_period_ts()

    for ts in [base_ts, base_ts + 300, base_ts - 300]:
        event = _fetch_event(ts)
        if not event:
            continue

        markets = event.get("markets", [])
        if not markets:
            continue

        # O evento tem 1 market com 2 outcomes (Up e Down)
        # Nota: a Gamma API devolve outcomes/prices/tokenIds como strings JSON
        import json as _json

        def _parse(val):
            if isinstance(val, str):
                return _json.loads(val)
            return val or []

        mkt = markets[0]
        outcomes     = _parse(mkt.get("outcomes", "[]"))       # ["Up", "Down"]
        prices       = _parse(mkt.get("outcomePrices", "[]"))  # ["0.335", "0.665"]
        token_ids    = _parse(mkt.get("clobTokenIds", "[]"))   # ["123...", "456..."]
        condition_id = mkt.get("conditionId", "")
        end_date     = event.get("endDate", "")

        if len(outcomes) < 2 or len(prices) < 2 or len(token_ids) < 2:
            log.warning(f"Estrutura inesperada no evento {ts}: outcomes={outcomes}")
            continue

        up_idx   = next((i for i, o in enumerate(outcomes) if o.lower() == "up"),   0)
        down_idx = next((i for i, o in enumerate(outcomes) if o.lower() == "down"), 1)

        up_token   = {"token_id": token_ids[up_idx],   "outcome": "Up",   "price": float(prices[up_idx])}
        down_token = {"token_id": token_ids[down_idx], "outcome": "Down", "price": float(prices[down_idx])}

        result = {
            "condition_id": condition_id,
            "market_slug":  f"btc-updown-5m-{ts}",
            "question":     event.get("title", ""),
            "end_date":     end_date,
            "period_ts":    ts,
            "up_token":     up_token,
            "down_token":   down_token,
            # campos extras para o executor de ordens
            "accepting_orders": True,
            "active":           True,
        }

        log.info(
            f"Market encontrado: {result['question']} | "
            f"Up@{up_token['price']:.3f} Down@{down_token['price']:.3f} | "
            f"Fecha: {end_date}"
        )
        return result

    return None


# ── CANDLE RESULT ─────────────────────────────────────────────────────────────

def get_result_from_polymarket(condition_id: str, timeout: int = 45) -> tuple[bool | None, float]:
    """
    Fonte de verdade: activity da Polymarket para o condition_id.

    Retorna (won, payout):
        (True,  payout) → ganhou — usdcSize > 0 no REDEEM
        (False, 0.0)    → perdeu — usdcSize == 0 no REDEEM
        (None,  0.0)    → timeout sem REDEEM encontrado

    NOTA: Binance NUNCA é usada para resultado — apenas a Polymarket é a
    fonte de verdade (o preço-de-referência pode diferir da vela Binance).
    """
    deadline = time.time() + timeout
    url = f"https://data-api.polymarket.com/activity?user={PROXY_WALLET}&limit=20"
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                for t in r.json():
                    if t.get("conditionId") == condition_id and t.get("type") == "REDEEM":
                        payout = float(t.get("usdcSize", 0))
                        won    = payout > 0
                        log.info(
                            f"Resultado via Polymarket REST: conditionId={condition_id[:16]}… "
                            f"{'GANHOU' if won else 'PERDEU'} (usdcSize={payout:.4f})"
                        )
                        return won, payout
        except Exception as e:
            log.warning(f"Erro consultando activity Polymarket: {e}")
        time.sleep(3)
    log.warning(f"Timeout {timeout}s aguardando REDEEM Polymarket para {condition_id[:16]}…")
    return None, 0.0


def get_result_from_binance_exact(block_end_ts: int) -> tuple[str | None, float, float]:
    """
    Busca o resultado exato usando o close da vela 5m que fechou em block_end_ts.

    A Polymarket usa o preço de fechamento da vela 5m como price-to-beat.
    Binance BTCUSDT 5m close é ~99% correlacionado com esse índice.

    Parâmetros:
        block_end_ts: timestamp Unix UTC do fechamento do bloco (múltiplo de 300)
                      ex: se o bloco é 19:50-19:55, block_end_ts = timestamp de 19:55

    Retorna:
        (direction, open_price, close_price)
        direction = "Up" se close >= open, "Down" caso contrário
        (None, 0.0, 0.0) se não encontrar a vela correta
    """
    # A vela que fecha em block_end_ts abre em block_end_ts - 300
    candle_open_ms = (block_end_ts - 300) * 1000

    try:
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={
                "symbol":    "BTCUSDT",
                "interval":  "5m",
                "startTime": candle_open_ms,
                "limit":     1,
            },
            timeout=5,
        )
        if r.status_code != 200:
            return None, 0.0, 0.0
        data = r.json()
        if not data:
            return None, 0.0, 0.0

        candle    = data[0]
        open_ms   = int(candle[0])   # open timestamp (ms)
        open_p    = float(candle[1])
        close_p   = float(candle[4])
        close_ms  = int(candle[6])   # close timestamp (ms)

        # Verify this is actually the candle we want
        if abs(open_ms - candle_open_ms) > 30_000:   # tolerance: 30s
            log.warning(
                f"Binance retornou vela errada: "
                f"esperado open={candle_open_ms}ms, recebido={open_ms}ms"
            )
            return None, 0.0, 0.0

        direction = "Up" if close_p >= open_p else "Down"
        log.info(
            f"[RESULT] Binance close: open=${open_p:,.2f} close=${close_p:,.2f} → {direction} "
            f"(close_ts={close_ms // 1000})"
        )
        return direction, open_p, close_p

    except Exception as e:
        log.warning(f"Erro buscando candle exato Binance: {e}")
        return None, 0.0, 0.0


# ── ORDER PLACEMENT ───────────────────────────────────────────────────────────

def place_bet(direction: str, amount_usdc: float) -> dict | None:
    """
    Coloca uma aposta de mercado (Market Order FOK) na direção indicada.

    direction:   'Up' ou 'Down'
    amount_usdc: valor em USDC a gastar (ex: 1.0, 2.0, 4.0…)

    Market Order: aceita o melhor preço disponível, executa imediatamente,
    mínimo $1 USDC. Não requer cálculo de shares nem tick size.
    """
    market = find_bettable_market()
    if not market:
        log.error("Nenhum mercado BTC 5m disponível para apostar")
        blog("skip", "Nenhum mercado BTC 5m disponível", ok=None)
        return None

    token    = market["up_token"] if direction == "Up" else market["down_token"]
    token_id = token["token_id"]

    # Use CLOB midpoint (authoritative) for backstop price check
    clob_mid = get_clob_midpoint(token_id)
    price    = clob_mid if clob_mid is not None else float(token["price"])
    psrc     = "CLOB" if clob_mid is not None else "Gamma"

    # Backstop safety filter (AutoState already checked, this is a last-line guard)
    if price < 0.30 or price > 0.53:
        log.warning(f"Backstop: preço {price:.3f} [{psrc}] fora de faixa (0.30–0.53), abortando")
        blog("skip", f"Backstop: preço {price:.3f} [{psrc}] fora de faixa — ordem cancelada", ok=None)
        return None

    log.info(f"Colocando Market Order: {direction} | ${amount_usdc:.2f} USDC | preço {price:.3f} [{psrc}]")

    try:
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY
        client = _get_client()

        # Market Order: price=0 significa aceitar o melhor preço disponível
        order_args = MarketOrderArgs(
            token_id=token_id,
            amount=amount_usdc,
            side=BUY,
            price=0,             # 0 = market price (sem limite)
            order_type=OrderType.FOK,
        )

        signed_order = client.create_market_order(order_args)
        if not signed_order:
            log.error("Resposta vazia do create_market_order")
            return None

        # FOK é o correto para Market Order na Polymarket: executa agora ou cancela
        post_resp = client.post_order(signed_order, OrderType.FOK)
        log.info(f"Ordem postada: {post_resp}")

        order_id = (post_resp or {}).get("orderID", "")
        if not order_id:
            order_id = (post_resp or {}).get("order", {}).get("orderID", "")

        blog("success", f"✓ {direction} ${amount_usdc:.2f} executado (id={order_id[:8] if order_id else 'n/a'})", ok=True)
        return {
            "order_id":    order_id,
            "market_slug": market.get("market_slug", ""),
            "condition_id":market.get("condition_id", ""),
            "period_ts":   market["period_ts"],
            "direction":   direction,
            "amount":      amount_usdc,
            "price":       price,
            "question":    market.get("question", ""),
            "placed_at":   datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        log.exception(f"Erro ao colocar ordem: {e}")
        blog("error", f"✗ Erro na ordem: {str(e)[:80]}", ok=False)
        return None


def cancel_open_orders(condition_id: str | None = None) -> bool:
    """Cancela ordens abertas (do mercado específico ou todas)."""
    try:
        client = _get_client()
        if condition_id:
            client.cancel_market_orders(
                asset_id=condition_id,
                side=None
            )
        else:
            client.cancel_all()
        log.info(f"Ordens canceladas (cid={condition_id})")
        return True
    except Exception as e:
        log.warning(f"Erro ao cancelar ordens: {e}")
        return False


# ── DIAGNÓSTICO / CREDENCIAIS ────────────────────────────────────────────────

def get_clob_midpoint(token_id: str) -> float | None:
    """
    Fetches the real CLOB midpoint price for a token via REST.
    This is the authoritative price — matches what Market Orders actually execute at.
    Much more accurate than Gamma API outcomePrices (which can be stale).
    """
    try:
        url = f"{CLOB_HOST}/midpoint?token_id={token_id}"
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            mid = r.json().get("mid")
            if mid is not None:
                return float(mid)
    except Exception as e:
        log.warning(f"Erro buscando midpoint CLOB: {e}")
    return None


def get_api_creds() -> dict | None:
    """
    Retorna as credenciais CLOB para autenticação no WebSocket user channel.
    Retorna {"apiKey": ..., "secret": ..., "passphrase": ...} ou None.
    """
    if not PRIVATE_KEY or PRIVATE_KEY.startswith("0xSUA_"):
        return None
    try:
        client = _get_client()
        creds  = client.create_or_derive_api_creds()
        return {
            "apiKey":     creds.api_key,
            "secret":     creds.api_secret,
            "passphrase": creds.api_passphrase,
        }
    except Exception as e:
        log.warning(f"Erro ao obter API creds: {e}")
        return None


def get_balance() -> float | None:
    """
    Retorna o saldo USDC real da proxy wallet via CLOB autenticado.
    USDC na Polygon tem 6 casas decimais → divide por 1_000_000.
    Retorna None se não configurado ou em caso de erro.
    """
    if not PRIVATE_KEY or PRIVATE_KEY.startswith("0xSUA_"):
        return None
    try:
        from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
        client  = _get_client()
        bal     = client.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        return round(int(bal["balance"]) / 1_000_000, 2)
    except Exception as e:
        log.warning(f"Erro ao buscar saldo CLOB: {e}")
        return None


def diagnose() -> dict:
    """
    Verifica se a configuração está correta.
    Útil para testar antes de ativar o modo auto.
    """
    result = {
        "env_ok":         bool(PRIVATE_KEY and not PRIVATE_KEY.startswith("0xSUA_")),
        "proxy_wallet":   PROXY_WALLET,
        "client_ok":      False,
        "market_found":   False,
        "market_question": None,
        "up_price":       None,
        "down_price":     None,
        "error":          None,
    }

    if not result["env_ok"]:
        result["error"] = "POLY_PRIVATE_KEY não configurada no .env"
        return result

    try:
        client = _get_client()
        result["client_ok"] = True
    except Exception as e:
        result["error"] = f"Falha no cliente CLOB: {e}"
        return result

    market = find_bettable_market()
    if market:
        result["market_found"]    = True
        result["market_question"] = market.get("question")
        result["up_price"]        = market["up_token"]["price"]
        result["down_price"]      = market["down_token"]["price"]
    else:
        result["error"] = "Nenhum mercado BTC 5m encontrado/disponível"

    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")
    print("=== ZigzagPro Executor — Diagnóstico ===")
    d = diagnose()
    for k, v in d.items():
        print(f"  {k}: {v}")
