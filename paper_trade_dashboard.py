from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
import streamlit as st


SCRIPT_DIR = Path(__file__).resolve().parent
ASSET_5M_CONFIG = {
    "btc": {"folder_name": "btc 5min", "file_prefix": "btc_updown"},
    "eth": {"folder_name": "eth 5min", "file_prefix": "eth_updown"},
    "sol": {"folder_name": "sol 5min", "file_prefix": "sol_updown"},
}


def _default_paths_for_asset(asset: str) -> tuple[Path, Path]:
    key = str(asset or "").strip().lower()
    cfg = ASSET_5M_CONFIG.get(key, ASSET_5M_CONFIG["btc"])
    base = SCRIPT_DIR / cfg["folder_name"]
    prefix = cfg["file_prefix"]
    return (base / f"{prefix}_paper_trade.csv", base / f"{prefix}_fast_capture.csv")


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception as exc:
        st.error(f"Falha ao ler {path}: {exc}")
        return pd.DataFrame()


def _to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _fmt_money(value: Optional[float]) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"${value:,.2f}"


def _sequence_label(row: pd.Series) -> str:
    pattern = [x.strip().upper() for x in str(row.get("sequence_pattern", "")).split(",") if x.strip()]
    if not pattern:
        return "N/A"
    try:
        idx = int(float(row.get("sequence_index_before", 0))) % len(pattern)
    except Exception:
        idx = 0
    side = pattern[idx]
    seen = 0
    for i in range(idx + 1):
        if pattern[i] == side:
            seen += 1
    return f"{side}_{seen}"


def _next_side(row: pd.Series) -> str:
    pattern = [x.strip().upper() for x in str(row.get("sequence_pattern", "")).split(",") if x.strip()]
    if not pattern:
        return "N/A"
    try:
        idx_after = int(float(row.get("sequence_index_after", 0)))
    except Exception:
        idx_after = 0
    return pattern[idx_after % len(pattern)]


def _prepare_paper_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    dt_cols = ("captured_at_utc", "start_time_utc", "end_time_utc", "entry_time_utc")
    for col in dt_cols:
        if col in out.columns:
            out[col] = _to_datetime(out[col])

    num_cols = (
        "shares",
        "entry_price",
        "entry_cost",
        "payout",
        "trade_pnl",
        "cash_after",
        "pnl_after",
        "sequence_index_before",
        "sequence_index_after",
    )
    for col in num_cols:
        if col in out.columns:
            out[col] = _to_numeric(out[col])

    won_true_values = {"1", "true", "yes", "y", "sim"}
    if "won" in out.columns:
        out["won_flag"] = out["won"].astype(str).str.strip().str.lower().isin(won_true_values)
    else:
        out["won_flag"] = False

    if "side" in out.columns:
        out["side"] = out["side"].astype(str).str.upper()
    if "resolved_result" in out.columns:
        out["resolved_result"] = out["resolved_result"].astype(str).str.upper()

    out["sequence_step_label"] = out.apply(_sequence_label, axis=1)
    out["next_side"] = out.apply(_next_side, axis=1)

    return out.sort_values("captured_at_utc")


def _prepare_capture_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    dt_cols = ("captured_at_utc", "start_time_utc", "end_time_utc", "target_ts_utc", "close_ts_utc")
    for col in dt_cols:
        if col in out.columns:
            out[col] = _to_datetime(out[col])

    num_cols = ("target_price", "close_price", "delta", "decision_delay_seconds")
    for col in num_cols:
        if col in out.columns:
            out[col] = _to_numeric(out[col])

    if "inferred_result" in out.columns:
        out["inferred_result"] = out["inferred_result"].astype(str).str.upper()

    return out.sort_values("captured_at_utc")


def _calc_max_drawdown(trades: pd.DataFrame) -> float:
    if trades.empty or "trade_pnl" not in trades.columns:
        return float("nan")
    curve = trades["trade_pnl"].fillna(0).cumsum()
    if curve.empty:
        return float("nan")
    drawdown = curve - curve.cummax()
    return float(drawdown.min())


def _format_gale_label(factor: Optional[float]) -> str:
    if factor is None or pd.isna(factor):
        return "N/A"
    rounded = round(float(factor), 4)
    if abs(rounded - round(rounded)) < 1e-9:
        return f"x{int(round(rounded))}"
    return f"x{rounded:.2f}"


def _prepare_gale_columns(paper: pd.DataFrame, base_shares_input: float) -> tuple[pd.DataFrame, Optional[float]]:
    if paper.empty or "shares" not in paper.columns:
        return paper, None

    out = paper.copy()
    valid_shares = out["shares"][(out["shares"] > 0) & out["shares"].notna()]
    if valid_shares.empty:
        return out, None

    auto_base = float(valid_shares.min())
    base_ref = auto_base if float(base_shares_input) <= 0 else float(base_shares_input)
    if base_ref <= 0:
        return out, None

    out["gale_factor"] = (out["shares"] / base_ref).replace([float("inf"), float("-inf")], pd.NA).round(4)
    out["gale_used"] = out["gale_factor"] > 1.0001
    out["gale_label"] = out["gale_factor"].apply(_format_gale_label)
    return out, base_ref


def render_header_status(paper_path: Path, capture_path: Path) -> None:
    st.caption("Dashboard web para acompanhar paper trade e inferencias do mercado BTC 5m.")
    c1, c2 = st.columns(2)

    def _file_status(path: Path) -> str:
        if not path.exists():
            return "arquivo nao encontrado"
        last_write = pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC")
        return f"ok | atualizado em {last_write.strftime('%Y-%m-%d %H:%M:%S')} UTC"

    c1.info(f"Paper CSV: `{paper_path}`\n\n{_file_status(paper_path)}")
    c2.info(f"Capture CSV: `{capture_path}`\n\n{_file_status(capture_path)}")


def render_kpis(paper: pd.DataFrame) -> None:
    st.subheader("Resumo de PnL")
    if paper.empty:
        st.info("Sem dados de paper trade ainda.")
        return

    total = len(paper)
    wins = int(paper["won_flag"].sum())
    losses = int(total - wins)
    win_rate = (wins / total) * 100 if total else 0.0

    total_entry = float(paper["entry_cost"].fillna(0).sum()) if "entry_cost" in paper.columns else 0.0
    total_payout = float(paper["payout"].fillna(0).sum()) if "payout" in paper.columns else 0.0
    total_pnl = float(paper["trade_pnl"].fillna(0).sum()) if "trade_pnl" in paper.columns else 0.0
    avg_pnl = float(paper["trade_pnl"].fillna(0).mean()) if "trade_pnl" in paper.columns and total else 0.0

    if "cash_after" in paper.columns and paper["cash_after"].notna().any():
        last_cash = float(paper["cash_after"].dropna().iloc[-1])
    else:
        last_cash = float("nan")

    max_dd = _calc_max_drawdown(paper)
    next_side = paper["next_side"].iloc[-1] if "next_side" in paper.columns else "N/A"

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Trades", total)
    c2.metric("Winrate", f"{win_rate:.2f}%")
    c3.metric("Wins / Losses", f"{wins} / {losses}")
    c4.metric("PnL total", _fmt_money(total_pnl))
    c5.metric("Cash atual", _fmt_money(last_cash))
    c6.metric("Proximo lado", next_side)

    c7, c8, c9, c10 = st.columns(4)
    c7.metric("Total entrada", _fmt_money(total_entry))
    c8.metric("Total payout", _fmt_money(total_payout))
    c9.metric("PnL medio/trade", _fmt_money(avg_pnl))
    c10.metric("Max drawdown", _fmt_money(max_dd))


def render_gale_section(paper: pd.DataFrame, gale_base_shares: Optional[float]) -> None:
    st.subheader("Analise de Gale")
    if paper.empty or "gale_factor" not in paper.columns or gale_base_shares is None:
        st.info("Sem dados suficientes para analise de gale.")
        return

    total = len(paper)
    gale_trades = int(paper["gale_used"].sum()) if "gale_used" in paper.columns else 0
    gale_pct = (100.0 * gale_trades / total) if total else 0.0

    trade_count_col = "slug" if "slug" in paper.columns else "captured_at_utc"

    level_stats = (
        paper.groupby(["gale_factor", "gale_label"], dropna=False)
        .agg(
            trades=(trade_count_col, "count"),
            wins=("won_flag", "sum"),
            winrate=("won_flag", "mean"),
            pnl_total=("trade_pnl", "sum"),
            avg_entry_cost=("entry_cost", "mean"),
            avg_shares=("shares", "mean"),
        )
        .reset_index()
    )
    level_stats["winrate"] = level_stats["winrate"] * 100.0
    level_stats["pct_trades"] = (level_stats["trades"] / max(total, 1)) * 100.0
    level_stats = level_stats.sort_values("gale_factor", ascending=True)

    most_common = level_stats.sort_values("trades", ascending=False).iloc[0]
    highest_factor = level_stats["gale_factor"].max() if "gale_factor" in level_stats.columns else float("nan")
    gale_only = paper[paper["gale_used"]]
    gale_only_winrate = (gale_only["won_flag"].mean() * 100.0) if not gale_only.empty else float("nan")
    gale_only_pnl = float(gale_only["trade_pnl"].sum()) if not gale_only.empty else 0.0
    gale_only_avg_entry = float(gale_only["entry_cost"].mean()) if not gale_only.empty else float("nan")

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Base shares", f"{gale_base_shares:.4f}")
    c2.metric("% trades com gale", f"{gale_pct:.2f}%")
    c3.metric(
        "Gale mais frequente",
        str(most_common["gale_label"]),
        f"{most_common['pct_trades']:.2f}% dos trades",
    )
    c4.metric("Maior gale usado", _format_gale_label(highest_factor))
    c5.metric("Winrate no gale", f"{gale_only_winrate:.2f}%" if pd.notna(gale_only_winrate) else "N/A")
    c6.metric("PnL no gale", _fmt_money(gale_only_pnl))

    c7, c8 = st.columns(2)
    with c7:
        fig_pct = px.bar(
            level_stats,
            x="gale_label",
            y="pct_trades",
            title="% de ocorrencia por nivel de gale",
            text=level_stats["pct_trades"].map(lambda v: f"{v:.1f}%"),
        )
        fig_pct.update_traces(textposition="outside")
        st.plotly_chart(fig_pct, use_container_width=True)

    with c8:
        fig_pnl = px.bar(
            level_stats,
            x="gale_label",
            y="pnl_total",
            title="PnL total por nivel de gale",
            text=level_stats["pnl_total"].map(lambda v: f"{v:+.2f}"),
        )
        fig_pnl.update_traces(textposition="outside")
        st.plotly_chart(fig_pnl, use_container_width=True)

    c9, c10 = st.columns(2)
    with c9:
        fig_win = px.bar(
            level_stats,
            x="gale_label",
            y="winrate",
            title="Winrate por nivel de gale",
            text=level_stats["winrate"].map(lambda v: f"{v:.1f}%"),
        )
        fig_win.update_traces(textposition="outside")
        st.plotly_chart(fig_win, use_container_width=True)

    with c10:
        fig_cost = px.bar(
            level_stats,
            x="gale_label",
            y="avg_entry_cost",
            title="Valor medio de entrada por nivel",
            text=level_stats["avg_entry_cost"].map(lambda v: f"{v:.2f}"),
        )
        fig_cost.update_traces(textposition="outside")
        st.plotly_chart(fig_cost, use_container_width=True)

    if not gale_only.empty:
        value_dist = (
            gale_only.assign(entry_cost_round=gale_only["entry_cost"].round(2))
            .groupby("entry_cost_round", dropna=False)
            .agg(trades=(trade_count_col, "count"))
            .reset_index()
            .sort_values("trades", ascending=False)
        )
        value_dist["pct"] = (value_dist["trades"] / max(len(gale_only), 1)) * 100.0
        st.markdown("**Valores de gale mais frequentes (entrada em $)**")
        st.dataframe(
            value_dist.head(15).rename(
                columns={
                    "entry_cost_round": "entry_cost_usd",
                    "trades": "qtd_trades",
                    "pct": "percentual_no_gale",
                }
            ),
            use_container_width=True,
        )
        st.caption(f"Entrada media quando ha gale: {_fmt_money(gale_only_avg_entry)}")


def render_paper_charts(paper: pd.DataFrame) -> None:
    st.subheader("Graficos do paper trade")
    if paper.empty:
        return

    plot_df = paper.sort_values("captured_at_utc").copy()
    plot_df["cum_pnl"] = plot_df["trade_pnl"].fillna(0).cumsum()

    c1, c2 = st.columns(2)
    with c1:
        if "cash_after" in plot_df.columns:
            fig_cash = px.line(
                plot_df,
                x="captured_at_utc",
                y="cash_after",
                title="Evolucao do caixa",
                markers=True,
            )
            st.plotly_chart(fig_cash, use_container_width=True)

    with c2:
        fig_pnl = px.line(
            plot_df,
            x="captured_at_utc",
            y="cum_pnl",
            title="PnL acumulado",
            markers=True,
        )
        st.plotly_chart(fig_pnl, use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        if "side" in plot_df.columns:
            by_side = (
                plot_df.groupby("side", dropna=False)["trade_pnl"]
                .sum()
                .reset_index()
                .sort_values("trade_pnl", ascending=False)
            )
            fig_side = px.bar(by_side, x="side", y="trade_pnl", title="PnL por lado")
            st.plotly_chart(fig_side, use_container_width=True)

    with c4:
        trade_count_col = "slug" if "slug" in plot_df.columns else "captured_at_utc"
        by_step = (
            plot_df.groupby("sequence_step_label", dropna=False)
            .agg(trades=(trade_count_col, "count"), winrate=("won_flag", "mean"))
            .reset_index()
        )
        by_step["winrate"] = by_step["winrate"] * 100
        fig_step = px.bar(by_step, x="sequence_step_label", y="winrate", title="Winrate por passo da sequencia")
        st.plotly_chart(fig_step, use_container_width=True)

    c5, c6 = st.columns(2)
    with c5:
        if "entry_price" in plot_df.columns:
            fig_hist = px.histogram(plot_df, x="entry_price", nbins=24, title="Distribuicao de preco de entrada")
            st.plotly_chart(fig_hist, use_container_width=True)

    with c6:
        if "entry_price" in plot_df.columns and "trade_pnl" in plot_df.columns:
            fig_scatter = px.scatter(
                plot_df,
                x="entry_price",
                y="trade_pnl",
                color="side",
                symbol="won_flag",
                title="Preco de entrada x PnL",
            )
            st.plotly_chart(fig_scatter, use_container_width=True)


def render_capture_section(capture: pd.DataFrame) -> None:
    st.subheader("Inferencia de mercado")
    if capture.empty:
        st.info("Sem dados de inferencia ainda.")
        return

    total = len(capture)
    up_count = int((capture["inferred_result"] == "UP").sum()) if "inferred_result" in capture.columns else 0
    down_count = int((capture["inferred_result"] == "DOWN").sum()) if "inferred_result" in capture.columns else 0

    if "delta" in capture.columns and capture["delta"].notna().any():
        avg_abs_delta = float(capture["delta"].abs().mean())
    else:
        avg_abs_delta = float("nan")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Ciclos inferidos", total)
    c2.metric("UP", up_count)
    c3.metric("DOWN", down_count)
    c4.metric("|Delta| medio", f"{avg_abs_delta:.2f}" if pd.notna(avg_abs_delta) else "N/A")

    c5, c6 = st.columns(2)
    with c5:
        fig_delta = px.line(
            capture,
            x="captured_at_utc",
            y="delta",
            color="inferred_result",
            title="Delta (close - target)",
            markers=True,
        )
        st.plotly_chart(fig_delta, use_container_width=True)

    with c6:
        if "target_price" in capture.columns and "close_price" in capture.columns:
            price_view = capture[["captured_at_utc", "target_price", "close_price"]].melt(
                id_vars=["captured_at_utc"],
                var_name="serie",
                value_name="price",
            )
            fig_prices = px.line(
                price_view,
                x="captured_at_utc",
                y="price",
                color="serie",
                title="Target vs close",
            )
            st.plotly_chart(fig_prices, use_container_width=True)


def render_table(paper: pd.DataFrame) -> None:
    st.subheader("Tabela detalhada")
    if paper.empty:
        return

    df = paper.copy()
    side_values = sorted(x for x in df["side"].dropna().unique().tolist() if str(x).strip())
    step_values = sorted(x for x in df["sequence_step_label"].dropna().unique().tolist() if str(x).strip())

    with st.expander("Filtros", expanded=True):
        selected_sides = st.multiselect("Lado", side_values, default=side_values)
        selected_steps = st.multiselect("Passo da sequencia", step_values, default=step_values)
        selected_result = st.selectbox("Resultado", ("Todos", "Vitoria", "Derrota"), index=0)

        date_min = df["captured_at_utc"].min()
        date_max = df["captured_at_utc"].max()
        default_period = (date_min.date(), date_max.date()) if pd.notna(date_min) and pd.notna(date_max) else None
        period = st.date_input("Periodo", value=default_period)

    if selected_sides:
        df = df[df["side"].isin(selected_sides)]
    if selected_steps:
        df = df[df["sequence_step_label"].isin(selected_steps)]

    if selected_result == "Vitoria":
        df = df[df["won_flag"]]
    elif selected_result == "Derrota":
        df = df[~df["won_flag"]]

    if isinstance(period, tuple) and len(period) == 2:
        start_dt, end_dt = period
        start_ts = pd.Timestamp(start_dt, tz="UTC")
        end_ts = pd.Timestamp(end_dt, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        df = df[(df["captured_at_utc"] >= start_ts) & (df["captured_at_utc"] <= end_ts)]

    view_cols = [
        "captured_at_utc",
        "slug",
        "side",
        "sequence_step_label",
        "next_side",
        "shares",
        "gale_label",
        "gale_factor",
        "gale_used",
        "entry_price",
        "entry_cost",
        "resolved_result",
        "won_flag",
        "trade_pnl",
        "cash_after",
        "pnl_after",
    ]
    view_cols = [col for col in view_cols if col in df.columns]
    display_df = df.sort_values("captured_at_utc", ascending=False)[view_cols]

    st.dataframe(display_df, use_container_width=True)
    st.download_button(
        "Baixar tabela filtrada (CSV)",
        data=display_df.to_csv(index=False).encode("utf-8"),
        file_name="paper_trade_dashboard_filtered.csv",
        mime="text/csv",
    )


def main() -> None:
    st.set_page_config(page_title="Dashboard Paper Trade 5m", layout="wide")

    with st.sidebar:
        st.header("Configuracao")
        asset = st.selectbox("Ativo", options=["btc", "eth", "sol"], index=0, format_func=lambda x: x.upper())
        default_paper_csv, default_capture_csv = _default_paths_for_asset(asset)
        paper_csv_path = Path(st.text_input("Paper CSV", value=str(default_paper_csv), key=f"paper_csv_{asset}"))
        capture_csv_path = Path(st.text_input("Capture CSV", value=str(default_capture_csv), key=f"capture_csv_{asset}"))
        gale_base_shares_input = st.number_input(
            "Base shares p/ gale (0=auto)",
            min_value=0.0,
            value=0.0,
            step=0.5,
            format="%.4f",
            help="0 usa o menor valor de shares encontrado no CSV como base.",
        )
        if st.button("Atualizar agora"):
            st.rerun()
        st.caption("Use os arquivos gerados pelo btc_updown_fast_resolver.py")

    st.title(f"Dashboard Web - {asset.upper()} 5m Paper Trade")

    render_header_status(paper_csv_path, capture_csv_path)

    paper = _prepare_paper_df(_read_csv(paper_csv_path))
    paper, gale_base_shares = _prepare_gale_columns(paper, gale_base_shares_input)
    capture = _prepare_capture_df(_read_csv(capture_csv_path))

    st.divider()
    render_kpis(paper)
    st.divider()
    render_gale_section(paper, gale_base_shares)
    st.divider()
    render_paper_charts(paper)
    st.divider()
    render_capture_section(capture)
    st.divider()
    render_table(paper)


if __name__ == "__main__":
    main()
