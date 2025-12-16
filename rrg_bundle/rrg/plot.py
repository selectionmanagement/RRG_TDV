from __future__ import annotations

from typing import Dict, Iterable, Literal, Optional, Set, Tuple

import pandas as pd
import plotly.graph_objects as go


_QUAD_COLORS = {
    "Leading": "#2ecc71",
    "Weakening": "#f39c12",
    "Lagging": "#e74c3c",
    "Improving": "#3498db",
}

LabelMode = Literal["none", "highlighted", "all"]
TailMode = Literal["none", "highlighted", "all"]
Theme = Literal["classic", "vivid", "pastel", "dark"]

_QUAD_COLORS_VIVID = {
    "Leading": "#00C853",
    "Weakening": "#FFC107",
    "Lagging": "#FF1744",
    "Improving": "#2979FF",
}

_THEME_BG: Dict[Theme, Dict[str, str]] = {
    "classic": {
        "Leading": "rgba(46,204,113,0.10)",
        "Weakening": "rgba(243,156,18,0.10)",
        "Lagging": "rgba(231,76,60,0.10)",
        "Improving": "rgba(52,152,219,0.10)",
    },
    "vivid": {
        "Leading": "rgba(0,200,83,0.32)",
        "Weakening": "rgba(255,193,7,0.34)",
        "Lagging": "rgba(255,23,68,0.30)",
        "Improving": "rgba(41,121,255,0.30)",
    },
    "pastel": {
        "Leading": "rgba(194,255,205,0.32)",
        "Weakening": "rgba(255,249,196,0.40)",
        "Lagging": "rgba(255,205,210,0.38)",
        "Improving": "rgba(187,222,251,0.36)",
    },
    "dark": {
        "Leading": "rgba(46,204,113,0.26)",
        "Weakening": "rgba(243,156,18,0.26)",
        "Lagging": "rgba(231,76,60,0.26)",
        "Improving": "rgba(52,152,219,0.26)",
    },
}

_THEME_STYLE: Dict[Theme, Dict[str, str]] = {
    "classic": {
        "template": "plotly_white",
        "paper_bg": "white",
        "plot_bg": "white",
        "font": "rgba(0,0,0,0.85)",
        "muted": "rgba(0,0,0,0.40)",
        "axis_line": "rgba(0,0,0,0.30)",
        "crosshair": "rgba(0,0,0,0.22)",
        "marker_text": "rgba(0,0,0,0.90)",
        "base_outline": "white",
        "hi_outline": "rgba(0,0,0,0.55)",
        "hover_bg": "white",
        "hover_font": "rgba(0,0,0,0.90)",
    },
    "pastel": {
        "template": "plotly_white",
        "paper_bg": "white",
        "plot_bg": "white",
        "font": "rgba(0,0,0,0.85)",
        "muted": "rgba(0,0,0,0.40)",
        "axis_line": "rgba(0,0,0,0.30)",
        "crosshair": "rgba(0,0,0,0.22)",
        "marker_text": "rgba(0,0,0,0.90)",
        "base_outline": "white",
        "hi_outline": "rgba(0,0,0,0.55)",
        "hover_bg": "white",
        "hover_font": "rgba(0,0,0,0.90)",
    },
    "vivid": {
        "template": "plotly_dark",
        "paper_bg": "#020617",
        "plot_bg": "#020617",
        "font": "rgba(255,255,255,0.90)",
        "muted": "rgba(255,255,255,0.55)",
        "axis_line": "rgba(255,255,255,0.70)",
        "crosshair": "rgba(255,255,255,0.30)",
        "marker_text": "rgba(255,255,255,0.95)",
        "base_outline": "rgba(255,255,255,0.15)",
        "hi_outline": "rgba(255,255,255,0.85)",
        "hover_bg": "#111b2b",
        "hover_font": "rgba(255,255,255,0.95)",
    },
    "dark": {
        "template": "plotly_dark",
        "paper_bg": "#0E1117",
        "plot_bg": "#0E1117",
        "font": "rgba(255,255,255,0.85)",
        "muted": "rgba(255,255,255,0.48)",
        "axis_line": "rgba(255,255,255,0.35)",
        "crosshair": "rgba(255,255,255,0.28)",
        "marker_text": "rgba(255,255,255,0.92)",
        "base_outline": "rgba(255,255,255,0.80)",
        "hi_outline": "rgba(255,255,255,0.95)",
        "hover_bg": "#0E1117",
        "hover_font": "rgba(255,255,255,0.92)",
    },
}

_THEME_POINT_COLORS: Dict[Theme, Dict[str, str]] = {
    "classic": _QUAD_COLORS,
    "pastel": _QUAD_COLORS,
    "vivid": _QUAD_COLORS_VIVID,
    "dark": _QUAD_COLORS,
}


def _label_for_symbol(points: pd.DataFrame, symbol: str) -> str:
    if "label" in points.columns and symbol in points.index:
        v = points.loc[symbol, "label"]
        return str(v)
    return symbol


def _calc_square_range(xy_minmax: Tuple[float, float, float, float]) -> Tuple[float, float, float, float, float]:
    x_min, x_max, y_min, y_max = xy_minmax
    span_x = max(abs(x_min - 100.0), abs(x_max - 100.0))
    span_y = max(abs(y_min - 100.0), abs(y_max - 100.0))
    span = max(span_x, span_y, 2.0)
    pad = max(0.8, span * 0.15)
    span = span + pad
    return 100.0 - span, 100.0 + span, 100.0 - span, 100.0 + span, span


def _collect_xy(
    *,
    points: pd.DataFrame,
    tails: Dict[str, pd.DataFrame],
    tail_symbols: Set[str],
) -> Tuple[float, float, float, float]:
    xs = points["rs_ratio"].astype(float).tolist()
    ys = points["rs_mom"].astype(float).tolist()

    for sym in tail_symbols:
        tail = tails.get(sym)
        if tail is None or tail.empty:
            continue
        xs.extend(tail["rs_ratio"].astype(float).tolist())
        ys.extend(tail["rs_mom"].astype(float).tolist())

    return min(xs), max(xs), min(ys), max(ys)


def _add_quadrant_backgrounds(
    fig: go.Figure,
    *,
    x_min: float,
    x_max: float,
    y_min: float,
    y_max: float,
    bg: Dict[str, str],
    crosshair_color: str,
) -> None:
    fig.add_shape(type="rect", x0=100, x1=x_max, y0=100, y1=y_max, fillcolor=bg["Leading"], line_width=0, layer="below")
    fig.add_shape(type="rect", x0=100, x1=x_max, y0=y_min, y1=100, fillcolor=bg["Weakening"], line_width=0, layer="below")
    fig.add_shape(type="rect", x0=x_min, x1=100, y0=y_min, y1=100, fillcolor=bg["Lagging"], line_width=0, layer="below")
    fig.add_shape(type="rect", x0=x_min, x1=100, y0=100, y1=y_max, fillcolor=bg["Improving"], line_width=0, layer="below")

    fig.add_shape(type="line", x0=100, x1=100, y0=y_min, y1=y_max, line=dict(color=crosshair_color, width=1), layer="below")
    fig.add_shape(type="line", x0=x_min, x1=x_max, y0=100, y1=100, line=dict(color=crosshair_color, width=1), layer="below")


def _add_quadrant_labels(
    fig: go.Figure,
    *,
    x_min: float,
    x_max: float,
    y_min: float,
    y_max: float,
    span: float,
    font_color: str,
) -> None:
    off = span * 0.06
    font = dict(size=12, color=font_color)
    fig.add_annotation(x=x_min + off, y=y_max - off, text="Improving", showarrow=False, xanchor="left", yanchor="top", font=font)
    fig.add_annotation(x=x_max - off, y=y_max - off, text="Leading", showarrow=False, xanchor="right", yanchor="top", font=font)
    fig.add_annotation(x=x_min + off, y=y_min + off, text="Lagging", showarrow=False, xanchor="left", yanchor="bottom", font=font)
    fig.add_annotation(x=x_max - off, y=y_min + off, text="Weakening", showarrow=False, xanchor="right", yanchor="bottom", font=font)


def _hovertemplate() -> str:
    return (
        "%{text}"
        "<br>RS-Ratio=%{x:.2f}"
        "<br>RS-Mom=%{y:.2f}"
        "<br>Quadrant=%{customdata[0]}"
        "<br>Distance=%{customdata[1]:.2f}"
        "<br>Speed=%{customdata[2]:.2f}"
        "<extra></extra>"
    )


def _scatter_points(
    *,
    df: pd.DataFrame,
    quad: str,
    mode: str,
    marker: dict,
    showlegend: bool,
    name: str,
    text_color: str,
    text_size: int,
) -> go.Scatter:
    labels = df["label"].astype(str) if "label" in df.columns else df.index.astype(str)
    distance = df["distance"] if "distance" in df.columns else pd.Series([float("nan")] * len(df), index=df.index)
    speed = df["speed"] if "speed" in df.columns else pd.Series([float("nan")] * len(df), index=df.index)

    custom = pd.concat([df["quadrant"].astype(str), distance.astype(float), speed.astype(float)], axis=1).to_numpy()

    return go.Scatter(
        x=df["rs_ratio"].astype(float),
        y=df["rs_mom"].astype(float),
        mode=mode,
        text=labels.tolist(),
        textposition="top center",
        textfont=dict(size=int(text_size), color=text_color),
        marker=marker,
        name=name,
        showlegend=showlegend,
        customdata=custom,
        hovertemplate=_hovertemplate(),
        cliponaxis=False,
    )


def build_rrg_figure(
    *,
    points: pd.DataFrame,
    tails: Dict[str, pd.DataFrame],
    highlighted_symbols: Optional[Iterable[str]] = None,
    label_mode: LabelMode = "highlighted",
    tail_mode: TailMode = "highlighted",
    theme: Theme = "classic",
    fixed_span: Optional[float] = None,
    title: str,
) -> go.Figure:
    fig = go.Figure()

    n_points = int(len(points))
    if n_points <= 15:
        base_marker_size = 12
        hi_marker_size = 16
        label_size = 14
        tick_size = 12
    elif n_points <= 30:
        base_marker_size = 11
        hi_marker_size = 15
        label_size = 13
        tick_size = 12
    elif n_points <= 60:
        base_marker_size = 10
        hi_marker_size = 14
        label_size = 12
        tick_size = 11
    else:
        base_marker_size = 9
        hi_marker_size = 12
        label_size = 10
        tick_size = 10

    highlight_set = {s for s in (highlighted_symbols or []) if s in points.index}
    if tail_mode == "all":
        tail_symbols = set(tails.keys())
    elif tail_mode == "highlighted":
        tail_symbols = {s for s in highlight_set if s in tails}
    else:
        tail_symbols = set()

    x_min_raw, x_max_raw, y_min_raw, y_max_raw = _collect_xy(points=points, tails=tails, tail_symbols=tail_symbols)
    x_min, x_max, y_min, y_max, span = _calc_square_range((x_min_raw, x_max_raw, y_min_raw, y_max_raw))

    if fixed_span is not None:
        effective_span = max(span, fixed_span)
        span = effective_span
        x_min = 100.0 - effective_span
        x_max = 100.0 + effective_span
        y_min = 100.0 - effective_span
        y_max = 100.0 + effective_span

    bg = _THEME_BG.get(theme, _THEME_BG["classic"])
    style = _THEME_STYLE.get(theme, _THEME_STYLE["classic"])
    quad_colors = _THEME_POINT_COLORS.get(theme, _QUAD_COLORS)
    _add_quadrant_backgrounds(
        fig,
        x_min=x_min,
        x_max=x_max,
        y_min=y_min,
        y_max=y_max,
        bg=bg,
        crosshair_color=style["crosshair"],
    )
    _add_quadrant_labels(
        fig,
        x_min=x_min,
        x_max=x_max,
        y_min=y_min,
        y_max=y_max,
        span=span,
        font_color=style["muted"],
    )

    # Tails (lines only, to keep it clean)
    if tail_mode != "none":
        for sym in sorted(tail_symbols):
            tail = tails.get(sym)
            if tail is None or tail.empty:
                continue
            quad = points.loc[sym, "quadrant"] if sym in points.index else "Lagging"
            color = quad_colors.get(str(quad), "#7f8c8d")
            strong = sym in highlight_set
            fig.add_trace(
                go.Scatter(
                    x=tail["rs_ratio"].astype(float),
                    y=tail["rs_mom"].astype(float),
                    mode="lines",
                    line=dict(width=3 if strong else 1, color=color, shape="spline", smoothing=1.1),
                    opacity=0.95 if strong else 0.30,
                    name=f"{_label_for_symbol(points, sym)} tail",
                    showlegend=False,
                    hoverinfo="skip",
                )
            )

    is_hi = points.index.to_series().isin(highlight_set)
    base = points.loc[~is_hi]
    hi = points.loc[is_hi]

    quad_order = ["Improving", "Leading", "Weakening", "Lagging"]
    base_mode = "markers+text" if label_mode == "all" else "markers"

    for quad in quad_order:
        dfq = base[base["quadrant"] == quad]
        if not dfq.empty:
            fig.add_trace(
                _scatter_points(
                    df=dfq,
                    quad=quad,
                    mode=base_mode,
                    marker=dict(
                        size=base_marker_size,
                        color=quad_colors.get(quad, "#7f8c8d"),
                        opacity=0.88,
                        line=dict(width=1, color=style["base_outline"]),
                    ),
                    showlegend=False,
                    name=quad,
                    text_color=style["marker_text"],
                    text_size=label_size,
                )
            )

    for quad in quad_order:
        dfq = hi[hi["quadrant"] == quad]
        if dfq.empty:
            continue
        mode = "markers+text" if label_mode == "highlighted" else "markers"
        if label_mode == "all":
            mode = "markers"
        fig.add_trace(
            _scatter_points(
                df=dfq,
                quad=quad,
                mode=mode,
                marker=dict(
                    size=hi_marker_size,
                    color=quad_colors.get(quad, "#7f8c8d"),
                    opacity=1.0,
                    line=dict(width=2, color=style["hi_outline"]),
                ),
                showlegend=False,
                name=f"{quad} (highlight)",
                text_color=style["marker_text"],
                text_size=label_size,
            )
        )

    if span <= 3:
        dtick = 0.5
        tickformat = ".1f"
    elif span <= 8:
        dtick = 1
        tickformat = ".0f"
    elif span <= 15:
        dtick = 2
        tickformat = ".0f"
    else:
        dtick = None
        tickformat = ".0f"

    xaxis = dict(
        range=[x_min, x_max],
        zeroline=False,
        showgrid=False,
        ticks="outside",
        showline=True,
        linecolor=style["axis_line"],
        mirror=True,
        tickformat=tickformat,
        tickfont=dict(color=style["font"], size=tick_size),
    )
    yaxis = dict(
        range=[y_min, y_max],
        zeroline=False,
        showgrid=False,
        ticks="outside",
        showline=True,
        linecolor=style["axis_line"],
        mirror=True,
        scaleanchor="x",
        scaleratio=1,
        tickformat=tickformat,
        tickfont=dict(color=style["font"], size=tick_size),
    )
    if dtick is not None:
        xaxis["dtick"] = dtick
        yaxis["dtick"] = dtick

    fig.update_layout(
        title=title,
        title_x=0.5,
        xaxis_title="RS Ratio",
        yaxis_title="RS Momentum",
        xaxis=xaxis,
        yaxis=yaxis,
        showlegend=False,
        hovermode="closest",
        margin=dict(l=10, r=10, t=60, b=20),
        height=850,
        paper_bgcolor=style["paper_bg"],
        plot_bgcolor=style["plot_bg"],
        font=dict(color=style["font"], size=max(12, tick_size + 1)),
        hoverlabel=dict(bgcolor=style["hover_bg"], font=dict(color=style["hover_font"])),
        template=style["template"],
    )
    return fig
