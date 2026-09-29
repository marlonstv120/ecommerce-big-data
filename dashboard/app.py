"""Interactive local dashboard over Gold Parquet tables and ML metrics."""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path

import plotly.graph_objects as go
import streamlit as st


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.eda import build_spark_session  # noqa: E402


GOLD_ROOT = ROOT / "data" / "processed" / "gold"
MODEL_METRICS_PATH = ROOT / "data" / "output" / "model_evaluation.json"

st.set_page_config(
    page_title="eCommerce | Funnel y compras",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded",
)


@st.cache_resource(show_spinner="Preparando Spark local…")
def get_spark():
    return build_spark_session("ecommerce-phase3-dashboard", shuffle_partitions=16)


def _python_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


@st.cache_data(ttl=900, show_spinner="Leyendo tablas Gold…")
def load_gold_tables(root_path: str):
    spark = get_spark()
    base = Path(root_path)
    table_names = (
        "funnel_by_month",
        "funnel_by_utc_weekday_hour",
        "purchases_by_category",
        "purchases_by_category_day",
        "purchases_by_utc_time",
    )
    tables = {}
    for table_name in table_names:
        path = base / table_name
        if not path.is_dir():
            raise FileNotFoundError(f"No se encontró la tabla Gold: {path}")
        tables[table_name] = [
            {key: _python_value(value) for key, value in row.asDict().items()}
            for row in spark.read.parquet(str(path)).collect()
        ]
    return tables


@st.cache_data(ttl=900)
def load_model_metrics(path_string: str):
    path = Path(path_string)
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None


def money(value: float | int | None) -> str:
    return "—" if value is None else f"{value:,.2f}"


st.title("Comportamiento de compra | REES46")
st.caption(
    "Octubre–noviembre de 2019 · eventos históricos · horas en UTC · "
    "la suma de price es un valor estimado por evento, no ingreso neto ni margen."
)

try:
    tables = load_gold_tables(str(GOLD_ROOT))
except Exception as error:
    st.error(f"No se pudieron cargar las tablas Gold: {error}")
    st.info("Ejecuta primero el pipeline de Fase 2 desde la raíz del proyecto.")
    st.stop()

model_metrics = load_model_metrics(str(MODEL_METRICS_PATH))
funnel_month = tables["funnel_by_month"]
funnel_hour = tables["funnel_by_utc_weekday_hour"]
purchases_category = tables["purchases_by_category"]
purchases_category_day = tables["purchases_by_category_day"]
purchases_time = tables["purchases_by_utc_time"]

month_values = sorted(row["event_month"] for row in funnel_month)
category_values = sorted(row["category_code"] for row in purchases_category)
purchase_dates = sorted(
    date.fromisoformat(row["purchase_date_utc"]) for row in purchases_time
)

with st.sidebar:
    st.header("Filtros")
    selected_months = st.multiselect("Mes de la sesión", month_values, default=month_values)
    selected_category = st.selectbox("Categoría de compra", ["Todas", *category_values])
    if purchase_dates:
        selected_date_range = st.date_input(
            "Fechas de compra (UTC)",
            value=(purchase_dates[0], purchase_dates[-1]),
            min_value=purchase_dates[0],
            max_value=purchase_dates[-1],
        )
    else:
        selected_date_range = ()

if len(selected_date_range) == 2:
    start_date, end_date = selected_date_range
else:
    start_date, end_date = (purchase_dates[0], purchase_dates[-1]) if purchase_dates else (None, None)

filtered_purchase_time = [
    row
    for row in purchases_time
    if (not selected_months or row["event_month"] in selected_months)
    and (start_date is None or date.fromisoformat(row["purchase_date_utc"]) >= start_date)
    and (end_date is None or date.fromisoformat(row["purchase_date_utc"]) <= end_date)
]
filtered_category_day = [
    row
    for row in purchases_category_day
    if (not selected_months or row["event_month"] in selected_months)
    and (selected_category == "Todas" or row["category_code"] == selected_category)
    and (start_date is None or date.fromisoformat(row["purchase_date_utc"]) >= start_date)
    and (end_date is None or date.fromisoformat(row["purchase_date_utc"]) <= end_date)
]
filtered_funnel_month = [
    row for row in funnel_month if not selected_months or row["event_month"] in selected_months
]

purchase_events = sum(row["purchase_event_count"] for row in filtered_category_day)
estimated_value = sum(row["estimated_value_sum_price"] or 0.0 for row in filtered_category_day)
view_sessions = sum(row["view_sessions"] for row in filtered_funnel_month)
purchase_sessions = sum(
    row["purchase_after_cart_sessions"] for row in filtered_funnel_month
)
view_purchase_rate = purchase_sessions / view_sessions if view_sessions else None

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Eventos de compra", f"{purchase_events:,}")
kpi2.metric("Suma estimada de price", money(estimated_value))
kpi3.metric("Sesiones con vista", f"{view_sessions:,}")
kpi4.metric("Vista → compra", "—" if view_purchase_rate is None else f"{view_purchase_rate:.2%}")

left, right = st.columns(2)
with left:
    st.subheader("Funnel secuencial por mes")
    stage_names = ["Vista", "Carrito después de vista", "Compra después de carrito"]
    colors = {"2019-10": "#277da1", "2019-11": "#f9844a"}
    figure = go.Figure()
    for row in filtered_funnel_month:
        figure.add_trace(
            go.Bar(
                name=row["event_month"],
                x=stage_names,
                y=[
                    row["view_sessions"],
                    row["cart_after_view_sessions"],
                    row["purchase_after_cart_sessions"],
                ],
                marker_color=colors.get(row["event_month"], "#577590"),
            )
        )
    figure.update_layout(barmode="group", yaxis_title="Sesiones", legend_title="Mes")
    st.plotly_chart(figure, width="stretch")

with right:
    st.subheader("Compras por fecha (UTC)")
    by_date: dict[str, int] = {}
    for row in filtered_category_day:
        by_date[row["purchase_date_utc"]] = (
            by_date.get(row["purchase_date_utc"], 0) + row["purchase_event_count"]
        )
    figure = go.Figure(
        go.Scatter(
            x=sorted(by_date),
            y=[by_date[key] for key in sorted(by_date)],
            mode="lines",
            line={"color": "#277da1", "width": 2},
        )
    )
    figure.update_layout(xaxis_title="Fecha UTC", yaxis_title="Eventos purchase")
    st.plotly_chart(figure, width="stretch")

left, right = st.columns(2)
with left:
    st.subheader("Compras por categoría")
    category_totals: dict[str, list[float]] = {}
    for row in filtered_category_day:
        totals = category_totals.setdefault(row["category_code"], [0, 0.0])
        totals[0] += row["purchase_event_count"]
        totals[1] += row["estimated_value_sum_price"] or 0.0
    category_rows = sorted(
        [
            {"category_code": category, "purchase_event_count": values[0], "estimated_value_sum_price": values[1]}
            for category, values in category_totals.items()
        ],
        key=lambda row: row["purchase_event_count"],
        reverse=True,
    )[:15]
    figure = go.Figure(
        go.Bar(
            x=[row["purchase_event_count"] for row in category_rows],
            y=[row["category_code"] for row in category_rows],
            orientation="h",
            marker_color="#43aa8b",
        )
    )
    figure.update_layout(yaxis={"categoryorder": "total ascending"}, xaxis_title="Eventos purchase")
    st.plotly_chart(figure, width="stretch")

with right:
    st.subheader("Conversión vista → compra por día y hora UTC")
    heat_rows = [
        row for row in funnel_hour if not selected_months or row["event_month"] in selected_months
    ]
    weekday_labels = ["Dom", "Lun", "Mar", "Mié", "Jue", "Vie", "Sáb"]
    values = [[None for _ in range(24)] for _ in range(7)]
    hover = [["Sin sesiones" for _ in range(24)] for _ in range(7)]
    grouped: dict[tuple[int, int], list[int]] = {}
    for row in heat_rows:
        key = (row["spark_weekday_sunday_1"] - 1, row["utc_hour"])
        totals = grouped.setdefault(key, [0, 0])
        totals[0] += row["view_sessions"]
        totals[1] += row["purchase_after_cart_sessions"]
    for (weekday, hour), (sessions, purchases) in grouped.items():
        values[weekday][hour] = purchases / sessions if sessions else None
        hover[weekday][hour] = f"Sesiones: {sessions:,}<br>Compras: {purchases:,}"
    figure = go.Figure(
        go.Heatmap(
            z=values,
            x=list(range(24)),
            y=weekday_labels,
            text=hover,
            hovertemplate="%{y} %{x}:00 UTC<br>%{z:.2%}<br>%{text}<extra></extra>",
            colorscale="Teal",
            colorbar={"title": "Conversión"},
        )
    )
    figure.update_layout(xaxis_title="Hora de la primera vista", yaxis_title="Día de semana")
    st.plotly_chart(figure, width="stretch")

st.subheader("Eventos de compra por hora UTC (todas las categorías)")
purchase_events_by_hour: dict[int, int] = {}
for row in filtered_purchase_time:
    purchase_events_by_hour[row["utc_hour"]] = (
        purchase_events_by_hour.get(row["utc_hour"], 0) + row["purchase_event_count"]
    )
hour_figure = go.Figure(
    go.Bar(
        x=list(range(24)),
        y=[purchase_events_by_hour.get(hour, 0) for hour in range(24)],
        marker_color="#f9844a",
    )
)
hour_figure.update_layout(xaxis_title="Hora UTC", yaxis_title="Eventos purchase")
st.plotly_chart(hour_figure, width="stretch")

st.divider()
st.header("Evaluación del modelo de compra posterior a 5 minutos")
if model_metrics is None:
    st.info("El modelo ML aún no se ha entrenado. Ejecuta `src/train_model.py` para añadir esta sección.")
else:
    metrics = model_metrics["metrics"]
    model_kpis = st.columns(5)
    model_kpis[0].metric("ROC-AUC", f"{metrics['roc_auc']:.3f}")
    model_kpis[1].metric("PR-AUC", f"{metrics['pr_auc']:.3f}")
    model_kpis[2].metric("Recall", f"{metrics['recall']:.2%}")
    model_kpis[3].metric("Precisión", f"{metrics['precision']:.2%}")
    model_kpis[4].metric("F1", f"{metrics['f1']:.3f}")
    st.caption(
        f"Prueba temporal: {model_metrics['temporal_split']['train_month']} → "
        f"{model_metrics['temporal_split']['test_month']}. "
        f"Muestra determinista {model_metrics['sampling']['deterministic_session_hash_percent']} % "
        f"por hash de sesión. Umbral {model_metrics['decision_threshold']:.2f}."
    )
    matrix = metrics["confusion_matrix"]
    matrix_figure = go.Figure(
        go.Heatmap(
            z=[
                [matrix["true_negative"], matrix["false_positive"]],
                [matrix["false_negative"], matrix["true_positive"]],
            ],
            x=["Predice no compra", "Predice compra"],
            y=["Real: no compra", "Real: compra"],
            text=[
                [f"{matrix['true_negative']:,}", f"{matrix['false_positive']:,}"],
                [f"{matrix['false_negative']:,}", f"{matrix['true_positive']:,}"],
            ],
            texttemplate="%{text}",
            colorscale="Blues",
            showscale=False,
        )
    )
    matrix_figure.update_layout(title="Matriz de confusión (test de noviembre)")
    left, right = st.columns(2)
    with left:
        st.plotly_chart(matrix_figure, width="stretch")
    with right:
        decile_counts: dict[int, list[int]] = {}
        for row in model_metrics["score_deciles"]:
            counts = decile_counts.setdefault(row["decile"], [0, 0])
            counts[row["actual_label"]] += row["session_count"]
        deciles = sorted(decile_counts)
        observed_rates = [
            counts[1] / sum(counts) if sum(counts) else 0.0
            for counts in (decile_counts[decile] for decile in deciles)
        ]
        score_figure = go.Figure(
            go.Scatter(
                x=deciles,
                y=observed_rates,
                mode="lines+markers",
                line={"color": "#277da1", "width": 3},
            )
        )
        score_figure.update_layout(
            title="Compra observada por decil de probabilidad", xaxis_title="Decil", yaxis_title="Tasa observada"
        )
        st.plotly_chart(score_figure, width="stretch")
    st.caption(model_metrics["target_definition"] + ". " + model_metrics["features_definition"] + ".")

with st.expander("Definiciones y limitaciones"):
    st.markdown(
        "- El funnel se calcula por `user_session` y exige el orden temporal vista → carrito → compra.\n"
        "- Los nulos de categoría se agrupan como `[sin categoría]`; no se eliminan eventos.\n"
        "- La suma de `price` es una aproximación por evento, no ingreso neto ni margen.\n"
        "- Los datos cubren octubre–noviembre de 2019 y no prueban causalidad ni estacionalidad anual.\n"
        "- La app consulta solo agregados Gold; no lee los CSV raw ni los carga en Pandas."
    )
