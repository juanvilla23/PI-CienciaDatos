import re
from io import BytesIO
from pathlib import Path
from typing import Optional, List

import boto3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="Refined Data & Model Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================================
# Styling
# ============================================================
CUSTOM_CSS = """
<style>
.main {
    background: linear-gradient(180deg, #f6f8fc 0%, #eef3fb 100%);
}
.block-container {
    padding-top: 1.2rem;
    padding-bottom: 2rem;
}
.hero {
    background: linear-gradient(135deg, #1f4fbf 0%, #5f8df7 48%, #7dc9ff 100%);
    padding: 1.4rem 1.6rem;
    border-radius: 22px;
    color: white;
    box-shadow: 0 12px 30px rgba(31, 79, 191, 0.22);
    margin-bottom: 1rem;
}
.hero h1 {
    margin: 0;
    font-size: 2rem;
    font-weight: 700;
}
.hero p {
    margin: 0.4rem 0 0 0;
    opacity: 0.95;
    font-size: 0.98rem;
}
.section-card {
    background: white;
    border-radius: 18px;
    padding: 1rem 1.1rem;
    box-shadow: 0 8px 24px rgba(25, 55, 109, 0.08);
    border: 1px solid rgba(95, 141, 247, 0.10);
    margin-bottom: 0.8rem;
}
.kpi-card {
    background: white;
    border-radius: 18px;
    padding: 1rem 1.1rem;
    box-shadow: 0 8px 24px rgba(25, 55, 109, 0.08);
    border-left: 6px solid #4f7cf7;
}
.kpi-label {
    color: #596579;
    font-size: 0.88rem;
    margin-bottom: 0.2rem;
}
.kpi-value {
    color: #15253b;
    font-size: 1.55rem;
    font-weight: 700;
    line-height: 1.2;
}
.kpi-sub {
    color: #718096;
    font-size: 0.8rem;
    margin-top: 0.15rem;
}
.small-note {
    color: #64748b;
    font-size: 0.85rem;
}
div[data-testid="stMetric"] {
    background: white;
    border: 1px solid rgba(95, 141, 247, 0.12);
    padding: 0.9rem;
    border-radius: 16px;
    box-shadow: 0 8px 24px rgba(25, 55, 109, 0.06);
}
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

st.markdown(
    """
    <div class='hero'>
        <h1>Dashboard de datos refinados y resultados del modelo</h1>
        <p>
            Panorama ejecutivo y operativo de los datos refinados, las variables seleccionadas y los insumos clave del modelo.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)


# ============================================================
# Helpers S3
# ============================================================
@st.cache_resource
def get_s3_client():
    session = boto3.Session(
        aws_access_key_id=st.secrets.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=st.secrets.get("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=st.secrets.get("AWS_SESSION_TOKEN"),
        region_name=st.secrets.get("AWS_DEFAULT_REGION"),
    )
    return session.client("s3")


def build_s3_url(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def human_bytes(num_bytes: float) -> str:
    if pd.isna(num_bytes):
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:,.2f} {unit}"
        size /= 1024
    return f"{size:,.2f} TB"


def classify_artifact(key: str) -> str:
    lowered = key.lower()
    if any(x in lowered for x in ["predictions", "prediction", "scored", "inference"]):
        return "model_predictions"
    if any(x in lowered for x in ["metrics", "metric", "evaluation", "auc", "roc", "confusion"]):
        return "model_metrics"
    if any(x in lowered for x in ["importance", "feature_selection", "binary_rank", "continuous_rank"]):
        return "feature_selection"
    if any(x in lowered for x in ["train_final_features", "train_features_selected", "refined", "features"]):
        return "refined_data"
    return "other"


def wrap_annotation_text(text: str, width: int = 26) -> str:
    if pd.isna(text):
        return ""
    words = str(text).split()
    lines = []
    current = ""

    for word in words:
        candidate = f"{current} {word}".strip()
        if len(candidate) <= width:
            current = candidate
        else:
            if current:
                lines.append(current)
            current = word

    if current:
        lines.append(current)

    if len(lines) > 3:
        lines = lines[:3]
        lines[-1] = lines[-1] + "..."

    return "<br>".join(lines)


def build_confusion_pct_matrix(conf_df: pd.DataFrame):
    required = {
        "clase_real",
        "clase_predicha",
        "tipo_resultado",
        "pct_dentro_clase_real",
        "interpretacion_negocio",
    }

    if conf_df.empty or not required.issubset(conf_df.columns):
        return pd.DataFrame(), {}

    temp = conf_df.copy()
    temp["pct_dentro_clase_real"] = pd.to_numeric(
        temp["pct_dentro_clase_real"], errors="coerce"
    ).fillna(0)

    row_order = ["No default real", "Default real"]
    col_order = ["Predijo no default", "Predijo default"]

    matrix = pd.DataFrame(0.0, index=row_order, columns=col_order)
    annotations = {}

    for _, row in temp.iterrows():
        real = row["clase_real"]
        pred = row["clase_predicha"]

        if real in matrix.index and pred in matrix.columns:
            pct = float(row["pct_dentro_clase_real"])
            matrix.loc[real, pred] = pct

            tipo = str(row["tipo_resultado"])
            interp = wrap_annotation_text(row["interpretacion_negocio"])

            annotations[(real, pred)] = (
                f"<b>{tipo}</b><br>"
                f"{pct:.1f}%<br>"
                f"{interp}"
            )

    return matrix, annotations


def infer_business_role(col: str) -> str:
    c = col.lower()
    if c == "target":
        return "objetivo"
    if "ext_source" in c:
        return "score externo"
    if "income" in c:
        return "ingreso"
    if "credit" in c and "bureau" not in c:
        return "crédito"
    if "annuity" in c:
        return "anualidad"
    if "payment" in c or "dpd" in c or "late_" in c or "underpaid" in c:
        return "comportamiento de pago"
    if "bureau" in c:
        return "bureau"
    if "social" in c:
        return "red social"
    if "days_" in c or "years" in c or "age" in c:
        return "tiempo"
    if "region" in c or "city" in c or "address" in c:
        return "geografía"
    if "document" in c:
        return "documentación"
    if "name_" in c or c.endswith("_idx"):
        return "categórica"
    return "otra"


# ============================================================
# Data loading
# ============================================================
@st.cache_data(show_spinner=False)
def list_s3_objects(bucket: str, prefix: str) -> pd.DataFrame:
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    rows = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            rows.append(
                {
                    "key": key,
                    "size_bytes": obj["Size"],
                    "size_mb": round(obj["Size"] / (1024 * 1024), 4),
                    "last_modified": pd.to_datetime(obj["LastModified"]),
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    parts = df["key"].str.split("/")
    df["nivel_0"] = parts.str[0].fillna("")
    df["artifact"] = parts.str[1].fillna("(sin artifact)")
    df["subartifact"] = parts.str[2].fillna("")
    df["filename"] = df["key"].str.split("/").str[-1]
    df["extension"] = df["filename"].str.extract(r"\.([^.]+)$")[0].str.lower().fillna("")
    df["artifact_type"] = df["key"].apply(classify_artifact)

    partition_maps = []
    for key in df["key"]:
        matches = re.findall(r"([A-Za-z0-9_\-]+)=([^/]+)", key)
        partition_maps.append(dict(matches))

    pcols = sorted({k for d in partition_maps for k in d.keys()})
    for col in pcols:
        df[col] = [d.get(col) for d in partition_maps]

    return df.sort_values(
        ["artifact_type", "artifact", "last_modified"],
        ascending=[True, True, False]
    ).reset_index(drop=True)


@st.cache_data(show_spinner=False)
def artifact_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = (
        df.groupby(["artifact_type", "artifact"], dropna=False)
        .agg(
            archivos=("key", "count"),
            size_bytes=("size_bytes", "sum"),
            ultima_actualizacion=("last_modified", "max"),
        )
        .reset_index()
        .sort_values(["artifact_type", "size_bytes", "archivos"], ascending=[True, False, False])
    )
    out["size_human"] = out["size_bytes"].apply(human_bytes)
    current_utc = pd.Timestamp.now(tz="UTC")
    out["dias_desde_actualizacion"] = (
        current_utc - out["ultima_actualizacion"]
    ).dt.total_seconds().div(86400).round(1)
    return out


@st.cache_data(show_spinner=False)
def read_preview(bucket: str, key: str, nrows: int = 500) -> pd.DataFrame:
    ext = Path(key).suffix.lower()
    s3 = get_s3_client()

    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read()
    data = BytesIO(raw)

    if ext == ".csv":
        return pd.read_csv(data, nrows=nrows)

    if ext == ".parquet":
        return pd.read_parquet(data).head(nrows)

    if ext in [".jsonl", ".ndjson"]:
        return pd.read_json(data, lines=True).head(nrows)

    if ext == ".json":
        return pd.read_json(data).head(nrows)

    raise ValueError(f"Formato no soportado para preview: {ext}")


@st.cache_data(show_spinner=False)
def read_multiple_parquets(bucket: str, keys: List[str], nrows: Optional[int] = None) -> pd.DataFrame:
    if not keys:
        return pd.DataFrame()

    s3 = get_s3_client()
    dfs = []
    rows_loaded = 0

    for key in keys:
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()
        data = BytesIO(raw)
        part_df = pd.read_parquet(data)

        if nrows is not None:
            remaining = nrows - rows_loaded
            if remaining <= 0:
                break
            part_df = part_df.head(remaining)

        dfs.append(part_df)
        rows_loaded += len(part_df)

        if nrows is not None and rows_loaded >= nrows:
            break

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


def list_artifact_parquet_keys(df: pd.DataFrame, artifact_name: str) -> List[str]:
    candidates = df[
        (
            df["artifact"].str.contains(artifact_name, case=False, na=False)
            | df["subartifact"].str.contains(artifact_name, case=False, na=False)
            | df["key"].str.contains(artifact_name, case=False, na=False)
        )
        & (df["extension"] == "parquet")
        & (~df["filename"].str.startswith("_", na=False))
    ].sort_values("key")

    return candidates["key"].tolist()


def choose_default_preview_key(refined_df: pd.DataFrame) -> Optional[str]:
    if refined_df.empty:
        return None

    preferred = refined_df[
        refined_df["key"].str.contains(
            "train_features_selected|train_final_features",
            case=False,
            na=False
        )
    ].copy()
    if not preferred.empty:
        preferred = preferred.sort_values(["size_bytes", "last_modified"], ascending=[False, False])
        return preferred.iloc[0]["key"]

    parquet_only = refined_df[refined_df["extension"] == "parquet"].copy()
    if not parquet_only.empty:
        parquet_only = parquet_only.sort_values(["size_bytes", "last_modified"], ascending=[False, False])
        return parquet_only.iloc[0]["key"]

    fallback = refined_df.sort_values(["size_bytes", "last_modified"], ascending=[False, False])
    return fallback.iloc[0]["key"]


def quick_profile(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["columna", "dtype", "nulos", "distintos", "pct_nulos", "rol_negocio"])

    out = pd.DataFrame(
        {
            "columna": df.columns,
            "dtype": [str(df[c].dtype) for c in df.columns],
            "nulos": [int(df[c].isna().sum()) for c in df.columns],
            "distintos": [int(df[c].nunique(dropna=True)) for c in df.columns],
            "pct_nulos": [round(float(df[c].isna().mean()) * 100, 2) for c in df.columns],
            "rol_negocio": [infer_business_role(c) for c in df.columns],
        }
    )
    return out.sort_values(["pct_nulos", "distintos"], ascending=[False, False])


def build_business_kpis(df: pd.DataFrame) -> List[dict]:
    kpis = []
    if df.empty:
        return kpis

    kpis.append({"label": "Filas del preview", "value": f"{len(df):,}", "sub": "muestra cargada"})
    kpis.append({"label": "Columnas", "value": f"{df.shape[1]:,}", "sub": "esquema visible"})

    if "target" in df.columns:
        valid_target = pd.to_numeric(df["target"], errors="coerce")
        if valid_target.notna().any():
            bad_rate = valid_target.mean() * 100
            kpis.append({"label": "Bad rate", "value": f"{bad_rate:.2f}%", "sub": "promedio en preview"})

    for col, label in [
        ("amt_income_total_cap", "Ingreso promedio"),
        ("amt_income_total", "Ingreso promedio"),
        ("amt_credit_cap", "Crédito promedio"),
        ("amt_credit", "Crédito promedio"),
        ("amt_annuity", "Anualidad promedio"),
    ]:
        if col in df.columns and pd.api.types.is_numeric_dtype(df[col]):
            value = pd.to_numeric(df[col], errors="coerce").mean()
            if pd.notna(value):
                kpis.append({"label": label, "value": f"{value:,.0f}", "sub": col})
                break

    if "ext_sources_mean" in df.columns:
        value = pd.to_numeric(df["ext_sources_mean"], errors="coerce").mean()
        if pd.notna(value):
            kpis.append({"label": "Score externo medio", "value": f"{value:.3f}", "sub": "ext_sources_mean"})

    if "late_payment_mean" in df.columns:
        value = pd.to_numeric(df["late_payment_mean"], errors="coerce").mean() * 100
        if pd.notna(value):
            kpis.append({"label": "Pago tardío medio", "value": f"{value:.2f}%", "sub": "late_payment_mean"})

    return kpis[:6]


def plot_kpi_cards(kpis: List[dict], columns_per_row: int = 3):
    if not kpis:
        return
    for i in range(0, len(kpis), columns_per_row):
        row = kpis[i: i + columns_per_row]
        cols = st.columns(len(row))
        for j, item in enumerate(row):
            cols[j].markdown(
                f"""
                <div class='kpi-card'>
                    <div class='kpi-label'>{item['label']}</div>
                    <div class='kpi-value'>{item['value']}</div>
                    <div class='kpi-sub'>{item['sub']}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def get_top_missing(profile: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    if profile.empty:
        return profile
    return profile[profile["pct_nulos"] > 0].head(top_n)


def extract_positive_probability(value):
    try:
        if isinstance(value, (list, tuple)) and len(value) >= 2:
            return float(value[1])
        if hasattr(value, "tolist"):
            arr = value.tolist()
            if isinstance(arr, list) and len(arr) >= 2:
                return float(arr[1])
        if isinstance(value, str):
            cleaned = value.strip().replace("[", "").replace("]", "")
            parts = [p.strip() for p in cleaned.split(",") if p.strip()]
            if len(parts) >= 2:
                return float(parts[1])
    except Exception:
        return None
    return None


def metrics_to_dict(metrics_df: pd.DataFrame) -> dict:
    if metrics_df.empty or not {"metrica", "valor"}.issubset(metrics_df.columns):
        return {}
    temp = metrics_df.copy()
    temp["metrica"] = temp["metrica"].astype(str)
    temp["valor"] = pd.to_numeric(temp["valor"], errors="coerce")
    return dict(zip(temp["metrica"], temp["valor"]))


def safe_read_artifact(bucket: str, df_files: pd.DataFrame, artifact_name: str, nrows: Optional[int] = None):
    keys = list_artifact_parquet_keys(df_files, artifact_name)

    if not keys:
        return pd.DataFrame(), f"No encontré parquet para {artifact_name}", []

    try:
        df = read_multiple_parquets(bucket, keys, nrows=nrows)
        return df, None, keys
    except Exception as e:
        return pd.DataFrame(), str(e), keys


# ============================================================
# Sidebar
# ============================================================
with st.sidebar:
    st.header("Configuración")
    bucket = st.text_input("Bucket", value="hcdr")
    prefix = st.text_input("Prefix base", value="refined/")
    preview_rows = st.slider("Filas en preview", min_value=100, max_value=2000, value=500, step=100)
    refresh = st.button("Recargar metadatos", use_container_width=True)
    st.markdown("---")
    st.caption("La parte superior resalta indicadores de negocio y calidad de datos; el detalle técnico queda disponible más abajo para exploración.")

if refresh:
    list_s3_objects.clear()
    artifact_summary.clear()
    read_preview.clear()
    read_multiple_parquets.clear()


# ============================================================
# Load S3 metadata
# ============================================================
with st.spinner("Leyendo artefactos desde S3..."):
    df_files = list_s3_objects(bucket, prefix)

if df_files.empty:
    st.warning("No encontré archivos bajo ese bucket/prefix.")
    st.stop()

summary = artifact_summary(df_files)
refined_df = df_files[df_files["artifact_type"] == "refined_data"].copy()
feature_sel_df = df_files[df_files["artifact_type"] == "feature_selection"].copy()
model_results_df = df_files[df_files["artifact_type"].isin(["model_metrics", "model_predictions"])].copy()

# default preview
selected_default_key = choose_default_preview_key(refined_df)
df_business_preview = pd.DataFrame()
preview_error = None
if selected_default_key:
    try:
        df_business_preview = read_preview(bucket, selected_default_key, nrows=preview_rows)
    except Exception as e:
        preview_error = str(e)


# ============================================================
# Executive header KPIs
# ============================================================
head1, head2, head3, head4 = st.columns(4)
head1.metric("Artefactos detectados", int(summary["artifact"].nunique()))
head2.metric("Archivos en refined", int(df_files.shape[0]))
head3.metric("Tamaño total", human_bytes(df_files["size_bytes"].sum()))
head4.metric(
    "Última actualización",
    df_files["last_modified"].max().strftime("%Y-%m-%d %H:%M") if not df_files.empty else "-",
)

if not df_business_preview.empty:
    plot_kpi_cards(build_business_kpis(df_business_preview), columns_per_row=3)
else:
    st.info("No pude cargar un preview por defecto del dataset refinado principal. Aun así puedes explorar archivos manualmente abajo.")
    if preview_error:
        st.caption(preview_error)

st.markdown("---")


# ============================================================
# Main tabs
# ============================================================
tab_refined, tab_model = st.tabs(["Datos refinados", "Resultados del modelo"])


# ============================================================
# TAB 1 - DATOS REFINADOS
# ============================================================
with tab_refined:
    st.markdown(
        "<div class='section-card'><h3 style='margin-top:0;'>Resumen ejecutivo del dataset refinado</h3><p class='small-note'>Indicadores y visuales de negocio construidos sobre un preview del artefacto refinado principal.</p></div>",
        unsafe_allow_html=True
    )

    profile = pd.DataFrame()
    if not df_business_preview.empty:
        profile = quick_profile(df_business_preview)
        top_missing = get_top_missing(profile, top_n=10)

        row_a_left, row_a_right = st.columns(2)
        with row_a_left:
            if "target" in df_business_preview.columns:
                target_series = pd.to_numeric(df_business_preview["target"], errors="coerce").fillna(0)
                target_df = pd.DataFrame(
                    {
                        "segmento": ["No default", "Default"],
                        "conteo": [(target_series == 0).sum(), (target_series == 1).sum()],
                    }
                )
                fig = px.pie(
                    target_df,
                    names="segmento",
                    values="conteo",
                    hole=0.62,
                    title="Distribución del target",
                )
                fig.update_traces(textposition="inside", textinfo="percent+label")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("El preview seleccionado no tiene columna target para mostrar la mezcla de clases.")

        with row_a_right:
            if not top_missing.empty:
                fig = px.bar(
                    top_missing.sort_values("pct_nulos", ascending=True),
                    x="pct_nulos",
                    y="columna",
                    orientation="h",
                    color="rol_negocio",
                    title="Top columnas con nulos",
                )
                fig.update_layout(xaxis_title="% nulos", yaxis_title="")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No se observan nulos en el preview cargado.")

        row_b_left, row_b_right = st.columns(2)
        with row_b_left:
            if any(c in df_business_preview.columns for c in ["amt_income_total", "amt_income_total_cap", "amt_credit", "amt_credit_cap"]):
                available = [
                    c for c in [
                        "amt_income_total_cap",
                        "amt_income_total",
                        "amt_credit_cap",
                        "amt_credit",
                        "amt_annuity"
                    ] if c in df_business_preview.columns
                ]
                melted = df_business_preview[available].melt(var_name="métrica", value_name="valor").dropna()
                fig = px.box(melted, x="métrica", y="valor", title="Montos principales")
                fig.update_layout(xaxis_title="", yaxis_title="Valor")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No encontré columnas de montos principales en el preview para construir la vista financiera.")

        with row_b_right:
            useful = [
                c for c in [
                    "ext_sources_mean",
                    "ext_sources_max",
                    "ext_sources_min",
                    "ext_source_2",
                    "ext_source_3",
                    "late_payment_mean",
                    "underpaid_mean",
                    "late_30_sum",
                    "credit_income_ratio",
                    "annuity_income_ratio",
                ] if c in df_business_preview.columns
            ]
            if useful:
                long_df = df_business_preview[useful].melt(var_name="feature", value_name="valor").dropna()
                fig = px.violin(
                    long_df,
                    x="feature",
                    y="valor",
                    box=True,
                    points=False,
                    title="Variables clave del modelo",
                )
                fig.update_layout(xaxis_title="", yaxis_title="Valor")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No encontré variables típicas de score y comportamiento en el preview para esta vista.")
    else:
        st.info("No pude generar la vista ejecutiva porque no se cargó un preview de datos refinados.")

    st.markdown(
        "<div class='section-card'><h3 style='margin-top:0;'>Selección de variables y señales del modelo</h3><p class='small-note'>Resumen de artefactos como importance_df, binary_rank y continuous_rank guardados en refined.</p></div>",
        unsafe_allow_html=True
    )

    refined_summary = summary[summary["artifact_type"].isin(["refined_data", "feature_selection"])].copy()
    if not feature_sel_df.empty:
        feature_sel_summary = (
            feature_sel_df.groupby(["artifact", "subartifact"], dropna=False)
            .agg(archivos=("key", "count"), size_mb=("size_mb", "sum"), ultima_actualizacion=("last_modified", "max"))
            .reset_index()
            .sort_values(["artifact", "subartifact"], ascending=True)
        )
        feature_sel_summary["size_mb"] = feature_sel_summary["size_mb"].round(2)
        st.dataframe(feature_sel_summary, use_container_width=True, height=240)
    else:
        st.info("No encontré artefactos de selección de variables bajo el prefix indicado.")
        feature_sel_summary = pd.DataFrame()

    graph_left, graph_right = st.columns(2)
    with graph_left:
        if refined_summary.empty:
            st.info("No encontré artefactos refinados o de selección de features bajo el prefix indicado.")
        else:
            fig = px.bar(
                refined_summary,
                x="artifact",
                y="size_bytes",
                color="artifact_type",
                title="Tamaño por artefacto refinado",
            )
            fig.update_layout(xaxis_title="", yaxis_title="Bytes")
            st.plotly_chart(fig, use_container_width=True)

    with graph_right:
        if not feature_sel_summary.empty:
            fig = px.sunburst(
                feature_sel_summary,
                path=["artifact", "subartifact"],
                values="archivos",
                title="Mapa de artefactos de feature selection"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("El mapa de artefactos de feature selection aparecerá aquí cuando existan esos archivos.")

    st.markdown(
        "<div class='section-card'><h3 style='margin-top:0;'>Preview detallado y perfil técnico</h3><p class='small-note'>Exploración manual del archivo refinado, con foco en monitoreo del dataset y calidad de datos.</p></div>",
        unsafe_allow_html=True
    )

    if not df_business_preview.empty:
        st.markdown(f"**Archivo principal sugerido**: `{selected_default_key}`")

    preview_options = refined_df.sort_values("last_modified", ascending=False)["key"].tolist() if not refined_df.empty else []
    selected_preview_key = None
    if preview_options:
        default_index = preview_options.index(selected_default_key) if selected_default_key in preview_options else 0
        selected_preview_key = st.selectbox("Selecciona un archivo refinado", preview_options, index=default_index)

    if selected_preview_key and st.button("Actualizar preview refinado", use_container_width=True):
        try:
            df_business_preview = read_preview(bucket, selected_preview_key, nrows=preview_rows)
            profile = quick_profile(df_business_preview)
            st.success("Preview actualizado. Revisa las métricas y gráficas refrescadas arriba.")
        except Exception as e:
            st.error(f"No pude cargar preview: {e}")

    if not df_business_preview.empty:
        st.dataframe(df_business_preview, use_container_width=True, height=320)
        st.dataframe(profile, use_container_width=True, height=280)

    st.markdown(
        "<div class='section-card'><h3 style='margin-top:0;'>Inventario de artefactos refinados</h3><p class='small-note'>Metadatos de archivos, tamaños, particiones y distribución por artefacto. Esta capa sigue disponible para monitoreo técnico.</p></div>",
        unsafe_allow_html=True
    )

    if refined_summary.empty:
        st.info("No encontré artefactos refinados o de selección de features bajo el prefix indicado.")
    else:
        show_df = refined_summary[[
            "artifact_type",
            "artifact",
            "archivos",
            "size_human",
            "ultima_actualizacion",
            "dias_desde_actualizacion"
        ]].copy()
        st.dataframe(show_df, use_container_width=True, height=260)

        file_filter_left, file_filter_mid, file_filter_right = st.columns(3)
        with file_filter_left:
            refined_artifacts = ["(todos)"] + sorted(refined_df["artifact"].dropna().unique().tolist())
            selected_artifact = st.selectbox("Filtrar artefacto", refined_artifacts)
        with file_filter_mid:
            ext_options = ["(todas)"] + sorted(refined_df["extension"].dropna().unique().tolist())
            selected_ext = st.selectbox("Filtrar extensión", ext_options)
        with file_filter_right:
            sort_mode = st.selectbox("Orden", ["Más recientes", "Más pesados", "Nombre"], index=0)

        df_refined_sel = refined_df.copy()
        if selected_artifact != "(todos)":
            df_refined_sel = df_refined_sel[df_refined_sel["artifact"] == selected_artifact].copy()
        if selected_ext != "(todas)":
            df_refined_sel = df_refined_sel[df_refined_sel["extension"] == selected_ext].copy()

        if sort_mode == "Más recientes":
            df_refined_sel = df_refined_sel.sort_values("last_modified", ascending=False)
        elif sort_mode == "Más pesados":
            df_refined_sel = df_refined_sel.sort_values("size_bytes", ascending=False)
        else:
            df_refined_sel = df_refined_sel.sort_values("key")

        rk1, rk2, rk3 = st.columns(3)
        rk1.metric("Archivos filtrados", int(df_refined_sel.shape[0]))
        rk2.metric("Tamaño filtrado", human_bytes(df_refined_sel["size_bytes"].sum()))
        rk3.metric("Artefactos visibles", int(df_refined_sel["artifact"].nunique()))

        st.dataframe(
            df_refined_sel[["artifact", "subartifact", "filename", "extension", "size_mb", "last_modified", "key"]],
            use_container_width=True,
            height=300,
        )

        base_cols = {
            "key",
            "size_bytes",
            "size_mb",
            "last_modified",
            "nivel_0",
            "artifact",
            "subartifact",
            "filename",
            "extension",
            "artifact_type"
        }
        partition_cols = [c for c in df_refined_sel.columns if c not in base_cols]
        if partition_cols:
            part_col = st.selectbox("Partición para analizar", partition_cols)
            part_summary = (
                df_refined_sel.groupby(part_col, dropna=False)
                .agg(archivos=("key", "count"), size_mb=("size_mb", "sum"))
                .reset_index()
                .sort_values("archivos", ascending=False)
            )
            part_summary["size_mb"] = part_summary["size_mb"].round(2)
            st.dataframe(part_summary, use_container_width=True, height=220)

            fig = px.bar(part_summary, x=part_col, y="archivos", color="size_mb", title=f"Distribución por {part_col}")
            fig.update_layout(xaxis_title="", yaxis_title="Archivos")
            st.plotly_chart(fig, use_container_width=True)


# ============================================================
# TAB 2 - RESULTADOS DEL MODELO
# ============================================================
with tab_model:
    st.markdown(
        "<div class='section-card'><h3 style='margin-top:0;'>Resultados del modelo</h3><p class='small-note'>Vista compacta del desempeño del random forest usando predicciones y matriz de confusión guardadas en refined.</p></div>",
        unsafe_allow_html=True,
    )

    model_summary = summary[summary["artifact_type"].isin(["model_metrics", "model_predictions"])].copy()

    metrics_preview, metrics_error, metrics_keys = safe_read_artifact(
        bucket, model_results_df, "model_metrics_random_forest", nrows=50
    )
    predictions_preview, predictions_error, predictions_keys = safe_read_artifact(
        bucket, model_results_df, "model_predictions_random_forest", nrows=preview_rows
    )
    confusion_preview, confusion_error, confusion_keys = safe_read_artifact(
        bucket, model_results_df, "confusion_matrix_random_forest", nrows=None
    )

    if model_summary.empty:
        left, right = st.columns([1.15, 1])

        with left:
            st.info(
                "Aún no detecté artefactos de resultados del modelo bajo refined/. "
                "Cuando guardes rutas como refined/model_metrics_random_forest/, "
                "refined/model_predictions_random_forest/ y "
                "refined/confusion_matrix_random_forest/, esta sección se llenará automáticamente."
            )

        with right:
            fig = go.Figure()
            fig.add_annotation(
                text="Sin resultados del modelo disponibles",
                x=0.5,
                y=0.55,
                showarrow=False,
                font=dict(size=20, color="#24438f"),
            )
            fig.update_xaxes(visible=False)
            fig.update_yaxes(visible=False)
            fig.update_layout(
                height=260,
                margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="white",
            )
            st.plotly_chart(fig, use_container_width=True)

    else:
        mk1, mk2, mk3, mk4 = st.columns(4)
        mk1.metric("Artefactos de resultados", int(model_summary["artifact"].nunique()))
        mk2.metric("Archivos de resultados", int(model_results_df.shape[0]))
        mk3.metric("Tamaño total", human_bytes(model_results_df["size_bytes"].sum()))
        mk4.metric("Última actualización", model_results_df["last_modified"].max().strftime("%Y-%m-%d %H:%M"))

        row_model_left, row_model_right = st.columns(2)

        with row_model_right:
            if not predictions_preview.empty and {"target", "prediction"}.issubset(predictions_preview.columns):
                pred_chart = predictions_preview.copy()
                pred_chart["target_label"] = pred_chart["target"].astype(str).map({
                    "0": "Real: no default",
                    "1": "Real: default"
                }).fillna("Real: otro")
                pred_chart["prediction_label"] = pred_chart["prediction"].astype(str).map({
                    "0.0": "Pred: no default",
                    "1.0": "Pred: default",
                    "0": "Pred: no default",
                    "1": "Pred: default"
                }).fillna("Pred: otro")

                grouped = pred_chart.groupby(["target_label", "prediction_label"]).size().reset_index(name="conteo")
                fig = px.bar(
                    grouped,
                    x="target_label",
                    y="conteo",
                    color="prediction_label",
                    barmode="group",
                    title="Comparación real vs predicho (preview)",
                )
                fig.update_layout(xaxis_title="", yaxis_title="Conteo")
                st.plotly_chart(fig, use_container_width=True)
            elif predictions_error:
                st.info(f"No pude leer las predicciones del modelo: {predictions_error}")
            else:
                st.info("No encontré columnas target y prediction para construir la comparación real vs predicho.")

        with row_model_left:
            if not confusion_preview.empty:
                pct_matrix, annotations_map = build_confusion_pct_matrix(confusion_preview)

                if not pct_matrix.empty:
                    fig = px.imshow(
                        pct_matrix,
                        text_auto=False,
                        aspect="auto",
                        color_continuous_scale="Blues",
                        title="Matriz de confusión por % dentro de clase real",
                    )

                    annotations = []
                    for i, real_label in enumerate(pct_matrix.index):
                        for j, pred_label in enumerate(pct_matrix.columns):
                            text = annotations_map.get(
                                (real_label, pred_label),
                                f"{pct_matrix.loc[real_label, pred_label]:.1f}%"
                            )
                            annotations.append(
                                dict(
                                    x=j,
                                    y=i,
                                    text=text,
                                    showarrow=False,
                                    font=dict(size=12, color="black"),
                                )
                            )

                    fig.update_layout(
                        xaxis_title="Clase predicha",
                        yaxis_title="Clase real",
                        annotations=annotations,
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No pude construir la matriz de confusión con pct_dentro_clase_real.")
            else:
                st.info(confusion_error or "No encontré un parquet legible en confusion_matrix_random_forest.")