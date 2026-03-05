import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import plotly.express as px
from pathlib import Path

st.set_page_config(page_title="Olist Churn Intelligence", layout="wide")

# --- Snowflake Connection ---
conn = st.connection("snowflake")

# --- Tabs ---
tab_dashboard, tab_docs = st.tabs(["Churn Dashboard", "Data Documentation"])

# ============================================================
# TAB 1: CHURN DASHBOARD
# ============================================================
with tab_dashboard:
    st.title("Customer Churn Intelligence")
    st.caption("Powered by dbt (SQL + Python) | Snowflake | scikit-learn")

    # Load data
    predictions = conn.query(
        "SELECT * FROM OLIST.MARTS.MRT_CHURN_PREDICTION", ttl=600
    )
    segments = conn.query(
        "SELECT * FROM OLIST.MARTS.MRT_CHURN_SEGMENT_SUMMARY", ttl=600
    )

    # --- KPI Row ---
    total = len(predictions)
    churned = len(predictions[predictions["CHURN_PREDICTION"] == 1])
    at_risk = len(
        predictions[predictions["CHURN_RISK_TIER"].isin(["High", "Critical"])]
    )
    revenue_at_risk = predictions[
        predictions["CHURN_RISK_TIER"].isin(["High", "Critical"])
    ]["LIFETIME_REVENUE"].sum()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Customers", f"{total:,}")
    col2.metric("Predicted Churned", f"{churned:,}", f"{churned/total:.0%}")
    col3.metric("High/Critical Risk", f"{at_risk:,}")
    col4.metric("Revenue at Risk", f"R${revenue_at_risk:,.0f}")

    st.divider()

    # --- Charts Row ---
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Churn Risk Distribution")
        risk_order = ["Low", "Medium", "High", "Critical"]
        risk_dist = (
            predictions["CHURN_RISK_TIER"]
            .value_counts()
            .reindex(risk_order)
            .reset_index()
        )
        risk_dist.columns = ["Risk Tier", "Count"]
        fig1 = px.pie(
            risk_dist,
            names="Risk Tier",
            values="Count",
            color="Risk Tier",
            color_discrete_map={
                "Low": "#2ecc71",
                "Medium": "#f39c12",
                "High": "#e74c3c",
                "Critical": "#8e44ad",
            },
        )
        st.plotly_chart(fig1, use_container_width=True)

    with col_right:
        st.subheader("Churn Rate by State (Top 10)")
        state_churn = (
            predictions.groupby("STATE")
            .agg(
                churn_rate=("CHURN_PREDICTION", "mean"),
                count=("CHURN_PREDICTION", "count"),
            )
            .reset_index()
            .sort_values("count", ascending=False)
            .head(10)
        )
        fig2 = px.bar(
            state_churn,
            x="STATE",
            y="churn_rate",
            color="churn_rate",
            color_continuous_scale="RdYlGn_r",
            labels={"churn_rate": "Churn Rate", "STATE": "State"},
        )
        fig2.update_layout(yaxis_tickformat=".0%")
        st.plotly_chart(fig2, use_container_width=True)

    # --- Scatter Plot ---
    st.subheader("Revenue vs. Churn Probability")
    sample_size = min(3000, len(predictions))
    fig3 = px.scatter(
        predictions.sample(sample_size, random_state=42),
        x="LIFETIME_REVENUE",
        y="CHURN_PROBABILITY",
        color="CHURN_RISK_TIER",
        color_discrete_map={
            "Low": "#2ecc71",
            "Medium": "#f39c12",
            "High": "#e74c3c",
            "Critical": "#8e44ad",
        },
        category_orders={"CHURN_RISK_TIER": risk_order},
        labels={
            "LIFETIME_REVENUE": "Lifetime Revenue (BRL)",
            "CHURN_PROBABILITY": "Churn Probability",
        },
        opacity=0.6,
    )
    st.plotly_chart(fig3, use_container_width=True)

    # --- At-Risk Customers Table ---
    st.subheader("At-Risk Customers (High + Critical)")
    at_risk_df = (
        predictions[predictions["CHURN_RISK_TIER"].isin(["High", "Critical"])]
        .sort_values("CHURN_PROBABILITY", ascending=False)
        .reset_index(drop=True)
    )
    st.dataframe(at_risk_df, use_container_width=True, hide_index=True)

# ============================================================
# TAB 2: DATA DOCUMENTATION (dbt docs site)
# ============================================================
with tab_docs:
    st.title("Data Documentation")
    st.caption(
        "Interactive dbt docs with full lineage graph | Generated via `dbt docs generate`"
    )

    target_dir = Path(__file__).parent / "target"
    index_path = target_dir / "index.html"
    manifest_path = target_dir / "manifest.json"
    catalog_path = target_dir / "catalog.json"

    if not all(p.exists() for p in [index_path, manifest_path, catalog_path]):
        st.warning(
            "dbt artifacts not found. Run `dbt docs generate` to create them."
        )
        st.stop()

    # Build self-contained dbt docs HTML by injecting manifest + catalog
    html = index_path.read_text()
    manifest_json = manifest_path.read_text()
    catalog_json = catalog_path.read_text()

    html = html.replace(
        '"MANIFEST.JSON INLINE DATA"', manifest_json
    ).replace(
        '"CATALOG.JSON INLINE DATA"', catalog_json
    )

    components.html(html, height=800, scrolling=True)
