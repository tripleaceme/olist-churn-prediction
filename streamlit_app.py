import json
import streamlit as st
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
# TAB 2: DATA DOCUMENTATION
# ============================================================
with tab_docs:
    st.title("Data Documentation")
    st.caption(
        "Auto-generated from dbt YAML definitions | Persisted to Snowflake via persist_docs"
    )

    # Load dbt manifest and catalog
    target_dir = Path(__file__).parent / "target"
    manifest_path = target_dir / "manifest.json"
    catalog_path = target_dir / "catalog.json"

    if not manifest_path.exists() or not catalog_path.exists():
        st.warning(
            "dbt artifacts not found. Run `dbt docs generate` to create them."
        )
        st.stop()

    manifest = json.loads(manifest_path.read_text())
    catalog = json.loads(catalog_path.read_text())

    # Build model metadata from manifest
    models = {}
    for node_id, node in manifest["nodes"].items():
        if node["resource_type"] == "model" and node["package_name"] == "olist_churn":
            models[node["unique_id"]] = {
                "name": node["name"],
                "description": node.get("description", ""),
                "schema": node.get("schema", ""),
                "materialized": node.get("config", {}).get("materialized", ""),
                "depends_on": [
                    d.split(".")[-1]
                    for d in node.get("depends_on", {}).get("nodes", [])
                ],
                "columns": {
                    col_name: col_info.get("description", "")
                    for col_name, col_info in node.get("columns", {}).items()
                },
                "tags": node.get("tags", []),
            }

    # Enrich with catalog info (data types, row counts)
    for node_id, cat_node in catalog.get("nodes", {}).items():
        if node_id in models:
            cat_columns = cat_node.get("columns", {})
            for col_name, col_info in cat_columns.items():
                col_lower = col_name.lower()
                if col_lower not in models[node_id]["columns"]:
                    models[node_id]["columns"][col_lower] = ""
                # Store type info
                models[node_id].setdefault("column_types", {})[col_lower] = col_info.get(
                    "type", "unknown"
                )
            stats = cat_node.get("stats", {})
            row_count = stats.get("row_count", {}).get("value")
            if row_count:
                models[node_id]["row_count"] = row_count

    # Group by layer
    layers = {"staging": [], "intermediate": [], "marts": []}
    for node_id, meta in models.items():
        schema = meta["schema"].lower()
        if schema in layers:
            layers[schema].append(meta)
        else:
            layers.setdefault("other", []).append(meta)

    # Sort within each layer
    for layer in layers.values():
        layer.sort(key=lambda m: m["name"])

    # --- Layer filter ---
    selected_layer = st.selectbox(
        "Filter by layer",
        ["All"] + [k.title() for k in layers if layers[k]],
    )

    # --- Render documentation ---
    for layer_name, layer_models in layers.items():
        if not layer_models:
            continue
        if selected_layer != "All" and selected_layer.lower() != layer_name:
            continue

        st.header(f"{layer_name.title()} Layer")

        for meta in layer_models:
            with st.expander(
                f"**{meta['name']}** — {meta['materialized']} | "
                + (f"{meta.get('row_count', '?')} rows" if meta.get('row_count') else meta['materialized'])
            ):
                st.markdown(f"**Description:** {meta['description']}")

                if meta["depends_on"]:
                    st.markdown(
                        f"**Depends on:** {', '.join(f'`{d}`' for d in meta['depends_on'])}"
                    )

                if meta["tags"]:
                    st.markdown(f"**Tags:** {', '.join(meta['tags'])}")

                # Column table
                col_types = meta.get("column_types", {})
                col_data = []
                for col_name, col_desc in meta["columns"].items():
                    col_data.append(
                        {
                            "Column": col_name,
                            "Type": col_types.get(col_name, ""),
                            "Description": col_desc,
                        }
                    )

                if col_data:
                    st.dataframe(
                        pd.DataFrame(col_data),
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.info("No column documentation found.")

    # --- Lineage summary ---
    st.divider()
    st.subheader("Pipeline Lineage")
    st.code(
        """
SOURCES (Snowflake OLIST.RAW_DATA)
  |
  v
STAGING (8 views - clean, rename, cast)
  stg_olist__customers, stg_olist__orders, stg_olist__order_items,
  stg_olist__order_payments, stg_olist__order_reviews, stg_olist__products,
  stg_olist__sellers, stg_olist__geolocation
  |
  v
INTERMEDIATE (5 views - business logic, features)
  int_order_enriched -> int_customer_order_summary
                            |
                        int_customer_rfm
                        int_customer_behavioral_features
                        int_customer_churn_labels
  |
  v
MARTS (4 tables - consumption-ready)
  mrt_customer_360 (SQL) -> mrt_churn_prediction (Python/ML)
                                |
                            mrt_churn_segment_summary (SQL)
  mrt_seller_performance (SQL)
""",
        language=None,
    )
