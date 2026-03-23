"""
Streamlit interview demo dashboard for Smart Price Analytics.

This app reads from the analytics schema in PostgreSQL and displays
simple business-facing metrics and charts that are easy to demo live.
"""

import os
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
import plotly.express as px
import streamlit as st

from db import DatabaseManager


st.set_page_config(
    page_title="Smart Price Analytics - Live Demo",
    page_icon="USD",
    layout="wide",
)


@st.cache_resource
def get_db_manager() -> DatabaseManager:
    """Build a DB manager using secrets/env with DATABASE_URL support."""

    def parse_database_url(db_url: str) -> dict:
        parsed = urlparse(db_url)
        if not parsed.hostname or not parsed.path:
            raise ValueError("Invalid DATABASE_URL format")

        db_name = parsed.path.lstrip("/")
        return {
            "host": parsed.hostname,
            "port": int(parsed.port or 5432),
            "database": db_name,
            "user": parsed.username,
            "password": parsed.password,
        }

    cfg = {}

    if hasattr(st, "secrets"):
        if "DATABASE_URL" in st.secrets and st.secrets.get("DATABASE_URL"):
            cfg = parse_database_url(str(st.secrets.get("DATABASE_URL")))
        elif "DB_HOST" in st.secrets:
            cfg = {
                "host": st.secrets.get("DB_HOST"),
                "port": int(st.secrets.get("DB_PORT", 5432)),
                "database": st.secrets.get("DB_NAME"),
                "user": st.secrets.get("DB_USER"),
                "password": st.secrets.get("DB_PASSWORD"),
            }

    if not cfg:
        env_db_url = os.getenv("DATABASE_URL")
        if env_db_url:
            cfg = parse_database_url(env_db_url)
        else:
            cfg = {
                "host": os.getenv("DB_HOST", "localhost"),
                "port": int(os.getenv("DB_PORT", 5432)),
                "database": os.getenv("DB_NAME", "smart_price_analytics"),
                "user": os.getenv("DB_USER", "postgres"),
                "password": os.getenv("DB_PASSWORD", ""),
            }

    return DatabaseManager(**cfg)


def render_connection_help(error_text: str) -> None:
    """Show concise troubleshooting guidance for cloud DB connection issues."""
    st.error("Could not connect to PostgreSQL for dashboard queries.")

    if "localhost" in error_text.lower() or "connection refused" in error_text.lower():
        st.warning(
            "Streamlit Cloud cannot use localhost. Add your remote PostgreSQL credentials in app Secrets."
        )

    st.markdown("Use one of these Secrets formats in Streamlit Cloud:")
    st.code(
        """# Option 1: Single URL
DATABASE_URL = \"postgresql://user:password@host:5432/smart_price_analytics\"

# Option 2: Individual fields
DB_HOST = \"your-db-host\"
DB_PORT = 5432
DB_NAME = \"smart_price_analytics\"
DB_USER = \"postgres\"
DB_PASSWORD = \"your-password\"""",
        language="toml",
    )

    st.info(
        "After saving Secrets: reboot the app and ensure analytics tables are populated by running pipeline.py at least once."
    )


def query_to_df(db: DatabaseManager, query: str) -> pd.DataFrame:
    """Run SQL and return a DataFrame with column names."""
    conn = db.get_connection()
    if conn is None:
        raise RuntimeError("Could not get database connection")

    try:
        return pd.read_sql_query(query, conn)
    finally:
        db.return_connection(conn)


def main() -> None:
    st.title("Smart Price Analytics - Interview Demo")
    st.caption("Live, query-backed dashboard from your analytics schema")

    st.markdown(
        "Use this app during interviews to show business metrics, trends, and SQL-backed insights."
    )

    try:
        db = get_db_manager()

        health = query_to_df(
            db,
            """
            SELECT
                COUNT(*)::INT AS product_count
            FROM analytics.dim_product;
            """,
        )

        if health.empty:
            st.warning("Database connected but no analytics data found.")
            return

        kpi_df = query_to_df(
            db,
            """
            WITH latest_stats AS (
                SELECT MAX(stats_date) AS latest_date
                FROM analytics.agg_brand_daily_stats
            )
            SELECT
                (SELECT COUNT(*) FROM analytics.dim_product)::INT AS total_products,
                (SELECT COUNT(DISTINCT brand) FROM analytics.dim_product WHERE brand IS NOT NULL)::INT AS total_brands,
                (SELECT ROUND(AVG(current_price)::NUMERIC, 2) FROM analytics.fact_price_history
                 WHERE price_date >= CURRENT_DATE - INTERVAL '30 days') AS avg_price_30d,
                (SELECT ROUND(MAX(discount_percentage)::NUMERIC, 2) FROM analytics.fact_price_history
                 WHERE price_date >= CURRENT_DATE - INTERVAL '30 days') AS max_discount_30d,
                (SELECT latest_date FROM latest_stats) AS latest_stats_date;
            """,
        )

        kpi = kpi_df.iloc[0]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Products Tracked", int(kpi["total_products"]))
        c2.metric("Brands", int(kpi["total_brands"]))
        c3.metric("Avg Price (30d)", f"INR {kpi['avg_price_30d']}")
        c4.metric("Max Discount (30d)", f"{kpi['max_discount_30d']}%")

        st.divider()

        trend_df = query_to_df(
            db,
            """
            SELECT
                brand,
                stats_date,
                avg_price,
                avg_discount,
                product_count
            FROM analytics.agg_brand_daily_stats
            WHERE stats_date >= CURRENT_DATE - INTERVAL '30 days'
              AND brand IS NOT NULL
            ORDER BY stats_date ASC;
            """,
        )

        st.subheader("Brand Price Trend (Last 30 Days)")
        if trend_df.empty:
            st.info("No trend data found yet. Run pipeline.py once to populate analytics tables.")
        else:
            fig = px.line(
                trend_df,
                x="stats_date",
                y="avg_price",
                color="brand",
                markers=True,
                title="Average Brand Price Over Time",
            )
            st.plotly_chart(fig, use_container_width=True)

        left, right = st.columns(2)

        with left:
            st.subheader("Top Discounts (Last 7 Days)")
            discounts_df = query_to_df(
                db,
                """
                SELECT
                    dp.brand,
                    dp.product_name,
                    fph.discount_percentage,
                    fph.current_price,
                    fph.source_marketplace,
                    fph.price_date
                FROM analytics.fact_price_history fph
                JOIN analytics.dim_product dp ON dp.product_id = fph.product_id
                WHERE fph.price_date >= CURRENT_DATE - INTERVAL '7 days'
                  AND fph.discount_percentage IS NOT NULL
                ORDER BY fph.discount_percentage DESC
                LIMIT 10;
                """,
            )
            st.dataframe(discounts_df, use_container_width=True, hide_index=True)

        with right:
            st.subheader("Most Volatile Products (90 Days)")
            volatility_df = query_to_df(
                db,
                """
                SELECT
                    dp.brand,
                    dp.product_name,
                    ROUND(STDDEV_POP(fph.current_price)::NUMERIC, 2) AS price_stddev,
                    ROUND((MAX(fph.current_price) - MIN(fph.current_price))::NUMERIC, 2) AS price_range
                FROM analytics.fact_price_history fph
                JOIN analytics.dim_product dp ON dp.product_id = fph.product_id
                WHERE fph.price_date >= CURRENT_DATE - INTERVAL '90 days'
                GROUP BY dp.brand, dp.product_name
                HAVING COUNT(*) >= 3
                ORDER BY price_stddev DESC NULLS LAST
                LIMIT 10;
                """,
            )
            st.dataframe(volatility_df, use_container_width=True, hide_index=True)

        st.divider()
        st.caption(
            f"Last refreshed: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        )

    except Exception as exc:
        render_connection_help(str(exc))


if __name__ == "__main__":
    main()
