"""
streamlit_app.py
----------------
Interactive dashboard for NYC Restaurant Inspection data.

Connects to PostgreSQL (local Docker container or Supabase) via SQLAlchemy
and presents 6 interactive visualizations with sidebar filters.

Run locally:
    streamlit run streamlit_app.py

In Docker:
    docker-compose up --build   (streamlit service starts automatically)

Dashboard includes:
    1. Summary metrics (total restaurants, inspections, % grade A)
    2. Grade distribution pie chart
    3. Average inspection score by borough (bar chart)
    4. Inspection score trend over time (line chart)
    5. Top 10 most-cited violation codes (horizontal bar)
    6. Average score by cuisine type (horizontal bar)
    7. Restaurant location map coloured by grade (scatter map)
"""

import os
import time

import psycopg
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------


def wait_for_data():
    for _ in range(30):
        try:
            conn = psycopg.connect(
                host=os.getenv("LOCAL_DB_HOST"),
                port=os.getenv("LOCAL_DB_PORT"),
                dbname=os.getenv("LOCAL_DB_NAME"),
                user=os.getenv("LOCAL_DB_USER"),
                password=os.getenv("LOCAL_DB_PASSWORD"),
            )

            with conn.cursor() as cur:
                cur.execute("SELECT ready FROM etl_status LIMIT 1")
                ready = cur.fetchone()

            conn.close()

            if ready and ready[0]:
                print("✅ Data ready")
                return

        except Exception as e:
            print(f"⏳ Waiting for data... {e}")

        time.sleep(2)

    raise Exception("❌ Data never became ready")


@st.cache_resource   # cache the engine for the lifetime of the session
def get_engine():
    """
    Create a SQLAlchemy engine from DATABASE_URL.

    psycopg3 (the driver we use) needs 'postgresql+psycopg://' as the scheme.
    If the URL already uses 'postgresql://' (common in connection strings copied
    from Supabase), we swap it automatically.
    """
    raw_url = os.getenv("DATABASE_URL")

    if not raw_url:
        raise RuntimeError("Database URL not found in .env file.")
    
    # SQLAlchemy with psycopg3 requires the '+psycopg' dialect suffix
    db_url = raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return create_engine(db_url, pool_pre_ping=True)


def run_query(sql: str, params: dict = None) -> pd.DataFrame:
    """Execute a SQL query and return results as a DataFrame."""
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


# ---------------------------------------------------------------------------
# Cached data loaders
# Each query is cached with @st.cache_data so repeated filter interactions
# don't re-hit the database every time a widget changes.
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)  # refresh every 5 minutes
def load_summary_metrics():
    """High-level counts shown at the top of the dashboard."""
    return run_query("""
        SELECT
            (SELECT COUNT(DISTINCT camis) FROM restaurants)           AS total_restaurants,
            (SELECT COUNT(*)              FROM inspections)           AS total_inspections,
            (SELECT MIN(inspection_date)  FROM inspections)           AS earliest_date,
            (SELECT MAX(inspection_date)  FROM inspections)           AS latest_date,
            (SELECT ROUND(
                100.0 * COUNT(*) FILTER (WHERE grade = 'A') / NULLIF(COUNT(*), 0), 1
             ) FROM inspections WHERE grade IS NOT NULL)              AS pct_grade_a
    """)


@st.cache_data(ttl=300)
def load_grade_distribution(boroughs: list, start_date: str, end_date: str):
    """Row counts per grade, filtered by borough and date range."""
    boro_filter = "AND r.boro = ANY(:boroughs)" if boroughs else ""
    return run_query(
        f"""
        SELECT i.grade, COUNT(*) AS count
        FROM   inspections i
        JOIN   restaurants r ON i.camis = r.camis
        WHERE  i.grade IS NOT NULL
          AND  i.grade != ''
          AND  i.inspection_date BETWEEN :start_date AND :end_date
          {boro_filter}
        GROUP  BY i.grade
        ORDER  BY count DESC
        """,
        {"boroughs": boroughs or [], "start_date": start_date, "end_date": end_date},
    )


@st.cache_data(ttl=300)
def load_score_by_borough(start_date: str, end_date: str):
    """Average inspection score and inspection count per borough."""
    return run_query(
        """
        SELECT  r.boro,
                ROUND(AVG(i.score)::numeric, 1) AS avg_score,
                COUNT(*)                         AS total_inspections
        FROM    inspections i
        JOIN    restaurants r ON i.camis = r.camis
        WHERE   i.score IS NOT NULL
          AND   r.boro  IS NOT NULL
          AND   r.boro  != ''
          AND   i.inspection_date BETWEEN :start_date AND :end_date
        GROUP   BY r.boro
        ORDER   BY avg_score ASC
        """,
        {"start_date": start_date, "end_date": end_date},
    )


@st.cache_data(ttl=300)
def load_score_trend(boroughs: list, start_date: str, end_date: str):
    """Monthly average inspection score, optionally filtered by borough."""
    boro_filter = "AND r.boro = ANY(:boroughs)" if boroughs else ""
    return run_query(
        f"""
        SELECT  DATE_TRUNC('month', i.inspection_date)  AS month,
                ROUND(AVG(i.score)::numeric, 1)          AS avg_score,
                COUNT(*)                                  AS inspections
        FROM    inspections i
        JOIN    restaurants r ON i.camis = r.camis
        WHERE   i.score          IS NOT NULL
          AND   i.inspection_date IS NOT NULL
          AND   i.inspection_date BETWEEN :start_date AND :end_date
          {boro_filter}
        GROUP   BY 1
        ORDER   BY 1
        """,
        {"boroughs": boroughs or [], "start_date": start_date, "end_date": end_date},
    )


@st.cache_data(ttl=300)
def load_top_violations(boroughs: list, start_date: str, end_date: str, limit: int = 10):
    """Most frequently cited violation codes, with their descriptions."""
    boro_filter = "AND r.boro = ANY(:boroughs)" if boroughs else ""
    return run_query(
        f"""
        SELECT  iv.violation_code,
                COALESCE(v.violation_description, iv.violation_code) AS description,
                v.critical_flag,
                COUNT(*) AS citation_count
        FROM    inspection_violations iv
        JOIN    inspections i  ON iv.inspection_id  = i.id
        JOIN    restaurants  r ON i.camis            = r.camis
        LEFT JOIN violations v ON iv.violation_code  = v.violation_code
        WHERE   i.inspection_date BETWEEN :start_date AND :end_date
          {boro_filter}
        GROUP   BY iv.violation_code, v.violation_description, v.critical_flag
        ORDER   BY citation_count DESC
        LIMIT   :limit
        """,
        {"boroughs": boroughs or [], "start_date": start_date, "end_date": end_date, "limit": limit},
    )


@st.cache_data(ttl=300)
def load_score_by_cuisine(start_date: str, end_date: str, min_inspections: int = 20):
    """
    Average score per cuisine type.
    Cuisines with fewer than min_inspections are excluded to avoid misleading
    averages from small sample sizes.
    """
    return run_query(
        """
        SELECT  r.cuisine_description,
                ROUND(AVG(i.score)::numeric, 1) AS avg_score,
                COUNT(*)                         AS total_inspections
        FROM    inspections i
        JOIN    restaurants r ON i.camis = r.camis
        WHERE   i.score IS NOT NULL
          AND   r.cuisine_description IS NOT NULL
          AND   i.inspection_date BETWEEN :start_date AND :end_date
        GROUP   BY r.cuisine_description
        HAVING  COUNT(*) >= :min_inspections
        ORDER   BY avg_score DESC
        LIMIT   25
        """,
        {"start_date": start_date, "end_date": end_date, "min_inspections": min_inspections},
    )


@st.cache_data(ttl=300)
def load_map_data(boroughs: list, grades: list):
    """
    Restaurant locations for the map, one point per restaurant using its
    most recently graded inspection.  Limited to 8 000 rows for performance.
    """
    boro_filter  = "AND r.boro  = ANY(:boroughs)"  if boroughs else ""
    grade_filter = "AND latest.grade = ANY(:grades)" if grades   else ""

    return run_query(
        f"""
        WITH latest_grade AS (
            -- For each restaurant, find the most recent graded inspection
            SELECT DISTINCT ON (camis)
                   camis, grade, score, inspection_date
            FROM   inspections
            WHERE  grade IS NOT NULL AND grade != ''
            ORDER  BY camis, inspection_date DESC
        )
        SELECT  r.camis,
                r.dba,
                r.boro,
                r.cuisine_description,
                r.latitude,
                r.longitude,
                latest.grade,
                latest.score,
                latest.inspection_date
        FROM    restaurants  r
        JOIN    latest_grade latest ON r.camis = latest.camis
        WHERE   r.latitude  IS NOT NULL
          AND   r.longitude IS NOT NULL
          AND   r.latitude  BETWEEN  40.4 AND 41.0   -- NYC bounding box
          AND   r.longitude BETWEEN -74.3 AND -73.7
          {boro_filter}
          {grade_filter}
        LIMIT   8000
        """,
        {"boroughs": boroughs or [], "grades": grades or []},
    )


@st.cache_data(ttl=600)
def load_borough_list() -> list:
    """All distinct borough values for the sidebar filter."""
    df = run_query("SELECT DISTINCT boro FROM restaurants WHERE boro IS NOT NULL ORDER BY boro")
    return df["boro"].tolist()


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="NYC Restaurant Inspections",
    page_icon="🍕",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🍕 NYC Restaurant Inspection Dashboard")
st.caption(
    "Data source: NYC DOHMH Restaurant Inspection Results — "
    "[NYC Open Data](https://data.cityofnewyork.us/d/43nn-pn8j)"
)

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

st.sidebar.header("Filters")

# Borough multiselect
all_boroughs = load_borough_list()
selected_boroughs = st.sidebar.multiselect(
    "Borough",
    options=all_boroughs,
    default=[],          # empty = all boroughs
    placeholder="All boroughs",
)

# Date range — slider over years so it stays compact
st.sidebar.subheader("Inspection date range")
metrics_df = load_summary_metrics()
data_start = pd.to_datetime(metrics_df["earliest_date"].iloc[0]).date()
data_end   = pd.to_datetime(metrics_df["latest_date"].iloc[0]).date()

date_range = st.sidebar.date_input(
    "From / To",
    value=(data_start, data_end),
    min_value=data_start,
    max_value=data_end,
)

# Handle the case where the user has only picked one date (the widget
# returns a single date until the second one is chosen)
if isinstance(date_range, tuple) and len(date_range) == 2:
    filter_start, filter_end = date_range
else:
    filter_start, filter_end = data_start, data_end

start_str = str(filter_start)
end_str   = str(filter_end)

# Grade filter (for the map only)
st.sidebar.subheader("Map grade filter")
selected_grades = st.sidebar.multiselect(
    "Show grades",
    options=["A", "B", "C", "Z", "P", "N"],
    default=["A", "B", "C"],
)

st.sidebar.markdown("---")
st.sidebar.caption(
    "Tip: leave Borough empty to see all 5 boroughs.  "
    "Narrow the date range to speed up queries."
)

# ---------------------------------------------------------------------------
# Section 1 — Summary Metrics
# ---------------------------------------------------------------------------

st.subheader("Overview")

col1, col2, col3, col4 = st.columns(4)

total_restaurants = int(metrics_df["total_restaurants"].iloc[0] or 0)
total_inspections = int(metrics_df["total_inspections"].iloc[0] or 0)
pct_a             = metrics_df["pct_grade_a"].iloc[0]
date_range_label  = f"{data_start} → {data_end}"

col1.metric("Restaurants",  f"{total_restaurants:,}")
col2.metric("Inspections",  f"{total_inspections:,}")
col3.metric("Grade A rate", f"{pct_a}%" if pct_a is not None else "N/A")
col4.metric("Data range",   date_range_label)

st.markdown("---")

# ---------------------------------------------------------------------------
# Section 2 — Grade Distribution  +  Score by Borough
# (side by side)
# ---------------------------------------------------------------------------

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Grade Distribution")
    grade_df = load_grade_distribution(selected_boroughs, start_str, end_str)

    if grade_df.empty:
        st.info("No data for selected filters.")
    else:
        # Colour-code grades: A = green, B = yellow, C = orange, rest = grey
        grade_colors = {"A": "#2ecc71", "B": "#f1c40f", "C": "#e67e22",
                        "Z": "#95a5a6", "P": "#bdc3c7", "N": "#ecf0f1"}
        colors = [grade_colors.get(g, "#999") for g in grade_df["grade"]]

        fig_grade = px.pie(
            grade_df,
            names="grade",
            values="count",
            color="grade",
            color_discrete_map=grade_colors,
            hole=0.35,      # donut style
        )
        fig_grade.update_traces(textposition="inside", textinfo="percent+label")
        fig_grade.update_layout(showlegend=True, margin=dict(t=10, b=10))
        st.plotly_chart(fig_grade, use_container_width=True)

with col_right:
    st.subheader("Average Inspection Score by Borough")
    st.caption("Lower score = fewer / less serious violations = better performance")
    boro_df = load_score_by_borough(start_str, end_str)

    if boro_df.empty:
        st.info("No data for selected filters.")
    else:
        fig_boro = px.bar(
            boro_df,
            x="avg_score",
            y="boro",
            orientation="h",
            color="avg_score",
            color_continuous_scale="RdYlGn_r",   # red = high score = bad
            text="avg_score",
            labels={"avg_score": "Avg Score", "boro": "Borough"},
        )
        fig_boro.update_traces(texttemplate="%{text}", textposition="outside")
        fig_boro.update_layout(coloraxis_showscale=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig_boro, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Section 3 — Score Trend Over Time
# ---------------------------------------------------------------------------

st.subheader("Monthly Average Inspection Score Over Time")
st.caption(
    "Each point is the average score across all inspections in that month.  "
    "A downward trend means improving restaurant hygiene."
)

trend_df = load_score_trend(selected_boroughs, start_str, end_str)

if trend_df.empty:
    st.info("No trend data for selected filters.")
else:
    fig_trend = px.line(
        trend_df,
        x="month",
        y="avg_score",
        markers=True,
        labels={"month": "Month", "avg_score": "Avg Inspection Score"},
    )
    fig_trend.update_traces(line_color="#3498db", marker_size=5)
    fig_trend.update_layout(margin=dict(t=10, b=10))
    st.plotly_chart(fig_trend, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Section 4 — Top Violations  +  Score by Cuisine
# (side by side)
# ---------------------------------------------------------------------------

col_viol, col_cuisine = st.columns(2)

with col_viol:
    st.subheader("Top 10 Most Cited Violations")
    viol_df = load_top_violations(selected_boroughs, start_str, end_str)

    if viol_df.empty:
        st.info("No violation data for selected filters.")
    else:
        # Truncate long descriptions so labels fit on the chart
        viol_df["short_desc"] = viol_df["description"].str[:55] + "…"

        # Colour critical violations red, non-critical blue
        viol_df["color"] = viol_df["critical_flag"].map(
            {"Critical": "#e74c3c", "Not Critical": "#3498db"}
        ).fillna("#95a5a6")

        fig_viol = px.bar(
            viol_df,
            x="citation_count",
            y="short_desc",
            orientation="h",
            color="critical_flag",
            color_discrete_map={
                "Critical":     "#e74c3c",
                "Not Critical": "#3498db",
            },
            labels={"citation_count": "Citations", "short_desc": "Violation"},
        )
        fig_viol.update_layout(
            yaxis={"categoryorder": "total ascending"},
            legend_title="Severity",
            margin=dict(t=10, b=10),
        )
        st.plotly_chart(fig_viol, use_container_width=True)

with col_cuisine:
    st.subheader("Average Score by Cuisine Type")
    st.caption("Top 25 cuisines with ≥ 20 inspections in selected range")
    cuisine_df = load_score_by_cuisine(start_str, end_str)

    if cuisine_df.empty:
        st.info("No cuisine data for selected filters.")
    else:
        fig_cuisine = px.bar(
            cuisine_df.sort_values("avg_score"),
            x="avg_score",
            y="cuisine_description",
            orientation="h",
            color="avg_score",
            color_continuous_scale="RdYlGn_r",
            text="avg_score",
            labels={"avg_score": "Avg Score", "cuisine_description": "Cuisine"},
        )
        fig_cuisine.update_traces(texttemplate="%{text}", textposition="outside")
        fig_cuisine.update_layout(
            coloraxis_showscale=False,
            yaxis={"categoryorder": "total ascending"},
            margin=dict(t=10, b=10),
        )
        st.plotly_chart(fig_cuisine, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Section 5 — Restaurant Map
# ---------------------------------------------------------------------------

st.subheader("Restaurant Map — Coloured by Most Recent Grade")
st.caption(
    "Each dot is one restaurant, coloured by its most recent inspection grade.  "
    "Use the Grade filter in the sidebar to show/hide grade categories.  "
    "Zoom and pan to explore neighbourhoods."
)

map_df = load_map_data(selected_boroughs, selected_grades)

if map_df.empty:
    st.info("No map data for selected filters.")
else:
    grade_color_map = {
        "A": "#2ecc71",
        "B": "#f1c40f",
        "C": "#e67e22",
        "Z": "#e74c3c",
        "P": "#9b59b6",
        "N": "#95a5a6",
    }

    fig_map = px.scatter_mapbox(
        map_df,
        lat="latitude",
        lon="longitude",
        color="grade",
        color_discrete_map=grade_color_map,
        hover_name="dba",
        hover_data={
            "boro":                 True,
            "cuisine_description":  True,
            "grade":                True,
            "score":                True,
            "inspection_date":      True,
            "latitude":             False,
            "longitude":            False,
        },
        mapbox_style="open-street-map",   # no Mapbox token required
        zoom=10,
        center={"lat": 40.7128, "lon": -74.0060},   # NYC centre
        opacity=0.7,
    )
    fig_map.update_layout(
        height=550,
        margin=dict(t=10, b=10),
        legend_title="Grade",
    )
    st.plotly_chart(fig_map, use_container_width=True)

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.markdown("---")
st.caption(
    "Built by David Arnold, Erica Chen, Zarko Dimitrov, and Arnav Karnati — "
    "NYC Restaurant Inspection Pipeline · Data Engineering Term Project"
)
