"""
streamlit_app.py
----------------
Interactive dashboard for NYC Restaurant Inspection data.

Connects directly to Supabase PostgreSQL — no local database, no cleaning.
Assumes data is already cleaned and loaded into the 4-table schema by the
daily GitHub Actions pipeline (fetch_new → clean_data → port_data).

Credential loading order:
    1. st.secrets  (Streamlit Community Cloud — add DATABASE_URL in the
                    app's Secrets settings as a TOML key)
    2. os.environ  (local dev — set DATABASE_URL in your shell or .env file)

All queries are written defensively:
    - COALESCE / NULLIF prevent division-by-zero and null propagation
    - IS NOT NULL / != '' guards keep nulls out of aggregations
    - HAVING COUNT(*) >= N excludes statistically meaningless small groups
    - Every query returns an empty DataFrame gracefully; the UI shows an
      info message rather than crashing

Deploy to Streamlit Community Cloud:
    1. Push this file (and requirements.txt) to GitHub
    2. Go to share.streamlit.io → New app → select this repo / file
    3. Under Advanced → Secrets, add:
            DATABASE_URL = "postgresql://postgres.<ref>:<password>@<host>:5432/postgres"
       (copy the Session Mode connection string from Supabase → Connect)
    4. Click Deploy
"""

import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load credentials
# ---------------------------------------------------------------------------

load_dotenv()  # no-op on Streamlit Cloud where st.secrets is used instead


def _get_db_url() -> str:
    """
    Return the database connection URL, trying st.secrets first then
    environment variables. Raises a clear error if neither is set.
    """
    # Streamlit Cloud stores secrets in st.secrets (TOML format)
    try:
        return st.secrets["DATABASE_URL"]
    except (KeyError, FileNotFoundError):
        pass

    url = os.environ.get("DATABASE_URL", "")
    if not url:
        st.error(
            "DATABASE_URL is not set. "
            "Add it to Streamlit Secrets (Streamlit Cloud) or your .env file (local)."
        )
        st.stop()

    return url


# ---------------------------------------------------------------------------
# Database engine — cached for the lifetime of the Streamlit session
# ---------------------------------------------------------------------------

@st.cache_resource
def get_engine():
    """
    Build a SQLAlchemy engine connected to Supabase PostgreSQL.

    psycopg3 (the driver in requirements.txt) requires the dialect prefix
    'postgresql+psycopg://' instead of the plain 'postgresql://' that
    Supabase connection strings use, so we normalise it here.
    """
    raw_url = _get_db_url()
    db_url  = raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return create_engine(db_url, pool_pre_ping=True, pool_size=3, max_overflow=2)


def query(sql: str, params: dict = None) -> pd.DataFrame:
    """
    Run a SQL query against Supabase and return a DataFrame.
    Returns an empty DataFrame (never raises) on connection or query errors
    so individual chart sections degrade gracefully.
    """
    try:
        with get_engine().connect() as conn:
            return pd.read_sql(text(sql), conn, params=params or {})
    except Exception as e:
        st.warning(f"Query failed: {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Cached data loaders — TTL 10 min so the page stays snappy on re-runs
# ---------------------------------------------------------------------------

@st.cache_data(ttl=600)
def load_summary():
    """Scalar metrics shown at the top of the page."""
    return query("""
        SELECT
            (SELECT COUNT(DISTINCT camis)
             FROM   restaurants)                                      AS total_restaurants,

            (SELECT COUNT(*)
             FROM   inspections)                                      AS total_inspections,

            (SELECT MIN(inspection_date)
             FROM   inspections
             WHERE  inspection_date IS NOT NULL)                      AS earliest_date,

            (SELECT MAX(inspection_date)
             FROM   inspections
             WHERE  inspection_date IS NOT NULL)                      AS latest_date,

            -- % of graded inspections that earned an A
            (SELECT ROUND(
                 100.0
                 * COUNT(*) FILTER (WHERE UPPER(TRIM(grade)) = 'A')
                 / NULLIF(COUNT(*) FILTER (WHERE grade IS NOT NULL
                                           AND   TRIM(grade) != ''), 0),
                 1)
             FROM inspections)                                        AS pct_grade_a
    """)


@st.cache_data(ttl=600)
def load_borough_list() -> list:
    df = query("""
        SELECT DISTINCT TRIM(boro) AS boro
        FROM   restaurants
        WHERE  boro IS NOT NULL AND TRIM(boro) != ''
        ORDER  BY boro
    """)
    return df["boro"].tolist() if not df.empty else []


@st.cache_data(ttl=600)
def load_grade_distribution(boroughs: tuple, start: str, end: str) -> pd.DataFrame:
    boro_clause = "AND TRIM(r.boro) = ANY(:boroughs)" if boroughs else ""
    return query(f"""
        SELECT UPPER(TRIM(i.grade)) AS grade,
               COUNT(*)             AS count
        FROM   inspections  i
        JOIN   restaurants  r ON r.camis = i.camis
        WHERE  i.grade          IS NOT NULL
          AND  TRIM(i.grade)    != ''
          AND  UPPER(TRIM(i.grade)) IN ('A','B','C','Z','P','N')
          AND  i.inspection_date IS NOT NULL
          AND  i.inspection_date BETWEEN :start AND :end
          {boro_clause}
        GROUP  BY 1
        ORDER  BY count DESC
    """, {"boroughs": list(boroughs), "start": start, "end": end})


@st.cache_data(ttl=600)
def load_score_by_borough(start: str, end: str) -> pd.DataFrame:
    return query("""
        SELECT TRIM(r.boro)                          AS boro,
               ROUND(AVG(i.score::numeric), 1)       AS avg_score,
               COUNT(*)                              AS inspections
        FROM   inspections i
        JOIN   restaurants r ON r.camis = i.camis
        WHERE  i.score          IS NOT NULL
          AND  r.boro           IS NOT NULL
          AND  TRIM(r.boro)     != ''
          AND  i.inspection_date BETWEEN :start AND :end
        GROUP  BY TRIM(r.boro)
        ORDER  BY avg_score ASC
    """, {"start": start, "end": end})


@st.cache_data(ttl=600)
def load_score_trend(boroughs: tuple, start: str, end: str) -> pd.DataFrame:
    boro_clause = "AND TRIM(r.boro) = ANY(:boroughs)" if boroughs else ""
    return query(f"""
        SELECT DATE_TRUNC('month', i.inspection_date)   AS month,
               ROUND(AVG(i.score::numeric), 1)           AS avg_score,
               COUNT(*)                                  AS inspections
        FROM   inspections i
        JOIN   restaurants r ON r.camis = i.camis
        WHERE  i.score          IS NOT NULL
          AND  i.inspection_date IS NOT NULL
          AND  i.inspection_date BETWEEN :start AND :end
          {boro_clause}
        GROUP  BY 1
        ORDER  BY 1
    """, {"boroughs": list(boroughs), "start": start, "end": end})


@st.cache_data(ttl=600)
def load_top_violations(boroughs: tuple, start: str, end: str) -> pd.DataFrame:
    boro_clause = "AND TRIM(r.boro) = ANY(:boroughs)" if boroughs else ""
    return query(f"""
        SELECT iv.violation_code,
               COALESCE(NULLIF(TRIM(v.violation_description),''),
                        iv.violation_code)               AS description,
               COALESCE(NULLIF(TRIM(v.critical_flag),''),
                        'Unknown')                       AS critical_flag,
               COUNT(*)                                  AS citations
        FROM   inspection_violations iv
        JOIN   inspections   i  ON i.id           = iv.inspection_id
        JOIN   restaurants   r  ON r.camis         = i.camis
        LEFT JOIN violations v  ON v.violation_code = iv.violation_code
        WHERE  iv.violation_code IS NOT NULL
          AND  i.inspection_date  IS NOT NULL
          AND  i.inspection_date  BETWEEN :start AND :end
          {boro_clause}
        GROUP  BY iv.violation_code, v.violation_description, v.critical_flag
        ORDER  BY citations DESC
        LIMIT  10
    """, {"boroughs": list(boroughs), "start": start, "end": end})


@st.cache_data(ttl=600)
def load_score_by_cuisine(start: str, end: str) -> pd.DataFrame:
    return query("""
        SELECT TRIM(r.cuisine_description)          AS cuisine,
               ROUND(AVG(i.score::numeric), 1)      AS avg_score,
               COUNT(*)                             AS inspections
        FROM   inspections i
        JOIN   restaurants r ON r.camis = i.camis
        WHERE  i.score                IS NOT NULL
          AND  r.cuisine_description  IS NOT NULL
          AND  TRIM(r.cuisine_description) != ''
          AND  i.inspection_date BETWEEN :start AND :end
        GROUP  BY TRIM(r.cuisine_description)
        HAVING COUNT(*) >= 20          -- exclude cuisines with tiny sample sizes
        ORDER  BY avg_score DESC
        LIMIT  25
    """, {"start": start, "end": end})


@st.cache_data(ttl=600)
def load_map_data(boroughs: tuple, grades: tuple) -> pd.DataFrame:
    """
    Most-recent graded inspection per restaurant, for the map.
    Filters to NYC's bounding box to drop clearly bad coordinates.
    Limited to 8 000 rows so the map renders quickly.
    """
    boro_clause  = "AND TRIM(r.boro)   = ANY(:boroughs)" if boroughs else ""
    grade_clause = "AND UPPER(TRIM(lg.grade)) = ANY(:grades)" if grades else ""
    return query(f"""
        WITH latest_grade AS (
            -- One row per restaurant: the most recent inspection that has a grade
            SELECT DISTINCT ON (camis)
                   camis,
                   UPPER(TRIM(grade))  AS grade,
                   score,
                   inspection_date
            FROM   inspections
            WHERE  grade IS NOT NULL
              AND  TRIM(grade) != ''
              AND  UPPER(TRIM(grade)) IN ('A','B','C','Z','P','N')
            ORDER  BY camis, inspection_date DESC NULLS LAST
        )
        SELECT r.camis,
               COALESCE(NULLIF(TRIM(r.dba),''), 'Unknown')               AS dba,
               COALESCE(NULLIF(TRIM(r.boro),''), 'Unknown')              AS boro,
               COALESCE(NULLIF(TRIM(r.cuisine_description),''), 'Other') AS cuisine,
               r.latitude,
               r.longitude,
               lg.grade,
               lg.score,
               lg.inspection_date
        FROM   restaurants  r
        JOIN   latest_grade lg ON lg.camis = r.camis
        WHERE  r.latitude   IS NOT NULL
          AND  r.longitude  IS NOT NULL
          AND  r.latitude   BETWEEN  40.4  AND  41.0
          AND  r.longitude  BETWEEN -74.3  AND -73.7
          {boro_clause}
          {grade_clause}
        LIMIT  8000
    """, {"boroughs": list(boroughs), "grades": list(grades)})


# ---------------------------------------------------------------------------
# Page layout
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title = "NYC Restaurant Inspections",
    page_icon  = "🍕",
    layout     = "wide",
    initial_sidebar_state = "expanded",
)

st.title("🍕 NYC Restaurant Inspection Dashboard")
st.caption(
    "Source: NYC DOHMH Restaurant Inspection Results — "
    "[NYC Open Data](https://data.cityofnewyork.us/d/43nn-pn8j)"
)

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

st.sidebar.header("Filters")

all_boroughs     = load_borough_list()
selected_boroughs = st.sidebar.multiselect(
    "Borough",
    options     = all_boroughs,
    default     = [],
    placeholder = "All boroughs",
)

summary_df = load_summary()

# Safe date bounds — fall back to sensible defaults if the DB is empty
try:
    data_start = pd.to_datetime(summary_df["earliest_date"].iloc[0]).date()
    data_end   = pd.to_datetime(summary_df["latest_date"].iloc[0]).date()
except Exception:
    from datetime import date
    data_start = date(2015, 1, 1)
    data_end   = date.today()

st.sidebar.subheader("Inspection date range")
date_range = st.sidebar.date_input(
    "From / To",
    value     = (data_start, data_end),
    min_value = data_start,
    max_value = data_end,
)

# The widget returns a single date while the user is picking the second one
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    filter_start, filter_end = date_range
else:
    filter_start, filter_end = data_start, data_end

start_str = str(filter_start)
end_str   = str(filter_end)

st.sidebar.subheader("Map grade filter")
selected_grades = st.sidebar.multiselect(
    "Show grades",
    options = ["A", "B", "C", "Z", "P", "N"],
    default = ["A", "B", "C"],
)

st.sidebar.markdown("---")
st.sidebar.caption("Leave Borough empty to include all 5 boroughs.")

# Convert to tuples so they're hashable for @st.cache_data
boro_tuple  = tuple(selected_boroughs)
grade_tuple = tuple(selected_grades)

# ---------------------------------------------------------------------------
# Section 1 — Summary metrics
# ---------------------------------------------------------------------------

st.subheader("Overview")
c1, c2, c3, c4 = st.columns(4)

try:
    total_r = int(summary_df["total_restaurants"].iloc[0] or 0)
    total_i = int(summary_df["total_inspections"].iloc[0] or 0)
    pct_a   = summary_df["pct_grade_a"].iloc[0]
    dr_lbl  = f"{data_start} → {data_end}"
except Exception:
    total_r = total_i = 0
    pct_a   = None
    dr_lbl  = "N/A"

c1.metric("Restaurants",  f"{total_r:,}")
c2.metric("Inspections",  f"{total_i:,}")
c3.metric("Grade A rate", f"{pct_a}%" if pct_a is not None else "N/A")
c4.metric("Data range",   dr_lbl)

st.markdown("---")

# ---------------------------------------------------------------------------
# Section 2 — Grade distribution  +  Score by borough
# ---------------------------------------------------------------------------

col_l, col_r = st.columns(2)

with col_l:
    st.subheader("Grade Distribution")
    grade_df = load_grade_distribution(boro_tuple, start_str, end_str)

    if grade_df.empty:
        st.info("No grade data for selected filters.")
    else:
        grade_colors = {
            "A": "#2ecc71", "B": "#f1c40f", "C": "#e67e22",
            "Z": "#e74c3c", "P": "#9b59b6", "N": "#95a5a6",
        }
        fig = px.pie(
            grade_df, names="grade", values="count",
            color="grade", color_discrete_map=grade_colors, hole=0.35,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

with col_r:
    st.subheader("Average Score by Borough")
    st.caption("Lower score = fewer / less serious violations = better")
    boro_df = load_score_by_borough(start_str, end_str)

    if boro_df.empty:
        st.info("No data for selected filters.")
    else:
        fig = px.bar(
            boro_df, x="avg_score", y="boro", orientation="h",
            color="avg_score", color_continuous_scale="RdYlGn_r",
            text="avg_score",
            labels={"avg_score": "Avg Score", "boro": "Borough"},
        )
        fig.update_traces(texttemplate="%{text}", textposition="outside")
        fig.update_layout(coloraxis_showscale=False, margin=dict(t=10, b=10))
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Section 3 — Score trend over time
# ---------------------------------------------------------------------------

st.subheader("Monthly Average Score Over Time")
st.caption("A downward trend means restaurants are improving.")

trend_df = load_score_trend(boro_tuple, start_str, end_str)

if trend_df.empty:
    st.info("No trend data for selected filters.")
else:
    fig = px.line(
        trend_df, x="month", y="avg_score", markers=True,
        labels={"month": "Month", "avg_score": "Avg Score"},
    )
    fig.update_traces(line_color="#3498db", marker_size=5)
    fig.update_layout(margin=dict(t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Section 4 — Top violations  +  Score by cuisine
# ---------------------------------------------------------------------------

col_v, col_c = st.columns(2)

with col_v:
    st.subheader("Top 10 Most Cited Violations")
    viol_df = load_top_violations(boro_tuple, start_str, end_str)

    if viol_df.empty:
        st.info("No violation data for selected filters.")
    else:
        viol_df["short_desc"] = viol_df["description"].str[:60] + "…"
        fig = px.bar(
            viol_df, x="citations", y="short_desc", orientation="h",
            color="critical_flag",
            color_discrete_map={"Critical": "#e74c3c", "Not Critical": "#3498db",
                                 "Unknown": "#95a5a6"},
            labels={"citations": "Citations", "short_desc": "Violation"},
        )
        fig.update_layout(
            yaxis={"categoryorder": "total ascending"},
            legend_title="Severity",
            margin=dict(t=10, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)

with col_c:
    st.subheader("Average Score by Cuisine")
    st.caption("Top 25 cuisines with ≥ 20 inspections in selected range")
    cuisine_df = load_score_by_cuisine(start_str, end_str)

    if cuisine_df.empty:
        st.info("No cuisine data for selected filters.")
    else:
        fig = px.bar(
            cuisine_df.sort_values("avg_score"),
            x="avg_score", y="cuisine", orientation="h",
            color="avg_score", color_continuous_scale="RdYlGn_r",
            text="avg_score",
            labels={"avg_score": "Avg Score", "cuisine": "Cuisine"},
        )
        fig.update_traces(texttemplate="%{text}", textposition="outside")
        fig.update_layout(
            coloraxis_showscale=False,
            yaxis={"categoryorder": "total ascending"},
            margin=dict(t=10, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# Section 5 — Restaurant map
# ---------------------------------------------------------------------------

st.subheader("Restaurant Map — Most Recent Grade")
st.caption(
    "Each dot is one restaurant coloured by its most recent inspection grade. "
    "Use the Grade filter in the sidebar to show/hide categories."
)

map_df = load_map_data(boro_tuple, grade_tuple)

if map_df.empty:
    st.info("No map data for selected filters.")
else:
    grade_color_map = {
        "A": "#2ecc71", "B": "#f1c40f", "C": "#e67e22",
        "Z": "#e74c3c", "P": "#9b59b6", "N": "#95a5a6",
    }
    fig = px.scatter_mapbox(
        map_df,
        lat="latitude", lon="longitude",
        color="grade",
        color_discrete_map=grade_color_map,
        hover_name="dba",
        hover_data={
            "boro": True, "cuisine": True, "grade": True,
            "score": True, "inspection_date": True,
            "latitude": False, "longitude": False,
        },
        mapbox_style="open-street-map",
        zoom=10,
        center={"lat": 40.7128, "lon": -74.0060},
        opacity=0.7,
    )
    fig.update_layout(height=550, margin=dict(t=10, b=10), legend_title="Grade")
    st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.markdown("---")
st.caption(
    "Built by David Arnold, Erica Chen, Zarko Dimitrov, and Arnav Karnati · "
    "NYC Restaurant Inspection Pipeline · Data Engineering Term Project"
)
