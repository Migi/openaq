import streamlit as st
import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
import io
import datetime
import pydeck
import dateutil

st.set_page_config(layout="wide")

# Connect to the RDS database
conn = mysql.connector.connect(
    host = "rds-openaq-michiel.cnyif0bzxpjl.eu-west-1.rds.amazonaws.com",
    user = st.secrets["DB_USERNAME"],
    password = st.secrets["DB_PASSWORD"],
    database = "db_openaq_michiel"
)

# Perform a query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query, index_col=None):
    return pd.read_sql_query(query, conn, index_col=index_col)

# Helper function to display a pie chart of a pandas dataframe.
# The dataframe must have a column named "count", which is used for the slice sizes
def make_pie_chart(df):
    indices = df["count"].tolist()

    # set up Matplotlib figure
    fig, ax = plt.subplots(figsize=(4.3, 3))
    ax.pie(indices, labels=(df.index), wedgeprops = { 'linewidth' : 5, 'edgecolor' : 'white' })

    # display a white circle in the middle of the pie chart
    p = plt.gcf()
    p.gca().add_artist(plt.Circle( (0,0), 0.5, color='white'))
    
    # save it to an image then display the image (allows streamlit to rescale the chart)
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    st.image(buf)

# Download the data
df = run_query("SELECT id, date_utc, parameter, value, unit, latitude, longitude, location, city FROM measurements WHERE country = 'BE'", index_col="id")
df["parameter"] = df["parameter"].map(lambda x: x.upper())

# Get the unique values for "city"
cities_df = df.groupby("city").agg(count = ("city", "count"))
cities = ["All"]
cities.extend(cities_df.index.values)

# Get the unique values for "parameter"
parameters_df = df.groupby("parameter").agg(count = ("parameter", "count"))
parameters = list(parameters_df.index.values)

# Create Streamlit Side bar

st.sidebar.header("Filters")
selected_timeframe = st.sidebar.selectbox("Use data from the last:", ["3 hours", "24 hours", "2 days", "3 days", "1 week", "1 month"], index=4)
selected_parameter = st.sidebar.selectbox("Air quality parameter:", parameters, index=4)
selected_city = st.sidebar.selectbox("Province:", cities, index=0)

# Filter a pandas dataframe by the selected sidebar filters
def filter_df_by_sidebar(df, use_timeframe=True, use_parameter=True, use_city=True):
    if use_timeframe:
        timeframe_val, timeframe_unit = selected_timeframe.split(" ")
        if timeframe_unit.startswith("hour"):
            delta = datetime.timedelta(hours=int(timeframe_val))
        elif timeframe_unit.startswith("day"):
            delta = datetime.timedelta(days=int(timeframe_val))
        elif timeframe_unit.startswith("week"):
            delta = datetime.timedelta(weeks=int(timeframe_val))
        elif timeframe_unit.startswith("month"):
            delta = dateutil.relativedelta.relativedelta(months=int(timeframe_val))
        else:
            assert False, f"Unknown timeframe unit {timeframe_unit}!"
        df = df[df["date_utc"] >= datetime.datetime.utcnow() - delta]
    if use_parameter:
        df = df[df["parameter"] == selected_parameter]
    if use_city and selected_city != "All":
        df = df[df["city"] == selected_city]
    return df

# Create df filtered by all sidebar filters
df_filtered = filter_df_by_sidebar(df)

# Write how many measurements we have left
st.sidebar.write(f"Number of measurements: {len(df_filtered)}")

# Create tabs
tab_live_map, tab_map_num_measurements, tab_time_series, tab_stats = st.tabs(["Live Map", "Number of measurements", "Time series", "Other"])

# Helper function to get a green-red color gradient for a certain value in a range
def get_value_color(min_value, max_value, value):
    # definition of gradient
    color_gradient = [
        (0., [32, 199, 57]),
        (0.5, [209, 232, 132]),
        (1.0, [230, 130, 12])
    ]

    # find color in gradient
    if value <= min_value:
        return color_gradient[0][1]
    elif value >= max_value:
        return color_gradient[-1][1]
    else:
        x = (value - min_value) / (max_value - min_value)
        for i in range(len(color_gradient)-1):
            if x <= color_gradient[i+1][0]:
                y = (x - color_gradient[i][0])/(color_gradient[i+1][0] - color_gradient[i][0])
                return [(1-y) * color_gradient[i][1][j] + y * color_gradient[i+1][1][j] for j in range(3)]

# Live map tab
with tab_live_map:
    # Ranges for the colors for each parameter
    parameter_range = {
        "CO": (0, 500),
        "NO2": (0, 50),
        "O3": (0, 200),
        "PM10": (0, 80),
        "PM25": (0, 30),
        "SO2": (0, 10)
    }

    # Filter on locations that are not null
    location_df = df_filtered.loc[df["longitude"].notnull()]
    
    if location_df.empty:
        st.warning("No measurements for the selected timeframe and parameter.")
    else:
        # Aggregate by measurement location
        location_df = location_df.groupby("location").agg(
            location = ("location", "min"),
            count = ("value", "count"),
            avg = ("value", "mean"),
            longitude = ("longitude", "mean"),
            latitude = ("latitude", "mean"),
            unit = ("unit", "min")
        )
        # Format the average of measurements to not have too many significant digits
        location_df["avg_formatted"] = location_df["avg"].apply(lambda x: str(round(x, 2)))
        for index, row in location_df.iterrows():
            color = get_value_color(parameter_range[selected_parameter][0], parameter_range[selected_parameter][1], row["avg"])
            location_df.at[index, "color_r"] = color[0]
            location_df.at[index, "color_g"] = color[1]
            location_df.at[index, "color_b"] = color[2]

        # Create PyDeck layer
        layers = [
            pydeck.Layer(
                "ScatterplotLayer",
                location_df,
                get_position=["longitude", "latitude"],
                opacity=0.7,
                stroked=False,
                filled=True,
                radius_min_pixels=10,
                radius_max_pixels=10,
                line_width_min_pixels=1,
                get_fill_color = "[color_r, color_g, color_b]",
                pickable=True,
                auto_highlight=True
            )
        ]

        # Plot the map
        st.pydeck_chart(
            pydeck.Deck(
                map_style=None,
                initial_view_state={
                    "latitude": (location_df["latitude"].min() + location_df["latitude"].max())*0.5 - 0.1,
                    "longitude": (location_df["longitude"].min() + location_df["longitude"].max())*0.5,
                    "zoom": 7.3,
                    "pitch": 30,
                    "height": 750
                },
                layers=layers,
                tooltip={
                    'html': '<b>Location ID:</b> {location}<br><b>Value:</b> {avg_formatted} {unit}<br><b>N° measurements:</b> {count}',
                    'style': {
                        'color': 'white'
                    }
                }
            ),
            use_container_width=True
        )

# Number of measurements tab
with tab_map_num_measurements:
    aggregate_parameters = st.checkbox("Aggregate all parameters (instead of only the one selected in the sidebar)", value=True)
    if aggregate_parameters:
        df_used = filter_df_by_sidebar(df, use_timeframe=True, use_parameter=False, use_city=True)
    else:
        df_used = df_filtered

    # take only measurements with coordinates
    df_used = df_used.loc[df["longitude"].notnull()]
    if df_used.empty:
        st.warning("No measurements for the selected timeframe and parameter.")
    else:
        # Create PyDeck layer
        layers = [
            pydeck.Layer(
                "HexagonLayer",
                df_used[["longitude", "latitude"]],
                get_position=["longitude", "latitude"],
                radius=2000,
                pickable=True,
                auto_highlight=True,
                extruded=True,
                elevation_scale=50
            )
        ]

        # Plot the map
        st.pydeck_chart(
            pydeck.Deck(
                map_style=None,
                initial_view_state={
                    "latitude": (df_used["latitude"].min() + df_used["latitude"].max())*0.5 - 0.1,
                    "longitude": (df_used["longitude"].min() + df_used["longitude"].max())*0.5,
                    "zoom": 7.3,
                    "pitch": 30,
                    "height": 750
                },
                layers=layers,
            ),
            use_container_width=True
        )

# Time series tab
with tab_time_series:
    if df_filtered.empty:
        st.warning("No measurements for the selected timeframe and parameter.")
    else:
        # Group by hour and date
        time_df = df_filtered.groupby(pd.Grouper(key="date_utc", freq="1h")).agg(
            count = ("parameter", "count"),
            value_sum = ("value", "sum"),
            avg = ("value", "mean")
        )
        # Sum the last 3 hours of measurements
        rolling_time_df = time_df.loc[time_df["count"] > 0].rolling("3h", min_periods=1).agg({
            "count": "sum",
            "value_sum": "sum"
        })
        # Calculate the average
        rolling_time_df["avg"] = rolling_time_df["value_sum"] / rolling_time_df["count"]

        # Plot the air quality chart
        st.line_chart(rolling_time_df, y="avg")
        st.bar_chart(time_df["count"])

        # Group by hour of the day
        hour_of_day_df = df.groupby(df_filtered["date_utc"].map(lambda x: str(x.hour).zfill(2) + ":00")).agg(
            count = ("parameter", "count")
        )

        col1, col2 = st.columns(2)
        with col1:
            make_pie_chart(hour_of_day_df)
        with col2:
            hour_of_day_df

# Other tab
with tab_stats:
    col1, col1b, col2, col2b = st.columns(4)

    with col1:
        st.header("Parameters")
        st.write("Total number of measurements per parameter:")
        make_pie_chart(parameters_df)

    with col1b:
        st.header("")
        st.write(parameters_df)

    with col2:
        st.header("Provinces")
        st.write("Total number of measurements per province:")
        make_pie_chart(cities_df)

    with col2b:
        st.header("")
        st.write(cities_df)