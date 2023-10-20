import pandas as pd
import plotly.express as px

mapbox_access_token = "pk.eyJ1Ijoia2FhamJvbGFuZCIsImEiOiJjbG5kejg0emIwOGRyMmxsZW9vaXYyMGswIn0.Rhnj7A5aOZh0JBebF4WaFQ"


# List of Used City of Calgary Datasets:
# Building Permits Dataset
# Census by Community 2019 (Updated: February 1, 2023)

# API URLs for building permits and vacant apartments data
building_permits_api_url = "https://data.calgary.ca/resource/c2es-76ed.json"
vacant_apartments_api_url = "https://data.calgary.ca/resource/rkfr-buzb.json"
# Fetch data from the APIs
df_building_permits = pd.read_json(building_permits_api_url)
df_vacant_apartments = pd.read_json(vacant_apartments_api_url)

# Rename the 'name' column in df_vacant_apartments to match the 'communityname' column in df_building_permits
df_vacant_apartments = df_vacant_apartments.rename(columns={"name": "communityname"})

# Merge the dataframes on the 'communityname' column
df_merged = df_building_permits.merge(
    df_vacant_apartments, on="communityname", how="left"
)

# Create a heat map using Plotly Express
fig = px.scatter_mapbox(
    df_merged,
    lat="latitude",
    lon="longitude",
    color="communityname",
    size_max=15,
    hover_name="communityname",  # Show communityname in hover information
    hover_data={
        "apt_vacant": True,
        "cnv_vacant": True,
        "dup_vacant": True,
        "mfh_vacant": True,
        "mul_vacant": True,
        "twn_vacant": True,
        "oth_vacant": True,
        "sf_vacant": True,
    },
    center=dict(lat=51.0447, lon=-114.0719),  # Calgary coordinates
    zoom=10,
    mapbox_style="mapbox://styles/mapbox/streets-v11",  # You can change the map style
    opacity=0.6,
    title="Calgary Building Permits Heat Map with Vacant Homes Availability",
)

# Update Mapbox layout
fig.update_layout(
    mapbox=dict(
        accesstoken=mapbox_access_token,
    )
)

# Show the interactive plot directly in a web browser (Firefox or Chrome)
fig.show()

# Write the plot into an HTML file which can be copied and opened at a desired time.
# fig.write_html("calgary_building_permits_map.html")
