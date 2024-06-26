import pandas as pd
import geopandas as gpd
import plotly.express as px

def get_data():
    # Read property value data from CSV
    property_value_data = pd.read_csv("Current_Year_Property_Assessments__Parcel__20231018.csv")

    # Calculate mean property value by community
    mean_property_value_by_community = property_value_data.groupby("COMM_NAME")["ASSESSED_VALUE"].mean().reset_index()
    mean_property_value_by_community = mean_property_value_by_community[mean_property_value_by_community["ASSESSED_VALUE"] <= 10000000]

    # Read community boundaries from GeoJSON
    community_boundaries = gpd.read_file("Community Boundaries 2011_20231018.geojson")

    # Merge property value data with community boundaries
    merged_data_all = community_boundaries.merge(
        mean_property_value_by_community,
        left_on="name",
        right_on="COMM_NAME",
        how="left"
    )

    # Define map center and zoom level
    map_center = {"lat": 51.0247, "lon": -114.0719}
    zoom_level = 9.8

    # Create choropleth map
    choropleth_map_all = px.choropleth_mapbox(
        merged_data_all,
        geojson=community_boundaries.geometry,
        locations=merged_data_all.index,
        color="ASSESSED_VALUE",
        color_continuous_scale=px.colors.sequential.YlOrRd,
        hover_name="COMM_NAME",
        title="Calgary Mean Property Value by Community",
        labels={"ASSESSED_VALUE": "Mean Property Value"},
        mapbox_style="carto-positron",
        center=map_center,
        zoom=zoom_level,
        opacity=0.5
    )

    # Update layout
    choropleth_map_all.update_layout(title_font=dict(family="Times New Roman", size=28))

    return choropleth_map_all
