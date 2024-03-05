import pandas as pd
import geopandas as gpd
import plotly.express as px

def get_data():
    development_permits_data = pd.read_csv("Development_Permits_20231020.csv")
    development_permits_data = development_permits_data.rename(columns={"COMM_NAME": "CommunityName"})

    agg_data = development_permits_data.groupby("CommunityName", as_index=False)["CommunityCode"].count()
    agg_data.rename(columns={"CommunityCode": "Development Permits"}, inplace=True)

    community_boundaries = gpd.read_file("Community Boundaries 2011_20231018.geojson")

    merged_data = community_boundaries.merge(agg_data, left_on="name", right_on="CommunityName", how="left")

    fig = px.choropleth(
        merged_data,
        geojson=community_boundaries.geometry,
        locations=merged_data.index,
        color="Development Permits",  
        color_continuous_scale=px.colors.sequential.Plasma,  
        hover_name="CommunityName",
        title="Calgary Development Permits by Community",
    )

    fig.update_geos(center_lon=-114.0719, center_lat=51.0447, projection_scale=450)

    return fig

get_data()
