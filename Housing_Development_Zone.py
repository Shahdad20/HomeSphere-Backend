import pandas as pd
import plotly.express as px
import geopandas as gpd

def get_data():
    # Read population data from CSV file
    population_data = pd.read_csv("Census_by_Community_2017_20231020.csv")
    
    # Aggregate population data by community
    agg_data = population_data.groupby("NAME", as_index=False)["RES_CNT"].sum()
    agg_data.rename(columns={"RES_CNT": "Population"}, inplace=True)

    # Read community boundaries from GeoJSON file
    community_boundaries = gpd.read_file("Community Boundaries 2011_20231018.geojson")

    # Merge population data with community boundaries
    merged_data = community_boundaries.merge(
        agg_data, left_on="name", right_on="NAME", how="left"
    )

    # Create choropleth map using Plotly Express
    fig = px.choropleth(
        merged_data,
        geojson=community_boundaries.geometry,
        locations=merged_data.index,
        color="Population",  
        color_continuous_scale=px.colors.sequential.Plasma,  
        hover_name="NAME",
        title="Calgary Population by Community",
    )
    
    # Update geos parameters for map layout
    fig.update_geos(center_lon=-114.0719, center_lat=51.0447, projection_scale=450)
    
    return fig
    
