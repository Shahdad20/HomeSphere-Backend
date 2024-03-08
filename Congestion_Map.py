import pandas as pd
import plotly.express as px
import geopandas as gpd

def get_data():
    property_value_data = pd.read_csv("Current_Year_Property_Assessments__Parcel__20231018.csv")
    
    building_permits_data = pd.read_csv("Building_Permits_20231018.csv")
    
    population_data = pd.read_csv("Census_by_Community_2017_20231020.csv")
    
    property_value_data = property_value_data.rename(columns={"COMM_NAME": "NAME"})
    
    agg_data = population_data.groupby("NAME", as_index=False)["RES_CNT"].sum()
   
    agg_data.rename(columns={"RES_CNT": "Population"}, inplace=True)

    community_boundaries = gpd.read_file("Community Boundaries 2011_20231018.geojson")

    merged_data = community_boundaries.merge(
        agg_data, left_on="name", right_on="NAME", how="left"
    )

    fig = px.choropleth_mapbox(
        merged_data,
        geojson=community_boundaries.geometry,
        locations=merged_data.index,
        color="Population",
        color_continuous_scale=px.colors.sequential.Rainbow,
        hover_name="NAME",
        center=dict(lat=51.0447, lon=-114.0719),  # Calgary coordinates
        zoom=10,
        mapbox_style="mapbox://styles/mapbox/light-v11",
        opacity=0.6,
    )

    # put colour bar inside of map
    fig.update_layout(
        mapbox=dict(
            accesstoken="pk.eyJ1Ijoia2FhamJvbGFuZCIsImEiOiJjbG5kejg0emIwOGRyMmxsZW9vaXYyMGswIn0.Rhnj7A5aOZh0JBebF4WaFQ",
        ),
        coloraxis_colorbar=dict(
            xanchor='right',
            yanchor='middle',
            title='Population',
            title_side='right',
            thickness=10,
            # Using a bolder font if available
            title_font=dict(family="Arial Black", size=14),
        ),
        autosize=True,
        margin=dict(l=0, r=0, t=0, b=0),
        hoverlabel=dict(  # Customizing hover label aesthetics
            bgcolor="#eeeee4",
            font_color="#696fa7",
            bordercolor="#eeeee4",
            # font_size=16,  # Font size
            # font_family="Rockwell",  # Font family
            # font_color="black",  # Font color
            # bordercolor="lightgray"  # Border color
        ),
        title="Calgary Population by Community",
    )
    
    fig.update_geos(center_lon=-114.0719, center_lat=51.0447, projection_scale=450)
    
    return fig

get_data()
