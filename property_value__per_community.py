import pandas as pd
import geopandas as gpd
import plotly.express as px

property_value_data = pd.read_csv(
    "Current_Year_Property_Assessments__Parcel__20231018.csv"
)
building_permits_data = pd.read_csv("Building_Permits_20231018.csv")

property_value_data = property_value_data.rename(columns={"COMM_NAME": "CommunityName"})

agg_data = property_value_data.groupby("CommunityName", as_index=False)[
    "ASSESSED_VALUE"
].mean()

agg_data = agg_data[agg_data["ASSESSED_VALUE"] <= 30000000]

community_boundaries = gpd.read_file("Community Boundaries 2011_20231018.geojson")

merged_data = community_boundaries.merge(
    agg_data, left_on="name", right_on="CommunityName", how="left"
)

fig = px.choropleth(
    merged_data,
    geojson=community_boundaries.geometry,
    locations=merged_data.index,
    color="ASSESSED_VALUE",
    color_continuous_scale=px.colors.sequential.Plasma,  
    hover_name="CommunityName",
    title="Calgary Mean Property Value by Community",
)

fig.update_geos(center_lon=-114.0719, center_lat=51.0447, projection_scale=450)

fig.show()
