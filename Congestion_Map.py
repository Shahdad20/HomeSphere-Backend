from flask import Flask, jsonify
from flask_cors import CORS  # Import CORS
import pandas as pd
import plotly.express as px

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

property_value_data = pd.read_csv(
    "Current_Year_Property_Assessments__Parcel__20231018.csv"
)
building_permits_data = pd.read_csv("Building_Permits_20231018.csv")


population_data = pd.read_csv("Census_by_Community_2017_20231020.csv")


property_value_data = property_value_data.rename(columns={"COMM_NAME": "NAME"})


agg_data = population_data.groupby("NAME", as_index=False)["RES_CNT"].sum()


agg_data.rename(columns={"RES_CNT": "Population"}, inplace=True)


community_boundaries = gpd.read_file("Community Boundaries 2011_20231018.geojson")


merged_data = community_boundaries.merge(
    agg_data, left_on="name", right_on="NAME", how="left"
)


fig = px.choropleth(
    merged_data,
    geojson=community_boundaries.geometry,
    locations=merged_data.index,
    color="Population",  
    color_continuous_scale=px.colors.sequential.Plasma,  
    hover_name="NAME",
    title="Calgary Population by Community",
)


fig.update_geos(center_lon=-114.0719, center_lat=51.0447, projection_scale=450)

# Endpoint to get community vacancy map data
@app.route('/api/community_map')
def get_community_map():
    # Return the map data as JSON
    return jsonify(fig.to_json())

# Endpoint to get congestion map data
@app.route('/api/congestion_map')
def get_congestion_map():
    congestion_map_data = {"message": "This is the congestion map data"}
    return jsonify(congestion_map_data)

if __name__ == '__main__':
    app.run(debug=True)

fig.show()
