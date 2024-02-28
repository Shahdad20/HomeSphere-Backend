from flask import Flask, jsonify, render_template
from flask_cors import CORS  # Import CORS
import pandas as pd
import plotly.express as px

def get_data():
    # API URLs for building permits and vacant apartments data
    building_permits_api_url = "https://data.calgary.ca/resource/c2es-76ed.json"
    vacant_apartments_api_url = "https://data.calgary.ca/resource/rkfr-buzb.json"
    
    # Fetch data from the APIs
    df_building_permits = pd.read_json(building_permits_api_url)
    df_vacant_apartments = pd.read_json(vacant_apartments_api_url)

    # Rename the 'name' column in df_vacant_apartments to match the 'communityname' column in df_building_permits
    df_vacant_apartments = df_vacant_apartments.rename(columns={"name": "communityname"})

    # Merge the dataframes on the 'communityname' column
    df_merged = df_building_permits.merge(df_vacant_apartments, on="communityname", how="left")

    # Create a heat map using Plotly Express
    fig = px.scatter_mapbox(
        df_merged,
        lat="latitude",
        lon="longitude",
        color="apt_vacant",
        size_max=15,
        hover_name="communityname",
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
        center=dict(lat=51.0447, lon=-114.0719),
        zoom=10,
        mapbox_style="mapbox://styles/mapbox/streets-v11",
        opacity=0.6,
        title="Calgary Building Permits Heat Map with Vacant Homes Availability",
        color_continuous_scale=px.colors.sequential.Rainbow,
    )

    # Update Mapbox layout
    fig.update_layout(
        mapbox=dict(
            accesstoken="pk.eyJ1Ijoia2FhamJvbGFuZCIsImEiOiJjbG5kejg0emIwOGRyMmxsZW9vaXYyMGswIn0.Rhnj7A5aOZh0JBebF4WaFQ",
        ),
        coloraxis_colorbar=dict(title="Community Vacancies"),
        coloraxis=dict(colorbar=dict(title="Community Vacancies")),
    )
    
    return fig

app = Flask(__name__, static_folder="../HomeSphere-Frontend/HomeSphere-frontend/my-app/public/build", static_url_path='/')
CORS(app)  # Enable CORS for all routes
