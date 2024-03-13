import json
import os
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Security, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from fastapi.staticfiles import StaticFiles

import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from models import KMeansPostalModelInput, KMeansCommunityModelInput
from utils import evaluate_clustering_performance, get_selected_features, wkb_to_wkt

# Load the environment variables
load_dotenv()

API_KEY = os.environ.get("API_KEY")
RDS_USERNAME = os.environ.get("RDS_USERNAME")
RDS_PASSWORD = os.environ.get("RDS_PASSWORD")
RDS_HOST = os.environ.get("RDS_HOST")
RDS_PORT = os.environ.get("RDS_PORT")
RDS_DATABASE = os.environ.get("RDS_DATABASE")

# Define the map constants
MAP_ZOOM = 9
MAP_CENTER = {"lat": 51.096, "lon": -113.954}

# Define the API key header
API_KEY_NAME = "AccessToken"
api_key_header = APIKeyHeader(name = API_KEY_NAME, auto_error = False)

# Define the API key model
async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key == API_KEY:
        return api_key
    else:
        raise HTTPException(status_code = 403, detail = "Could not validate credentials")

# Define the FastAPI app
app = FastAPI(root_path = "/api")

# Mount the static files
app.mount("/static", StaticFiles(directory = "static"), name = "static")

app.add_middleware(
    CORSMiddleware,
    allow_origins = ["*"],
    allow_credentials = True,
    allow_methods = ["*"],
    allow_headers = ["*"],
)

# Create a connection to the database
connection_string = (
    f"postgresql+asyncpg://{RDS_USERNAME}:{RDS_PASSWORD}@" + 
    f"{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}"
)

# Create an engine to connect to the database
engine = create_async_engine(connection_string)

# Define a session class to use for the database
AsyncSessionLocal = sessionmaker(
    autocommit = False, autoflush = False, bind = engine, class_ = AsyncSession
)

# Define a function to get the database session
async def get_db_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session
        
# Utility functions to fetch data
async def fetch_postal_codes_with_assessed_values(db: AsyncSession):
    async with db as session:
        result = await db.execute(text("SELECT * FROM postal_codes_with_assessed_values;"))
        result_list = result.mappings().all()
        return result_list
    
async def fetch_combined_boundaries_and_profile_data(db: AsyncSession):
    async with db as session:
        result = await db.execute(text("SELECT * FROM combined_boundaries_and_profile_data;"))
        result_list = result.mappings().all()
        return result_list
    
async def fetch_exclude_communities_gdf(db: AsyncSession):
    async with db as session:
        result = await db.execute(text("SELECT * FROM excluded_communities_gdf;"))
        result_list = result.mappings().all()
        return result_list
    
async def fetch_exclude_postal_codes_gdf(db: AsyncSession):
    async with db as session:
        result = await db.execute(text("SELECT * FROM excluded_postal_codes_gdf;"))
        result_list = result.mappings().all()
        return result_list

# Define the routes for the FastAPI app
@app.get("/building_permits")
async def get_building_permits(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT * FROM building_permits;"))
        result_list = result.mappings().all()
        return result_list        

@app.get("/combined_boundaries_and_profile_data")
async def get_combined_boundaries_and_profile_data(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    result_list = await fetch_combined_boundaries_and_profile_data(db)
    return result_list

@app.get("/community_crime_statistics")
async def get_community_crime_statistics(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT * FROM community_crime_statistics;"))
        result_list = result.mappings().all()
        return result_list
    
@app.get("/community_disorder_statistics")
async def get_community_disorder_statistics(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT * FROM community_disorder_statistics;"))
        result_list = result.mappings().all()
        return result_list
    
@app.get("/community_district_boundaries")
async def get_community_district_boundaries(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT * FROM community_district_boundaries;"))
        result_list = result.mappings().all()
        return result_list
    
@app.get("/community_profiles")
async def get_community_profiles(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT * FROM community_profiles;"))
        result_list = result.mappings().all()
        return result_list
    
@app.get("/community_services")
async def get_community_services(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT * FROM community_services;"))
        result_list = result.mappings().all()
        return result_list
    
@app.get("/current_year_property_assessments")
async def get_current_year_property_assessments(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT * FROM current_year_property_assessments;"))
        result_list = result.mappings().all()
        return result_list
    
@app.get("/development_permits")
async def get_development_permits(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT * FROM development_permits;"))
        result_list = result.mappings().all()
        return result_list

@app.get("/excluded_communities")
async def get_excluded_communities(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    result_list = await fetch_exclude_communities_gdf(db)
    return result_list

@app.get("/excluded_postal_codes")
async def get_excluded_postal_codes(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):  
    result_list = await fetch_exclude_postal_codes_gdf(db)
    return result_list
    
@app.get("/land_use_districts")
async def get_land_use_districts(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT * FROM land_use_districts;"))
        result_list = result.mappings().all()
        return result_list
    
@app.get("/land_use_districts_info")
async def get_land_use_districts_info(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT * FROM land_use_districts_info;"))
        result_list = result.mappings().all()
        return result_list
    
@app.get("/postal_boundaries")
async def get_postal_boundaries(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT * FROM postal_boundaries;"))
        result_list = result.mappings().all()
        return result_list
    
@app.get("/postal_codes_with_assessed_values")
async def get_postal_codes_with_assessed_values(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    result_list = await fetch_postal_codes_with_assessed_values(db)
    return result_list
    
@app.get("/schools")
async def get_schools(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT * FROM schools;"))
        result_list = result.mappings().all()
        return result_list
    
@app.get("/transit_stops")
async def get_transit_stops(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT * FROM ne_transit_stops_gdf;"))
        result_list = result.mappings().all()
        return result_list
    
@app.get("/vacant_apartments")
async def get_vacant_apartments(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT * FROM vacant_apartments;"))
        result_list = result.mappings().all()
        return result_list
    
@app.get("/maps/congestion")
async def get_congestion_map(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT map_json FROM map_data WHERE name = :name"), {"name": "congestion_map"})        
        map_data = result.scalar_one_or_none()
        if map_data is None:
            return HTTPException(status_code = 404, detail = "Map data not found")
        return json.loads(map_data)

@app.get("/maps/housing_development_zone")
async def get_housing_development_zone_map(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT map_json FROM map_data WHERE name = :name"), {"name": "housing_development_zone_map"})        
        map_data = result.scalar_one_or_none()
        if map_data is None:
            return HTTPException(status_code = 404, detail = "Map data not found")
        return json.loads(map_data)
    
@app.get("/maps/property_value_per_community")
async def get_property_value_per_community_map(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT map_json FROM map_data WHERE name = :name"), {"name": "property_value_per_community_map"})        
        map_data = result.scalar_one_or_none()
        if map_data is None:
            return HTTPException(status_code = 404, detail = "Map data not found")
        return json.loads(map_data)
    
@app.get("/maps/vacancy_per_community")
async def get_vacancy_per_community_map(db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    async with db as session:
        result = await session.execute(text("SELECT map_json FROM map_data WHERE name = :name"), {"name": "vacancy_per_community_map"})        
        map_data = result.scalar_one_or_none()
        if map_data is None:
            return HTTPException(status_code = 404, detail = "Map data not found")
        return json.loads(map_data)

@app.post("/api/kmeans_postal")
async def get_kmeans_postal(model: KMeansPostalModelInput, db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    selected_features = get_selected_features(model, "postal")

    kmeans_postal = KMeans(n_clusters = model.n_clusters, random_state = model.random_state)

    postal_dataset = await fetch_postal_codes_with_assessed_values(db)
    postal_df = pd.DataFrame(postal_dataset)
    postal_df["geometry"] = postal_df["geometry"].apply(wkb_to_wkt)
    postal_gdf = gpd.GeoDataFrame(postal_df, geometry = "geometry", crs = "EPSG:4326")
    postal_gdf_selected_features = postal_gdf[selected_features]

    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(postal_gdf_selected_features)
    kmeans_postal.fit(scaled_data)

    postal_gdf["KMeans Postal Cluster"] = kmeans_postal.labels_

    excluded_postal_codes = await fetch_exclude_postal_codes_gdf
    excluded_postal_codes_df = pd.DataFrame(excluded_postal_codes)
    excluded_postal_codes_df["geometry"] = excluded_postal_codes_df["geometry"].apply(wkb_to_wkt)
    excluded_postal_codes_gdf = gpd.GeoDataFrame(excluded_postal_codes_df, geometry = "geometry", crs = "EPSG:4326")

    fig = px.choropleth_mapbox(
        postal_gdf,
        geojson = postal_gdf.geometry,
        locations = postal_gdf.index,
        color = "KMeans Postal Cluster",
        color_continuous_scale = px.colors.qualitative.Vivid,
        hover_name = "Community Name",
        title = "Calgary Postal Codes by K-Means Cluster",
        center = MAP_CENTER,
        zoom = MAP_ZOOM,
        opacity = 0.5,
        mapbox_style = "carto-positron"
    )

    excluded_postal_codes_geojson = json.loads(excluded_postal_codes_gdf.geometry.to_json())

    excluded_postal_codes_layer = go.Choroplethmapbox(
        geojson = excluded_postal_codes_geojson,
        locations = excluded_postal_codes_gdf.index,
        z = [1] * len(excluded_postal_codes_gdf),
        colorscale = ["#AAAAAA", "#AAAAAA"],
        showscale = False,
        hoverinfo = "text",
    )
    
    fig.add_trace(excluded_postal_codes_layer)

    return fig.to_json()


@app.post("/api/kmeans_community")
async def get_kmeans_community(model: KMeansCommunityModelInput, db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    selected_features = get_selected_features(model, "community")
    kmeans_community = KMeans(n_clusters = model.n_clusters, random_state = model.random_state)

    community_dataset = await fetch_combined_boundaries_and_profile_data(db)
    community_df = pd.DataFrame(community_dataset)
    community_df["geometry"] = community_df["geometry"].apply(wkb_to_wkt)
    community_gdf = gpd.GeoDataFrame(community_df, geometry = "geometry", crs = "EPSG:4326")
    community_gdf_selected_features = community_gdf[selected_features]

    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(community_gdf_selected_features)
    kmeans_community.fit(scaled_data)

    community_gdf["KMeans Community Cluster"] = kmeans_community.labels_

    excluded_communities = await fetch_exclude_communities_gdf(db)
    excluded_communities_df = pd.DataFrame(excluded_communities)
    excluded_communities_df["geometry"] = excluded_communities_df["geometry"].apply(wkb_to_wkt)
    excluded_communities_gdf = gpd.GeoDataFrame(excluded_communities_df, geometry = "geometry", crs = "EPSG:4326")

    fig = px.choropleth_mapbox(
        community_gdf,
        geojson = community_gdf.geometry,
        locations = community_gdf.index,
        color = "KMeans Community Cluster",
        color_continuous_scale = px.colors.qualitative.Vivid,
        hover_name = "Community Name",
        title = "Calgary Communities by K-Means Cluster",
        center = MAP_CENTER,
        zoom = MAP_ZOOM,
        opacity = 0.5,
        mapbox_style = "carto-positron"
    )

    excluded_communities_geojson = json.loads(excluded_communities_gdf.geometry.to_json())

    excluded_communities_layer = go.Choroplethmapbox(
        geojson = excluded_communities_geojson,
        locations = excluded_communities_gdf.index,
        z = [1] * len(excluded_communities_gdf),
        colorscale = ["#AAAAAA", "#AAAAAA"],
        showscale = False,
        hoverinfo = "text",
    )
    
    fig.add_trace(excluded_communities_layer)

    return fig.to_json()

@app.post("/api/kmeans_postal_info")
async def get_kmeans_postal_info(model: KMeansPostalModelInput, db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    selected_features = get_selected_features(model, "postal")

    kmeans_postal = KMeans(n_clusters = model.n_clusters, random_state = model.random_state)

    postal_dataset = await fetch_postal_codes_with_assessed_values(db)
    postal_df = pd.DataFrame(postal_dataset)
    postal_df["geometry"] = postal_df["geometry"].apply(wkb_to_wkt)
    postal_gdf = gpd.GeoDataFrame(postal_df, geometry = "geometry", crs = "EPSG:4326")
    postal_gdf_selected_features = postal_gdf[selected_features]

    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(postal_gdf_selected_features)
    kmeans_postal.fit(scaled_data)

    postal_gdf["KMeans Postal Cluster"] = kmeans_postal.labels_
    postal_gdf_selected_features = postal_gdf.select_dtypes(include = [np.number])
    postal_gdf_selected_features = postal_gdf_selected_features[selected_features + ["KMeans Postal Cluster"]]

    result = postal_gdf_selected_features.groupby("KMeans Postal Cluster").mean().reset_index()
    return result.to_json(orient = "records")

@app.post("/api/kmeans_community_info")
async def get_kmeans_community_info(model: KMeansCommunityModelInput, db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    selected_features = get_selected_features(model, "community")
    kmeans_community = KMeans(n_clusters = model.n_clusters, random_state = model.random_state)

    community_dataset = await fetch_combined_boundaries_and_profile_data(db)
    community_df = pd.DataFrame(community_dataset)
    community_df["geometry"] = community_df["geometry"].apply(wkb_to_wkt)
    community_gdf = gpd.GeoDataFrame(community_df, geometry = "geometry", crs = "EPSG:4326")
    community_gdf_selected_features = community_gdf[selected_features]

    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(community_gdf_selected_features)
    kmeans_community.fit(scaled_data)

    community_gdf["KMeans Community Cluster"] = kmeans_community.labels_
    community_gdf_selected_features = community_gdf.select_dtypes(include = [np.number])
    community_gdf_selected_features = community_gdf_selected_features[selected_features + ["KMeans Community Cluster"]]

    result = community_gdf_selected_features.groupby("KMeans Community Cluster").mean().reset_index()
    return result.to_json(orient = "records")

@app.post("/api/kmeans_postal_info_full")
async def get_kmeans_postal_info_full(model: KMeansPostalModelInput, db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    selected_features = get_selected_features(model, "postal")

    kmeans_postal = KMeans(n_clusters = model.n_clusters, random_state = model.random_state)

    postal_dataset = await fetch_postal_codes_with_assessed_values(db)
    postal_df = pd.DataFrame(postal_dataset)
    postal_df["geometry"] = postal_df["geometry"].apply(wkb_to_wkt)
    postal_gdf = gpd.GeoDataFrame(postal_df, geometry = "geometry", crs = "EPSG:4326")
    postal_gdf_selected_features = postal_gdf[selected_features]

    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(postal_gdf_selected_features)
    kmeans_postal.fit(scaled_data)

    postal_gdf["KMeans Postal Cluster"] = kmeans_postal.labels_
    postal_code_count_by_community_and_clusters = postal_gdf.groupby(["Community Name", "KMeans Postal Cluster"])[selected_features].agg(["mean", "median", "std"])

    return postal_code_count_by_community_and_clusters.to_json(orient = "records")

@app.post("/api/kmeans_community_info_full")
async def get_kmeans_community_info_full(model: KMeansCommunityModelInput, db: AsyncSession = Depends(get_db_session), api_key: str = Security(get_api_key)):
    selected_features = get_selected_features(model, "community")
    kmeans_community = KMeans(n_clusters = model.n_clusters, random_state = model.random_state)

    community_dataset = await fetch_combined_boundaries_and_profile_data(db)
    community_df = pd.DataFrame(community_dataset)
    community_df["geometry"] = community_df["geometry"].apply(wkb_to_wkt)
    community_gdf = gpd.GeoDataFrame(community_df, geometry = "geometry", crs = "EPSG:4326")
    community_gdf_selected_features = community_gdf[selected_features]

    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(community_gdf_selected_features)
    kmeans_community.fit(scaled_data)

    community_gdf["KMeans Community Cluster"] = kmeans_community.labels_
    postal_code_count_by_community_and_clusters = community_gdf.groupby(["Community Name", "KMeans Community Cluster"])[selected_features].agg(["mean", "median", "std"])

    return postal_code_count_by_community_and_clusters.to_json(orient = "records")

# Run the FastAPI app
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host = "0.0.0.0", port = 8000)
