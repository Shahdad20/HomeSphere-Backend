#!/usr/bin/env python3
from datetime import datetime
import json
import logging
import os
import sys
from dotenv import load_dotenv
import geopandas as gpd
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text, Column, String
from sqlalchemy.dialects.postgresql import JSON, insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, declarative_base

# Create a logs directory if it does not exist
log_directory = "logs"
os.makedirs(log_directory, exist_ok = True)

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a file handler which logs messages
now = datetime.now()
logfile = os.path.join(log_directory, f"P3_logs_{now.strftime('%Y-%m-%d')}.log")
file_handler = logging.FileHandler(logfile)
file_handler.setLevel(logging.INFO)

# Create a formatter and set the formatter for the handler
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

# Define the coordinate reference systems
COORD_CRS = "EPSG:4326"
UTM_CRS = "EPSG:32612"

# Load the environment variables
load_dotenv()

RDS_USERNAME = os.environ.get("RDS_USERNAME")
RDS_PASSWORD = os.environ.get("RDS_PASSWORD")
RDS_HOST = os.environ.get("RDS_HOST")
RDS_PORT = os.environ.get("RDS_PORT")
RDS_DATABASE = os.environ.get("RDS_DATABASE")

MAP_ZOOM = 9.65
MAP_CENTER = {"lat": 51.115, "lon": -113.954}

# Define the base class for the database
Base = declarative_base()

# Define the class for the map data
class MapData(Base):
    __tablename__ = "map_data"
    name = Column(String, primary_key = True)
    map_json = Column(JSON)

# Create a connection to the database
connection_string = (
    f"postgresql://{RDS_USERNAME}:{RDS_PASSWORD}@" + 
    f"{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}"
)
db_engine = create_engine(connection_string)

# Check if the connection to the database was successful
try:
    with db_engine.connect() as connection:
        result = connection.execute(text("SELECT 1;"))

    logger.info("Connection to the database was successful.")
except SQLAlchemyError as e:
    logger.exception("An error occurred connecting to the database.")
    sys.exit(1)
    
# Create the tables in the database
Base.metadata.create_all(db_engine)

Session = sessionmaker(bind = db_engine)

def create_congestion_map():
    community_profiles_df = pd.read_sql_table(
        "community_profiles", 
        db_engine
    )
    logger.info("create_congestion_map(): Community profiles loaded.")

    community_profiles_df.rename(columns={"Count of Population in Private Households": "Population"}, inplace=True)
    population_data = community_profiles_df[["Community Name", "Population"]] 

    community_boundaries_gdf = gpd.read_postgis(
        "SELECT * FROM community_district_boundaries;", 
        db_engine,
        crs = COORD_CRS,
        geom_col = "geometry"
    )

    logger.info("create_congestion_map(): Community district boundaries loaded.")

    merged_data = community_boundaries_gdf.merge(
        population_data, 
        left_on = "Community Name",
        right_on = "Community Name",
        how = "left"
    )

    logger.info("create_congestion_map(): Data merged.")

    excluded_communities_gdf = gpd.read_postgis(
        "SELECT * FROM excluded_communities_gdf;", 
        db_engine,
        crs = COORD_CRS,
        geom_col = "geometry"
    )

    fig = px.choropleth_mapbox(
        merged_data,
        geojson = community_boundaries_gdf.geometry,
        locations = merged_data.index,
        color = "Population",
        color_continuous_scale = px.colors.sequential.YlOrRd,
        hover_name = "Community Name",
        title = "Calgary Population by Community",
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

    logger.info("create_congestion_map(): Choropleth map created.")
    logger.info("create_congestion_map(): Finished.")

    return fig.to_json()

def create_housing_development_zone_map():
    development_permits_df = pd.read_sql_table(
        "development_permits", 
        db_engine
    )
    
    development_permits_df = development_permits_df.rename(columns = {"CommunityName": "Community Name"})

    aggregated_permits = development_permits_df.groupby("Community Name", as_index = False)["CommunityCode"].count()
    aggregated_permits.rename(columns = {"CommunityCode": "Development Permits"}, inplace = True)

    logger.info("create_housing_development_zone_map(): Development permits data loaded.")

    community_boundaries_gdf = gpd.read_postgis(
        "SELECT * FROM community_district_boundaries;", 
        db_engine,
        crs = COORD_CRS,
        geom_col = "geometry"
    )

    logger.info("create_housing_development_zone_map(): Community district boundaries loaded.")

    merged_data = community_boundaries_gdf.merge(
        aggregated_permits, 
        left_on = "Community Name",
        right_on = "Community Name",
        how = "left"
    )

    logger.info("create_housing_development_zone_map(): Data merged.")

    excluded_communities_gdf = gpd.read_postgis(
        "SELECT * FROM excluded_communities_gdf;", 
        db_engine,
        crs = COORD_CRS,
        geom_col = "geometry"
    )

    fig = px.choropleth_mapbox(
        merged_data,
        geojson = community_boundaries_gdf.geometry,
        locations = merged_data.index,
        color = "Development Permits",
        color_continuous_scale = px.colors.sequential.YlOrRd,
        hover_name = "Community Name",
        title = "Calgary Development Permits by Community",
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

def create_property_value_per_community_map():
    current_year_property_assessments_df = gpd.read_postgis(
        "SELECT * FROM current_year_property_assessments;", 
        db_engine, 
        crs = COORD_CRS, 
        geom_col = "geometry"
    )

    mean_property_value_by_community = current_year_property_assessments_df.groupby("COMM_NAME")["ASSESSED_VALUE"].mean().reset_index()
    mean_property_value_by_community = mean_property_value_by_community[mean_property_value_by_community["ASSESSED_VALUE"] <= 10000000]

    logger.info("create_property_value_per_community_map(): Property value data loaded.")

    community_boundaries_gdf = gpd.read_postgis(
        "SELECT * FROM community_district_boundaries;", 
        db_engine,
        crs = COORD_CRS,
        geom_col = "geometry"
    )

    logger.info("create_property_value_per_community_map(): Community district boundaries loaded.")

    merged_data = community_boundaries_gdf.merge(
        mean_property_value_by_community, 
        left_on = "Community Name",
        right_on = "COMM_NAME",
        how = "left"
    )

    logger.info("create_property_value_per_community_map(): Data merged.")

    excluded_communities_gdf = gpd.read_postgis(
        "SELECT * FROM excluded_communities_gdf;", 
        db_engine,
        crs = COORD_CRS,
        geom_col = "geometry"
    )

    fig = px.choropleth_mapbox(
        merged_data,
        geojson = community_boundaries_gdf.geometry,
        locations = merged_data.index,
        color = "ASSESSED_VALUE",
        color_continuous_scale = px.colors.sequential.YlOrRd,
        hover_name = "COMM_NAME",
        title = "Calgary Median Property Value by Community",
        labels = {"ASSESSED_VALUE": "Median Property Value"},
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

    logger.info("create_property_value_per_community_map(): Map created.")

    return fig.to_json()

def create_vacancy_per_community_map():
    building_permits_df = pd.read_sql_table(
        "building_permits", 
        db_engine
    )

    logger.info("create_vacancy_per_community_map(): Building permits data loaded.")

    vacant_apartments_df = pd.read_sql_table(
        "vacant_apartments", 
        db_engine
    )

    logger.info("create_vacancy_per_community_map(): Vacant apartments data loaded.")

    vacant_apartments_df = vacant_apartments_df.rename(columns = {"NAME": "CommunityName"})

    merged_data = building_permits_df.merge(
        vacant_apartments_df,
        on = "CommunityName",
        how = "left"
    )

    fig = px.scatter_mapbox(
        merged_data,
        lat = "Latitude",
        lon = "Longitude",
        color = "Number of Vacant Single-Family Homes",
        size_max = 15,
        hover_name = "CommunityName",
        hover_data = {
            "Number of Vacant Apartments": True,
            "Number of Vacant Converted Structures": True,
            "Number of Vacant Duplexes": True,
            "Number of Vacant Multi-Family Homes": True,
            "Number of Vacant Multi-Plexes": True,
            "Number of Vacant Townhouses": True,
            "Number of Vacant Other Structures": True,
            "Number of Vacant Single-Family Homes": True,
        },
        center = MAP_CENTER,
        zoom = 8.5,
        mapbox_style = "carto-positron",
        opacity = 0.6,
        title = "Calgary Building Permits Heat Map with Vacant Homes Availability",
        color_continuous_scale = px.colors.sequential.Rainbow
    )

    fig.update_layout(
        coloraxis_colorbar = {"title": "Community Vacancies"},
    )

    logger.info("create_vacancy_per_community_map(): Map created.")

    return fig.to_json()

# Create the maps and save them to the database
congestion_map = create_congestion_map()

with Session() as session:
    # Define the insert statement for upsert
    stmt = insert(MapData).values(name = "congestion_map", map_json = congestion_map)

    # Specify the upsert behavior on conflict on the 'name' column
    do_update_stmt = stmt.on_conflict_do_update(
        index_elements = ["name"],  # Column causing the conflict
        set_ = dict(map_json = congestion_map)  # How to update the row
    )

    # Execute the upsert statement
    session.execute(do_update_stmt)
    session.commit()

    logger.info("create_congestion_map(): Successfully saved to the database.")

housing_development_zone_map = create_housing_development_zone_map()

with Session() as session:
    # Define the insert statement for upsert
    stmt = insert(MapData).values(name = "housing_development_zone_map", map_json = housing_development_zone_map)

    # Specify the upsert behavior on conflict on the 'name' column
    do_update_stmt = stmt.on_conflict_do_update(
        index_elements = ["name"],  # Column causing the conflict
        set_ = dict(map_json = housing_development_zone_map)  # How to update the row
    )

    # Execute the upsert statement
    session.execute(do_update_stmt)
    session.commit()

    logger.info("create_housing_development_zone_map(): Successfully saved to the database.")

property_value_per_community_map = create_property_value_per_community_map()

with Session() as session:
    # Define the insert statement for upsert
    stmt = insert(MapData).values(name = "property_value_per_community_map", map_json = property_value_per_community_map)

    # Specify the upsert behavior on conflict on the 'name' column
    do_update_stmt = stmt.on_conflict_do_update(
        index_elements = ["name"],  # Column causing the conflict
        set_ = dict(map_json = property_value_per_community_map)  # How to update the row
    )

    # Execute the upsert statement
    session.execute(do_update_stmt)
    session.commit()

    logger.info("create_property_value_per_community_map(): Successfully saved to the database.")

vacancy_per_community_map = create_vacancy_per_community_map()

with Session() as session:
    # Define the insert statement for upsert
    stmt = insert(MapData).values(name = "vacancy_per_community_map", map_json = vacancy_per_community_map)

    # Specify the upsert behavior on conflict on the 'name' column
    do_update_stmt = stmt.on_conflict_do_update(
        index_elements = ["name"],  # Column causing the conflict
        set_ = dict(map_json = vacancy_per_community_map)  # How to update the row
    )

    # Execute the upsert statement
    session.execute(do_update_stmt)
    session.commit()

    logger.info("create_vacancy_per_community_map(): Successfully saved to the database.")