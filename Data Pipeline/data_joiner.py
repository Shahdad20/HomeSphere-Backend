#!/usr/bin/env python3
import csv
from datetime import datetime
import logging
import os
import sys
import time
from dotenv import load_dotenv
import geopandas as gpd
import pandas as pd
from scipy.spatial import KDTree
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Create a logs directory if it does not exist
log_directory = "logs"
os.makedirs(log_directory, exist_ok = True)

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a file handler which logs messages
now = datetime.now()
logfile = os.path.join(log_directory, f"P2_logs_{now.strftime('%Y-%m-%d')}.log")
file_handler = logging.FileHandler(logfile)
file_handler.setLevel(logging.INFO)

# Create a formatter and set the formatter for the handler
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

# Increase the maximum field size for CSV files
csv.field_size_limit(sys.maxsize)

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

def save_to_database(df, table_name, is_geospatial = False):
    """
    This function saves the DataFrame or GeoDataFrame to the database as a table with 
    the specified name.

    Args:
    - df: The DataFrame or GeoDataFrame to be saved
    - table_name: The name of the table to be created in the database
    - is_geospatial: A boolean indicating whether the DataFrame is a GeoDataFrame
    """

    try:
        if is_geospatial:
            # If GeoDataFrame, use the to_postgis method to save the data to the database
            df.to_postgis(table_name, db_engine, if_exists = "replace", index = False)
        else:
            # If DataFrame, use the to_sql method to save the data to the database
            df.to_sql(table_name, db_engine, if_exists = "replace", index = False)
        logger.info(f"Data successfully saved to table '{table_name}'.")
    except SQLAlchemyError as e:
        logger.exception("An error occurred saving data to the database")

def create_combined_boundaries_and_profile_data_gdf():
    '''
    Create a GeoDataFrame that combines the community district boundaries,  the 
    community profiles data, total crimes, and total disorders data.
    '''

    # Import the community district boundaries, community profiles, total crimes, 
    # total disorders, and transit stops data
    community_boundaries_gdf = gpd.read_postgis(
        "SELECT * FROM community_district_boundaries;", 
        db_engine, 
        crs = COORD_CRS, 
        geom_col = "geometry"
    )
    community_boundaries_gdf = community_boundaries_gdf.to_crs(UTM_CRS)

    transit_stops_gdf = gpd.read_postgis(
        "SELECT * FROM transit_stops;", 
        db_engine, 
        crs = COORD_CRS, 
        geom_col = "geometry"
    )
    transit_stops_gdf = transit_stops_gdf.to_crs(UTM_CRS)

    ne_transit_stops = gpd.sjoin(
        transit_stops_gdf,
        community_boundaries_gdf,
        how = "inner",
        predicate = "within"
    )

    ne_transit_stops = ne_transit_stops.drop(columns = ["index_right", "CREATED_DT", "MODIFIED_DT", "Status", "TeleRide Number"])

    community_profiles_gdf = pd.read_sql_table("community_profiles", db_engine)

    total_crimes_df = pd.read_sql_table("community_crime_statistics", db_engine)
    total_disorders_df = pd.read_sql_table("community_disorder_statistics", db_engine)

    transit_stops_by_community_gdf = ne_transit_stops.groupby("Community Name")["Stop Name"].count().reset_index()
    transit_stops_by_community_gdf.columns = ["Community Name", "Transit Stops Count"]

    logger.info("create_combined_boundaries_and_profile_data_gdf(): Data successfully imported.")

    # Merge the two GeoDataFrames on the "Community Name" column
    combined_gdf = community_boundaries_gdf.merge(
        community_profiles_gdf,
        on = "Community Name"
    )

    combined_gdf = combined_gdf.merge(
        transit_stops_by_community_gdf,
        on = "Community Name"
    )

    logger.info("create_combined_boundaries_and_profile_data_gdf(): Data successfully merged.")

    # Do one hot encoding on the most common dwelling types
    most_common_encoded = pd.get_dummies(
        combined_gdf["Most Common Dwelling Type"], 
        prefix = "Most Common"
    )
    second_most_common_encoded = pd.get_dummies(
        combined_gdf["Second Most Common Dwelling Type"], 
        prefix = "Second Most Common"
    )
    third_most_common_encoded = pd.get_dummies(
        combined_gdf["Third Most Common Dwelling Type"], 
        prefix = "Third Most Common"
    )

    logger.info("create_combined_boundaries_and_profile_data_gdf(): Data successfully one hot encoded.")
    
    combined_gdf = pd.concat(
        [
            combined_gdf, 
            most_common_encoded, 
            second_most_common_encoded, 
            third_most_common_encoded
        ], 
        axis = 1
    )

    combined_gdf.drop(
        columns = [
            "Most Common Dwelling Type", 
            "Second Most Common Dwelling Type", 
            "Third Most Common Dwelling Type"
        ], 
        axis = 1, 
        inplace = True
    )

    # Merge the combined GeoDataFrame with the total crimes and total disorders DataFrames
    combined_gdf = combined_gdf.merge(
        total_crimes_df,
        on = "Community Name"
    )

    combined_gdf = combined_gdf.merge(
        total_disorders_df,
        on = "Community Name"
    )
        
    # Drop the columns with the "_right" or "_left" suffix
    columns_with_right_index = [column for column in combined_gdf.columns if column.endswith("_right") or column.endswith("_left")]
    combined_gdf.drop(columns = columns_with_right_index, inplace = True)

    logger.info("create_combined_boundaries_and_profile_data_gdf(): Data successfully merged with total crimes and total disorders data.")
    
    combined_gdf = combined_gdf.to_crs(COORD_CRS)
    logger.info("create_combined_boundaries_and_profile_data_gdf(): CRS changed back to COORD_CRS successfully.")
    
    logger.info("create_combined_boundaries_and_profile_data_gdf(): Finished successfully.")

    return combined_gdf

def create_postal_codes_with_assessed_values_gdf(combined_gdf):
    '''
    Create a GeoDataFrame that combines the postal boundaries and the current year 
    property assessments data.

    Args:
    - combined_gdf: The combined_boundaries_and_profile_data_gdf GeoDataFrame that was
      previously created
    '''

    # Convert the combined_gdf to the UTM coordinate reference system
    combined_gdf = combined_gdf.to_crs(UTM_CRS)

    # Select the required columns from the combined_gdf
    combined_gdf = combined_gdf[["Community Name", "Class", "Sector", "SRG", "geometry"]]

    # Import the required datasets
    postal_boundaries_gdf = gpd.read_postgis(
        "SELECT * FROM postal_boundaries;", 
        db_engine, 
        crs = COORD_CRS, 
        geom_col = "geometry"
    )

    postal_boundaries_gdf = postal_boundaries_gdf.to_crs(UTM_CRS)

    current_year_property_assessments_df = gpd.read_postgis(
        "SELECT * FROM current_year_property_assessments;", 
        db_engine, 
        crs = COORD_CRS, 
        geom_col = "geometry"
    )

    current_year_property_assessments_df = current_year_property_assessments_df.to_crs(UTM_CRS)

    land_use_districts_gdf = gpd.read_postgis(
        "SELECT * FROM land_use_districts;",
        db_engine,
        crs = COORD_CRS,
        geom_col = "geometry"
    )

    land_use_districts_gdf = land_use_districts_gdf.to_crs(UTM_CRS)

    schools_gdf = gpd.read_postgis(
        "SELECT * FROM schools;",
        db_engine,
        crs = COORD_CRS,
        geom_col = "geometry"
    )

    schools_gdf = schools_gdf.to_crs(UTM_CRS)

    community_services_gdf = gpd.read_postgis(
        "SELECT * FROM community_services;",
        db_engine,
        crs = COORD_CRS,
        geom_col = "geometry"
    )

    community_services_gdf = community_services_gdf.to_crs(UTM_CRS)

    community_boundaries_gdf = gpd.read_postgis(
        "SELECT * FROM community_district_boundaries;", 
        db_engine, 
        crs = COORD_CRS, 
        geom_col = "geometry"
    )
    community_boundaries_gdf = community_boundaries_gdf.to_crs(UTM_CRS)

    transit_stops_gdf = gpd.read_postgis(
        "SELECT * FROM transit_stops;", 
        db_engine, 
        crs = COORD_CRS, 
        geom_col = "geometry"
    )
    transit_stops_gdf = transit_stops_gdf.to_crs(UTM_CRS)

    ne_transit_stops = gpd.sjoin(
        transit_stops_gdf,
        community_boundaries_gdf,
        how = "inner",
        predicate = "within"
    )

    ne_transit_stops = ne_transit_stops.drop(columns = ["index_right", "CREATED_DT", "MODIFIED_DT", "Status", "TeleRide Number"])
    ctrain_stops = ne_transit_stops[ne_transit_stops["Stop Name"].str.contains("LRT")]
    bus_stops = ne_transit_stops[~ne_transit_stops["Stop Name"].str.contains("LRT")]

    logger.info("create_postal_codes_with_assessed_values_gdf(): Data successfully imported.")

    postal_codes_and_communities_gdf = gpd.sjoin(
        postal_boundaries_gdf, 
        combined_gdf, 
        how = "inner", 
        predicate = "intersects"
    )

    postal_codes_and_communities_gdf = postal_codes_and_communities_gdf.drop(
        columns = ["index_right"]
    )

    # Drop rows with missing values in the "Class", "Community Name", "Sector", and "SRG" columns,
    # as they are not worth keeping
    postal_codes_and_communities_gdf = postal_codes_and_communities_gdf.dropna(
        subset = ["Class", "Community Name", "Sector", "SRG"]
    )

    postal_codes_and_communities_gdf.reset_index(drop = True, inplace = True)

    # Replace the existing Longitude and Latitude values with the centroid of the polygon to get a more accurate calculation
    postal_codes_and_communities_gdf["Longitude"] = postal_codes_and_communities_gdf["geometry"].centroid.x
    postal_codes_and_communities_gdf["Latitude"] = postal_codes_and_communities_gdf["geometry"].centroid.y
    
    logger.info("create_postal_codes_with_assessed_values_gdf(): postal_codes_and_communities_gdf finished.")

    property_assessments_with_postal_codes_joined = gpd.sjoin(
        current_year_property_assessments_df, 
        postal_codes_and_communities_gdf, 
        how = "left", 
        predicate = "within"
    )

    property_assessments_with_postal_codes_gdf = property_assessments_with_postal_codes_joined[
        [*current_year_property_assessments_df.columns, "Postal Code"]
    ]
    property_assessments_with_postal_codes_gdf = property_assessments_with_postal_codes_gdf.dropna(
        subset = ["Postal Code"]
    )
    property_assessments_with_postal_codes_gdf.reset_index(drop = True, inplace = True)

    property_assessments_grouped_gdf = property_assessments_with_postal_codes_gdf.groupby("Postal Code")
    median_assessed_values_and_property_size_gdf = property_assessments_grouped_gdf.agg(
        {"RE_ASSESSED_VALUE": "median", "LAND_SIZE_SM": "median"}
    )
    median_assessed_values_and_property_size_gdf = median_assessed_values_and_property_size_gdf.reset_index()
    median_assessed_values_and_property_size_gdf.columns = [
        "Postal Code", 
        "Median Assessed Value", 
        "Median Land Size"
    ]
    median_assessed_values_and_property_size_gdf = median_assessed_values_and_property_size_gdf.dropna(
        subset = ["Median Assessed Value", "Median Land Size"]
    )
    median_assessed_values_and_property_size_gdf = median_assessed_values_and_property_size_gdf.reset_index()
    median_assessed_values_and_property_size_gdf.drop(
        columns = ["index"], 
        inplace = True
    )

    postal_codes_with_assessed_values_gdf = pd.merge(
        postal_codes_and_communities_gdf, 
        median_assessed_values_and_property_size_gdf, 
        on = "Postal Code", 
        how = "left"
    )
    postal_codes_with_assessed_values_gdf = postal_codes_with_assessed_values_gdf.dropna(
        subset = ["Median Assessed Value"]
    )
    postal_codes_with_assessed_values_gdf.reset_index(
        drop = True, inplace = True
    )

    logger.info("create_postal_codes_with_assessed_values_gdf(): assessments merged successfully.")

    postal_codes_with_assessed_values_gdf = gpd.sjoin(
        postal_codes_with_assessed_values_gdf, 
        land_use_districts_gdf, 
        how = "left", 
        predicate = "intersects"
    )
    postal_codes_with_assessed_values_gdf = postal_codes_with_assessed_values_gdf.drop(
        columns = ["index_right"]
    )

    logger.info("create_postal_codes_with_assessed_values_gdf(): land use districts merged successfully.")

    schools_gdf["Longitude"] = schools_gdf["geometry"].x
    schools_gdf["Latitude"] = schools_gdf["geometry"].y

    school_types = ["Elementary", "Junior High", "Senior High"]

    for school_type in school_types:
        filtered_schools_gdf = schools_gdf[schools_gdf[school_type] == "Y"]

        tree = KDTree(filtered_schools_gdf[["Longitude", "Latitude"]].values)
        distances, indices = tree.query(postal_codes_with_assessed_values_gdf[["Longitude", "Latitude"]].values, k = 1)

        postal_codes_with_assessed_values_gdf[f"Distance To Closest {school_type}"] = distances

    logger.info("create_postal_codes_with_assessed_values_gdf(): schools merged successfully.")

    community_services_types = list(community_services_gdf["Type"].explode().unique())

    for service in community_services_types:
        community_services_filtered_df = community_services_gdf[community_services_gdf["Type"] == service]
        community_services_filtered_gdf = gpd.GeoDataFrame(community_services_filtered_df, geometry = "geometry", crs = UTM_CRS)
        community_services_filtered_gdf["Longitude"] = community_services_filtered_gdf["geometry"].x
        community_services_filtered_gdf["Latitude"] = community_services_filtered_gdf["geometry"].y

        tree = KDTree(community_services_filtered_gdf[["Longitude", "Latitude"]].values)
        distances, indices = tree.query(postal_codes_with_assessed_values_gdf[["Longitude", "Latitude"]].values, k = 1)

        postal_codes_with_assessed_values_gdf[f"Distance To Closest {service}"] = distances

    logger.info("create_postal_codes_with_assessed_values_gdf(): community services merged successfully.")

    ctrain_stops["Longitude"] = ctrain_stops["geometry"].x
    ctrain_stops["Latitude"] = ctrain_stops["geometry"].y

    ctrain_tree = KDTree(ctrain_stops[["Longitude", "Latitude"]].values)
    c_train_distances, indices = ctrain_tree.query(postal_codes_with_assessed_values_gdf[["Longitude", "Latitude"]].values, k = 1)

    postal_codes_with_assessed_values_gdf[f"Distance To Closest CTrain Station"] = c_train_distances

    bus_stops["Longitude"] = bus_stops["geometry"].x
    bus_stops["Latitude"] = bus_stops["geometry"].y

    bus_stop_tree = KDTree(bus_stops[["Longitude", "Latitude"]].values)
    bus_stop_distances, indices = bus_stop_tree.query(postal_codes_with_assessed_values_gdf[["Longitude", "Latitude"]].values, k = 1)

    postal_codes_with_assessed_values_gdf[f"Distance To Closest Bus Stop"] = bus_stop_distances

    postal_codes_with_assessed_values_gdf["1 KM Buffer"] = postal_codes_with_assessed_values_gdf.geometry.buffer(1000)

    postal_codes_with_assessed_values_gdf["School Count Within 1KM"] = 0
    postal_codes_with_assessed_values_gdf["Services Count Within 1KM"] = 0

    for index, postal_code in postal_codes_with_assessed_values_gdf.iterrows():
        buffer = postal_code["1 KM Buffer"]

        school_count = schools_gdf[schools_gdf.within(buffer)].shape[0]
        postal_codes_with_assessed_values_gdf.at[index, "School Count Within 1KM"] = school_count

        service_count = community_services_gdf[community_services_gdf.within(buffer)].shape[0]
        postal_codes_with_assessed_values_gdf.at[index, "Services Count Within 1KM"] = service_count

    # Drop the columns with the "_right" or "_left" suffix
    columns_with_suffix = [column for column in postal_codes_with_assessed_values_gdf.columns if column.endswith("_right") or column.endswith("_left")]
    postal_codes_with_assessed_values_gdf.drop(columns = columns_with_suffix, inplace = True)

    logger.info("create_postal_codes_with_assessed_values_gdf(): buffer count added successfully.")

    postal_codes_with_assessed_values_gdf = postal_codes_with_assessed_values_gdf.to_crs(COORD_CRS)
    logger.info("create_postal_codes_with_assessed_values_gdf(): CRS changed back to COORD_CRS successfully.")

    logger.info("create_postal_codes_with_assessed_values_gdf(): finished successfully.")

    return postal_codes_with_assessed_values_gdf

def create_land_use_districts_info():
    '''
    Create a DataFrame that contains the land use districts and their respective 
    descriptions.
    '''

    land_use_districts_gdf = gpd.read_postgis(
        "SELECT * FROM land_use_districts;",
        db_engine,
        crs = COORD_CRS,
        geom_col = "geometry"
    )

    land_use_districts_gdf = land_use_districts_gdf.to_crs(UTM_CRS)

    land_use_districts_info = land_use_districts_gdf[
        ["Land Use Code", "Land Use Major"]
    ].drop_duplicates()
    land_use_districts_info.reset_index(drop = True, inplace = True)

    logger.info("create_land_use_districts_info(): finished successfully.")

    return land_use_districts_info

def create_excluded_communities_gdf():
    '''
    Create a GeoDataFrame that contains the communities that should be excluded from the 
    analysis.
    '''

    community_district_boundaries_full = gpd.read_postgis(
        "SELECT * FROM community_district_boundaries_full;",
        db_engine,
        crs = COORD_CRS,
        geom_col = "geometry"
    )

    community_district_boundaries_filtered = gpd.read_postgis(
        "SELECT * FROM community_district_boundaries;",
        db_engine,
        crs = COORD_CRS,
        geom_col = "geometry"
    )

    community_district_boundaries_filtered_gdf = community_district_boundaries_filtered.to_crs(UTM_CRS)

    mask = community_district_boundaries_full["Community Name"].isin(community_district_boundaries_filtered["Community Name"])
    excluded_communities_gdf = community_district_boundaries_full[~mask]

    excluded_communities_gdf = excluded_communities_gdf.drop(columns = ["CREATED_DT", "MODIFIED_DT"])

    logger.info("create_excluded_communities_gdf(): excluded_communities_gdf created successfully.")

    return excluded_communities_gdf

def create_excluded_postal_codes_gdf(excluded_communities_gdf):
    '''
    Create a GeoDataFrame that contains the postal codes that should be excluded from the 
    analysis.

    Args:
    - excluded_communities_gdf: The GeoDataFrame that contains the communities that should 
      be excluded from the analysis.
    '''

    postal_boundaries_gdf = gpd.read_postgis(
        "SELECT * FROM postal_boundaries;", 
        db_engine, 
        crs = COORD_CRS, 
        geom_col = "geometry"
    )

    excluded_postal_codes_gdf = gpd.sjoin(
        postal_boundaries_gdf,
        excluded_communities_gdf,
        how = "inner",
        predicate = "intersects"
    )

    excluded_postal_codes_gdf = excluded_postal_codes_gdf.drop(columns = ["index_right"])

    logger.info("create_excluded_areas_gdf(): excluded_postal_codes_gdf successfully created.")

    return excluded_postal_codes_gdf

def create_NE_transit_stops():
    '''
    Get the transit stops in the northeast area of Calgary.
    '''

    transit_stops_gdf = gpd.read_postgis(
        "SELECT * FROM transit_stops;", 
        db_engine, 
        crs = COORD_CRS, 
        geom_col = "geometry"
    )

    community_district_boundaries_gdf = gpd.read_postgis(
        "SELECT * FROM community_district_boundaries;", 
        db_engine, 
        crs = COORD_CRS, 
        geom_col = "geometry"
    )

    NE_transit_stops = gpd.sjoin(
        transit_stops_gdf,
        community_district_boundaries_gdf,
        how = "inner",
        predicate = "within"
    )

    NE_transit_stops = NE_transit_stops.drop(columns = ["index_right", "CREATED_DT", "MODIFIED_DT", "Status", "TeleRide Number"])

    return NE_transit_stops

total_run_time = 0
operation_count = 0
start_time = time.time()

combined_boundaries_and_profile_data_gdf = create_combined_boundaries_and_profile_data_gdf()
save_to_database(combined_boundaries_and_profile_data_gdf, "combined_boundaries_and_profile_data", True)
operation_count += 1

logger.info("combined_boundaries_and_profile_data_gdf successfully saved to the database.")

postal_codes_with_assessed_values_gdf = create_postal_codes_with_assessed_values_gdf(
    combined_boundaries_and_profile_data_gdf
)
save_to_database(postal_codes_with_assessed_values_gdf, "postal_codes_with_assessed_values", True)
operation_count += 1

logger.info("postal_codes_with_assessed_values_gdf successfully saved to the database.")

land_use_districts_info_df = create_land_use_districts_info()
save_to_database(land_use_districts_info_df, "land_use_districts_info")
operation_count += 1

logger.info("land_use_districts_info_df successfully saved to the database.")

excluded_communities_gdf = create_excluded_communities_gdf()
save_to_database(excluded_communities_gdf, "excluded_communities_gdf", True)
operation_count += 1

logger.info("excluded_communities_gdf successfully saved to the database.")

excluded_postal_codes_gdf = create_excluded_postal_codes_gdf(excluded_communities_gdf)
save_to_database(excluded_postal_codes_gdf, "excluded_postal_codes_gdf", True)
operation_count += 1

NE_transit_stops_gdf = create_NE_transit_stops()
save_to_database(NE_transit_stops_gdf, "ne_transit_stops_gdf", True)
operation_count += 1

logger.info("excluded_postal_codes_gdf successfully saved to the database.")

end_time = time.time()
total_run_time = end_time - start_time
print(f"Total run time: {total_run_time:.2f} seconds")
print(f"Average time per operation: {total_run_time / operation_count:.2f} seconds")

logger.info("Success! Data joiner finished successfully.")
