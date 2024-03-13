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
import requests
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
logfile = os.path.join(log_directory, f"P1_logs_{now.strftime('%Y-%m-%d')}.log")
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

def construct_dataset_url(dataset_id):
    """
    This function constructs the URL for a City of Calgary Open Data Portal dataset with 
    the specified ID.

    Args:
    - dataset_id: The ID of the dataset on the City of Calgary Open Data Portal
    """
    base_url = "https://data.calgary.ca/api/views/"
    access_type = "/rows.csv?accessType=DOWNLOAD&api_foundry=true"
    return f"{base_url}{dataset_id}{access_type}"

datasets_info = {
    "building_permits": {
        "url": construct_dataset_url("c2es-76ed"),
    },
    "community_crime_statistics": {
        "url": construct_dataset_url("78gh-n26t"),
        "filters": {"Year": 2023},
        "group_by": "Community Name",
        "columns_to_remove": ["Sector", 
                              "Resident Count", 
                              "ID", 
                              "Community Center Point", 
                              "Year", 
                              "Month", 
                              "Category", 
                              "Date"],
        "columns_to_rename": {"Crime Count": "Community Crime Count 2023"}
    },
    "community_disorder_statistics": {
        "url": construct_dataset_url("h3h6-kgme"),
        "filters": {"Year": 2023},
        "group_by": "Community Name",
        "columns_to_remove": ["Year", 
                              "Month", 
                              "Category"],
        "columns_to_rename": {"Event Count": "Community Disorder Count 2023"},
    },
    "community_district_boundaries": {
        "url": construct_dataset_url("surr-xmvs"),
        "filters": {"SECTOR": "NORTHEAST", "CLASS": "Residential"},
        "filters_exclude": {"NAME": "HOMESTEAD"},
        "columns_to_remove": ["CLASS_CODE", 
                              "COMM_CODE", 
                              "COMM_STRUCTURE"],
        "columns_to_rename": {"CLASS": "Class", 
                              "NAME": "Community Name", 
                              "SECTOR": "Sector", 
                              "MULTIPOLYGON": "geometry"},
        "drop_if_empty": "SRG",
        "convert_to_gpd": True,
    },
    "community_district_boundaries_full": {
        "url": construct_dataset_url("surr-xmvs"),
        "filters": {"SECTOR": "NORTHEAST"},
        "columns_to_remove": ["CLASS_CODE", 
                              "COMM_CODE", 
                              "COMM_STRUCTURE"],
        "columns_to_rename": {"CLASS": "Class", 
                              "NAME": "Community Name", 
                              "SECTOR": "Sector", 
                              "MULTIPOLYGON": "geometry"},
        "convert_to_gpd": True,
    },
    "community_profiles": {
        "local_path": "../data/Communities_Profile_Data_Extracted.csv",
        "columns_to_float": ["Median Household Income", 
                             "Median Owner Monthly Shelter Cost", 
                             "Median Renter Monthly Shelter Cost"],
    },
    "community_services": {
        "url": construct_dataset_url("x34e-bcjz"),
        "filters_exclude": {"TYPE": "Visitor_Info"},
        "columns_to_rename": {"TYPE": "Type", 
                              "NAME": "Name", 
                              "ADDRESS": "Address", 
                              "COMM_CODE": "Community Code", 
                              "POINT": "geometry"},
        "convert_to_gpd": True,
    },
    "current_year_property_assessments": {
        "url": construct_dataset_url("4bsw-nn7w"),
        "columns_to_remove": ["ROLL_NUMBER", 
                              "ASSESSMENT_CLASS", 
                              "ASSESSMENT_CLASS_DESCRIPTION", 
                              "COMM_CODE", "NR_ASSESSED_VALUE", 
                              "FL_ASSESSED_VALUE", "YEAR_OF_CONSTRUCTION", 
                              "MOD_DATE", "SUB_PROPERTY_USE", 
                              "UNIQUE_KEY", 
                              "LAND_SIZE_SF", 
                              "LAND_SIZE_AC"],
        "columns_to_rename": {"MULTIPOLYGON": "geometry"},
        "convert_to_gpd": True,
    },
    "development_permits": {
        "url": construct_dataset_url("6933-unw5"),
    },
    "land_use_districts": {
        "url": construct_dataset_url("qe6k-p9nh"),
        "columns_to_remove": ["DESCRIPTION", 
                              "GENERALIZE", 
                              "DC_BYLAW", 
                              "DC_SITE_NO", 
                              "DENSITY", 
                              "HEIGHT", 
                              "FAR"],
        "columns_to_rename": {"LU_BYLAW": "Land Use Bylaw", 
                              "LU_CODE": "Land Use Code", 
                              "LABEL": "Land Use Label", 
                              "MAJOR": "Land Use Major", 
                              "MULTIPOLYGON": "geometry"},
        "convert_to_gpd": True,
    },
    "postal_boundaries": {
        "local_path_gpd": "../data/LDU/LDU.shp",
        "columns_to_remove": ["UID", 
                              "PCA_ID", 
                              "PROV", 
                              "PREC_CODE", 
                              "PCA_COUNT", 
                              "DOM_PCA", 
                              "MULTI_PC", 
                              "DEL_M_ID", 
                              "ACCURACY", 
                              "ACQ_TECH", 
                              "SHAPE_Leng", 
                              "SHAPE_Area"],
        "columns_to_rename": {"POSTALCODE": "Postal Code", 
                              "MUNICIPAL": "City", 
                              "LONGITUDE": "Longitude", 
                              "LATITUDE": "Latitude"},
        "convert_to_gpd": True,
    },
    "schools": {
        "url": construct_dataset_url("fd9t-tdn2"),
        "columns_to_remove": ["CITY", 
                              "PROVINCE", 
                              "PHONE_NO", 
                              "FAX_NO", 
                              "EMAIL", 
                              "GLOBAL_ID", 
                              "DATA_SOURCE"],
        "columns_to_rename": {"BOARD": "Board", 
                              "NAME": "Name", 
                              "ADDRESS_AB": "Address", 
                              "POSTAL_COD": "Postal Code", 
                              "GRADES": "Grades", 
                              "POSTSECOND": "Post Secondary", 
                              "ELEM": "Elementary", 
                              "JUNIOR_H": "Junior High", 
                              "SENIOR_H": "Senior High", 
                              "POINT": "geometry"},
        "filters": {"BOARD": "The Calgary School Division"},
        "convert_to_gpd": True,
    },
    "transit_stops": {
        "url": construct_dataset_url("muzh-c9qc"),
        "columns_to_remove": ["CREATE_DT_UTC", 
                              "MOD_DT_UTC", 
                              "GLOBALID"],
        "columns_to_rename": {"TELERIDE_NUMBER": "TeleRide Number", 
                              "STOP_NAME": "Stop Name", 
                              "STATUS": "Status",
                              "POINT": "geometry"},
        "filters": {"STATUS": "ACTIVE"},
        "convert_to_gpd": True,
    },
    "vacant_apartments": {
        "url": construct_dataset_url("rkfr-buzb"),
        "columns_to_rename": {"name": "CommunityName",
                              "APT_VACANT": "Number of Vacant Apartments",
                              "CNV_VACANT": "Number of Vacant Converted Structures",
                              "DUP_VACANT": "Number of Vacant Duplexes",
                              "MFH_VACANT": "Number of Vacant Multi-Family Homes",
                              "MUL_VACANT": "Number of Vacant Multi-Plexes",
                              "OTH_VACANT": "Number of Vacant Other Structures",
                              "SF_VACANT": "Number of Vacant Single-Family Homes",
                              "TWN_VACANT": "Number of Vacant Townhouses",
                              "multipolygon": "geometry"},
        "columns_to_remove": ["SF_UC", "SF_NA", "OTH_STRTY", "DWELSZ_1", "DWELSZ_2", 
                        "DWELSZ_3", "DWELSZ_4_5", "DWELSZ_6", "MALE_CNT", "FEMALE_CNT", 
                        "MALE_0_4", "MALE_5_14", "MALE_15_19", "MALE_20_24", "MALE_25_34", 
                        "MALE_35_44", "MALE_45_54", "MALE_55_64", "MALE_65_74", "MALE_75", 
                        "FEM_0_4", "FEM_5_14", "FEM_15_19", "FEM_20_24", "FEM_25_34", "FEM_35_44", 
                        "FEM_45_54", "FEM_55_64", "FEM_65_74", "FEM_75", "MF_0_4", "MF_5_14", 
                        "MF_15_19", "MF_20_24", "MF_25_34", "MF_35_44", "MF_45_54", "MF_55_64", 
                        "MF_65_74", "MF_75", "OTHER_CNT", "OTHER_0_4", "OTHER_5_14", "OTHER_15_19", 
                        "OTHER_20_24", "OTHER_25_34", "OTHER_35_44", "OTHER_45_54", "OTHER_55_64", 
                        "OTHER_65_74", "OTHER_75", "TWN_UC", "TWN_NA", "SF_NO_RES", "SF_OCCPD", 
                        "SF_OWNED", "SF_PERSON", "OTH_UC", "OTH_NA", 
                        "TWN_NO_RES", "TWN_OCCPD", "TWN_OWNED", "TWN_PERSON", "MUL_UC", "MUL_NA", 
                        "OTH_NO_RES", "OTH_OCCPD", "MFH_UC", "MFH_NA", "MUL_NO_RES", "MUL_OCCPD", 
                        "DUP_UC", "DUP_NA", "MFH_NO_RES", "MFH_OCCPD", "MFH_OWNED", "MFH_PERSON", 
                        "MUL_OWNED", "MUL_PERSON","OTH_OWNED", "OTH_PERSON", "CNV_UC", "CNV_NA", 
                        "DUP_NO_RES", "DUP_OCCPD", "DUP_OWNED", "DUP_PERSON", "APT_UC", "APT_NA", 
                        "CNV_NO_RES", "CNV_OCCPD", "CNV_OWNED", "CNV_PERSON", "COMM_STRUCTURE", 
                        "CNSS_YR", "FOIP_IND", "RES_CNT", "DWELL_CNT", "PRSCH_CHLD", "ELECT_CNT", 
                        "EMPLYD_CNT", "OWNSHP_CNT", "DOG_CNT", "CAT_CNT", "PUB_SCH", "SEP_SCH", 
                        "PUBSEP_SCH", "OTHER_SCH", "UNKNWN_SCH", "SING_FAMLY", 
                        "DUPLEX", "MULTI_PLEX", "APARTMENT", "TOWN_HOUSE", 
                        "MANUF_HOME", "CONV_STRUC", "COMUNL_HSE", "RES_COMM", 
                        "OTHER_RES", "NURSING_HM", "OTHER_INST", "HOTEL_CNT", 
                        "OTHER_MISC", "APT_NO_RES", "APT_OCCPD", "APT_OWNED", "APT_PERSON"],
    },
}

files = []

def process_and_store_dataset(dataset_name, dataset_details):
    """
    This function retrieves and processes the dataset specified in the dataset_details dictionary.

    If a URL is specified, the dataset is downloaded and stored in a CSV file. If a local_path is 
    specified, the dataset is read from the local file. If a local_path_gpd is specified, 
    the dataset is read from the local file as a GeoDataFrame.

    The dataset is then processed according to the filters, group_by, columns_to_remove, 
    columns_to_rename, drop_if_empty, columns_to_float, and convert_to_gpd specifications in the
    dataset_details dictionary.

    By default, the dataset is stored in the database as a DataFrame. If convert_to_gpd is set 
    to True, the dataset is stored as a GeoDataFrame. GeoDataFrames are stored in the database 
    as PostGIS tables and use the UTM coordinate reference system.

    Args:
    - dataset_name: The name of the dataset
    - dataset_details: A dictionary containing the details of the dataset to be processed
    """

    logger.info(f"Retrieving and processing dataset: {dataset_name}")

    # Check if the dataset is to be downloaded from a URL
    if "url" in dataset_details and dataset_details["url"]:
        response = requests.get(dataset_details["url"])
        filename = f"{dataset_name}.csv"

        if not (200 <= response.status_code < 300):
            logger.error(f"Failed to download {dataset_name} dataset. Status code: {response.status_code}")
            sys.exit(1)

        files.append(filename)

        with open(filename, "wb") as file:
            file.write(response.content)

        try:
            df = pd.read_csv(filename)
        except FileNotFoundError:
            logger.error(f"File {filename} not found.")
            sys.exit(1)
        except PermissionError:
            logger.error(f"Permission denied to open {filename}.")
            sys.exit(1)
        except pd.errors.ParserError:
            logger.error(f"Error parsing {filename}.")
            sys.exit(1)
        except Exception as e:
            logger.exception(f"An error occurred reading {filename}: {e}.")
            sys.exit(1)

    # Check if the dataset is to be read from a local file (if it is a CSV)
    elif "local_path" in dataset_details and dataset_details["local_path"]:
        try:
            df = pd.read_csv(dataset_details["local_path"])
        except FileNotFoundError:
            logger.error(f"File {dataset_details['local_path']} not found.")
        except PermissionError:
            logger.error(f"Permission denied for file {dataset_details['local_path']}.")
        except pd.errors.ParserError:
            logger.error(f"Error parsing the CSV file {dataset_details['local_path']}.")
        except Exception as e:
            logger.exception(f"An unexpected error occurred while reading {dataset_details['local_path']}: {e}")

    # Check if the dataset is to be read from a local file (if it is a GeoDataFrame)
    elif "local_path_gpd" in dataset_details and dataset_details["local_path_gpd"]:
        try:
            df = gpd.read_file(dataset_details["local_path_gpd"])
        except FileNotFoundError:
            logger.error(f"File {dataset_details['local_path_gpd']} not found.")
        except PermissionError:
            logger.error(f"Permission denied for file {dataset_details['local_path_gpd']}.")
        except pd.errors.ParserError:
            logger.error(f"Error parsing the CSV file {dataset_details['local_path_gpd']}.")
        except Exception as e:
            logger.exception(f"An unexpected error occurred while reading {dataset_details['local_path_gpd']}: {e}")

        df = df.set_crs("EPSG:4326")
        df = df.set_geometry("geometry")

    # Filter specific columns given in the dataset_details dictionary
    if "filters" in dataset_details and dataset_details["filters"]:
        for column, value in dataset_details["filters"].items():
            df = df[df[column] == value]

    # Filter specific columns to exclude given in the dataset_details dictionary
    if "filters_exclude" in dataset_details and dataset_details["filters_exclude"]:
        for column, value in dataset_details["filters_exclude"].items():
            df = df[df[column] != value]

     # Group the dataset by the specified column
    if "group_by" in dataset_details and dataset_details["group_by"]:
        df = df.groupby(dataset_details["group_by"]).sum().reset_index()

    # Remove specific columns given in the dataset_details dictionary
    if "columns_to_remove" in dataset_details and dataset_details["columns_to_remove"]:
        df.drop(columns = dataset_details["columns_to_remove"], inplace = True)

    # Rename specific columns given in the dataset_details dictionary
    if "columns_to_rename" in dataset_details and dataset_details["columns_to_rename"]:
        df.rename(columns = dataset_details["columns_to_rename"], inplace = True)

    # Drop rows with empty values in the specified column
    if "drop_if_empty" in dataset_details and dataset_details["drop_if_empty"]:
        df = df.dropna(subset = [dataset_details["drop_if_empty"]])

    # Convert specific columns to float
    if "columns_to_float" in dataset_details and dataset_details["columns_to_float"]:
        for column in dataset_details["columns_to_float"]:
            df[column] = df[column].astype(float)

    # Convert the DataFrame to a GeoDataFrame and convert the geometry column to the UTM CRS
    if "convert_to_gpd" in dataset_details and dataset_details["convert_to_gpd"]:
        if not df.empty and isinstance(df["geometry"].iloc[0], str):
            df["geometry"] = gpd.GeoSeries.from_wkt(df["geometry"])
        df = gpd.GeoDataFrame(df, geometry = "geometry", crs = COORD_CRS)
        df = df.to_crs(COORD_CRS)

    return df

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

total_run_time = 0

for dataset_name, dataset_details in datasets_info.items():
    start_time = time.time()

    df = process_and_store_dataset(dataset_name, dataset_details)

    if "convert_to_gpd" in dataset_details and dataset_details["convert_to_gpd"]:
        save_to_database(df, dataset_name, True)
    else:
        save_to_database(df, dataset_name)
        
    end_time = time.time()

    run_time = end_time - start_time
    total_run_time += run_time

for file in files:
    os.remove(file)

logger.info("Data retrieval and storage process completed.")

print(f"Total run time: {total_run_time:.2f} seconds")
print(f"Average run time: {total_run_time / len(datasets_info):.2f} seconds")
