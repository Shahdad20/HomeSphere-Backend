import os
from dotenv import load_dotenv
import pytest
import requests

# Load the environment variables
load_dotenv()

API_KEY = os.environ.get("API_KEY")

headers = {
    "AccessToken": API_KEY  
}

base_url = "http://home-sphere.ca/api/"

def test_get_building_permits():
    dataset = "building_permits"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"PermitNum", "StatusCurrent", "AppliedDate", "IssuedDate",
       "CompletedDate", "PermitType", "PermitTypeMapped", "PermitClass",
       "PermitClassGroup", "PermitClassMapped", "WorkClass", "WorkClassGroup",
       "WorkClassMapped", "Description", "ApplicantName", "ContractorName",
       "HousingUnits", "EstProjectCost", "TotalSqFt", "OriginalAddress",
       "CommunityCode", "CommunityName", "Latitude", "Longitude",
       "LocationCount", "LocationTypes", "LocationAddresses", "LocationsWKT",
       "LocationsGeoJSON", "Point"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)

def test_get_combined_boundaries_and_profile_data():
    dataset = "combined_boundaries_and_profile_data"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"Class", "Community Name", "Sector", "SRG", "geometry",
       "Count of Population in Private Households", "Median Household Income",
       "Count of Population considered Low Income",
       "Count of Private Households", "Count of Owner Households",
       "Count of Renter Households", "Count of Private Households With Income",
       "Count of Households with LT 30 Pct of Total Income on Shelter",
       "Count of Households with GT 30 Pct of Total Income on Shelter",
       "Median Owner Monthly Shelter Cost",
       "Median Renter Monthly Shelter Cost",
       "Count of Households that Require Maintenance or Minor Repairs",
       "Count of Households that Require Major Repairs",
       "Count of Suitable Households", "Count of Unsuitable Households",
       "Count of Most Common Dwelling Type",
       "Count of Second Most Common Dwelling Type",
       "Count of Third Most Common Dwelling Type",
       "Most Common_Apartment less than 5 storeys", "Most Common_Row",
       "Most Common_Single-detached", "Second Most Common_Apartment duplex",
       "Second Most Common_Apartment less than 5 storeys",
       "Second Most Common_Apartment more than 5 storeys",
       "Second Most Common_Movable dwelling", "Second Most Common_Row",
       "Second Most Common_Semi-detached",
       "Second Most Common_Single-detached",
       "Third Most Common_Apartment duplex",
       "Third Most Common_Apartment less than 5 storeys",
       "Third Most Common_Apartment more than 5 storeys",
       "Third Most Common_Row", "Third Most Common_Semi-detached",
       "Community Crime Count 2023", "Community Disorder Count 2023"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)

def test_get_community_crime_statistics():
    dataset = "community_crime_statistics"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"Community Name", "Community Crime Count 2023"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)

def test_get_community_disorder_statistics():
    dataset = "community_disorder_statistics"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"Community Name", "Community Disorder Count 2023"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)

def test_get_community_district_boundaries():
    dataset = "community_district_boundaries"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"Class", "Community Name", "Sector", "SRG", "geometry"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)

def test_get_community_profiles():
    dataset = "community_profiles"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"Community Name", "Count of Population in Private Households",
       "Median Household Income", "Count of Population considered Low Income",
       "Count of Private Households", "Count of Owner Households",
       "Count of Renter Households", "Count of Private Households With Income",
       "Count of Households with LT 30 Pct of Total Income on Shelter",
       "Count of Households with GT 30 Pct of Total Income on Shelter",
       "Median Owner Monthly Shelter Cost",
       "Median Renter Monthly Shelter Cost",
       "Count of Households that Require Maintenance or Minor Repairs",
       "Count of Households that Require Major Repairs",
       "Count of Suitable Households", "Count of Unsuitable Households",
       "Most Common Dwelling Type", "Count of Most Common Dwelling Type",
       "Second Most Common Dwelling Type",
       "Count of Second Most Common Dwelling Type",
       "Third Most Common Dwelling Type",
       "Count of Third Most Common Dwelling Type"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)

def test_get_community_services():
    dataset = "community_services"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"Type", "Name", "Address", "Community Code", "geometry"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)

def test_get_current_year_property_assessments():
    dataset = "current_year_property_assessments"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"ROLL_YEAR", "ADDRESS", "ASSESSED_VALUE", "RE_ASSESSED_VALUE",
       "COMM_NAME", "LAND_USE_DESIGNATION", "PROPERTY_TYPE", "LAND_SIZE_SM",
       "geometry"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)

def test_get_development_permits():
    dataset = "development_permits"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"point", "PermitNum", "Address", "Applicant", "Category", 
                        "Description", "ProposedUseCode", "ProposedUseDescription", 
                        "PermittedDiscretionary", "LandUseDistrict", 
                        "LandUseDistrictDescription", "Concurrent LOC",
                        "StatusCurrent", "AppliedDate", "DecisionDate", "ReleaseDate",
                        "MustCommenceDate", "CanceledRefusedDate", "Decision", "DecisionBy",
                        "SDABNumber", "SDABHearingDate", "SDABDecision", "SDABDecisionDate",
                        "CommunityCode", "CommunityName", "Ward", "Quadrant", "Latitude",
                        "Longitude", "LocationCount", "LocationTypes", "LocationAddresses",
                        "LocationsGeoJSON", "LocationsWKT"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)

def test_get_land_use_districts():
    dataset = "land_use_districts"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"Land Use Bylaw", "Land Use Code", "Land Use Label", 
                        "Land Use Major", "geometry"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)


def test_get_land_use_districts_info():
    dataset = "land_use_districts_info"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"Land Use Code", "Land Use Major"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)

def test_get_postal_boundaries():
    dataset = "postal_boundaries"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"Postal Code", "City", "Longitude", "Latitude", "geometry"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)

def test_get_postal_codes_with_assessed_values():
    dataset = "postal_codes_with_assessed_values"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"Postal Code", "City", "Longitude", "Latitude", 
                        "geometry", "Class", "Community Name", "Sector", "SRG", 
                        "Count of Population in Private Households", 
                        "Median Household Income", 
                        "Count of Population considered Low Income",
                        "Count of Private Households", "Count of Owner Households",
                        "Count of Renter Households", "Count of Private Households With Income",
                        "Count of Households with LT 30 Pct of Total Income on Shelter",
                        "Count of Households with GT 30 Pct of Total Income on Shelter",
                        "Median Owner Monthly Shelter Cost",
                        "Median Renter Monthly Shelter Cost",
                        "Count of Households that Require Maintenance or Minor Repairs",
                        "Count of Households that Require Major Repairs",
                        "Count of Suitable Households", "Count of Unsuitable Households",
                        "Count of Most Common Dwelling Type",
                        "Count of Second Most Common Dwelling Type",
                        "Count of Third Most Common Dwelling Type",
                        "Most Common_Apartment less than 5 storeys", "Most Common_Row",
                        "Most Common_Single-detached", "Second Most Common_Apartment duplex",
                        "Second Most Common_Apartment less than 5 storeys",
                        "Second Most Common_Apartment more than 5 storeys",
                        "Second Most Common_Movable dwelling", "Second Most Common_Row",
                        "Second Most Common_Semi-detached",
                        "Second Most Common_Single-detached",
                        "Third Most Common_Apartment duplex",
                        "Third Most Common_Apartment less than 5 storeys",
                        "Third Most Common_Apartment more than 5 storeys",
                        "Third Most Common_Row", "Third Most Common_Semi-detached",
                        "Community Crime Count 2023", "Community Disorder Count 2023",
                        "Median Assessed Value", "Median Land Size", "Land Use Bylaw",
                        "Land Use Code", "Land Use Label", "Land Use Major",
                        "Distance To Closest Elementary", "Distance To Closest Junior High",
                        "Distance To Closest Senior High",
                        "Distance To Closest Community Centre",
                        "Distance To Closest Attraction", "Distance To Closest Visitor Info",
                        "Distance To Closest Court", "Distance To Closest Library",
                        "Distance To Closest Hospital", "Distance To Closest PHS Clinic",
                        "Distance To Closest Social Dev Ctr", "1 KM Buffer",
                        "School Count Within 1KM", "Services Count Within 1KM"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)

def test_get_schools():
    dataset = "schools"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"Board", "Name", "Address", "Postal Code", "Grades", 
                        "Post Secondary", "Elementary", "Junior High", 
                        "Senior High", "geometry"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)

def test_get_vacant_apartments():
    dataset = "vacant_apartments"
    response = requests.get(base_url + dataset, headers = headers)
    data = response.json()

    expected_columns = {"CLASS", "CLASS_CODE", "COMM_CODE", "NAME", "SECTOR", "SRG", 
                        "Number of Vacant Apartments", "Number of Vacant Converted Structures", 
                        "Number of Vacant Duplexes", "Number of Vacant Multi-Family Homes", 
                        "Number of Vacant Multi-Plexes", "Number of Vacant Other Structures", 
                        "Number of Vacant Townhouses", "Number of Vacant Single-Family Homes", 
                        "geometry"}

    assert response.status_code == 200
    assert len(data) > 0
    assert all(expected_columns.issubset(data[0].keys()) for item in data)
