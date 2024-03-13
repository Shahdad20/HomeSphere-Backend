from pydantic import BaseModel

class KMeansPostalModelInput(BaseModel):
    median_assessed_value: bool = True
    median_land_size: bool = True
    distance_to_closest_elementary: bool = True
    distance_to_closest_junior_high: bool = True
    distance_to_closest_senior_high: bool = True
    distance_to_closest_community_centre: bool = True
    distance_to_closest_attraction: bool = True
    distance_to_closest_visitor_info: bool = True
    distance_to_closest_court: bool = True
    distance_to_closest_library: bool = True
    distance_to_closest_hospital: bool = True
    distance_to_closest_phs_clinic: bool = True
    distance_to_closest_social_dev_centre: bool = True
    distance_to_nearest_bus_stop: bool = True
    distance_to_nearest_ctrain_station: bool = True
    school_count_within_1km: bool = True
    service_count_within_1km: bool = True

    n_clusters: int = 3
    random_state: int = 42

class KMeansCommunityModelInput(BaseModel):
    count_of_population_in_private_households: bool = True
    median_household_income: bool = True
    count_of_population_considered_low_income: bool = True
    count_of_private_households: bool = True
    count_of_owner_households: bool = True
    count_of_renter_households: bool = True
    count_of_private_households_with_income: bool = True
    count_of_households_with_lt_30_pct_of_total_income_on_shelter: bool = True
    count_of_households_with_gt_30_pct_of_total_income_on_shelter: bool = True
    median_owner_monthly_shelter_cost: bool = True
    median_renter_monthly_shelter_cost: bool = True
    count_of_households_that_require_maintenance: bool = True
    count_of_households_that_require_major_repairs: bool = True
    count_of_suitable_households: bool = True
    count_of_unsuitable_households: bool = True
    community_crime_count: bool = True
    community_disorder_count: bool = True
    transit_stops_count: bool = True

    n_clusters: int = 3
    random_state: int = 42
