import binascii
from sklearn.metrics import silhouette_score, calinski_harabasz_score, davies_bouldin_score
from shapely import wkb, wkt
from typing import List, Union
from models import KMeansPostalModelInput, KMeansCommunityModelInput

def wkb_to_wkt(wkb_hex):
    """
    Converts Well-Known Binary (WKB) to Well-Known Text (WKT). Spatial data is stored as WKB instead of WKT in the database to save space.

    Parameters:
    wkb_hex: The WKB to be converted into WKT
    """
    return wkb.loads(binascii.unhexlify(wkb_hex))

def evaluate_clustering_performance(X, cluster_labels):
    """
    Evaluate clustering performance using various metrics.

    X: The original data used for clustering - should be scaled.
    cluster_labels: The cluster labels assigned to each data point.
    """

    # Silhouette coefficient: Higher is better (-1 to 1)
    silhouette_avg = silhouette_score(X, cluster_labels)

    # Calinski-Harabasz Index: Higher is better
    calinski_harabasz = calinski_harabasz_score(X, cluster_labels)

    # Davies-Bouldin Index: Lower is better
    davies_bouldin = davies_bouldin_score(X, cluster_labels)

    print(f"Silhouette Coefficient: {silhouette_avg:.3f}")
    print(f"Calinski-Harabasz Index: {calinski_harabasz:.3f}")
    print(f"Davies-Bouldin Index: {davies_bouldin:.3f}")

    return (silhouette_avg, calinski_harabasz, davies_bouldin)

postal_features_dict = {
    "Median Assessed Value": "median_assessed_value",
    "Median Land Size": "median_land_size",
    "Distance To Closest Elementary": "distance_to_closest_elementary",
    "Distance To Closest Junior High": "distance_to_closest_junior_high",
    "Distance To Closest Senior High": "distance_to_closest_senior_high",
    "Distance To Closest Community Centre": "distance_to_closest_community_centre",
    "Distance To Closest Attraction": "distance_to_closest_attraction",
    "Distance To Closest Visitor Info": "distance_to_closest_visitor_info",
    "Distance To Closest Court": "distance_to_closest_court",
    "Distance To Closest Library": "distance_to_closest_library",
    "Distance To Closest Hospital": "distance_to_closest_hospital",
    "Distance To Closest PHS Clinic": "distance_to_closest_phs_clinic",
    "Distance To Closest Social Dev Ctr": "distance_to_closest_social_dev_centre",
    "Distance To Nearest Bus Stop": "distance_to_nearest_bus_stop",
    "Distance To Nearest CTrain Station": "distance_to_nearest_ctrain_station",
    "School Count Within 1KM": "school_count_within_1km",
    "Services Count Within 1KM": "service_count_within_1km",
}

community_features_dict = {
    "Count of Population in Private Households": "count_of_population_in_private_households",
    "Median Household Income": "median_household_income",
    "Count of Population considered Low Income": "count_of_population_considered_low_income",
    "Count of Private Households": "count_of_private_households",
    "Count of Owner Households": "count_of_owner_households",
    "Count of Renter Households": "count_of_renter_households",
    "Count of Private Households With Income": "count_of_private_households_with_income",
    "Count of Households with LT 30 Pct of Total Income on Shelter": "count_of_households_with_lt_30_pct_of_total_income_on_shelter",
    "Count of Households with GT 30 Pct of Total Income on Shelter": "count_of_households_with_gt_30_pct_of_total_income_on_shelter",
    "Median Owner Monthly Shelter Cost": "median_owner_monthly_shelter_cost",
    "Median Renter Monthly Shelter Cost": "median_renter_monthly_shelter_cost",
    "Count of Households that Require Maintenance or Minor Repairs": "count_of_households_that_require_maintenance",
    "Count of Households that Require Major Repairs": "count_of_households_that_require_major_repairs",
    "Count of Suitable Households": "count_of_suitable_households",
    "Count of Unsuitable Households": "count_of_unsuitable_households",
    "Community Crime Count 2023": "community_crime_count",
    "Community Disorder Count 2023": "community_disorder_count",
    "Transit Stops Count": "transit_stops_count",
}

def get_selected_features(model: Union[KMeansPostalModelInput, KMeansCommunityModelInput], feature_dict: str) -> List[str]:
    """
    Get the selected features from the model input and the feature dictionary.

    model: The model input.
    feature_dict: The feature dictionary.
    """

    if feature_dict == "postal":
        feature_dict = postal_features_dict
    elif feature_dict == "community":
        feature_dict = community_features_dict

    selected_features = []

    for field_name, field_value in model.dict().items():
        if field_value:
            for feature_name, model_field_name in feature_dict.items():
                if model_field_name == field_name:
                    selected_features.append(feature_name)
                    break

    return selected_features
