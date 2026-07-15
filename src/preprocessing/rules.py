"""
Cleaning rules for GTFS tables.

This module defines the required columns for each GTFS table.
Rows missing values in these columns will be removed during preprocessing.
"""

REQUIRED_COLUMNS = {
    "agency": ["agency_id"],
    "calendar": ["service_id"],
    "calendar_dates": ["service_id", "date"],
    "feed_info": [],
    "frequencies": ["trip_id"],
    "routes": ["route_id"],
    "shapes": ["shape_id"],
    "stop_times": [
        "trip_id",
        "arrival_time",
        "departure_time",
        "stop_id",
    ],
    "stops": [
        "stop_id",
        "stop_lat",
        "stop_lon",
    ],
    "trips": [
        "trip_id",
        "route_id",
        "service_id",
    ],
}