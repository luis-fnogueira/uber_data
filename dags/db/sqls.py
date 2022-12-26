class Sqls:

    CREATE_TABLE_RAW_DATA = """
    

    CREATE TABLE IF NOT EXISTS uber_data.raw_data (
    "City" varchar NULL,
    "Product Type" varchar NULL,
    "Trip or Order Status" varchar NULL,
    "Request Time" timestamp NULL,
    "Begin Trip Time" timestamp NULL,
    "Begin Trip Lat" float8 NULL,
    "Begin Trip Lng" float8 NULL,
    "Begin Trip Address" varchar NULL,
    "Dropoff Time" timestamp NULL,
    "Dropoff Lat" float8 NULL,
    "Dropoff Lng" float8 NULL,
    "Dropoff Address" varchar NULL,
    "Distance (miles)" float8 NULL,
    "Fare Amount" float8 NULL,
    "Fare Currency" varchar NULL
    );"""


    READ_RAW_DATA = "SELECT * FROM uber_data.raw_data;"

    CREATE_TABLE_STRUCTURED_DATA = """
    
        CREATE TABLE IF NOT EXISTS uber_data.structured_data (
            "City" varchar NULL,
            "Product Type" varchar NULL,
            "Trip or Order Status" varchar NULL,
            "Request Time" timestamp NULL,
            "Begin Trip Time" timestamp NULL,
            "Begin Trip Lat" float8 NULL,
            "Begin Trip Lng" float8 NULL,
            "Begin Trip Address" varchar NULL,
            "Dropoff Time" timestamp NULL,
            "Dropoff Lat" float8 NULL,
            "Dropoff Lng" float8 NULL,
            "Dropoff Address" varchar NULL,
            "Distance (miles)" float8 NULL,
            "Fare Amount" float8 NULL,
            "Fare Currency" varchar NULL
            );
    
    """