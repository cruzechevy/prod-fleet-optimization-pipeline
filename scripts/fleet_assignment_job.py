from pyspark.sql import SparkSession
from pyspark.sql.functions import col,rand,floor,round,to_timestamp,unix_timestamp,expr,count
from pyspark.sql import functions as F
import pandas as pd
import random
import os 
import yaml

import os

def load_config():
    with open("/app/config/config.yaml", "r") as f:
        return yaml.safe_load(f)

config = load_config()



def create_spark_session():
    spark = SparkSession.builder \
    .appName("FleetOptimization") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .getOrCreate()


    spark.sparkContext.setLogLevel("WARN")

    return spark



def prepare_trips(trips_df,routes_df):
    """
    Filters feasible trips and prioritizes for scheduling
    """

    # Step 1 — keep only feasible trips
    trips_df = trips_df.filter(
        col("estimated_duration_hours") <= col("delivery_deadline_hours")
    )

    # Step 2 — prioritize shorter trips (throughput optimization)
    trips_df = trips_df.orderBy("estimated_duration_hours")

    # Step 3 — limit to capacity (for now fixed)
    trips_df = trips_df.limit(8000)

    start_date = "2026-01-01"
    trips_df = trips_df.withColumn(
    "trip_date",
    F.expr(f"date_add('{start_date}', cast(rand() * 7 as int))")
    )

    trips_df = trips_df.withColumn(
        "planned_start_hour",
        (F.rand() * 24).cast("int")
    )

    routes_df = routes_df.select('route_id','avg_speed_kmph','traffic_factor')

    joined_df = trips_df.join(
        routes_df,
        on="route_id",
        how="left"
    )

    joined_df = joined_df.withColumn(
        "estimated_duration_hours",
        round(col("distance_km")/col("avg_speed_kmph")/col("traffic_factor"),2)
    )

    joined_df = joined_df.withColumn(
        "trip_timestamp",
        to_timestamp(col("trip_date"))
    )

    joined_df = joined_df.withColumn(
        "planned_start_time",
        col("trip_timestamp") + expr("INTERVAL 1 HOUR") * col("planned_start_hour")
    )

    joined_df = joined_df.withColumn(
        "planned_start_time",
        (round(unix_timestamp(col("planned_start_time")) / 1800) * 1800).cast("timestamp")
    )


    joined_df = joined_df.withColumn(
        "trip_end_time",
        col("planned_start_time") + expr("INTERVAL 1 HOUR") * col("estimated_duration_hours")
    )

    joined_df = joined_df.withColumn(
        "trip_end_time",
        (round(unix_timestamp(col("trip_end_time")) / 1800) * 1800).cast("timestamp")
    )


    joined_df = joined_df.withColumn(
        "next_available_time",
        col("trip_end_time") + expr("INTERVAL 1 HOUR") * 24
    )

    trips_df = joined_df

    return trips_df



def run_scheduler(trips_df, drivers_df, trucks_df):
    """
    Runs scheduling logic using Pandas (stateful)
    """

    # Convert to Pandas
    trips_pd = trips_df.orderBy("planned_start_time").toPandas()
    drivers_pd = drivers_df.toPandas()
    trucks_pd = trucks_df.toPandas()

    driver_ids = drivers_pd["driver_id"].tolist()

    # Config
    TOP_K_DRIVERS = 5
    TOP_M_TRUCKS = 3
    REST_HOURS = 8
    FUEL_PRICE = 100
    DRIVER_RATE = 200

    # State
    driver_next_available = {}
    assignments = []

    # Helper functions
    def get_available_drivers(start_time):
        return [
            d for d in driver_ids
            if d not in driver_next_available or start_time >= driver_next_available[d]
        ]

    def get_top_trucks(load):
        valid = trucks_pd[trucks_pd["capacity_tons"] >= load]
        return valid.sort_values("fuel_efficiency_kmpl", ascending=False).head(TOP_M_TRUCKS)

    def compute_cost(trip, truck):
        fuel_cost = (trip["distance_km"] / truck["fuel_efficiency_kmpl"]) * FUEL_PRICE
        driver_cost = trip["estimated_duration_hours"] * DRIVER_RATE
        return fuel_cost + driver_cost

    # Main loop
    for _, trip in trips_pd.iterrows():

        trip_id = trip["trip_id"]
        start = trip["planned_start_time"]
        end = trip["trip_end_time"]
        load = trip["load_tons"]

        available = get_available_drivers(start)

        if not available:
            continue

        selected_drivers = random.sample(
            available,
            min(TOP_K_DRIVERS, len(available))
        )

        top_trucks = get_top_trucks(load)

        if top_trucks.empty:
            continue

        best_cost = float("inf")
        best_driver = None
        best_truck = None

        for driver in selected_drivers:
            for _, truck in top_trucks.iterrows():

                cost = compute_cost(trip, truck)

                if cost < best_cost:
                    best_cost = cost
                    best_driver = driver
                    best_truck = truck["truck_id"]

        assignments.append({
            "trip_id": trip_id,
            "driver_id": best_driver,
            "truck_id": best_truck,
            "planned_start_time": start,
            "trip_end_time": end,
            "total_cost": best_cost
        })

        driver_next_available[best_driver] = end + pd.Timedelta(hours=REST_HOURS)

    return pd.DataFrame(assignments)

def main():
    # 1. Create Spark Session
    spark = create_spark_session()

    print("Spark session created")


    # 2. Read data

    BASE_PATH = os.environ.get("DATA_PATH", config["paths"]["data_path"])

    trips_path = os.path.join(BASE_PATH, config["paths"]["trips"])
    drivers_path = os.path.join(BASE_PATH, config["paths"]["drivers"])
    trucks_path = os.path.join(BASE_PATH, config["paths"]["trucks"])
    routes_path = os.path.join(BASE_PATH, config["paths"]["routes"])
    output_path = os.path.join(BASE_PATH, config["paths"]["output"])
    


    # trips_df = spark.read.parquet(trips_path)
    trips_df = spark.read.format("delta").load(trips_path)
    routes_df = spark.read.format("delta").load(routes_path)
    drivers_df = spark.read.format("delta").load(drivers_path)
    trucks_df = spark.read.format("delta").load(trucks_path)
    
    print("Data loaded")
    print("Trips:", trips_df.count())

    
    # 3. Transform (feasible filter etc.)
    trips_df = prepare_trips(trips_df,routes_df)
    print("Trips after filtering:", trips_df.count())


    # 4. Run scheduler

    assignments_pd = run_scheduler(trips_df, drivers_df, trucks_df)
    print("Assignments created:", len(assignments_pd))

    # 5. Write output
    assignments_df = spark.createDataFrame(assignments_pd)
    #output_path = os.getenv("OUTPUT_PATH","/app/data/silver/final_assignments")
    assignments_df.write.format("delta").mode("overwrite").save(output_path)

    print(f"Assignments written to {output_path}")

if __name__ == "__main__":
    main()