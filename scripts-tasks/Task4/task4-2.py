import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, avg, col

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Task4_2")\
        .getOrCreate()

    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    # configure access to AWS S3 bucket
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # location of shared s3 bucket path
    shared_bucket_path = "s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/"

    # Load rideshare_data.csv
    rideshare_data_df = spark.read.format("csv").option("header", "true").load(shared_bucket_path + "rideshare_data.csv")
 
    # Load taxi_zone_lookup.csv
    taxi_zone_lookup_df = spark.read.format("csv").option("header", "true").load(shared_bucket_path + "taxi_zone_lookup.csv")
    
    # joining rideshare_data_df and taxi_zone_lookup_df using pickup_location
    # and renaming the columns 
    pick_up_joined_df = rideshare_data_df.join(taxi_zone_lookup_df, rideshare_data_df.pickup_location == taxi_zone_lookup_df.LocationID, "inner")
    pick_up_joined_df = pick_up_joined_df.withColumnRenamed("Borough", "Pickup_Borough") \
        .withColumnRenamed("Zone", "Pickup_Zone") \
        .withColumnRenamed("service_zone", "Pickup_service_zone ")
    pick_up_joined_df = pick_up_joined_df.drop("LocationID")

    # joining pick_up_joined_df and taxi_zone_lookup_df using dropoff_location
    # and renaming the columns 
    fully_joined_df = pick_up_joined_df.join(taxi_zone_lookup_df, pick_up_joined_df.dropoff_location  == taxi_zone_lookup_df.LocationID, "inner")
    fully_joined_df = fully_joined_df.withColumnRenamed("Borough", "Dropoff_Borough") \
        .withColumnRenamed("Zone", "Dropoff_Zone") \
        .withColumnRenamed("service_zone", "Dropoff_service_zone")
    fully_joined_df = fully_joined_df.drop("LocationID")

    # converting unix timestamp of 'date' column to 'yyyy-mm-dd' format
    fully_joined_df = fully_joined_df.withColumn("date", from_unixtime(col("date"), "yyyy-MM-dd"))

    # ------------------Task 4 part 2---------------------------------------
   
    # selecting only needed columns 
    time_of_day_df = fully_joined_df.select("time_of_day", "trip_length")

    # filtering dataframe by time_of_day
    evening_df = time_of_day_df.filter(col("time_of_day") == "evening")
    morning_df = time_of_day_df.filter(col("time_of_day") == "morning")
    afternoon_df = time_of_day_df.filter(col("time_of_day") == "afternoon")
    night_df = time_of_day_df.filter(col("time_of_day") == "night")

    # creating list of tuples of average trip_length per time_of_day
    data = [
        ("evening", evening_df.select(avg("trip_length")).collect()[0][0]),
        ("morning", morning_df.select(avg("trip_length")).collect()[0][0]),
        ("afternoon", afternoon_df.select(avg("trip_length")).collect()[0][0]),
        ("night", night_df.select(avg("trip_length")).collect()[0][0])
    ]

    # creating new dataframe with time_of_day and average_drive_total_pay
    avg_trip_length_df = spark.createDataFrame(data, ["time_of_day", "average_trip_length"])

    # ordering dataframe by average_drive_total_pay
    avg_trip_length_df = avg_trip_length_df.orderBy(col("average_trip_length").desc())

    # printing resulting dataframe
    avg_trip_length_df.show()

    # Stop the Spark session
    spark.stop()