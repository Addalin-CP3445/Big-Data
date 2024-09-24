import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, count, lit, concat

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Task7")\
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

    # ------------------Task 7---------------------------------------

    # selecting only needed columns 
    filtered_df = fully_joined_df.select("Pickup_Zone", "Dropoff_Zone", "business")

    # Aggregating the count of routes by Pickup_Zone, Dropoff_Zone, and business 
    route_count_df = filtered_df.groupBy("Pickup_Zone", "Dropoff_Zone", "business").agg(count("*").alias("count"))

    # Grouping the aggregated route counts by Pickup_Zone and Dropoff_Zone
    # pivoting on business to create separate columns for Uber and Lyft counts
    # and filling any missing values with 0
    uber_lyft_df = route_count_df.groupBy("Pickup_Zone", "Dropoff_Zone").pivot("business").sum("count").na.fill(0)

    # Adding a new column 'total_count' that represents the sum of counts for Uber and Lyft
    # ordering the DataFrame by 'total_count' in descending order
    # and limiting the result to the top 10 rows
    uber_lyft_df = uber_lyft_df.withColumn("total_count", col("Uber") + col("Lyft")).orderBy(col("total_count").desc()).limit(10)

    # Adding a new column 'Route' by concatenating Pickup_Zone and Dropoff_Zone columns
    uber_lyft_df = uber_lyft_df.withColumn("Route", concat(uber_lyft_df["Pickup_Zone"], lit(" to "), uber_lyft_df["Dropoff_Zone"]))

    # dropping unnecessary columns
    uber_lyft_df = uber_lyft_df.drop("Pickup_Zone").drop("Dropoff_Zone")

    # printing resulting dataframe 
    uber_lyft_df.show(truncate=False)

    # stop spark session 
    spark.stop()