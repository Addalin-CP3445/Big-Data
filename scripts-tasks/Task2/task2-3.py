import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, month, concat
from pyspark.sql.types import FloatType

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Task2_3")\
        .getOrCreate()

    # spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.broadcastTimeout", "600")  # Set timeout to 10 minutes (600 seconds)

    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    shared_bucket_path = "s3a://" + s3_data_repository_bucket + "/ECS765/rideshare_2023/"

    # Load rideshare_data.csv:
    rideshare_data_df = spark.read.format("csv").option("header", "true").load(shared_bucket_path + "rideshare_data.csv")
 
    # Load taxi_zone_lookup.csv:
    taxi_zone_lookup_df = spark.read.format("csv").option("header", "true").load(shared_bucket_path + "taxi_zone_lookup.csv")
    
    pick_up_joined_df = rideshare_data_df.join(taxi_zone_lookup_df, rideshare_data_df.pickup_location == taxi_zone_lookup_df.LocationID, "inner")
    pick_up_joined_df = pick_up_joined_df.withColumnRenamed("Borough", "Pickup_Borough") \
        .withColumnRenamed("Zone", "Pickup_Zone") \
        .withColumnRenamed("service_zone", "Pickup_service_zone")
    pick_up_joined_df = pick_up_joined_df.drop("LocationID")

    fully_joined_df = pick_up_joined_df.join(taxi_zone_lookup_df, pick_up_joined_df.dropoff_location  == taxi_zone_lookup_df.LocationID, "inner")
    fully_joined_df = fully_joined_df.withColumnRenamed("Borough", "Dropoff_Borough") \
        .withColumnRenamed("Zone", "Dropoff_Zone") \
        .withColumnRenamed("service_zone", "Dropoff_service_zone")
    fully_joined_df = fully_joined_df.drop("LocationID")

    fully_joined_df = fully_joined_df.withColumn("date", from_unixtime(col("date"), "yyyy-MM-dd"))

    # ------------------Task 2 3---------------------------------------

    driver_total_pay_df = fully_joined_df.select("business", "date", "driver_total_pay")

    # Extract month from the 'date' column
    driver_total_pay_df = driver_total_pay_df.withColumn("month", month(col("date")))

    # Cast the "driver_total_pay" column to FloatType
    driver_total_pay_df = driver_total_pay_df.withColumn("driver_total_pay", col("driver_total_pay").cast(FloatType()))

    # Create 'business_month' column
    driver_total_pay_df = driver_total_pay_df.withColumn("business_month", concat(col("business"), col("month")))

    # grouping by 'business_month' and adding the driver's earnings
    driver_earnings_df = driver_total_pay_df.groupBy("business_month").sum("driver_total_pay").withColumnRenamed("sum(driver_total_pay)", "driver_earnings")

    # repartitioning the dataframe to a single file
    driver_earnings_df = driver_earnings_df.repartition(1)

    # writing as csv file to the shared bucket 
    driver_earnings_df.write.csv(shared_bucket_path + "total_trip_count_part_3_newest", header=True)

    # Stop the Spark session
    spark.stop()
