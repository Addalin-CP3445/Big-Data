import os
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType
from graphframes import GraphFrame

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12") \
        .appName("graphframes") \
        .getOrCreate()

    sqlContext = SQLContext(spark)
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
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

    # ---------------------------Task 8---------------------------------------------------

# part 1
    vertex_schema = StructType([
        StructField("LocationID", StringType(), True),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True)
    ])

    edge_schema = StructType([
        StructField("pickup_location", StringType(), True),
        StructField("dropoff_location", StringType(), True),
    ])

# part 2
    # Vertices DataFrame
    verticesDF = taxi_zone_lookup_df.selectExpr("LocationID as id", "Borough", "Zone", "service_zone")
    
    # Edges DataFrame
    edgesDF = fully_joined_df.selectExpr("pickup_location as src", "dropoff_location as dst")

# part 3
    # Create a graph
    graph = GraphFrame(verticesDF, edgesDF)


# part 5
    
    # Perform PageRank
    page_rank_df = graph.pageRank(resetProbability=0.17, tol=0.01)
    
    # Sort vertices by descending PageRank
    sorted_page_rank_df = page_rank_df.vertices.select("id", "pagerank").orderBy("pagerank", ascending=False)
    
    # Show top 5 results
    sorted_page_rank_df.show(5)








