# import dependent packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.window import Window

# Create Spark Session

spark = SparkSession  \
        .builder  \
        .appName("RetailProjectUpGrad")  \
        .getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

# Read input from Kafka Queue
# https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html

order_json = spark  \
        .readStream  \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
        .option("subscribe", "real-time-project") \
        .option("startingOffsets", "latest")  \
        .load()


# JSON Schema Definition
JSON_Schema = StructType() \
        .add("items", ArrayType(StructType([
        StructField("SKU", StringType()),
        StructField("title", StringType()),
        StructField("unit_price", FloatType()),
        StructField("quantity", IntegerType()) 
        ]))) \
        .add("type", StringType()) \
        .add("country",StringType()) \
        .add("invoice_no", LongType()) \
        .add("timestamp", TimestampType())

# Casting JSON data to string and storing it
order_stream = order_json.select(from_json(col("value").cast("string"), JSON_Schema).alias("data")).select("data.*")

#Functions to impute additional columns
def total_cost(items, type):
    total_cost = 0
    for item in items:
        total_cost = total_cost + item['unit_price'] * item['quantity']
    if type == "RETURN":
        return total_cost * (-1)
    else:
        return total_cost
    

def total_items(items):
    total_count = 0
    for item in items:
        total_count = total_count + item['quantity']
    return total_count

def is_order(type):
   if type=="ORDER":
       return 1
   else:
       return 0
   
def is_return(type):
   if type=="RETURN":
       return 1
   else:
       return 0
   
# Define UDF
udf_total_cost = udf(total_cost, FloatType())
udf_total_items = udf(total_items, IntegerType())
udf_is_order = udf(is_order, IntegerType())
udf_is_return = udf(is_return, IntegerType())


# Adding new columns to dataframe
extended_order_stream = order_stream \
        .withColumn("total_cost",udf_total_cost(order_stream.items, order_stream.type)) \
        .withColumn("total_items",udf_total_items(order_stream.items)) \
        .withColumn("is_order", udf_is_order(order_stream.type)) \
        .withColumn("is_return", udf_is_return(order_stream.type))

# Write to Console
console_extended_order_stream = extended_order_stream \
       .select("invoice_no", "country", "timestamp","total_cost","total_items","is_order","is_return") \
       .writeStream \
       .outputMode("append") \
       .format("console") \
       .option("truncate", "false") \
       .trigger(processingTime="1 minute") \
       .start()

# Calculate time based KPIs
agg_time = extended_order_stream \
    .withWatermark("timestamp","1 minute") \
    .groupby(window("timestamp", "1 minute")) \
    .agg(count("invoice_no").alias("OPM"),
        sum("total_cost").alias("total_sale_volume"),
        avg("total_cost").alias("average_transaction_size"),
        avg("is_Return").alias("rate_of_return")) \
    .select("window","OPM","total_sale_volume","average_transaction_size","rate_of_return")

# Calculate time and country based KPIs
agg_time_country = extended_order_stream \
    .withWatermark("timestamp", "1 minutes") \
    .groupBy(window("timestamp", "1 minutes"), "country") \
    .agg(sum("total_cost").alias("total_sale_volume"),
        count("invoice_no").alias("OPM"),
        avg("is_Return").alias("rate_of_return")) \
    .select("window","country", "OPM","total_sale_volume","rate_of_return")

# Write time based KPI values
ByTime = agg_time.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "time_KPI/") \
    .option("checkpointLocation", "time_KPI/checkpoint/") \
    .trigger(processingTime="1 minutes") \
    .start()


# Write time and country based KPI values
ByTime_country = agg_time_country.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("path", "time_country_KPI/") \
    .option("checkpointLocation", "time_country_KPI/checkpoint/") \
    .trigger(processingTime="1 minutes") \
    .start()

console_extended_order_stream.awaitTermination()
ByTime.awaitTermination()
ByTime_country.awaitTermination()

