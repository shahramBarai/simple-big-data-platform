from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "client_1_data"
KAFKA_OUTPUT_TOPIC = "client_1_report"

# create a SparkSession
spark = SparkSession.builder\
    .appName("keyed_stream_processing") \
    .config("spark.executor.cores", 1) \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "8")

# define the schema for the data
schema = StructType([
    StructField("time", LongType()),
    StructField("readable_time", TimestampType()),
    StructField("acceleration", FloatType()),
    StructField("acceleration_x", FloatType()),
    StructField("acceleration_y", FloatType()),
    StructField("acceleration_z", FloatType()),
    StructField("battery", IntegerType()),
    StructField("humidity", FloatType()),
    StructField("pressure", FloatType()),
    StructField("temperature", FloatType()),
    StructField("dev-id", StringType())
])

# Reduce logging
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse the Kafka messages into a structured format using the defined schema
df = df.select(from_json(decode(col('value'), "utf-8"), schema).alias('value')).select('value.*')

# Add a column to parsed_df to count the incoming rows
df = df.withColumn("total_incoming_rows", lit(1))

# Calculate latency
parsed_df = df.withColumn('processing_time_ns', (unix_timestamp(current_timestamp()) * 1e9) - col('time'))

# define the filter condition
filter_condition = (col("temperature") > -30) & (col("temperature") < 40) \
                   & (col("humidity") > 0) & (col("humidity") > 100) \
                   & (col("acceleration") > -1500) & (col("acceleration") > 1500) \
                   & (col("pressure") > 500) & (col("pressure") > 1500)

# create a new column "out_of_range" which is 1 if the filter condition is 'true', 0 otherwise
parsed_df = parsed_df.withColumn('out_of_range_count', when(filter_condition, 1).otherwise(0))

# group the data by device ID and a 1-minute window
parsed_df_1 = parsed_df\
    .withWatermark("readable_time", "60 seconds") \
    .groupBy(window('readable_time', '60 seconds'), 'dev-id') \
    .agg({'temperature': 'avg', 'acceleration': 'avg', 'humidity': 'avg', 'pressure': 'avg',
          'total_incoming_rows': 'sum', 'processing_time_ns': 'avg', 'out_of_range_count': 'sum'
          }) \
    .withColumnRenamed("avg(temperature)", "temperature") \
    .withColumnRenamed("avg(acceleration)", "acceleration") \
    .withColumnRenamed("avg(humidity)", "humidity") \
    .withColumnRenamed("avg(pressure)", "pressure") \
    .withColumnRenamed("avg(processing_time_ns)", "processing_time_ns") \
    .withColumnRenamed("sum(out_of_range_count)", "out_of_range_count") \
    .withColumnRenamed("sum(total_incoming_rows)", "total_output_rows") \
    .withColumn('throughput_row_s', col('total_output_rows') / 60) \
    .select("dev-id", "window",
            'total_output_rows', 'processing_time_ns',
            'throughput_row_s'
            ) \

# 'out_of_range_count',
# 'temperature', 'acceleration', 'humidity', 'pressure',

#     .select(col("window").alias("key"), to_json(struct("*")).alias("value"))
#
# output_1 = parsed_df_1.writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
#     .option("topic", KAFKA_OUTPUT_TOPIC) \
#     .option("checkpointLocation", "/tmp/kafka-checkpoint") \
#     .outputMode("update") \
#     .trigger(processingTime="60 seconds") \
#     .start()

output_1 = parsed_df_1.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="60 seconds") \
    .start()

output_1.awaitTermination()
