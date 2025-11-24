from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, split, expr, from_json, column
from pyspark.sql.types import StructType, StructField, ArrayType, DoubleType, IntegerType, StringType

def create_spark_session():
    """Initialize and return a Spark session."""
    return SparkSession.builder \
        .appName("WeatherDataProcessing") \
        .getOrCreate()

def calculate_word_count(df):
    """Calculate the word count for the 'weather_description' field."""
    return df.select(size(split(col("weather_description"), "\\s+")).alias("word_count"))

def main():
    # Initialize Spark session
    spark = create_spark_session()

    # Define Kafka configurations
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'weather'

    # Define the schema for the JSON data
    schema = StructType([
        StructField("coord", StructType([
            StructField("lon", DoubleType()),
            StructField("lat", DoubleType())
        ])),
        StructField("weather", ArrayType(StructType([
            StructField("id", IntegerType()),
            StructField("main", StringType()),
            StructField("description", StringType()),
            StructField("icon", StringType())
        ]))),
        StructField("base", StringType()),
        StructField("main", StructType([
            StructField("temp", DoubleType()),
            StructField("feels_like", DoubleType()),
            StructField("temp_min", DoubleType()),
            StructField("temp_max", DoubleType()),
            StructField("pressure", IntegerType()),
            StructField("humidity", IntegerType()),
            StructField("sea_level", IntegerType()),
            StructField("grnd_level", IntegerType())
        ])),
        StructField("visibility", IntegerType()),
        StructField("wind", StructType([
            StructField("speed", DoubleType()),
            StructField("deg", IntegerType())
        ])),
        StructField("clouds", StructType([
            StructField("all", IntegerType())
        ])),
        StructField("dt", IntegerType()),
        StructField("sys", StructType([
            StructField("type", IntegerType()),
            StructField("id", IntegerType()),
            StructField("country", StringType()),
            StructField("sunrise", IntegerType()),
            StructField("sunset", IntegerType())
        ])),
        StructField("timezone", IntegerType()),
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("cod", IntegerType())
    ])

    # Read data from Kafka
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", "10") \
        .load()

    # Convert the binary 'value' column to a string
    raw_data = kafka_stream.selectExpr("CAST(value AS STRING)")

    # Parse the JSON data
    parsed_data = raw_data.select(from_json(col("value"), schema).alias("json_data"))

    # Extract fields from parsed JSON
    weather_data = parsed_data.select(
        col("json_data.weather")[0].getField("description").alias("weather_description"),
        col("json_data.main.temp").alias("temperature")
    )

    # Write parsed data to a Parquet file
    query_parsed_data_parquet = parsed_data \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "C:/Users/kacik/Documents/parsed_data") \
        .option("checkpointLocation", "C:/Users/kacik/Documents/checkpoint/parsed_data") \
        .start()

    # Debug: Show raw JSON and parsed data
    query_raw_data = raw_data \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Calculate word count
    word_count_stream = calculate_word_count(weather_data)

    # Debug: Show word counts
    query_word_count = word_count_stream \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Calculate the average number of words
    average_word_count = word_count_stream \
        .groupBy() \
        .agg(expr("avg(word_count)").alias("average_word_count"))

    # Debug: Show average word count
    query_avg_word_count = average_word_count \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Calculate the average temperature
    average_temperature = weather_data \
        .select(col("temperature").cast("double")) \
        .groupBy() \
        .agg(expr("avg(temperature)").alias("average_temperature"))

    # Debug: Show average temperature
    query_avg_temperature = average_temperature \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Await termination
    query_raw_data.awaitTermination()
    query_word_count.awaitTermination()
    query_avg_word_count.awaitTermination()
    query_avg_temperature.awaitTermination()
    query_parsed_data_parquet.awaitTermination()

if __name__ == "__main__":
    main()
