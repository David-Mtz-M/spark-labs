from pyspark.sql import SparkSession

# Create a spark session
spark = SparkSession.builder.appName("HelloDavid").getOrCreate()

# Use sql() to write a raw SQL query
df = spark.sql("SELECT 'Hello David!' as hello")

# Print the dataframe
df.show()

# Write the result to a JSON file
df.write.mode("overwrite").json("results")
