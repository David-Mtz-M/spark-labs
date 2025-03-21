from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession.builder.appName("temperature_change").getOrCreate()

    print("Reading temperature dataset.csv ...")
    path_temperature = "dataset.csv"
    df_temp = spark.read.csv(path_temperature, header=True, inferSchema=True)

    # Rename columns for easier access
    df_temp = df_temp.withColumnRenamed("Area", "Country") \
                     .withColumnRenamed("Year", "Year") \
                     .withColumnRenamed("Value", "TemperatureChange")

    # Create or replace a temporary view called "temperature"
    df_temp.createOrReplaceTempView("temperature")

    # Query 1: Describe the schema of the temperature dataset
    query = "DESCRIBE temperature"
    spark.sql(query).show(20)

    # Query 2: Get the average temperature change per country
    query = """
        SELECT Country, AVG(TemperatureChange) as Avg_Temperature_Change
        FROM temperature
        GROUP BY Country
        ORDER BY Avg_Temperature_Change DESC
    """
    df_avg_temp = spark.sql(query)
    print("Average Temperature Change per Country:")
    df_avg_temp.show(20)

    # Query 3: Find the top 10 years with the highest temperature change globally
    query = """
        SELECT Year, AVG(TemperatureChange) as Global_Avg_Temp_Change
        FROM temperature
        GROUP BY Year
        ORDER BY Global_Avg_Temp_Change DESC
        LIMIT 10
    """
    df_top_years = spark.sql(query)
    print("Top 10 Years with Highest Temperature Change:")
    df_top_years.show()

    # Query 4: Get temperature change for a specific country (e.g., Mexico)
    query = """
        SELECT Year, TemperatureChange
        FROM temperature
        WHERE Country = 'Mexico'
        ORDER BY Year
    """
    df_mx_temp = spark.sql(query)
    print("Temperature Change in Mexico:")
    df_mx_temp.show()

    # Save results to JSON
    results = {
        "average_temperature_change": df_avg_temp.toJSON().collect(),
        "top_years_temperature_change": df_top_years.toJSON().collect(),
        "mexico_temperature_change": df_mx_temp.toJSON().collect()
    }

    # Overwrite the results folder with structured JSON data
    with open('results/data.json', 'w') as file:
        json.dump(results, file)

    df_avg_temp.write.mode("overwrite").json("results/avg_temp")
    df_top_years.write.mode("overwrite").json("results/top_years")
    df_haiti_temp.write.mode("overwrite").json("results/mexico")

    spark.stop()
