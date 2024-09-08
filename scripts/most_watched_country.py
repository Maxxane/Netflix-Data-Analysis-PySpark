from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Initialize Spark session
spark = SparkSession.builder.appName("Most Watched Country").getOrCreate()

# Load the dataset
df = spark.read.csv("C:/Users/Admin/PycharmProjects/Netflix-Data-Analysis-PySpark/data/netflix_series_10_columns_data.csv", header=True, inferSchema=True)

# Group by 'Country of Origin', sum the 'Total Watches', and sort in descending order
most_watched_country_df = (df.groupBy("Country of Origin")
                           .agg(sum("Total Watches").alias("Total_Watches"))
                           .orderBy(col("Total_Watches").desc()))

# Show the result
most_watched_country_df.show(1)

# Stop the Spark session
spark.stop()
