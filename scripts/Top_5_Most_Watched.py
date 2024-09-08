from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("MTop 5 Most Watched Genres Globally").getOrCreate()

# Load the dataset
df = spark.read.csv("C:/Users/Admin/PycharmProjects/Netflix-Data-Analysis-PySpark/data/netflix_series_10_columns_data.csv", header=True, inferSchema=True)

Most_watched=df.groupby('Genre').sum('Total Watches')

# Show the results
Most_watched.sort(F.col('sum(Total Watches)').desc()).show(5)

# Stop the Spark session
spark.stop()