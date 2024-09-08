from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("Longest Series by language").getOrCreate()

# Load the dataset
df = spark.read.csv("C:/Users/Admin/PycharmProjects/Netflix-Data-Analysis-PySpark/data/netflix_series_10_columns_data.csv", header=True, inferSchema=True)

Long_Series=df.select(['Series Name','Total Seasons','Language'])

# Define a window specification
# Partition by Language and order by Total Seasons in descending order
Window4=Window.partitionBy("Language").orderBy(F.col('Total Seasons').desc())

# Use row_number() to rank the series within each Language
Long_Series=Long_Series.withColumn("Rank",F.row_number().over(Window4))
long_running=Long_Series.filter(F.col('Rank')==1)

# Show the results
long_running.show()

# Stop the Spark session
spark.stop()