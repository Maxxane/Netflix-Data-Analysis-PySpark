from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Most Watched Country").getOrCreate()

# Load the dataset
df = spark.read.csv("C:/Users/Admin/PycharmProjects/Netflix-Data-Analysis-PySpark/data/netflix_series_10_columns_data.csv", header=True, inferSchema=True)

# Define a window specification
# Partition by Genre and order by Total Watches in descending order
window_8=Window.partitionBy("Genre").orderBy(F.col("Total Watches").desc())

# Use row_number() to rank the series within each genre
ds=df.withColumn("Rank",F.row_number().over(window_8))


# Show the results
ds.filter(F.col("Rank")==1).select(["Lead Actor",'Genre','Series Name']).show()


# Stop the Spark session
spark.stop()