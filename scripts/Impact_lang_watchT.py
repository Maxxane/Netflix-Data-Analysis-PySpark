from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("Impact of Language on Watch Time").getOrCreate()

# Load the dataset
df = spark.read.csv("C:/Users/Admin/PycharmProjects/Netflix-Data-Analysis-PySpark/data/netflix_series_10_columns_data.csv", header=True, inferSchema=True)

# Use Groupby to group the Average Watch Time as per the Language
ds=df.groupBy("Language").avg('Average Watch Time (minutes)')


# Show the results
ds.sort(F.col('avg(Average Watch Time (minutes))').desc()).show()

# Stop the Spark session
spark.stop()