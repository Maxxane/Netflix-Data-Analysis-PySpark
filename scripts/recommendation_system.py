from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("Most Watched Country").getOrCreate()

# Load the dataset
df = spark.read.csv("C:/Users/Admin/PycharmProjects/Netflix-Data-Analysis-PySpark/data/netflix_series_10_columns_data.csv", header=True, inferSchema=True)

# Take Inputs from the user
genre=input(f"Select Your Genre")
language=input(f"Select Your Language")

# Recommendation based on the inputs
ds=df.filter(F.col("Genre")==genre).filter(F.col("Language")==language).orderBy(F.col("Rating").desc())

# Show the results
ds.show(3)


# Stop the Spark session
spark.stop()