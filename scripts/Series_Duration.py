from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from matplotlib import pyplot as plt
import pandas

# Initialize Spark session
spark = SparkSession.builder.appName("Most Watched Country").getOrCreate()

# Load the dataset
df = spark.read.csv("C:/Users/Admin/PycharmProjects/Netflix-Data-Analysis-PySpark/data/netflix_series_10_columns_data.csv", header=True, inferSchema=True)

season_rating_analysis = df.groupBy("Total Seasons").agg(F.avg("Rating").alias("Avg_Rating"))

# Sort by 'Total Seasons' to observe trends
forplot=season_rating_analysis.orderBy("Total Seasons")
forplot=forplot.toPandas()
print(forplot)

# Set figure size
plt.figure(figsize=(10,6))
# Set Title and Labels
plt.title("Average Rating for Shows with Total Seasons")
plt.xlabel("Total Seasons")
plt.ylabel("Average Rating")

# Plot data from 'forplot'
plt.plot(forplot["Total Seasons"], forplot["Avg_Rating"])

# Display the plot
plt.show()

# Stop the Spark session
spark.stop()