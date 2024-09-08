from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder.appName("Trends in Series Production").getOrCreate()

# Load the dataset
df = spark.read.csv("C:/Users/Admin/PycharmProjects/Netflix-Data-Analysis-PySpark/data/netflix_series_10_columns_data.csv", header=True, inferSchema=True)

# Group by 'Release Year' and count the number of series released
ds = df.groupBy('Release Year').count()

# Convert to pandas DataFrame for plotting
ds_pandas = ds.toPandas()

# Sort by 'Release Year' to ensure the line plot is continuous
ds_pandas.sort_values('Release Year', inplace=True)

# Plot
plt.figure(figsize=(12, 6))
plt.plot(ds_pandas['Release Year'], ds_pandas['count'], marker='o', linestyle='-', color='b')
plt.title('Number of Series Released Each Year')
plt.xlabel('Year')
plt.ylabel('Count')
plt.xticks(rotation=45)  # Rotate x labels for better readability
plt.grid(True)
plt.tight_layout()  # Adjust plot to fit labels
plt.show()

# Stop the Spark session
spark.stop()