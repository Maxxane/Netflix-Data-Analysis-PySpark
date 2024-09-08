from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Initialize Spark session
spark = SparkSession.builder.appName("Most watched content").getOrCreate()
# Load the dataset
df = spark.read.csv("C:/Users/Admin/PycharmProjects/Netflix-Data-Analysis-PySpark/data/netflix_series_10_columns_data.csv", header=True, inferSchema=True)

Most_watched_content=df.select(['Country of Origin','Total Watches','Average Watch Time (minutes)'])
Most_watched_content=Most_watched_content.withColumn('Total Time Watched overall',col('Total Watches') * col('Average Watch Time (minutes)'))
Most_watched_content.groupby('Country of Origin').sum("Total Time Watched overall").withColumnRenamed("Total Time Watched overall","Total Time Watched overall_sum").show()
