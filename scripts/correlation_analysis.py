from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
import pyspark.ml.feature as feat
from pyspark.ml.stat import Correlation
import numpy

# Initialize Spark session
spark = SparkSession.builder.appName("Correlation Analysis").getOrCreate()

# Load the dataset
df = spark.read.csv("C:/Users/Admin/PycharmProjects/Netflix-Data-Analysis-PySpark/data/netflix_series_10_columns_data.csv", header=True, inferSchema=True)

# First method- best for two variable- directly from dataframe api
corr_anl=df.corr("Rating","Total Watches")

print(f'by First method correlation between rating and Total Watches is {corr_anl}')
# Second method - best for multivariate
# Assemble feature vector
# Define the feature and label columns & Assemble the feature vector
assembler=feat.VectorAssembler(inputCols=['Rating','Total Watches'],outputCol="Features")
vector=assembler.transform(df).select("Features")
corre_2=Correlation.corr(vector,"Features").head()[0]
print(f'by Second method correlation between rating and Total Watches is {corre_2[0,1]}')

# Show the results
if int(corr_anl)>0:
    print(f'As per Analysis this is positive correlation')
elif int(corr_anl)<0:
    print(f'As per Analysis this is inversely correlated')
else:
    print(f'As per Analysis no correlation found')


# Stop the Spark session
spark.stop()