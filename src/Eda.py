from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("CalculMoyenne").getOrCreate()

# Load your DataFrame from a CSV file using semicolon as a separator
# Replace 'your_path' and 'your_file.csv' with your actual path and file
df = spark.read.csv(
    'data/data_departements.csv',
    header=True,
    inferSchema=True,
    sep=';')
