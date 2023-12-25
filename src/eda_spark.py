"""
EDA (Exploratory Data Analysis) with PySpark
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,isnan, when, count


# Create a Spark session
spark = SparkSession.builder.appName("CalculMoyenne").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load your DataFrame from a CSV file using semicolon as a separator
# Replace 'your_path' and 'your_file.csv' with your actual path and file
df = spark.read.csv(
    'data/data_departements_2023.csv',
    header=True,
    inferSchema=True,
    sep=';')
# Selecting specific columns from the DataFrame
df2 = df.select(
    df["NUM_POSTE"],
    df["NOM_USUEL"],
    df["LAT"],
    df["LON"],
    df["AAAAMMJJ"],
    df["TNTXM"],
    df["DG"],
    df['RR'],
    df["FF2M"]
)

# Filter rows where AAAAMMJJ >= 20100101
filtered_df2 = df2.filter(col("AAAAMMJJ") >= 20100101)

# Show the resulting DataFrame


# Write the filtered DataFrame to a new CSV file
filtered_df2.coalesce(1).write.csv("data/data_2023.csv", sep=",", header=True, mode="overwrite")
