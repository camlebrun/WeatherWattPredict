from pyspark.sql import SparkSession

# Créer une session Spark
spark = SparkSession.builder.appName("example").getOrCreate()

# Charger les données à partir d'un fichier CSV
df = spark.read.csv("data/data_departements.csv", header=True, inferSchema=True, sep=";")

# Afficher les 5 premières lignes des données
df.show(5)
