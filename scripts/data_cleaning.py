from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("RetailDataCleaning").getOrCreate()

# Load datasets
customers_df = spark.read.csv("data/customers.csv", header=True, inferSchema=True)
orders_df = spark.read.csv("data/orders.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/products.csv", header=True, inferSchema=True)

# Data Cleaning: Remove duplicates
customers_df = customers_df.dropDuplicates()
orders_df = orders_df.dropDuplicates()
products_df = products_df.dropDuplicates()

# Handle missing values
customers_df = customers_df.fillna({'age': 0, 'city': 'Unknown'})
orders_df = orders_df.fillna({'quantity': 1, 'price': 0.0})
products_df = products_df.fillna({'category': 'Unknown'})

# Convert date columns to date type
orders_df = orders_df.withColumn('order_date', col('order_date').cast('date'))

# Save cleaned datasets
customers_df.write.csv('data/cleaned/customers.csv', header=True)
orders_df.write.csv('data/cleaned/orders.csv', header=True)
products_df.write.csv('data/cleaned/products.csv', header=True)

spark.stop()
