from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count

# Initialize Spark session
spark = SparkSession.builder.appName("RetailAggregations").getOrCreate()

# Load datasets
orders_df = spark.read.csv("data/cleaned/orders.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("data/cleaned/customers.csv", header=True, inferSchema=True)

# 1. Join orders and customers to get customer details with total spend
customer_orders = orders_df.join(customers_df, 'customer_id') \
    .groupBy('customer_id', 'name') \
    .agg(sum('price').alias('total_spent')) \
    .orderBy('total_spent', ascending=False)

customer_orders.show()

# 2. Best-selling product by total quantity sold
best_selling_products = orders_df.groupBy('product_id') \
    .agg(sum('quantity').alias('total_quantity_sold')) \
    .orderBy('total_quantity_sold', ascending=False)

best_selling_products.show()

spark.stop()
