from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, month

# Initialize Spark session
spark = SparkSession.builder.appName("RetailEDA").getOrCreate()

# Load datasets
orders_df = spark.read.csv("data/cleaned/orders.csv", header=True, inferSchema=True)
products_df = spark.read.csv("data/cleaned/products.csv", header=True, inferSchema=True)

# 1. Total sales by product category
sales_by_category = orders_df.join(products_df, 'product_id') \
    .groupBy('category') \
    .agg(sum('quantity').alias('total_sales'), sum('price').alias('total_revenue')) \
    .orderBy('total_sales', ascending=False)

sales_by_category.show()

# 2. Top 5 customers by total purchase amount
top_customers = orders_df.groupBy('customer_id') \
    .agg(sum('price').alias('total_spent')) \
    .orderBy('total_spent', ascending=False) \
    .limit(5)

top_customers.show()

# 3. Monthly sales trend
monthly_sales = orders_df.withColumn('month', month('order_date')) \
    .groupBy('month') \
    .agg(sum('price').alias('total_sales')) \
    .orderBy('month')

monthly_sales.show()

spark.stop()
