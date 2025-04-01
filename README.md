Retail Data Analysis with PySpark:

This project performs data analysis on retail datasets using Apache Spark (PySpark). The goal is to clean, explore, and perform aggregations on large retail datasets, which can be useful for gaining insights into customer behavior, sales trends, and product performance.

Project Overview

Datasets:

customers.csv: Contains customer information such as ID, name, age, and city.

orders.csv: Contains order transactions, including order ID, product ID, quantity, and price.

products.csv: Contains product information such as ID, name, category, and price.

Scripts:

data_cleaning.py: This script cleans the data by handling missing values, removing duplicates, and converting data types.

eda.py: This script performs Exploratory Data Analysis (EDA), such as analyzing total sales by product category, identifying top customers, and exploring monthly sales trends.

aggregations.py: This script calculates aggregations like total revenue by customer and identifies the best-selling products.


Setup Instructions

1. Install dependencies

To run the scripts, you'll need to have Apache Spark and PySpark installed. You can set up a virtual environment and install the required dependencies using the following commands:

pip install pyspark

Alternatively, you can use Docker or a cloud-based platform like Databricks to run the project.




2. Running the Scripts
   
After installing PySpark, you can run the scripts as follows:

a. Data Cleaning

This script reads the raw CSV files, handles missing values, removes duplicates, and standardizes data types.

python scripts/data_cleaning.py

b. Exploratory Data Analysis (EDA)

The EDA script provides insights into the retail data, such as total sales by product category and top customers.

python scripts/eda.py

c. Aggregations

The aggregation script performs calculations like total revenue per customer and identifies the best-selling products.

python scripts/aggregations.py
