#!/usr/bin/env python
# coding: utf-8

# In[12]:


pip install pyspark


# In[13]:


import pandas as pd


# In[20]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Create a SparkSession
spark = SparkSession.builder \
    .appName("SalesDataAnalysis") \
    .getOrCreate()

# Read the CSV file into a DataFrame
sales_df = spark.read.csv("sales_data.csv", header=True, inferSchema=True)

# Data cleaning: handling missing values and removing duplicates
cleaned_sales_df = sales_df.dropna().dropDuplicates()

# Calculate total sales amount for each product
product_sales_df = cleaned_sales_df.groupBy("Product").agg(sum("Sales").alias("TotalSales"))

# Output the results to a new CSV file
product_sales_df.write.csv("total_sales_per_product.csv", header=True)

# Stop the SparkSession
spark.stop()


# In[21]:


pd.read_csv("sales_data.csv")


# In[22]:


pd.read_csv("total_sales_per_product.csv")


# In[ ]:




