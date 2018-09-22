import os
import requests
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import mean, min, max

# --- set up a spark session
spark = SparkSession \
            .builder \
            .appName("PySpark Example One") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()

# --- import web data
url = "https://assets.datacamp.com/production/course_1975/datasets/titanic_all_numeric.csv"
response = requests.get(url)
raw_data = response.content.split("\n")


# --- build a pyspark data frame
data_dict = {k: [] for k in raw_data[0].split(",")}
raw_data = raw_data[1:]

# --- fill dictionary with correct data
for i, _ in enumerate(list(data_dict.keys())):
    for row in raw_data:
        current_row = row.split(",")
        for j, _ in enumerate(current_row):
            if j == i:
                data_dict.get(list(data_dict.keys())[i]) \
                            .append(current_row[j])

# ---  view data to fix column lengths
for i, _ in enumerate(list(data_dict.keys())):
    print(len(data_dict.get(list(data_dict.keys())[i])))
    
data_dict.get('fare').pop(0)

# --- create pandas data frame
df = pd.DataFrame(data_dict)

# --- convert to spark data frame
spark_df = spark.createDataFrame(df)

# ---  spark data frame operations
#spark_df.show()

subset = spark_df.select(spark_df['age'], spark_df['male'], spark_df['survived'])


# --  subset into adults and children
children = subset.filter(subset['age'] < 21)
children.select([mean('age'), min('age'), max('age')]).show()
adults = subset.filter(subset['age'] >= 21)
adults.select([mean('age'), min('age'), max('age')]).show()



