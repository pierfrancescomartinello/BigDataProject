from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

spark = SparkSession.builder.getOrCreate()

df = pd.read_csv(r'data/sondaggio_sostenibilità_unipa.csv')
df.head()

# drop first test answer and last unused column
df.drop(0, inplace=True)
df.drop("Unnamed: 81", axis=1, inplace=True)

df = spark.createDataFrame(df)

df.show(1)
