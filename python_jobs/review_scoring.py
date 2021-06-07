

import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.functions import when
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import log10
from pyspark.sql.types import StringType
from pyspark.ml.feature import StringIndexer
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import length
from textblob import TextBlob
import re
from datetime import datetime
import awswrangler as wr
import os
import pandas as pd
import numpy as np

def get_score(comment):
    try:
        text = TextBlob(comment)
        return text.sentiment.polarity
    except Exception:
        return 'null'
        
def clean(comment):
    try:
        return clean_sentence(comment)
    except Exception:
        return 'null'
'''def get_score(comment):
    try:
        analyzer = SentimentIntensityAnalyzer()
        return str(analyzer.polarity_scores(comment)['compound'])
    except Exception:
        return '0'''




def clean_sentence(sentence):
    sequence = sentence
    new = re.sub(r"#[A-Za-z0-9]+", "", sequence)
    new = re.sub(r"https?://[A-Za-z0-9./]+", "", new)
    new = re.sub(r"\.", " ", new)  # delete points and replace with white space
    new = re.sub("[^a-zA-Z ]", "", new)  # Only letters and spaces
    new = re.sub(" [^aeiou] ", " ", new)  # Remove single consonants
    new = re.sub("\n", " ", new)
    new = re.sub("[ ]+", " ", new)  # remove multiple white spaces
    new = new.strip()
    new = new.lower()
    return new
    


path = 'reviews.csv'

df = spark.read.csv(path,sep=',',header = True, inferSchema=True)
df = df.limit(1000000)
udf_clean = udf(lambda x: clean(x), StringType())
udf_score = udf(lambda x: get_score(x), StringType())

df = df.withColumn('listing_id', 
                        when(col('listing_id').cast('Integer').isNull(), 
                                                lit('null')).otherwise(col('listing_id').cast('Integer')))
df = df.withColumn('reviewer_id', 
                        when(col('reviewer_id').cast('Integer').isNull(), 
                                                lit('null')).otherwise(col('reviewer_id')))
print("-------------------------------")

df = df.na.drop(subset=['listing_id','comments','reviewer_id'])



df = df.withColumn('clean_comment',udf_clean(df.comments))
pandas_df = df.select("*").toPandas()
pandas_df['score'] = pandas_df['clean_comment'].apply(lambda x: get_score(x))
print(pandas_df.head(10))
pandas_df = pandas_df.drop(['comments','date','id'],axis=1)
pandas_df.to_csv('review/reviews.csv',index=False)
pandas_df.to_parquet('reviews.parquet',index=False)