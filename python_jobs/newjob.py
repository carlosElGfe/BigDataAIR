
import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.functions import when
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import log10
from pyspark.sql.types import StringType
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import length
from textblob import TextBlob
import re
from datetime import datetime
import awswrangler as wr
import os

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

def get_scores():
    spark = SparkSession.builder.getOrCreate()

    path = 'reviews.csv'

    df = spark.read.csv(path,sep=',',header = True, inferSchema=True)
    df = df.limit(1000)
    udf_clean = udf(clean_sentence, StringType())
    df = df.withColumn('clean_comment',udf_clean(df.comments))
    udf_score = udf(lambda x: get_score(x), StringType())
    df = df.withColumn('listing_id', 
                            when(col('listing_id').cast('Integer').isNull(), 
                                                    lit('null')).otherwise(col('listing_id').cast('Integer')))
    df = df.withColumn('reviewer_id', 
                            when(col('reviewer_id').cast('Integer').isNull(), 
                                                    lit('null')).otherwise(col('reviewer_id')))
    print("-------------------------------")
    
    #df = df.na.drop(subset=['listing_id','comments','reviewer_id'])
    print(datetime.now())
    print(df.show())
    #print(df.show())
    print(datetime.now())
    #df = df.withColumn('english_clean_comment',udf_translate(col('clean_comment')))
    print(datetime.now())
    df = df.withColumn('score',udf_score(col('clean_comment')))
    print(datetime.now())
    df = df.drop('comments')
    #df = df.drop('clean_comment')
    df = df.drop('date')
    df = df.drop('id')
    df = df.filter(length(df.reviewer_name) < 18) 
    #df.write.mode('overwrite').save("awspath/done3.csv",header=True)
    #df.write.mode('overwrite').parquet('onli.parquet')
    
    df.coalesce(1).write.mode('overwrite').csv("review",header = True)