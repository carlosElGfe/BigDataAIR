import findspark
findspark.init()
findspark.find()

from pyspark.sql.functions import col
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf
from pyspark.sql.functions import when
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import log10
from pyspark.sql.types import StringType
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import length
from textblob import TextBlob
from googletrans import Translator
import re
from datetime import datetime
import awswrangler as wr
import os

def get_listings():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv('listings.csv',sep=',',header = True, inferSchema=True)
    #---TYPE FIXING
    fixes = ['id','host_id','latitude','longitude','number_of_reviews','reviews_per_month','price']
    for fix in fixes:
        if fix == 'id' or fix == 'host_id' or  fix == 'number_of_reviews':
            df = df.withColumn(fix, when(col(fix).cast('Integer').isNull(), lit('null')).otherwise(col(fix)))
            df = df.withColumn(fix,col(fix).cast("Integer"))
        else:
            df = df.withColumn(fix, when(col(fix).cast('Float').isNull(), lit('null')).otherwise(col(fix)))
            df = df.withColumn(fix,col(fix).cast("Float"))

    #---FIXING OUTLINERS

    df_distinct = df.select(countDistinct('room_type'))
    df = df.filter((df.room_type == 'Entire home/apt')|(df.room_type== 'Private room')|(df.room_type == 'Shared room'))
    ##count distinct
    df_distinct = df.select(countDistinct('room_type'))
    #print(df.show())
    print(df.show())
    
    #---NULL HANDLING 
    #counting how many of rooms and neighbourhoods groups and rooms
    df.na.drop(subset=['price','id'])
    df = df.withColumn('reviews_per_month', when(df.reviews_per_month.isNull(), lit(0.0)).otherwise(df.reviews_per_month))

    df = df.drop('neighbourhood_index')
    df = df.drop('room_type_index')

    #---FROM NOMINAL TO NUMERICAL TRANSFORMATION
    indexer = StringIndexer(inputCol="neighbourhood_group", outputCol="neighbourhood_index")
    '''indexer_rooms = StringIndexer(inputCol="room_type", outputCol="room_type_index")
    df = indexer.fit(df).transform(df)
    df = indexer_rooms.fit(df).transform(df)
    '''



    ######################################################################## enable this for categorical agrupacion of stay ranges!
    # df = df.withColumn('minimum_stay_range',
    #                    when(col('minimum_nights')<7,1)
    #                    .when((col('minimum_nights')>=7) & (col('minimum_nights')<14) ,2)
    #                    .when((col('minimum_nights')>=14) & (col('minimum_nights')<30) ,3)
    #                    .when((col('minimum_nights')>=30) & (col('minimum_nights')<90) ,4)
    #                    .when((col('minimum_nights')>=90) & (col('minimum_nights')<365) ,5).otherwise(6))
    df = df.withColumn('log_minimum_nights',log10(col('minimum_nights')))
    #df = df.drop('minimum_stay_range')
    df = df.drop('neighbourhood_group')
    df = df.drop('room_type')
    df = df.drop('longitude')
    df = df.drop('latitude')
    df = df.drop('last_review')
    df = df.drop('neighbourhood')

    df = df.drop('host_name')
    df = df.drop('name')
    df = df.drop('calculated_host_listings_count')
    # print(df.show())
    # # print(df["neighbourhood_index"])


    pandas_df = df.select("*").toPandas()
    print(pandas_df.head(10))
    pandas_df.to_parquet('listings.parquet',index=False)
    # # import csv
    #df.write.mode("overwrite").option("partitionOverwriteMode", "dynamic").partitionBy("id").parquet("s3://aypmd-grupo5/parquet/amsterdam/listing.parquet")
    #df.coalesce(1).write.mode('overwrite').csv("done",header = True)
    #df.coalesce(1).write.mode('overwrite').parquet("parquettt")

    # # address = "s3://aypmd-grupo5/parquet/amsterdam/listing.parquet"
    # # df.write.parquet(address)

    # # with open("s3://aypmd-grupo5/parquet/Amsterdam/listing.csv", "w") as f:
    # #     writer = csv.DictWriter(f, fieldnames = df.columns)
    # #     writer.writerow(dict(zip(fieldnames, fieldnames)))
    # #     for row in df.toLocalIterator():
    # #         writer.writerow(row.asDict())