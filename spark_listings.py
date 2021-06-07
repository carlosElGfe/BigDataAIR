def separator():
  print("---------------")

#using snake case for this assignment


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
from pyspark.ml.feature import StringIndexer

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('listings.csv',sep=',',header = True, inferSchema=True)

#---TYPE FIXING
fixes = ['id','host_id','latitude','longitude','number_of_reviews','reviews_per_month','price']
for fix in fixes:
  if fix == 'id' or fix == 'host_id' or  fix == 'number_of_reviews':
    df = df.withColumn(fix, 
                      when(col(fix).cast('Integer').isNull(), 
                                                lit('null')).otherwise(col(fix)))
    df = df.withColumn(fix,col(fix).cast("Integer"))
  else:    
    df = df.withColumn(fix, 
                      when(col(fix).cast('Float').isNull(), 
                                                lit('null')).otherwise(col(fix)))
    df = df.withColumn(fix,col(fix).cast("Float"))

#---FIXING OUTLINERS
print("count distinct of room")
df_distinct = df.select(countDistinct('room_type'))
df = df.filter((df.room_type == 'Entire home/apt')|(df.room_type== 'Private room')|(df.room_type == 'Shared room'))
print("count distinct of room after filter")
df_distinct = df.select(countDistinct('room_type'))
df = df.filter(df.availability_365 > 0)
df = df.filter(df.price > 0)                    
#---NULL HANDLING 
#counting how many of rooms and neighbourhoods groups and rooms
df.na.drop(subset=['price','id'])
df = df.withColumn('reviews_per_month', 
                   when(df.reviews_per_month.isNull(), 
                                             lit(0.0)).otherwise(df.reviews_per_month))

#we can see that there are 31 neighborhoods

#check all nulls
'''for i in df.schema.names:

  total = df.filter(col(i).isNull()).count()
  print(i + " " +str(total))'''

#---FROM NOMINAL TO NUMERICAL TRANSFORMATION
print(df.show())
indexer = StringIndexer(inputCol="neighbourhood_group", outputCol="neighbourhood_index")
print(df.show())
indexer_rooms = StringIndexer(inputCol="room_type", outputCol="room_type_index")
df = indexer.fit(df).transform(df)
df = indexer_rooms.fit(df).transform(df)
#print(df.show())


'''for i in range(0,365):
  try:
    if df.filter(col('minimum_nights')==i).count() > 0:
      print( str(i)+" minimum nights apears "+str(df.filter(col('minimum_nights')==i).count()))
  except Exception:
    pass'''
#---AGREGATION
df = df.withColumn('minimum_stay_range',
                   when(col('minimum_nights')<7,1)
                   .when((col('minimum_nights')>=7) & (col('minimum_nights')<14) ,2)
                   .when((col('minimum_nights')>=14) & (col('minimum_nights')<30) ,3)
                   .when((col('minimum_nights')>=30) & (col('minimum_nights')<90) ,4)
                   .when((col('minimum_nights')>=90) & (col('minimum_nights')<365) ,5).otherwise(6))
df = df.withColumn('log_minimum_nights',log10(col('minimum_nights')))

#DROPING UNDESIRED COLUMNS
df = df.drop('neighbourhood_group')
df = df.drop('room_type')
df = df.drop('longitude')
df = df.drop('latitude')
df = df.drop('last_review')
df = df.drop('neighbourhood')
df = df.drop('minimum_nights')
df = df.drop('minimum_stay_range')
df = df.drop('host_name')
df = df.drop('name')
df = df.drop('calculated_host_listings_count')
df.write.csv('done_listing.csv')

#df.write.parquet('ready.parquet')


