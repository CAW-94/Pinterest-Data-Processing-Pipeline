import s3tospark
from pyspark.sql.functions import *
from pyspark.sql.window import Window

connector = s3tospark.S3Connector()

s3_df = connector.get_jsons(1,50)
s3_df.show()
####------------TRANSFORMATIONS FOR SPARK JOBS--------------####
s3_df_clean = s3_df.drop(s3_df._corrupt_record)
s3_df_clean = s3_df_clean.drop(s3_df.save_location)
s3_df_clean = s3_df_clean.dropna('all')
s3_df_clean = s3_df_clean.withColumn('follower_count', regexp_replace('follower_count','k','000'))
s3_df_clean = s3_df_clean.withColumn('follower_count', regexp_replace('follower_count','M','000000'))
s3_df_clean = s3_df_clean.withColumn('follower_count', regexp_replace('follower_count','User Info Error','Null'))
s3_df_clean = s3_df_clean.withColumn('follower_count', col('follower_count').cast('int'))
s3_df_clean = s3_df_clean.withColumn('id', row_number().over(Window.orderBy(monotonically_increasing_id())) - 1)
s3_df_clean.show()
s3_df_clean.printSchema()

connector.close_spark()

# s3_df_clean.write\
# .options(catalog=catalog)\
# .format("org.apache.spark.sql.execution.datasources.hbase")\
# .save()



