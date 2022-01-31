# import findspark

# findspark.init()

# import multiprocessing

# import pyspark

# cfg = (
#     pyspark.SparkConf()
#     # Setting where master node is located [cores for multiprocessing]
#     .setMaster(f"local[{multiprocessing.cpu_count()}]")
#     # Setting application name
#     .setAppName("Pintrest_Test")
#     # Setting config value via string
#     .set("spark.eventLog.enabled", False)
#     # Setting environment variables for executors to use
#     .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
#     # Setting memory if this setting was not set previously
#     .setIfMissing("spark.executor.memory", "1g")
# )

# # Getting a single variable
# print(cfg.get("spark.executor.memory"))
# # Listing all of them in string readable format
# print(cfg.toDebugString())

# session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()

# df = session.read

import os
import findspark
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell"
findspark.init()

access_id=
access_key=

sc=spark.sparkContext

hadoop_conf=sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
hadoop_conf.set("fs.s3n.awsAccessKeyId", access_id)
hadoop_conf.set("fs.s3n.awsSecretAccessKey", access_key)

df = spark.read.json('s3://aicore-pintrestproject/pintrest_message_value_jsons/')
df.show()