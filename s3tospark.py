#%%
import findspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os
import multiprocessing
import json
import aws_session
findspark.init()

class S3Connector():

    def __init__(self):
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3 pyspark-shell'

        conf = (
        SparkConf()
        .set('spark.executor.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true')
        .set('spark.driver.extraJavaOptions','-Dcom.amazonaws.services.s3.enableV4=true')
        .setAppName('pyspark_aws')
        .setMaster(f"local[{multiprocessing.cpu_count()}]")
        .setIfMissing("spark.executor.memory", "2g")
            )  
        self.sc=SparkContext(conf=conf)
        self.sc.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')
        self.spark=SparkSession(self.sc)
        self.s3_resource = aws_session.session.resource('s3')

    def get_jsons(self,start_json,n_jsons):

        content_object = self.s3_resource.Object('aicore-pintrestproject', f'pintrest_message_value_jsons/pintrest{start_json}.json')
        file_content = content_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        s3_df = self.spark.read.json(sc.parallelize([json_content]))

        obj = start_json + 1


        while obj < :
            content_object = self.s3_resource.Object("aicore-pintrestproject", f"pintrest_message_value_jsons/pintrest{obj}.json")
            file_content = (content_object.get()['Body'].read())
            json_content = str(json.loads(file_content))
            print(json_content)
            json_content = json_content.replace("'",'"')
            json_content = json_content.encode('utf_8').decode('utf_8')
            print(f'appending json {obj}')
            s3_df = s3_df.unionByName(spark.read.json(sc.parallelize([json_content])),allowMissingColumns=True)
            obj += 1


        # # # s3_df = spark.read.json(sc.parallelize([json_content]))
        s3_df.show()
        #s3_df.show()
        s3_df_clean = s3_df.drop(s3_df._corrupt_record)
        s3_df_clean = s3_df_clean.dropna()
        #s3_df_clean = s3_df_clean.drop(" _corrupt_record")
        s3_df_clean.show()
        # # s3_df.printSchema()
        # # # s3_df_a.show()



