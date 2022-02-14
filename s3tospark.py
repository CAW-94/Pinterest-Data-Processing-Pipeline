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
        s3_df = self.spark.read.json(self.sc.parallelize([json_content]))

        obj = start_json + 1
        max_obj = start_json + n_jsons


        while obj < max_obj:
            content_object = self.s3_resource.Object("aicore-pintrestproject", f"pintrest_message_value_jsons/pintrest{obj}.json")
            file_content = (content_object.get()['Body'].read())
            json_content = str(json.loads(file_content))
            print(json_content)
            json_content = json_content.replace("'",'"')
            json_content = json_content.encode('utf_8').decode('utf_8')
            print(f'appending json {obj}')
            s3_df = s3_df.unionByName(self.spark.read.json(self.sc.parallelize([json_content])),allowMissingColumns=True)
            obj += 1
        self.sc.stop()
        return s3_df

class HBaseConnector():
    
    def __init__(self):
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11'
    



