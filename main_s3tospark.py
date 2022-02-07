import s3tospark

connector = s3tospark.S3Connector()

s3_df = connector.get_jsons(1,50)
s3_df.show()
s3_df_clean = s3_df.drop(s3_df._corrupt_record)
s3_df_clean = s3_df_clean.dropna()
s3_df_clean.show()