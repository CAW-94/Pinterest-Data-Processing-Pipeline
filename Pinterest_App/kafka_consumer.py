from kafka import KafkaConsumer
from json import loads
import json
import boto3
import aws_session

# #CODE FROM WEBSCRAPER
# s3object = self.s3_resource.Object('bookingbucket', f'hotel_jsons/hotel{i+1}.json')
# self.s3_client = aws_session.session.client('s3')
# self.s3_resource = aws_session.session.resource('s3')
# s3object.put(Body=(bytes(json.dumps(hotel_detail_dict).encode('UTF-8'))))


consumer = KafkaConsumer('PintrestData',bootstrap_servers=['localhost:9092'],api_version = (0,10,1),value_deserializer=lambda x: loads(x.decode('utf-8')), auto_offset_reset = 'earliest')

# for message in consumer:
#     # message value and key are raw bytes -- decode if necessary!
#     # e.g., for unicode: `message.value.decode('utf-8')`
#     print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
#                                           message.offset, message.key,
#                                            message.value))


# s3_client = session.session.client('s3')
# s3_resource = session.session.resource('s3')

s3_resource = aws_session.session.resource('s3')

for i, message in enumerate(consumer):
    s3object = s3_resource.Object('aicore-pintrestproject',f'pintrest_message_value_jsons/pintrest{i+1}.json')
    s3object.put(Body=(bytes(json.dumps(message.value).encode('UTF-8'))))
    print(f'Uploaded {i+1}')
