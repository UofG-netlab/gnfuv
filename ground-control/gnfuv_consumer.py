'''

GNFUV Python Controller

'''

from kafka import KafkaConsumer
# import avro.schema
import json

consumer = KafkaConsumer('leader-data-avg', bootstrap_servers='192.168.2.250:9092', value_deserializer=lambda m: json.loads(m.decode('ascii')))

# Read messages
for msg in consumer:
    print(msg)
