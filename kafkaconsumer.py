import pandas as pd
from kafka import KafkaConsumer
from time import sleep
from json import loads
import json
from s3fs import S3FileSystem

from configuration import config

consumer = KafkaConsumer(config["topic"], bootstrap_servers=[config["IP"]],
                         value_deserializer=lambda x: loads(x.decode('utf-8')))

s3 = S3FileSystem()

for count, i in enumerate(consumer):
    with s3.open(f's3://{config["IP"]}/covid_records_{count}.json', 'w') as file:
        json.dump(i.value, file)
