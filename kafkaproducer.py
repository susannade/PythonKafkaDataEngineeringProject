import pandas as pd
from kafka import KafkaProducer
from time import sleep
from json import dumps

from configuration import config

df = pd.read_csv('data/covid_worldwide.csv')

producer = KafkaProducer(bootstrap_servers=[config["IP"]],
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# simulation of realtime data
while True:
    dict_send = df.sample(1).to_dict(orient="records")[0]
    producer.send(config["topic"], value=dict_send)
    sleep(2)



