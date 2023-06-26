from kafka import KafkaConsumer
import json

if __name__ == "__main__":
    consumer= KafkaConsumer(
        'registered_user',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        group_id ='consumer_group_b'
    )
    print('starting the consumer')
    
    for msg in consumer:
        print("registerd user = {}".format(json.loads(msg.value)))
