from confluent_kafka import Consumer
import argparse
import json

KAFKA_BOOTSTRAP_SERVER="localhost:9093"


if __name__ == '__main__':

    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--topic', help='kafka topic')
    parser.add_argument('-g', '--consumer_group', help='kafka group', default='kafka')
    args = parser.parse_args()
    # declare the topic and consumer group
    kafka_consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
        'group.id': args.consumer_group,
        })
    kafka_consumer.subscribe([args.topic])

    while True:
        # consume a message from kafka, wait 1 second
        msg = kafka_consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f'Consumer error: {msg.error()}')
            continue
        # print out, you can store data to db and do other tasks
        # the assumption is the data in json to we parse it into json
        #but you can change it to any format you want
        json_value =json.loads(msg.value().decode('utf-8'))
        print(f'Received message: {json_value}')

    kafka_consumer.close()
