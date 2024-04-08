import argparse
from confluent_kafka import Producer
import pandas as pd
import json
import time
import datetime
import calendar

# A common function to convert timestamp to string
def datetime_converter(dt):
    if isinstance(dt, datetime.datetime):
        return dt.__str__()

# Delivery callback function
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Kafka bootstrap server address
KAFKA_BOOTSTRAP_SERVER = "localhost:9093"

if __name__ == '__main__':
    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', help='Input file', default='../../data/data.csv')
    parser.add_argument('-c', '--chunksize', help='Chunk size for big file', default=90)
    parser.add_argument('-s', '--sleeptime', help='Sleep time in seconds')
    parser.add_argument('-t', '--topic', help='Kafka topic', default='client_1_data')
    parser.add_argument('-id', '--deviceID', help='Set the deice id', default='C2:9A:9F:E5:58:27')
    args = parser.parse_args()


    # Read large input file in chunks
    INPUT_DATA_FILE = args.input_file
    chunksize = int(args.chunksize)
    sleeptime = int(args.sleeptime)
    KAFKA_TOPIC = args.topic
    input_data = pd.read_csv(INPUT_DATA_FILE, parse_dates=['time'], iterator=True, chunksize=chunksize)

    # Kafka producer instance
    kafka_producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER})

    # Process each chunk of input data
    for chunk_data in input_data:
        chunk = chunk_data.dropna()
        for index, row in chunk.iterrows():
            data = row.to_dict()
            # Set the device's measured time to the current time (needed for latency)
            data['time'] = time.time_ns()
            data['readable_time'] = datetime.datetime.now().isoformat()
            data['dev_id'] = args.deviceID
            # Convert the data row to a JSON string
            json_data = json.dumps(data, default=datetime_converter)
            # Send the data to Kafka
            print(f'DEBUG: {json_data}\n')
            kafka_producer.produce(KAFKA_TOPIC, json_data.encode('utf-8'), callback=delivery_report)
            kafka_producer.flush()
            # Wait for some time before sending the next chunk, for emulation
            time.sleep(sleeptime)