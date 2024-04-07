from confluent_kafka import Consumer
from cassandra.cluster import Cluster
import argparse
import json
import datetime
import dateutil.parser

# Define Kafka bootstrap server
KAFKA_BOOTSTRAP_SERVER="localhost:9093"

# Define the Cassandra IP address and keyspace name
CASSANDRA_IP_ADDRESS = ['172.18.0.2', '172.18.0.3']
CASSANDRA_KEYSPACE = 'korkeasaari'

# Connect to the Cassandra cluster
cluster = Cluster(CASSANDRA_IP_ADDRESS)
session = cluster.connect(CASSANDRA_KEYSPACE)

# Function to write data to Cassandra
def write_to_cassandra(data):
    # Prepare Cassandra INSERT statement
    insert_statement = session.prepare('INSERT INTO turtle (time, readable_time, acceleration, acceleration_x, acceleration_y, acceleration_z, battery, humidity, pressure, temperature, dev_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')

    # Convert timestamp to ISO 8601 format
    iso_timestamp = dateutil.parser.parse(data["readable_time"]).isoformat()

    # Parse ISO 8601 timestamp using datetime.datetime.fromisoformat()
    readable_time = datetime.datetime.fromisoformat(iso_timestamp)

    # Execute INSERT statement with data from Kafka
    session.execute(insert_statement, (
        data["time"],
        readable_time,
        data["acceleration"],
        data["acceleration_x"],
        data["acceleration_y"],
        data["acceleration_z"],
        data["battery"],
        data["humidity"],
        data["pressure"],
        data["temperature"],
        data["dev-id"]
    ))

if __name__ == '__main__':

    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--topic', help='kafka topic')
    parser.add_argument('-g', '--consumer_group', help='kafka topic')
    args = parser.parse_args()

    # Connect to Kafka
    kafka_consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
        'group.id': args.consumer_group,
        })
    kafka_consumer.subscribe([args.topic])

    # Continuously read messages from Kafka and write them to Cassandra
    while True:

        msg = kafka_consumer.poll(1.0)  # Wait for 1 second to receive messages
        if msg is None:
            continue
        if msg.error():
            print(f'Kafka consumer error: {msg.error()}')
            continue

        # Parse the received JSON data
        json_value = json.loads(msg.value().decode('utf-8'))

        # Print the received message
        print(f'Received message: {json_value}')

        # Write the received data to Cassandra
        write_to_cassandra(json_value)

    # Close the Kafka consumer and Cassandra session
    kafka_consumer.close()
    session.shutdown()
    cluster.shutdown()