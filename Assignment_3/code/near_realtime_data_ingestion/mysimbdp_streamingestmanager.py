from cassandra.cluster import Cluster
from confluent_kafka import Consumer
import importlib
import json
import os

REPORT_DIRECTORY = 'mysimbdp_stream_ingest_monitor.json'

# Function to get json data from a file
def get_json_data(file_path):
    with open(file_path) as f:
        return json.load(f)


# Function to update json data in a file
def update_json_data(file_path, data):
    with open(file_path, 'w') as f:
        json.dump(data, f)


class StreamManager:
    def __init__(self, customer_info_file):
        self.customer_info_file = customer_info_file
        self.customer_info = get_json_data(customer_info_file)

    def add_time(self, client_id, name, time):
        self.customer_info[client_id][name] = time
        update_json_data(REPORT_DIRECTORY, self.customer_info)
    
    # check if client ID is in customer data
    def check_ID(self, id):
        if client_id not in self.customer_info:
            return 'Client not found in customer data', 403
        return 'Client found', 201

    def check_client(self, client_id, file_size):
        # check if client ID is in customer data
        if client_id not in self.customer_info:
            return 'Client not found in customer data', 403

        # check if file size exceeds maximum
        if file_size > self.customer_info[client_id]['max_data_size']:
            return 'Data size exceeds maximum allowed size: ' + str(file_size) + '/' + str(self.customer_info[client_id]), 405

        # check if the client has enough memory left
        new_uploaded_data_size = self.customer_info[client_id]['uploaded_data_size'] + file_size
        if new_uploaded_data_size > self.customer_info[client_id]['max_memory_size']:
            return 'There is not enough memory on the client disk for the data.', 405

        # check if the data average ingestion time is too high
        if self.customer_info[client_id]['average_ingestion_time'] > self.customer_info[client_id]['max_ingest_speed_bs']:
            return 'The data ingest speed is too high, slow down!', 405

        # update customer data
        self.customer_info[client_id]['number_of_ingested_messages'] += 1
        self.customer_info[client_id]['uploaded_data_size'] = new_uploaded_data_size
        update_json_data(self.customer_info_file, self.customer_info)

        return 'OK', 201


# Define Kafka bootstrap server
KAFKA_BOOTSTRAP_SERVER="localhost:9093"

# Define the Cassandra IP address and keyspace name
CASSANDRA_IP_ADDRESS = ['172.18.0.2', '172.18.0.3']


if __name__ == '__main__':
    # Connect to Kafka
    kafka_consumer = Consumer({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
        'group.id': 'streamingestmanager'
    })
    kafka_consumer.subscribe(['client_1_data', 'client_2_data'])

    # Open the Cassandra cluster connection
    cluster = Cluster(CASSANDRA_IP_ADDRESS)

    # Continuously read messages from Kafka and write them to Cassandra
    while True:
        msg = kafka_consumer.poll(1.0)  # Wait for 1 second to receive messages
        if msg is None:
            continue
        if msg.error():
            print(f'Kafka consumer error: {msg.error()}')
            continue

        c_name = msg.topic()

        print("")
        print(f"-----> New message from <{c_name}> <-------")
        # Parse the received JSON data
        json_value = json.loads(msg.value().decode('utf-8'))

        print("-----> Connect to the Cassandra cluster")
        session = cluster.connect(c_name)
        print("-----> Connecting to client app....")
        clientApp = importlib.import_module('clientstreamingestapp_' + c_name)
        print("-----> Connecting to client app: DONE")

        print("-----> Call the client app ingestion() function to process the file....")
        report = clientApp.ingestion(session, json_value)
        print("-----> Client app ingestion(): DONE")

        print(f"-----> Close the Cassandra session: <{c_name}>")
        session.shutdown()

        # Get the report file path and the existing reports data
        reports = get_json_data(REPORT_DIRECTORY)

        # Add the report data to the existing reports
        print("-----> Adding the report data to the existing report")
        for key in report:
            reports[c_name][key] = report[key]

        # Add total and manager process time to client report
        reports[c_name]['average_ingestion_time'] = (float(reports[c_name]['average_ingestion_time']) + float(reports[c_name]['receive_time']) - float(reports[c_name]['last_ingest_time'])) / 2
        reports[c_name]['last_ingest_time'] = reports[c_name]['ingest_end_time']

        # Update the reports data file
        update_json_data(REPORT_DIRECTORY, reports)
        print("------> Message from <{c_name}> processed! <-------")
        print("")

    # Close the Kafka consumer and Cassandra session
    kafka_consumer.close()
    cluster.shutdown()
