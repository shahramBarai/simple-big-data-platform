from cassandra.cluster import Cluster
import importlib
import json
import os

REPORT_DIRECTORY = 'client-reports/'        # Directory for client reports

# Create the directory if it doesn't exist
if not os.path.exists(REPORT_DIRECTORY):
    os.makedirs(REPORT_DIRECTORY)

# Function to get json data from a file
def get_json_data(file_path):
    with open(file_path) as f:
        return json.load(f)

# Function to update json data in a file
def update_json_data(file_path, data):
    with open(file_path, 'w') as f:
        json.dump(data, f)

# Class to manage batch uploads for clients
class BatchManager:

    def __init__(self, customer_info_file):
        self.customer_info_file = customer_info_file
        self.customer_info = get_json_data(customer_info_file)

    def set_receive_time_and_size(self, client_name, r_time, r_size):
        file_path = REPORT_DIRECTORY + client_name + '_report.json'
        update_json_data(file_path, {r_time: {'file_received_size': r_size}})

    def update_files_number(self, client_id, num):
        self.customer_info[client_id]['uploaded_files_number'] += num

    def check_client(self, client_id, file_extension, file_size):
        # check if client ID is in customer data
        if client_id not in self.customer_info:
            return 'Client not found in customer data', 403

        # check if file type is allowed for client
        if file_extension not in self.customer_info[client_id]['file_type']:
            return 'File type not allowed for client', 405

        # check if file size exceeds maximum
        if file_size > self.customer_info[client_id]['max_file_size']:
            return 'File size exceeds maximum allowed size: ' + str(file_size) + '/' + str(self.customer_info[client_id]), 405

        # check if maximum number of files has been uploaded
        if self.customer_info[client_id]['uploaded_files_number'] >= self.customer_info[client_id]['max_files_number']:
            return 'Maximum number of files uploaded', 405

        # check if the client has enough memory left
        new_uploaded_data_size = self.customer_info[client_id]['uploaded_data_size'] + file_size
        if new_uploaded_data_size > self.customer_info[client_id]['max_memory_size']:
            return 'There is not enough memory on the client disk for the file.', 405

        # update customer data
        self.customer_info[client_id]['uploaded_files_number'] += 1
        self.customer_info[client_id]['uploaded_data_size'] = new_uploaded_data_size
        update_json_data(self.customer_info_file, self.customer_info)

        return 'OK', 201


UPLOAD_DIRECTORY = 'client-staging-input-directory/'        # Directory for client uploads
CASSANDRA_IP_ADDRESS = ['172.18.0.2', '172.18.0.3']         # Cassandra IP addresses


if __name__ == '__main__':
    # Continuously loop and process files
    while True:
        # Get a list of files in the upload directory
        client_staging_input_directory = os.listdir(UPLOAD_DIRECTORY)
        cluster = Cluster(CASSANDRA_IP_ADDRESS)

        # Check if there are any files to process
        if len(client_staging_input_directory) > 0:
            # Get the first file in the list
            file = client_staging_input_directory[0]
            print("Starting " + file + " processing....")

            # Get the timestamp and client name from the filename
            f_time = file.split('.')[0].split('_')[0]
            c_name = file.split('.')[0].split('_')[-1]
            # Get the file extension
            f_extension = file.split('.')[-1]

            # Connect to the Cassandra cluster
            session = cluster.connect(c_name)

            print("")
            # Import the client app module
            print("-----> Connecting to client app....")
            clientApp = importlib.import_module('clientbatchingestapp_' + c_name + '_' + f_extension)
            print("-----> Connecting to client app: DONE")
            # Call the client app ingestion() function to process the file
            print("-----> Client app ingestion() start...")
            report = clientApp.ingestion(session, UPLOAD_DIRECTORY + file)
            print("-----> Client app ingestion(): DONE")
            # Close the Cassandra session
            session.shutdown()
            # Get the report file path and the existing reports data
            f_path = REPORT_DIRECTORY + c_name + '_report.json'
            reports = get_json_data(f_path)

            # Add the report data to the existing reports
            print("-----> Adding the report data to the existing report")
            for key in report:
                reports[f_time][key] = report[key]

            # Add total and manager process time to client report
            reports[f_time]['manager_process_time'] = reports[f_time]['ingest_start_time'] - (int(f_time) / 1000000000)
            reports[f_time]['total_process_time'] = reports[f_time]['ingest_end_time'] - (int(f_time) / 1000000000)

            # Update the reports data file
            update_json_data(f_path, reports)

            # Remove the processed file from the upload directory
            print("-----> Removing the processed <" + file + "> file from the upload directory...")
            os.remove(UPLOAD_DIRECTORY + file)
            print("")

        cluster.shutdown()