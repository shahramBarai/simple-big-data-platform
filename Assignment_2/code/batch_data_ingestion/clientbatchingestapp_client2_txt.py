import os
import csv
import time


# Creates the 'azure_functions_invocation_trace' table in the Cassandra keyspace if it doesn't exist
def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS azure_functions_invocation_trace (
            app text,
            func text,
            end_timestamp text,
            duration text,
            PRIMARY KEY (app, func, end_timestamp)
        );
    """)


# Writes data from the TXT file to the 'azure_functions_invocation_trace' table in Cassandra
def write_to_cassandra(session, data):
    # Prepare Cassandra INSERT statement
    insert_statement = session.prepare('INSERT INTO azure_functions_invocation_trace (app, func, end_timestamp, duration) VALUES (?, ?, ?, ?)')

    # Execute INSERT statement with data from TXT file
    session.execute(insert_statement, (
        data["app"],
        data["func"],
        data["end_timestamp"],
        data["duration"]
    ))


# Ingests data from a TXT file into a Cassandra database
def ingestion(session, file_path):

    rows_ingested = 0  # Store the number of rows that have been added to the Cassandra
    start_time = time.time()  # Start time for ingestion

    # Create the 'azure_functions_invocation_trace' table if it doesn't exist
    create_table(session)

    # Open the TXT file and read the data
    with open(file_path, 'r') as f:
        # Skip the header line
        next(f)

        # Counts how many lines in a file, needed for report
        rows_total = len(f.readlines())
        f.seek(0, os.SEEK_SET)  # seek back to start position of stream, otherwise save() will write a 0 byte file

        # Write each row from the TXT file to the table in Cassandra
        for line in f:
            # Split the line by comma
            app, func, end_timestamp, duration = line.split(",")

            # Create a dictionary with keys and values
            row = {
                "app": app,
                "func": func,
                "end_timestamp": end_timestamp,
                "duration": duration
            }
            write_to_cassandra(session, row)

    end_time = time.time()  # End time for ingestion

    # Create a report containing details of the ingestion process
    report = {
        'file_name': file_path.split('/')[-1],
        'file_path': file_path,
        'ingest_start_time': start_time,
        'ingest_end_time': end_time,
        'ingest_time': end_time - start_time,
        'rows_total': rows_total,
        'rows_ingested': rows_ingested,
        'rows_failed': rows_total - rows_ingested,
        'file_size': os.path.getsize(file_path),
        'ingest_rate_bs': os.path.getsize(file_path) / (end_time - start_time),
        'ingest_rate_rs': rows_ingested / (end_time - start_time)
    }

    return report
