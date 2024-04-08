import os
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
        str(data["app"]),
        str(data["func"]),
        str(data["end_timestamp"]),
        str(data["duration"])
    ))


# Ingests data from a CSV file into a Cassandra database
def ingestion(session, json_data):
    # Create the 'turtle' table if it doesn't exist
    create_table(session)

    start_time = time.time()     # Start time for ingestion

    # Write json data to the 'turtle' table in Cassandra
    write_to_cassandra(session, json_data)

    end_time = time.time()       # End time for ingestion

    # Create a report containing details of the ingestion process
    report = {
        'ingest_end_time': end_time,
        'ingest_file_size': len(json_data),
        'ingest_rate_bs':  len(json_data) / (end_time - start_time)
    }

    return report