import dateutil.parser
import datetime
import time


# Creates the 'turtle' table in the Cassandra keyspace if it doesn't exist
def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS turtle (
            time text,
            readable_time timestamp,
            acceleration float,
            acceleration_x int,
            acceleration_y int,
            acceleration_z int,
            battery int,
            humidity float,
            pressure float,
            temperature float,
            dev_id text,
            PRIMARY KEY (dev_id, readable_time)
        );
    """)


# Writes data from the CSV file to the 'turtle' table in Cassandra
def write_to_cassandra(session, data):
    # Prepare Cassandra INSERT statement
    insert_statement = session.prepare('INSERT INTO turtle (time, readable_time, acceleration, acceleration_x, acceleration_y, acceleration_z, battery, humidity, pressure, temperature, dev_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')

    # Convert timestamp to ISO 8601 format
    iso_timestamp = dateutil.parser.parse(data["readable_time"]).isoformat()

    # Parse ISO 8601 timestamp using datetime.datetime.fromisoformat()
    readable_time = datetime.datetime.fromisoformat(iso_timestamp)

    # Execute INSERT statement with data from CSV
    session.execute(insert_statement, (
        data["time"],
        readable_time,
        float(data["acceleration"]),
        int(data["acceleration_x"]),
        int(data["acceleration_y"]),
        int(data["acceleration_z"]),
        int(data["battery"]),
        float(data["humidity"]),
        float(data["pressure"]),
        float(data["temperature"]),
        data["dev-id"]
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