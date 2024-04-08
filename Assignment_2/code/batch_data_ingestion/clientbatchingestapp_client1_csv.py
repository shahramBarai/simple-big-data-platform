import os, datetime, time, csv
import dateutil.parser


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
def ingestion(session, file_path):
    # Create the 'turtle' table if it doesn't exist
    create_table(session)

    # Open the CSV file and read the data
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)

        # Counts how many lines in a file, needed for report
        with open(file_path, 'r') as file2:
            rows_total = sum(1 for row in file2)

        rows_ingested = 0               # Store the number of rows that have been added to the Cassandra
        start_time = time.time()     # Start time for ingestion

        # Write each row from the CSV file to the 'turtle' table in Cassandra
        for row in reader:
            write_to_cassandra(session, row)
            rows_ingested += 1

        end_time = time.time()       # End time for ingestion

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
        'ingest_rate_rs': rows_total / (end_time - start_time)
    }

    return report
