# First Assignment - Building Big Data Platforms

The goal of this assignment is to help understand system design and provisioning for big data platforms, especially in the core components related to databases/data storage.

### Directory structure:

- **_code_:** source code
- **_data_**: sample data that was used in the tests
- **_reports:_** include assignment report and deployment guid

### Architecture:

The diagram below depicts the architecture of my Big Data platform design. Each main components description and function is as follows:

- **mysimbdp-coredms** is a key component of the mysimbdp big data platform, responsible for storing and managing data.
  It is uses Apache Cassandra as its underlying database management system.
  Its main function is to store and manage data.
- **mysimbdp-dataingest** is a key component of the mysimbdp big data platform, responsible for reading data from external data sources and storing it in mysimbdp-coredms.
  It is uses Apache Kafka as its underlying messaging system to manage data ingestion.
  Its main function is to read and store data in mysimbdp-coredms.
- **mysimbdp-daas** (not implemented) provides APIs to external data producers/consumers for storing and retrieving data in/from mysimbdp-coredms, thereby acting as a bridge between them.
  It manages data authentication and authorization, as well as data transformation and quality checks.

The following are the interactions between the main platform components:

- **kafka_producer.py** sends the data to the Kafka topic specified as an argument.
- **kafka_consumer-cassandra_ingestion.py** subscribes to the Kafka topic specified as an argument and consumes data from it.
  For each message received, the message data is parsed and passed to the _write_to_cassandra()_ function.
- **write_to_cassandra()** prepares an INSERT statement and executes it with the data from the Kafka message.

![reports/architecture.png](reports/architecture.png)

For more information about desing and technical issues, check **_Reports/Assignment_1_Report_.**
