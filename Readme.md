Big Data Services
This repository contains programs and services for integrating, testing, and querying data using Apache Iceberg, Kafka, and Spark.

Table of Contents
Overview
Programs
Integration Service
Order Data Source Testing
Iceberg Data Query
Usage
Prerequisites
Change the kafka url in the bigdata/src/main/resources/config.properties

Running the Programs

Build and Package
Overview
This Maven-based project provides three main Scala programs:

Integration Service: A service to handle data integration processes.
Order Data Source Testing: A test program for publishing order data logs to Kafka.
Iceberg Data Query: A query program to retrieve order data from an Apache Iceberg table.
Each program is located in the bigdata/src/main/scala/com/zetaris/app/ directory.

Programs
1. Integration Service
   File: bigdata/src/main/scala/com/zetaris/app/IntegrationService.scala
   The Integration Service is the core service that manages data integration processes between various systems.

2. Order Data Source Testing
   File: bigdata/src/main/scala/com/zetaris/app/PublishLog2Kafka.scala
   This program publishes order logs to a Kafka topic for testing and validating the data ingestion pipeline.

3. Iceberg Data Query
   File: bigdata/src/main/scala/com/zetaris/app/QueryOrdersFromIceberg.scala
   This program queries the orders table stored in Apache Iceberg, allowing you to validate and analyze the stored data.

Usage
Prerequisites
Before running any program, ensure the following dependencies and services are properly configured:

Maven: Installed and added to your system's PATH.
Kafka: A running Kafka cluster.
Spark: A running Spark environment with Iceberg support.
Iceberg: A properly configured Iceberg table catalog.
Install the project dependencies:

bash
```
mvn clean install
```
Running the Programs
Use the following Maven commands to run each program:

Integration Service
To start the Integration Service:

bash
```
mvn exec:java -Dexec.mainClass="com.zetaris.app.IntegrationService"
```
Order Data Source Testing
To run the order data source testing program:

bash
```
mvn exec:java -Dexec.mainClass="com.zetaris.app.PublishLog2Kafka"
```
This will publish sample order logs to a Kafka topic for testing the pipeline.

Iceberg Data Query
To query data from the Apache Iceberg table:

bash
```
mvn exec:java -Dexec.mainClass="com.zetaris.app.QueryOrdersFromIceberg"
```
This program retrieves order data from the Iceberg orders table, sorting and filtering as defined in the implementation.

Build and Package
To package the project into a runnable JAR:

bash
Copy code
```
The resulting JAR file will be located in the target/ directory.
```

Setup and Run Integration tests
1. Configure Kafka
   Ensure Kafka is running and accessible. Update src/main/resources/config.properties with the correct Kafka configuration:

properties
Copy code
kafka.bootstrap-servers = localhost:9092
2. Build the Project
   Build the project using Maven:

bash
```
mvn clean install
```
3. Start the Integration Service
   Run the IntegrationService located at src/main/java/com/zetaris/app/IntegrationService.java. This service initializes the Spark session, configures Kafka, and manages the Iceberg table.

bash
```
mvn exec:java -Dexec.mainClass=com.zetaris.app.IntegrationService
```
4. Run the Test Suite
   Execute the test suite using Maven:

bash
```
mvn test
```
Test Cases
The test suite includes the following cases:

Test Insert 2 New Orders: Validates that two new orders are successfully inserted into the Iceberg table.
Test Insert One Duplicated Order: Ensures duplicate orders are not reinserted.
Test Update the Status of One Order: Verifies that the order status is updated correctly.
Test Event Sequential Consistency: Ensures the sequence of events in the Iceberg table is consistent.
Test Data Integrity: Validates the integrity of specific fields in the inserted records.
Logs will provide additional insights during execution.

Key Files
Kafka Configuration: src/main/resources/config.properties
Integration Service: src/main/java/com/zetaris/app/IntegrationService.java
Test Suite: src/test/java/com/zetaris/app/IntegrationServiceTest.java
