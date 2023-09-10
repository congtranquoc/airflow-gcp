# Tiki and Newegg graphic cards - Data Pipeline Using Apache Airflow
## Purpose
This project aims to build a Data Pipeline using Apache Airflow to automate the data flow from various projects. Specifically, the pipeline will perform the following steps:

1. **Extract Data**:
   - Data needs to be extracted from the following sources and stored in Google Cloud.
   - Storage:
     - Data crawled from the Newegg website stored in MySQL
     - Data crawled from Tiki stored in MongoDB
2. **Transform Data**:
   - Load data from Google Cloud Storage into Data Warehouse Staging.
   - Process data from Tiki:
     - Remove HTML tags from all fields.
     - Determine the creation date of the products.
     - Only consider products that are in stock.
     - Calculate the total amount of money each product has sold on Tiki.
   - Create a schema and transform the processed data into the Data Warehouse.
   - Partition data by category.
3. **Load Data:**:
   - All dashboard requirements from previous projects will be rewritten in SQL. The necessary information for visualization will be stored in separate tables.
   - Schedule dashboard data runs at 7 a.m. daily to update Data Studio.
   - The entire process will be set up using Apache Airflow.

4. **Airflow Data Flow Requirements:**:
   - In case of errors, the data flow will automatically retry up to 3 times.
   - Each retry will have a 5-minute interval.
   - The system will send error notification emails if retries are unsuccessful.
   - Logging will be recorded in files for error tracing.
## Deployment Guide
1. **Install Apache Airflow:** Ensure that you have Apache Airflow installed before deploying this pipeline. Refer to the official Airflow documentation for installation and configuration.

2. **Set up Airflow DAGs:** Place the DAGs (Dataflow Directed Acyclic Graphs) in the dags directory. These DAGs should be configured to perform the extraction, transformation, and loading of data as required by the project.

3. **Configure Airflow:** Configure Airflow to ensure it adheres to the retry, logging, and email error notification requirements.

4. **Deploy the Pipeline:** Deploy the pipeline by running the DAGs in Airflow.

5. **Monitor and Supervise:** Use the Airflow interface to monitor the progress of the pipeline and ensure it operates as expected.

## Important Note

This project focuses on synchronizing Tiki's product data (1.7M records) and Newegg graphic cards (3K7 records) with Google Cloud's Data Warehouse using BigQuery. It encompasses various technical aspects such as Compute Engine setup, MongoDB management, ETL processes (AIRFLOW), and data visualization. The project aims to empower data analysis and facilitate informed decision-making.
