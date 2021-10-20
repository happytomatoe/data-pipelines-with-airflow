# Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


# How to run the dag
- Create redshift connections in airflow. Choose connection type - Postgres and fill out all the needed details 
- Set AWS_KEY, AWS_SECRET is airflow variables or set the environment variables with same names
- Execute create_tables.sql to populate tables in redshift

# Honorable mentions
Docker compose file from https://github.com/xnuinside/airflow_in_docker_compose