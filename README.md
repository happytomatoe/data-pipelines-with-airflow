# Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


# Prerequisites to run the dag
- Setup connections in airflow
    - aws_credentials connection. Choose connection type - Amazon Web service
      Set the login to access_key and password to access_secret_key
    - redshift connection. Choose connection type - Postgres and fill out all the needed details
- Execute create_tables.sql to populate tables in redshift

# Honorable mentions
- [Airflow documentation](http://airflow.apache.org/docs/apache-airflow/1.10.1/index.html)
- [Redshift copy command documentation](https://docs.aws.amazon.com/redshift/latest/dg/t_loading-tables-from-s3.html)
- Adapted docker-compose from Docker compose file from https://github.com/xnuinside/airflow_in_docker_compose