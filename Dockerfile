# Use the official Airflow image as the base image
FROM apache/airflow:2.5.1

# Install dbt using pip
RUN pip install dbt-core dbt-postgres  # Add other dbt adapters as needed

# If you are using a specific directory for dbt projects, make sure it exists
RUN mkdir -p /opt/airflow/dbt

# Set the environment variables for dbt (if needed)
ENV DBT_PROFILES_DIR=/opt/airflow/dbt/profiles

# Expose the port Airflow web server runs on
EXPOSE 8080

# Run Airflow commands
ENTRYPOINT ["airflow"]
CMD ["webserver"]