from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta  # Import timedelta
import psycopg2
import json
from xml.etree import ElementTree as ET
import csv
import re

# Define your database connection details
dbname = "dbt_poc"  # Update dbname
user = "postgres"  # Update user
password = "admin"  # Update password
host = "host.docker.internal"  # Use the Docker service name here
port = "5432"

def extract_and_rebuild_xml(xml_file, metadata_table, output_csv):
    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        cur = conn.cursor()
        cur.execute(f"SELECT tag FROM {metadata_table} WHERE active = TRUE")
        necessary_tags = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
    except psycopg2.OperationalError as e:
        print("Error connecting to the database:", e)
        return  # Exit function if connection fails

    with open(xml_file, 'r', encoding='utf-8') as f:
        xml_content = f.read()

    root_tag = "genericSMF"
    pattern = rf'(<{root_tag}>.*?</{root_tag}>)'
    xml_documents = re.findall(pattern, xml_content, re.DOTALL)

    all_xml_str = []
    all_json_data = []

    for xml_doc in xml_documents:
        try:
            tree = ET.ElementTree(ET.fromstring(xml_doc))
            root = tree.getroot()

            def filter_tags(element):
                if element.tag in necessary_tags:
                    return element
                else:
                    return [child for child in element.iter() if child.tag in necessary_tags]

            filtered_elements = filter_tags(root)

            new_root = ET.Element(root.tag)
            for element in filtered_elements:
                new_root.append(element)

            xml_str = ET.tostring(new_root, encoding='unicode')
            json_data = json.dumps(xml_to_dict(xml_str), indent=4)
            all_xml_str.append(xml_str.strip())
            all_json_data.append(json_data.strip())
        except ET.ParseError as e:
            print(f"Error parsing XML document: {e}")
            continue

    with open(output_csv, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["attbr_xml", "attbr_json"])  
        for xml_str, json_data in zip(all_xml_str, all_json_data):
            writer.writerow([xml_str, json_data])

def xml_to_dict(xml_str):
    root = ET.fromstring(xml_str)
    return {root.tag: xml_to_dict_helper(root)}

def xml_to_dict_helper(element):
    if len(element) == 0:
        return element.text if element.text else ''
    result = {}
    for child in element:
        child_result = xml_to_dict_helper(child)
        if child.tag in result:
            if isinstance(result[child.tag], list):
                result[child.tag].append(child_result)
            else:
                result[child.tag] = [result[child.tag], child_result]
        else:
            result[child.tag] = child_result
    return result

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'xml_processing_dag',
    default_args=default_args,
    description='A DAG to process XML data',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    file_sensor = FileSensor(
        task_id='file_sensor',
        fs_conn_id='file_system',  # This needs to be set up in the Airflow connections
        filepath='/usr/local/airflow/madhu/SMFsourcefile.xml',
        poke_interval=5,  # Check for the file every 30 seconds
        timeout=600  # Timeout after 10 minutes if the file is not found
    ) 

    extract_xml_task = PythonOperator(
        task_id='extract_xml',
        python_callable=extract_and_rebuild_xml,
        op_kwargs={
            'xml_file': "/usr/local/airflow/madhu/SMFsourcefile.xml",
            'metadata_table': "metadata.metadata_table",
            'output_csv': "/opt/airflow/dbt/seeds/xml_data.csv"
        }
    )

    # dbt_seed = BashOperator(
    #     task_id='dbt_seed',
    #     bash_command='cd /opt/airflow/dbt && dbt seed'
    # )


    file_sensor >> extract_xml_task 