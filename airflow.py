#Importing the libraries
from airflow import DAG
from datetime import datetime
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.operators.dummy_operator import DummyOperator

#initiating DAG 
dag = DAG(dag_id= 'dataflow_pipe', start_date=datetime.today(), catchup=False, schedule_interval='@once')

load_from_GCS_to_BQ = BeamRunPythonPipelineOperator(
    task_id="load_from_GCS_to_BQ",
    runner="DataflowRunner",
    py_file="gs://us-central1-mwproject-4fbfed69-bucket/dags/task.py",
    pipeline_options={'tempLocation': 'gs://us-central1-mwproject-4fbfed69-bucket/dags/test', 
                      'stagingLocation': 'gs://us-central1-mwproject-4fbfed69-bucket/dags/test'},
    py_options=[],
    py_requirements=['apache-beam[gcp]==2.40.0'],
    py_interpreter='python3',
    py_system_site_packages=False,
    dataflow_config=DataflowConfiguration(job_name='gcs-to-bq', project_id='dataflow-pipeline-maven', location="us-central1"),
    dag=dag)

problem_statement1 = BeamRunPythonPipelineOperator(
    task_id="problem_statement1",
    runner="DataflowRunner",
    py_file="gs://us-central1-mwproject-4fbfed69-bucket/dags/task1.py",
    pipeline_options={'tempLocation': 'gs://us-central1-mwproject-4fbfed69-bucket/dags/test', 
                      'stagingLocation': 'gs://us-central1-mwproject-4fbfed69-bucket/dags/test'},
    py_options=[],
    py_requirements=['apache-beam[gcp]==2.40.0'],
    py_interpreter='python3',
    py_system_site_packages=False,
    dataflow_config=DataflowConfiguration(job_name='prob-stat-1', project_id='dataflow-pipeline-maven', location="us-central1"),
    dag=dag)

problem_statement2 = BeamRunPythonPipelineOperator(
    task_id="problem_statement2",
    runner="DataflowRunner",
    py_file="gs://us-central1-mwproject-4fbfed69-bucket/dags/task2.py",
    pipeline_options={'tempLocation': 'gs://us-central1-mwproject-4fbfed69-bucket/dags/test', 
                      'stagingLocation': 'gs://us-central1-mwproject-4fbfed69-bucket/dags/test'},
    py_options=[],
    py_requirements=['apache-beam[gcp]==2.40.0'],
    py_interpreter='python3',
    py_system_site_packages=False,
    dataflow_config=DataflowConfiguration(job_name='prob-stat-2', project_id='dataflow-pipeline-maven', location="us-central1"),
    dag=dag)


Start = DummyOperator(task_id='Start')
End = DummyOperator(task_id='End')

Start >> load_from_GCS_to_BQ >> [problem_statement1, problem_statement2] >> End