import logging, json
import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT_ID = "dataflow-pipeline-maven"
SCHEMA = parse_table_schema_from_json(json.dumps(json.load(open("/home/airflow/gcs/dags/schema1.json"))))

def run(argv=None):    
    query =  """SELECT
                    *,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Created_Time,
                    FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', CURRENT_DATETIME()) AS Modified_Time
                FROM `dataflow-pipeline-maven.Employee.employee_temp`"""
    
    p= beam.Pipeline(options=PipelineOptions())
    (p                                    
     | 'Query from BigQuery' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
     | 'Write to BigQuery' >> beam.io.WriteToBigQuery('{0}:Employee.employee_data'.format(PROJECT_ID),
                                                schema=SCHEMA,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()