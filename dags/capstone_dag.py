"""Main DAG file for ETL data pipeline."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (S3ToRedshiftOperator,
                       RenderToS3Operator,
                       LoadFactDimOperator,
                       DataQualityOperator)
from helpers import *

default_args = {
    'owner': 'Norhan Zaied',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 8,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False

}

dag = DAG('capstone_dag',
          default_args=default_args,
          description='Load and transform capstone project data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
          )


start_data_to_redshift_operation = DummyOperator(
    task_id='Begin_Migrating_Data_To_Staging_Tables',  dag=dag)

end_data_to_redshift_operation = DummyOperator(
    task_id='Begin_Loading_Data_To_Fact_Dimension_Tables',  dag=dag)

end_of_tasks = DummyOperator(
    task_id='End_Of_Execution',  dag=dag)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id='redshift',
    target_tables=["dim_airport_table", "dim_demographic_table", "dim_visitor_table", "fact_city_data_table"],
)

stage_immigration_to_redshift = S3ToRedshiftOperator(
        task_id='Stage_immigration',
        dag=dag,
        table='immigration_table',
        drop_table=True,
        s3_bucket='udend-processed-data',
        s3_folder='immigration',
        aws_connection_id='aws_credentials',
        redshift_connection_id='redshift',
        create_query=globals()['immigration_table'],
        copy_options="json 'auto'"
    )
    

stage_temperature_to_redshift = S3ToRedshiftOperator(
        task_id='Stage_temperature',
        dag=dag,
        table='temperature_table',
        drop_table=True,
        s3_bucket='udend-processed-data',
        s3_folder='temperature',
        aws_connection_id='aws_credentials',
        redshift_connection_id='redshift',
        create_query=globals()['temperature_table'],
        copy_options="json 'auto'"
    )

stage_airport_to_redshift = S3ToRedshiftOperator(
        task_id='Stage_airport',
        dag=dag,
        table='airport_table',
        drop_table=True,
        s3_bucket='udend-processed-data',
        s3_folder='airport',
        aws_connection_id='aws_credentials',
        redshift_connection_id='redshift',
        create_query=globals()['airport_table'],
        copy_options="json 'auto'"
    )

stage_demographics_to_redshift = S3ToRedshiftOperator(
        task_id='Stage_demographics',
        dag=dag,
        table='demographics_table',
        drop_table=True,
        s3_bucket='udend-processed-data',
        s3_folder='demographics',
        aws_connection_id='aws_credentials',
        redshift_connection_id='redshift',
        create_query=globals()['demographics_table'],
        copy_options="json 'auto'"
    )
start_data_to_redshift_operation >> stage_immigration_to_redshift >> end_data_to_redshift_operation
start_data_to_redshift_operation >> stage_temperature_to_redshift >> end_data_to_redshift_operation
start_data_to_redshift_operation >> stage_airport_to_redshift >> end_data_to_redshift_operation
start_data_to_redshift_operation >> stage_demographics_to_redshift >> end_data_to_redshift_operation



load_fact_dim_city_data = LoadFactDimOperator(
        task_id='Load_city_data',
        dag=dag,
        conn_id='redshift',
        target_table='fact_city_data_table',
        drop_table=True,
        create_query=globals()['fact_city_data_table'],
        insert_query=globals()['fact_city_table_insert'],
        append=False
    )

load_fact_dim_demographic = LoadFactDimOperator(
        task_id='Load_demographic_data',
        dag=dag,
        conn_id='redshift',
        target_table='dim_demographic_table',
        drop_table=True,
        create_query=globals()['dim_demographic_table'],
        insert_query=globals()['dim_demographic_table_insert'],
        append=False
    )

load_fact_airport = LoadFactDimOperator(
        task_id='Load_airport_data',
        dag=dag,
        conn_id='redshift',
        target_table='dim_airport_table',
        drop_table=True,
        create_query=globals()['dim_airport_table'],
        insert_query=globals()['dim_airport_table_insert'],
        append=False
    )
load_fact_visitor = LoadFactDimOperator(
        task_id='Load_visitor_data',
        dag=dag,
        conn_id='redshift',
        target_table='dim_visitor_table',
        drop_table=True,
        create_query=globals()['dim_visitor_table'],
        insert_query=globals()['dim_visitor_table_insert'],
        append=False
    )
end_data_to_redshift_operation >> load_fact_dim_city_data >> run_quality_checks
end_data_to_redshift_operation >> load_fact_dim_demographic >> run_quality_checks
end_data_to_redshift_operation >> load_fact_airport >> run_quality_checks
end_data_to_redshift_operation >> load_fact_visitor >> run_quality_checks
run_quality_checks >> end_of_tasks


