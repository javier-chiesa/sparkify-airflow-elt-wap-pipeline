"""Airflow DAG for data warehouse ETL pipeline with WAP pattern.

This DAG implements a Write-Audit-Publish pattern for loading data from S3
into Redshift, transforming it into fact and dimension tables, and promoting
validated data to production.

The pipeline consists of:
1. Staging: Load raw data from S3 to Redshift staging tables
2. Write: Transform and load data into fact and dimension tables (dev schema)
3. Audit: Execute data quality checks
4. Publish: Promote validated data to production schema
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task_group

from helpers import (
    STAGING_CONFIG,
    FACT_CONFIG,
    DIMENSION_CONFIG,
    REDSHIFT_CONN_ID,
    AWS_CONN_ID,
    S3_REGION
)
from airflow.operators.dummy_operator import DummyOperator
from operators import (
    StageToRedshiftOperator, 
    LoadFactOperator,
    LoadDimensionOperator, 
    DataQualityOperator,
    PublishProductionOperator
)

# Default arguments for all tasks
DEFAULT_ARGS = {
    'owner': 'Javier Chiesa',
    'start_date': datetime(2018, 1, 1),
    'depends_on_past': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


def create_staging_tasks(config):
    """Create staging tasks for loading data from S3 to Redshift.
    
    Args:
        config: Configuration dictionary for staging tables
    Returns:
        Task with the StageToRedshiftOperator
    """
    staging_task = StageToRedshiftOperator(
        task_id=f"stage_{config['table']}",
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_conn_id=AWS_CONN_ID,
        table=config['table'],
        s3_bucket='javier-chiesa',
        s3_key=config['s3_key'],
        s3_region=S3_REGION,
        json_path=config['json_path'],
        delete_by_date=config['delete_by_date']
    )
    
    return staging_task

def create_fact_table_group(config):
    """Create a WAP task group for a single fact table.
    
    Args:
        config: Configuration dictionary for fact tables
        
    Returns:
        TaskGroup containing Write, Audit, and Publish tasks
    """
    @task_group(
        group_id=f"WAP_{config['table']}_fact_table",
        ui_color='#F98866'
    )
    def fact_wap_group():
        """WAP pattern task group for fact table."""
        write_task = LoadFactOperator(
            task_id=f'write_{config["table"]}',
            redshift_conn_id=REDSHIFT_CONN_ID,
            destination_table=config['table'],
            sql_query=config['sql_query']
        )

        audit_task = DataQualityOperator(
            task_id=f'audit_{config["table"]}',
            redshift_conn_id=REDSHIFT_CONN_ID,
            table=config['table'],
            checks=config['checks']
        )

        publish_task = PublishProductionOperator(
            task_id=f'publish_{config["table"]}',
            redshift_conn_id=REDSHIFT_CONN_ID,
            table=config['table'],
            load_mode=config['load_mode'],
            pk_columns=config.get('pk_columns', [])
        )

        # Define WAP sequence
        write_task >> audit_task >> publish_task
    
    return fact_wap_group()


def create_dimension_table_group(config):
    """Create a WAP task group for a single dimension table.
    
    Args:
        config: Configuration dictionary for the dimension table
        
    Returns:
        TaskGroup containing Write, Audit, and Publish tasks
    """
    @task_group(
        group_id=f"WAP_{config['table']}_dim_table",
        ui_color='#80CED7'
    )
    def dimension_wap_group():
        """WAP pattern task group for dimension table."""
        write_task = LoadDimensionOperator(
            task_id=f'write_{config["table"]}',
            redshift_conn_id=REDSHIFT_CONN_ID,
            destination_table=config['table'],
            sql_query=config['sql_query']
        )

        audit_task = DataQualityOperator(
            task_id=f'audit_{config["table"]}',
            redshift_conn_id=REDSHIFT_CONN_ID,
            table=config['table'],
            checks=config['checks']
        )

        publish_task = PublishProductionOperator(
            task_id=f'publish_{config["table"]}',
            redshift_conn_id=REDSHIFT_CONN_ID,
            table=config['table'],
            load_mode=config['load_mode'],
            pk_columns=config.get('pk_columns', [])
        )

        # Define WAP sequence
        write_task >> audit_task >> publish_task
    
    return dimension_wap_group()


@dag(
    default_args=DEFAULT_ARGS,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 0 * * *',
    catchup=False,
    max_active_runs=1
)
def sparkify_elt_wap():
    """Define the data warehouse ETL pipeline DAG.
    
    Pipeline flow:
    Start -> Staging -> Fact Tables -> Dimension Tables -> End
    """
    # Start task
    start_task = DummyOperator(task_id='Start_execution')
    
    # Create staging tasks
    staging_tasks = [create_staging_tasks(config) for config in STAGING_CONFIG]
    
    # Create fact table WAP groups
    fact_groups = [create_fact_table_group(config) for config in FACT_CONFIG]
    
    # Create dimension table WAP groups
    dimension_groups = [
        create_dimension_table_group(config) for config in DIMENSION_CONFIG
    ]
    
    # End task
    end_task = DummyOperator(task_id='End_execution')


    # Define pipeline dependencies
    start_task >> staging_tasks
    
    # Each staging task to all fact table groups
    for staging_task in staging_tasks:
        for fact_group in fact_groups:
            staging_task >> fact_group
    
    # Each fact group to all dimension groups
    for fact_group in fact_groups:
        for dimension_group in dimension_groups:
            fact_group >> dimension_group
    
    # All dimension groups to end
    for dimension_group in dimension_groups:
        dimension_group >> end_task


# Instantiate the DAG
sparkify_elt_wap_dag = sparkify_elt_wap()