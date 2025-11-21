"""Staging operator for loading data from S3 to Redshift.

This operator uses the COPY command to efficiently load data from S3
into Redshift staging tables with support for incremental and full loads.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator


class StageToRedshiftOperator(BaseOperator):
    """Copy data from S3 to Redshift staging table.
    
    Args:
        redshift_conn_id: Airflow connection ID for Redshift
        aws_conn_id: Airflow connection ID for AWS credentials
        table: Target staging table name
        s3_bucket: S3 bucket name
        s3_key: S3 key/prefix (supports Jinja templating)
        s3_region: AWS region for S3 bucket
        json_path: JSON path file or 'auto' for auto-detection
        delete_by_date: If True, delete by execution date; else truncate
    """

    ui_color = '#358140'
    template_fields = ('s3_key',)
    
    def __init__(
        self,
        redshift_conn_id='',
        aws_conn_id='',
        table='',
        s3_bucket='',
        s3_key='',
        s3_region='',
        json_path='',
        delete_by_date=False,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.json_path = json_path
        self.delete_by_date = delete_by_date

    def execute(self, context):
        """Execute S3 to Redshift COPY operation."""
        # Get AWS credentials
        aws_hook = AwsBaseHook(aws_conn_id=self.aws_conn_id, client_type='s3')
        aws_credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Handle deletion strategy
        if self.delete_by_date:
            execution_date = context.get('ds')
            self.log.info(
                f'Deleting records from staging.{self.table} for '
                f'execution date: {execution_date}'
            )
            delete_sql = f"""
                DELETE FROM staging.{self.table}
                WHERE DATE(
                    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'
                ) = '{execution_date}'
            """
            redshift.run(delete_sql)
            self.log.info(
                f'Deleted records for {execution_date} from staging.{self.table}'
            )
        else:
            self.log.info(f'Truncating table: staging.{self.table}')
            redshift.run(f'TRUNCATE TABLE staging.{self.table}')

        # Prepare S3 path and JSON format
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'

        if self.json_path == 'auto':
            json_format = "'auto'"
        else:
            json_format = f"'s3://{self.s3_bucket}/{self.json_path}'"

        # Execute COPY command
        self.log.info(
            f'Copying data from {s3_path} to Redshift table: staging.{self.table}'
        )
        copy_sql = f"""
            COPY staging.{self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{aws_credentials.access_key}'
            SECRET_ACCESS_KEY '{aws_credentials.secret_key}'
            FORMAT AS JSON {json_format}
            REGION '{self.s3_region}'
        """

        self.log.info(f'Executing COPY command to staging.{self.table}')
        redshift.run(copy_sql)
        self.log.info(f'COPY command completed for staging.{self.table}')