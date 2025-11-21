"""Dimension table loader operator for Redshift.

This operator loads data into dimension audit tables
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class LoadDimensionOperator(BaseOperator):
    """Load data into a dimension audit table in Redshift.
    
    Args:
        redshift_conn_id: Airflow connection ID for Redshift
        destination_table: Target dimension audit table name
        sql_query: SQL query to extract source data
    """

    template_fields = ('sql_query',)
    ui_color = '#FFA500'

    def __init__(
        self,
        redshift_conn_id='',
        destination_table='',
        sql_query='',
        **kwargs
    ):
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table
        self.sql_query = sql_query

    def execute(self, context):
        """Execute dimension audit table load operation."""
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Truncate audit
        self.log.info(f'Truncating dim audit table audit.{self.destination_table}')
        truncate_sql = f"""
            TRUNCATE TABLE audit.{self.destination_table}
        """
        self.log.info('Executing TRUNCATE query')
        redshift.run(truncate_sql)

        # Insert data
        self.log.info(
            f'Loading data into dimension audit table audit.{self.destination_table}'
        )
        insert_sql = f"""
            INSERT INTO audit.{self.destination_table}
            {self.sql_query}
        """
        self.log.info('Executing INSERT query')
        redshift.run(insert_sql)

        # Log final row count
        count_sql = f"SELECT COUNT(*) FROM audit.{self.destination_table}"
        result = redshift.get_first(count_sql)
        self.log.info(f'Total rows in audit.{self.destination_table}: {result[0]}')
        self.log.info(
            f'Successfully loaded data into audit.{self.destination_table}'
        )