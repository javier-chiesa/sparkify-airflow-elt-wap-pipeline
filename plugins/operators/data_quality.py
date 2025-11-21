"""Data quality operator for validating data in Redshift audit tables.

This operator performs various data quality checks including null checks,
uniqueness validation, and range validations.
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class DataQualityOperator(BaseOperator):
    """Execute data quality checks on specified audit table columns.
    
    Args:
        redshift_conn_id: Airflow connection ID for Redshift
        table: Name of the table to validate
        checks: Dictionary of check types and columns to validate
    """

    ui_color = '#89DA59'

    def __init__(
        self,
        redshift_conn_id='',
        table='',
        checks=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.checks = checks if checks is not None else {}

    def execute(self, context):
        """Execute all configured data quality checks."""
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check_type, columns in self.checks.items():
            for column in columns:
                self._execute_check(redshift, check_type, column)

    def _execute_check(self, redshift, check_type, column):
        """Execute a specific data quality check.
        
        Args:
            redshift: PostgresHook instance
            check_type: Type of check to perform
            column: Column name to validate
            
        Raises:
            ValueError: If data quality check fails
        """
        if check_type == 'not_null':
            self._check_not_null(redshift, column)
        elif check_type == 'unique':
            self._check_unique(redshift, column)
        elif check_type == 'greater_than_zero':
            self._check_greater_than_zero(redshift, column)
        else:
            raise ValueError(f"Unknown check type: {check_type}")

    def _check_not_null(self, redshift, column):
        """Verify column contains no NULL values."""
        sql = f"SELECT COUNT(*) FROM audit.{self.table} WHERE {column} IS NULL"
        result = redshift.get_first(sql)
        
        if result[0] > 0:
            raise ValueError(
                f"Data quality check failed: Column {column} in audit table "
                f"audit.{self.table} contains NULL values."
            )
        
        self.log.info(
            f"Data quality check passed: Column {column} in audit table "
            f"audit.{self.table} contains no NULL values."
        )

    def _check_unique(self, redshift, column):
        """Verify column contains only unique values."""
        sql = f"""
            SELECT COUNT({column}) - COUNT(DISTINCT {column}) 
            FROM audit.{self.table}
        """
        result = redshift.get_first(sql)
        
        if result[0] > 0:
            raise ValueError(
                f"Data quality check failed: Column {column} in audit table "
                f"audit.{self.table} contains duplicate values."
            )
        
        self.log.info(
            f"Data quality check passed: Column {column} in audit table "
            f"audit.{self.table} contains unique values."
        )

    def _check_greater_than_zero(self, redshift, column):
        """Verify column contains only positive values."""
        sql = f"SELECT COUNT(*) FROM audit.{self.table} WHERE {column} <= 0"
        result = redshift.get_first(sql)
        
        if result[0] > 0:
            raise ValueError(
                f"Data quality check failed: Column {column} in audit table "
                f"audit.{self.table} contains values not greater than zero."
            )
        
        self.log.info(
            f"Data quality check passed: Column {column} in audit table "
            f"audit.{self.table} contains values greater than zero."
        )