"""Production publish operator for promoting data from audit to prod.

This operator implements the final 'Publish' step in the WAP pattern,
promoting validated data to production schema.

Supported load modes:
- append
- merge (requires pk_columns)
- partition_overwrite
- full_refresh
"""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class PublishProductionOperator(BaseOperator):
    """Publish validated data from audit schema to prod schema.
    
    Args:
        redshift_conn_id: Airflow connection ID for Redshift
        table: Table name to publish (without schema prefix)
        load_mode: Load mode for publishing (append, merge, partition_overwrite, full_refresh)
        pk_columns: List of primary keys required for merge mode
    """
    
    ui_color = '#4ECDC4'

    def __init__(
        self,
        redshift_conn_id='',
        table='',
        load_mode='',
        pk_columns=None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_mode = load_mode
        self.pk_columns = pk_columns or []

    def execute(self, context):
        """Execute production publish operation."""
        self.log.info(
            f"Starting production publish for table: {self.table}, mode={self.load_mode}"
        )

        dwh_ds = context["ds"]
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        delete_partition_sql = f"""
            DELETE FROM prod.{self.table}
            WHERE dwh_ds = '{dwh_ds}';
        """

        insert_sql = f"""
            INSERT INTO prod.{self.table}
            SELECT * FROM audit.{self.table};
        """

        truncate_sql = "TRUNCATE TABLE {schema}.{table};"

        try:

            if self.load_mode == 'append':
                self.log.info(
                    f"Appending data to prod table prod.{self.table} (dwh_ds = {dwh_ds})"
                )
                redshift.run(insert_sql)

            elif self.load_mode == 'partition_overwrite':
                self.log.info(
                    f"Overwriting partition for ds={dwh_ds} in prod.{self.table}"
                )
                redshift.run(delete_partition_sql)
                redshift.run(insert_sql)

            elif self.load_mode == 'full_refresh':
                self.log.info(
                    f"Performing FULL REFRESH on prod.{self.table}"
                )
                redshift.run(truncate_sql.format(schema='prod', table=self.table))
                redshift.run(insert_sql)

            elif self.load_mode == 'merge':

                if not self.pk_columns:
                    raise ValueError(
                        f"MERGE mode requires pk_columns for table {self.table}"
                    )

                self.log.info(
                    f"Merging data into prod.{self.table} using PK columns: {self.pk_columns}"
                )

                join_condition = " AND ".join(
                    [f"prod.{self.table}.{col} = audit.{col}" for col in self.pk_columns]
                )

                delete_merge_sql = f"""
                    DELETE FROM prod.{self.table}
                    WHERE EXISTS (
                        SELECT 1 
                        FROM audit.{self.table} audit
                        WHERE {join_condition}
                    );
                """

                redshift.run(delete_merge_sql)
                redshift.run(insert_sql)

            else:
                raise ValueError(
                    f"Invalid load_mode '{self.load_mode}' for table {self.table}"
                )

            redshift.run(truncate_sql.format(schema='audit', table=self.table))

            count_sql = f"SELECT COUNT(*) FROM prod.{self.table};"
            row_count = redshift.get_first(count_sql)[0]

            self.log.info(
                f"Publish completed for {self.table}. Total rows now in prod: {row_count}"
            )

        except Exception as e:
            self.log.error(
                f"Failed to publish audit.{self.table} to production"
            )
            self.log.error(f"Error: {str(e)}")
            raise
