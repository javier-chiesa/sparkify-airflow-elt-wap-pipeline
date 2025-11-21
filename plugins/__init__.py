from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
        operators.PublishProductionOperator
    ]
    helpers = [
        helpers.SqlQueries,
        helpers.STAGING_CONFIG,
        helpers.FACT_CONFIG,
        helpers.DIMENSION_CONFIG,
        helpers.REDSHIFT_CONN_ID,
        helpers.AWS_CONN_ID,
        helpers.S3_REGION
    ]