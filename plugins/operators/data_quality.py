from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 checks=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks or []

    def execute(self, context):
        if not self.checks:
            raise ValueError("No data quality checks provided")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for i, check in enumerate(self.checks, start=1):
            sql = check["sql"]
            expected = check["expected"]

            self.log.info("Running data quality check %s: %s", i, sql)
            records = redshift.get_records(sql)

            if not records or not records[0]:
                raise ValueError(f"Data quality check {i} returned no results for SQL: {sql}")

            actual = records[0][0]

            if actual != expected:
                raise ValueError(
                    f"Data quality check {i} failed. SQL: {sql} | expected={expected}, actual={actual}"
                )

            self.log.info("Data quality check %s passed (expected=%s, actual=%s)", i, expected, actual)
