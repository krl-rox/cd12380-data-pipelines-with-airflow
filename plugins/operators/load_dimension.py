from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 sql_stmt="",
                 mode="append",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode == "truncate-insert":
            self.log.info("Truncating dimension table %s before insert", self.table)
            redshift.run(f"TRUNCATE TABLE {self.table}")

        insert_sql = f"INSERT INTO {self.table} {self.sql_stmt}"

        self.log.info("Loading dimension table %s (mode=%s)", self.table, self.mode)
        redshift.run(insert_sql)
        self.log.info("LoadDimensionOperator completed for %s", self.table)
