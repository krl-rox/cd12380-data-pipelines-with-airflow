from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                aws_credentials_id="aws_credentials",
                table="",
                s3_bucket="",
                s3_key="",
                json_format="auto",
                region="us-west-2",
                truncate_table=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format
        self.region = region
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Fetching AWS credentials from Airflow connection: %s", self.aws_credentials_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        self.log.info("Staging data from %s to Redshift table %s", s3_path, self.table)

        if self.truncate_table:
            self.log.info("Truncating table %s before COPY", self.table)
            redshift.run(f"TRUNCATE TABLE {self.table}")

        json_clause = "'auto'" if self.json_format == "auto" else f"'{self.json_format}'"

        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            REGION '{self.region}'
            FORMAT AS JSON {json_clause}
            TIMEFORMAT AS 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
        """

        self.log.info("Running COPY command for table %s", self.table)
        redshift.run(copy_sql)
        self.log.info("COPY completed for table %s", self.table)
