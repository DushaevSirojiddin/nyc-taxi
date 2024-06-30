from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import re
import datetime

class S3ToRedshiftOperator(BaseOperator):
    ui_color = "#8D909B"

    @apply_defaults
    def __init__(self,
                 aws_credentials="",
                 redshift_conn_id="",
                 bucket="",
                 prefix="",
                 table="",
                 regex="",
                 sql="",
                 append=False,
                 *args, **kwargs):
        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials = aws_credentials
        self.redshift_conn_id = redshift_conn_id
        self.bucket = bucket
        self.prefix = prefix
        self.table = table
        self.regex = regex  # Regex should be fully formed when passed to the operator
        self.sql = sql
        self.append = append

    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id=self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.aws_credentials)

        keys = s3_hook.list_keys(bucket_name=self.bucket, prefix=self.prefix)
        expr = re.compile(self.regex)
        new_files = [key for key in keys if expr.match(key)]
        processed_files = [record[0] for record in
                           redshift_hook.get_records("SELECT filename FROM dev.bronze.processed_files_metadata")]

        for file in new_files:
            if file not in processed_files:
                s3_path = f"s3://{self.bucket}/{file}"
                formatted_sql = self.sql.format(
                    table=self.table,
                    s3_path=s3_path,
                    access_key=credentials.access_key,
                    secret_key=credentials.secret_key
                )
                if not self.append:
                    self.log.info(f"Truncating table {self.table}...")
                    redshift_hook.run(f"TRUNCATE TABLE {self.table};")

                self.log.info(f"Copying data from {s3_path} to Redshift table {self.table}...")
                redshift_hook.run(formatted_sql)
                self.log.info(f"Successfully copied data from {s3_path} to Redshift table {self.table}.")
                redshift_hook.run(f"INSERT INTO dev.bronze.processed_files_metadata (filename) VALUES ('{file}')")