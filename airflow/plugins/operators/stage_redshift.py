from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        copy {}
        from '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        format as {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket="",
                 s3_key="",
                 create_table_sql = "",
                 format_mode = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.create_table_sql = create_table_sql
        self.format_mode = format_mode
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
         
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.format_mode
        )
        self.log.info("Dropping table: {}".format(self.table))
        redshift_hook.run("drop table if exists {}".format(self.table))
        self.log.info("Creating table: {}".format(self.table))
        redshift_hook.run(self.create_table_sql)
        self.log.info("Copying data from S3 to Redshift table: {}".format(self.table))
        redshift_hook.run(formatted_sql)
        





