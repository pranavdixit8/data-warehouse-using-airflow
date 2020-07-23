from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 create_table_sql = "",
                 insert_table_sql = "",
                 truncate = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_table_sql = create_table_sql
        self.insert_table_sql = insert_table_sql
        self.truncate = truncate

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        if self.truncate:  
            self.log.info("dropping table: {} if it exists ".format(self.table))
            redshift_hook.run("drop table if exists {}".format(self.table))
            self.log.info("Creating  table: {}".format(self.table))
            redshift_hook.run(self.create_table_sql)
            
        self.log.info("Creating  table: {}  if not exists".format(self.table))
        redshift_hook.run(self.create_table_sql)
        self.log.info("Inserting data into table: {}".format(self.table))
        redshift_hook.run("INSERT INTO {} ".format(self.table) + self.insert_table_sql)
