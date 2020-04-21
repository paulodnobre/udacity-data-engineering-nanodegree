from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 truncate_table=True,
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id = redshift_conn_id
        self.truncate_table=truncate_table
        self.sql=sql

    def execute(self, context):
        
        redshift = PostgresHook(self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info(f'Truncating Table {self.table}')
            redshift.run(f"TRUNCATE {self.table}")
            
        self.log.info(f'Running query {self.sql}')
        redshift.run(f"INSERT INTO {self.table} {self.sql}")
