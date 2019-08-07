from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_template="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_template = sql_template

    def execute(self, context):

      try:
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
      except Exception as error:
        print("set redshift connection in airflow connections")
        raise(error)
      
      self.log.info("Truncating Redshift table")
      redshift.run("TRUNCATE TABLE {}".format(self.table))
      
      self.log.info("Loading data in the dimension table")
      redshift.run("INSERT INTO {0}\n{1}".format(self.table, self.sql_template))