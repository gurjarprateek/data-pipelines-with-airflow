import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

	ui_color = '#89DA59'

	@apply_defaults
	def _init_(self, redshift_conn_id="", table_list="", *args, **kwargs):
		super(DataQualityOperator, self)._init_(*args, **kwargs)
		self.redshift_conn_id = redshift_conn_id
		self.table_list = table_list

	def execute(self, context):

		try:
			redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
		except Exception as error:
			print("set redshift connection in airflow connections")
			raise error

		for table in self.table_list:
			records = redshift.get_records(f"SELECT count(*) FROM {self.table}")
			if len(records) < 1 or len(records[0]) < 1:
				raise ValueError(f"Data Quality Check Failed. {self.table} returned no results")
			num_records = records[0][0]
			if num_records < 1:
				raise ValueError(f"Data Quality check Failed. {self.table} contained 0 rows")
			logging.info(f"Data Quality on table {self.table} check passed with {records[0][0]} records")