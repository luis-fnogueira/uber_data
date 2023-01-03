# Importing libraries
import logging
import pandas as pd
from airflow import utils
from datetime import timedelta
from airflow.models import Variable
from airflow.decorators import dag, task

from db.sqls import Sqls
from db.postgres import Postgres
from aux.transform import Transform

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Defining constants.
DAG_NAME = "uber_data_etl"
DAG_NAME_PARAMS = "_".join([DAG_NAME, "params"])

# The database credentials are stored in Airflow Variables
PARAMS = Variable.get(key=DAG_NAME_PARAMS, default_var=False, deserialize_json=True)

# Capturing the arguments needed to execute the DAG.
TIMEOUT = "30"

SCHEDULE = None

default_args = {
    "owner": "Luis Felipe",
    "start_date": utils.dates.days_ago(0),
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=int(TIMEOUT)),
    "catchup": False,
}


doc_md = """
### Uber data ETL
#### Purpose
This DAG triggers a ETL process of Uber related data.
"""

dag_params = {
    "dag_id": DAG_NAME,
    "schedule_interval": SCHEDULE,
    "dagrun_timeout": timedelta(minutes=int(TIMEOUT)),
    "catchup": False,
    "default_args": default_args,
    "max_active_runs": 1,
    "tags": ["Uber", "etl"],
    "doc_md": doc_md,
}

# Creating the DAG.
@dag(**dag_params)
def taskflow():
    @task(task_id="create_tables")
    def create_tables():

        db = Postgres(credentials=PARAMS)

        logger.info("[LOADING] creating table raw_data")
        db.execute_query(query=Sqls.CREATE_TABLE_RAW_DATA)
        logger.info("[DONE] raw_data succesfully created!")

        logger.info("[LOADING] creating table structured_data")
        db.execute_query(query=Sqls.CREATE_TABLE_STRUCTURED_DATA)
        logger.info("[DONE] structured_data succesfully created!")

    @task(task_id="extract")
    def extract():

        logger.info("[LOADING] Reading data to CSV")
        df = pd.read_csv(filepath_or_buffer="/opt/airflow/dags/original_trips_data.csv")

        logger.info("[LOADING] Removing timezone substring from columns")
        cols = ["Request Time", "Begin Trip Time", "Dropoff Time"]
        Transform.remove_tmz(cols=cols, df=df)

        logger.info("[LOADING] Inserting data in Postgres")
        db = Postgres(credentials=PARAMS)
        db.send_data(data=df, table_name="raw_data", schema="uber_data")

    @task(task_id="transform")
    def transform():

        conn = Postgres(credentials=PARAMS)

        sql_query = pd.read_sql_query(sql=Sqls.READ_RAW_DATA, con=conn.get_conn())

        RAW_COLUMNS = [
            "City",
            "Product Type",
            "Trip or Order Status",
            "Request Time",
            "Begin Trip Time",
            "Begin Trip Lat",
            "Begin Trip Lng",
            "Begin Trip Address",
            "Dropoff Time",
            "Dropoff Lat",
            "Dropoff Lng",
            "Dropoff Address",
            "Distance (miles)",
            "Fare Amount",
            "Fare Currency",
        ]

        df = pd.DataFrame(data=sql_query, columns=RAW_COLUMNS)

        logger.info("Starting transformation...")
        logger.info("[1] Removing trips not completed...")
        Transform.remove_not_completed(dataframe=df)

        logger.info("[2] Changing all trips types to uppercase...")
        Transform.types_all_upper(dataframe=df)

        logger.info("[3] Checking if cities are correct...")
        Transform.check_city(dataframe=df)

        logger.info("[4] All passed: writing result df to CSV...")
        df.to_csv("./dags/transformed_data.csv", encoding="utf-8", index=False)

    @task(task_id="load")
    def load():
        logger.info("[LOADING] Reading data to CSV")
        df = pd.read_csv(filepath_or_buffer="./dags/transformed_data.csv")

        logger.info("[LOADING] Inserting data in Postgres")
        db = Postgres(credentials=PARAMS)
        db.send_data(data=df, table_name="structured_data", schema="uber_data")

    # [Workflow] Defining the DAG tasks workflow.
    create_tables() >> extract() >> transform() >> load()


dag = taskflow()
