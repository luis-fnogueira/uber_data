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
PARAMS = Variable.get(
    key=DAG_NAME_PARAMS,
    default_var=False,
    deserialize_json=True
    )

# Capturing the arguments needed to execute the DAG.
TIMEOUT = "30"

SCHEDULE = None

default_args = {
    "owner"           : "Luis Felipe",
    "start_date"      : utils.dates.days_ago(0),
    "depends_on_past" : False,
    "email"           : [""],
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : 0,
    "retry_delay"     : timedelta(minutes = int(TIMEOUT)),
    "catchup"         : False
}

# Defining the DAG parameters.
#
# dag_id           : The id of the DAG.
# schedule_interval: Defines how often that DAG runs, this timedelta object
#                    gets added to your latest task instance’s execution_date
#                    to figure out the next schedule.
# dagrun_timeout   : Specify how long a DagRun should be up before timing out
#                    / failing, so that new DagRuns can be created. The timeout
#                    is only enforced for scheduled DagRuns, and only once the
#                    # of active DagRuns == max_active_runs.
# catchup          : Perform scheduler catchup (or only run latest)?
#                    Defaults to True.
# default_args     : Definning a dictionary of default parameters to be used as
#                    constructor keyword parameters when initialising
#                    operators.
# max_active_runs  : Maximum number of active DAG runs, beyond this number of
#                    DAG runs in a running state, the scheduler won’t create
#                    new active DAG runs.

doc_md = """
### Uber data ETL
#### Purpose
This DAG triggers a ETL process of Uber related data.

"""

dag_params = {
    "dag_id"           : DAG_NAME,
    "schedule_interval": SCHEDULE,
    "dagrun_timeout"   : timedelta(minutes = int(TIMEOUT)),
    "catchup"          : False,
    "default_args"     : default_args,
    "max_active_runs"  : 1,
    "tags":['Uber', 'etl'],
    "doc_md": doc_md
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
        df = pd.read_csv(filepath_or_buffer="/home/luis-fnogueira/projects/personal/uber_data/dags/uber_data/original_trips_data.csv")
               
        logger.info("[LOADING] Removing timezone substring from columns")
        cols = ["Request Time", "Begin Trip Time", "Dropoff Time"]
        Transform.remove_tmz(cols=cols, df=df)
        
        logger.info("[LOADING] Inserting data in Postgres")
        db = Postgres(credentials=PARAMS)
        db.send_data(data=df, table_name="raw_data", schema="uber_data")

    # [Workflow] Defining the DAG tasks workflow.
    create_tables() >> extract()

dag = taskflow()