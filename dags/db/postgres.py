import logging
import psycopg2
import pandas as pd
from sqlalchemy import create_engine


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Postgres:

    def __init__(self, credentials: dict) -> None:
        
        self.__CREDENTIALS = credentials

        self.URI = f"postgresql://{credentials['user']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['database']}"


    def __get_conn(self):

        try:

            return psycopg2.connect(
                host = self.__CREDENTIALS['host'],
                port = self.__CREDENTIALS['port'],
                user = self.__CREDENTIALS['user'],
                password = self.__CREDENTIALS['password'],
                database = self.__CREDENTIALS['database']
                )

        except Exception as error:

            logger.error(error)


    def execute_query(self, query: str, vars: str = "") -> None:

        """
        Abstraction of a query execution.
        Args:
            Query: str. A query to be run.
            Vars: str. Variables to be inserted.
        Return:
            None, it executes a query in a DB.

        """

        conn = self.__get_conn()
        cur = conn.cursor()

        cur.execute(query=query, vars=(vars, ))

        conn.commit() 
        conn.close()
        cur.close()

    def send_data(self, data: pd.DataFrame, table_name: str, schema: str) -> None:

        """ Creating method to load data from a Dataframe to the Database.
        Args:
            data: Pandas Dataframe. Pandas dataframe to be loaded.
            conn_id: str. Connection to the database.
            table_name: str.Table name in the database
            schema: str. Schema in the database

        Returns: Inserts data into database.
        """

        try:

            # Running query SQL and getting data how DataFrame.
            
            data.to_sql(
                name      = table_name,
                con       = create_engine(self.URI),
                schema    = schema,
                if_exists = "append",
                index     = False,
                dtype     = {}
            )

            # Returning the dataframe.

            logger.info(f"[LOADING] {str(data.shape[0])} records were loaded into: {schema}.{table_name}")

        except Exception as error:

            # Printing generated error.
            
            logger.error(error)