import logging
import psycopg2
from sqlalchemy import create_engine


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Postgres:

    def __init__(self, credentials: dict) -> None:
        
        self.__CREDENTIALS = credentials

        self.__URI = f"postgresql://{credentials['user']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['database']}"


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


    def send_data_to_postgres(self, data: dict, table: str, schema: str) -> None:

        pass

   