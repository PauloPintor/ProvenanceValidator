import psycopg2
from psycopg2 import OperationalError
import logging


class PostgreSQLConnector:
    def __init__(self, dbname, user, password, host="localhost", port=5432):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.logger = logging.getLogger(__name__)

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            self.logger.info("Database connection established successfully")
        except OperationalError as e:
            self.logger.error(f"Error connecting to database: {e}")
            raise

    def execute_query(self, query, params=None):
        if self.connection is None:
            self.logger.error(f"Failed to establish database connection.")
            raise

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                self.connection.commit()
                print("Query executed successfully")
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            self.connection.rollback()
            raise
        finally:
            self.close()

    def fetch_results(self, query, extraCommands=None, params=None):
        if self.connection is None:
            self.logger.error(f"No database connection. Call connect() first.")
            raise

        try:
            with self.connection.cursor() as cursor:
                if extraCommands:
                    for command in extraCommands.split(";"):
                        command = command.strip()
                        if command:
                            cursor.execute(command)

                cursor.execute(query, params)
                rows = cursor.fetchall()

                column_names = (
                    [desc[0] for desc in cursor.description]
                    if cursor.description
                    else []
                )

                return rows, column_names
        except Exception as e:
            self.logger.error(f"Fetching results failed: {e}")
            raise

    def close(self):
        if self.connection:
            self.connection.close()
            self.logger.info("Database connection closed")
