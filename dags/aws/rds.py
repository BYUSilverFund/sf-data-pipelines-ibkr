import os

import dotenv
import jinja2
import polars as pl
import psycopg2
from sqlalchemy import create_engine

dotenv.load_dotenv(override=True)


class RDS:
    def __init__(
        self,
        db_endpoint: str | None = None,
        db_name: str | None = None,
        db_user: str | None = None,
        db_password: str | None = None,
        db_port: str | None = None,
    ):
        self.db_endpoint = db_endpoint or os.getenv("DB_ENDPOINT")
        self.db_name = db_name or os.getenv("DB_NAME")
        self.db_user = db_user or os.getenv("DB_USER")
        self.db_password = db_password or os.getenv("DB_PASSWORD")
        self.db_port = db_port or os.getenv("DB_PORT")
        self.connection = None
        self.cursor = None
        self._engine = None

    def connect(self):
        if self.db_endpoint is None:
            self.db_endpoint = os.getenv("DB_ENDPOINT")
            self.db_name = os.getenv("DB_NAME")
            self.db_user = os.getenv("DB_USER")
            self.db_password = os.getenv("DB_PASSWORD")
            self.db_port = os.getenv("DB_PORT")

        if self.connection is None or self.connection.closed != 0:
            self.connection = psycopg2.connect(
                host=self.db_endpoint,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
                port=self.db_port,
            )
            self.cursor = self.connection.cursor()
        return self.connection

    @property
    def engine(self):
        if self._engine is None:
            uri = f"postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_endpoint}:{self.db_port}/{self.db_name}"
            self._engine = create_engine(uri)
        return self._engine

    def execute(self, query_string: str) -> list[tuple[any]]:
        self.connect()
        self.cursor.execute(query_string)

        if self.cursor.description:  # Means it's a SELECT or returning rows
            rows = self.cursor.fetchall()
            return rows
        else:
            self.connection.commit()
            return None

    def execute_sql_file(self, file_name: str) -> list[tuple[any]]:
        self.connect()
        with open(file_name, "r") as file:
            self.cursor.execute(file.read())

            if self.cursor.description:  # Means it's a SELECT or returning rows
                rows = self.cursor.fetchall()
                return rows
            else:
                self.connection.commit()
                return None

    def execute_sql_template_file(
        self, file_name: str, params: dict
    ) -> list[tuple[any]]:
        self.connect()
        with open(file_name, "r") as file:
            template = jinja2.Template(source=file.read())

            self.cursor.execute(template.render(params))

            if self.cursor.description:  # Means it's a SELECT or returning rows
                rows = self.cursor.fetchall()
                return rows
            else:
                self.connection.commit()
                return None

    def read_sql(self, query_string: str) -> pl.DataFrame:
        self.connect()
        return pl.read_database(
            query=query_string,
            connection=self.connection,
        )

    def execute_to_df(self, query_string: str) -> pl.DataFrame:
        return self.read_sql(query_string)

    def stage_dataframe(self, df: pl.DataFrame, table_name: str):
        df.write_database(
            table_name=table_name,
            connection=self.engine,
            if_table_exists="replace",
        )


db = RDS()
