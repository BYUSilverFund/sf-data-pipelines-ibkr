import polars as pl
from sqlalchemy import create_engine
import psycopg2
from rich import print
import jinja2

class RDS:

    def __init__(self, db_endpoint: str, db_name: str, db_user: str, db_password: str, db_port: str):
        self.db_endpoint = db_endpoint
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_port = db_port

        try:
            # Establish the connection
            self.connection = psycopg2.connect(
                host=self.db_endpoint,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password,
                port=self.db_port
            )

            # Create a cursor object
            self.cursor = self.connection.cursor()

            # Create the SQLAlchemy engine
            self.uri = f'postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_endpoint}:{self.db_port}/{self.db_name}'
            self.engine = create_engine(self.uri)

        except Exception as e:
            print(f'Error: {e}')
    
    def execute(self, query_string: str) -> list[tuple[any]]:
        
        self.cursor.execute(query_string)
        
        if self.cursor.description:  # Means it's a SELECT or returning rows
            rows = self.cursor.fetchall()
            return rows
        
        else:
            self.connection.commit()
            return None
        
    def execute_sql_file(self, file_name: str) -> list[tuple[any]]:

        with open(file_name, 'r') as file:
            self.cursor.execute(file.read())
            
            if self.cursor.description:  # Means it's a SELECT or returning rows
                rows = self.cursor.fetchall()
                return rows
            
            else:
                self.connection.commit()
                return None
            
    def execute_sql_template_file(self, file_name: str, params: dict) -> list[tuple[any]]:
        with open(file_name, 'r') as file:
            template = jinja2.Template(source=file.read())

            self.cursor.execute(template.render(params))
            
            if self.cursor.description:  # Means it's a SELECT or returning rows
                rows = self.cursor.fetchall()
                return rows
            
            else:
                self.connection.commit()
                return None
            
    def execute_to_df(self, query_string: str) -> list[tuple[any]]:
        return pl.read_database(
            query=query_string,
            connection=self.connection,
        ) 
        
    def stage_dataframe(self, df: pl.DataFrame, table_name: str):
        df.write_database(
            table_name=table_name, 
            connection=self.engine, 
            if_table_exists='replace', 
        )