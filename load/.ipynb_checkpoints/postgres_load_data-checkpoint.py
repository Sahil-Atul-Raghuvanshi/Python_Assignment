from utils.timestamp import get_last_load_timestamp
from db_config.database_config import DatabaseConfig
import pickle
import pandas as pd

#class to load the transformed data to postgreSQL
class PostgresLoadData:
    #constructor to get the connection variable object named "engine" and store it in class instance variable named "engine"
    def __init__(self):
        """
        Initializes the PostgresLoadData object by establishing a connection to the PostgreSQL database.
        
        This constructor creates an engine object using the DatabaseConfig class, which is used to 
        manage interactions with the PostgreSQL database.
        """
        # engine is an object created by create_engine() and is an instance of the sqlalchemy.engine.Engine class.
        # It encapsulates the core functionality needed to connect to the database and manage interactions. 
        self.engine = DatabaseConfig().get_postgres_engine()

    def load_to_postgres(self, df, table_name):
        """
        Loads the provided DataFrame into a specified PostgreSQL table.
        
        This method checks if the table exists and appends data if it already exists. If the table 
        is empty (i.e., no previous data is loaded), it will replace the existing table.

        Parameters:
        df (DataFrame): The pandas DataFrame containing the data to be loaded into PostgreSQL.
        table_name (str): The name of the PostgreSQL table where the data will be loaded.
        """
        if not df.empty:
            # Load data into PostgreSQL
            # engine is an object created by create_engine() and is an instance of the sqlalchemy.engine.Engine class.
            # It encapsulates the core functionality needed to connect to the database and manage interactions.
            """
            Saves the current last load timestamps to a pickle file.
            """
            last_load_timestamp=get_last_load_timestamp(table_name,self.engine)
            with open(f'utils/previous_load_timestamps/{table_name}.pkl', "wb") as f:
                pickle.dump(last_load_timestamp, f)
            print(last_load_timestamp)
            if_exists_option = 'replace' if last_load_timestamp == pd.Timestamp.min else 'append'
            df.to_sql(table_name, self.engine, if_exists=if_exists_option, index=False)
            # to_sql is a method that allows you to write records stored in a dataframe to a SQL database
            #Parameters
            # 1) first parameter is the name of the table
            # 2) engine is SQLAlchemy object containing connection to database
            # 3) if_exists='replace' specifies replace the table if already existing in database
            # 4) index=False specifies not to write index from dataframe onto the SQL table
            print(f"Data loaded successfully into the {table_name} table in PostgreSQL.")
