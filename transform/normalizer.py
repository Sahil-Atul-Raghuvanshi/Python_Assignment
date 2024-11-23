from load.postgres_load_data import PostgresLoadData # for loading normalized join tables
import pandas as pd

# Class for normalization of tables and loading the normalized tables to postgreS
class Normalizer:
    def __init__(self, db_config):
        """
        Initializes the Normalizer object with a database configuration and prepares the PostgresLoader for data insertion.

        Parameters:
        db_config (object): The database configuration needed to set up the connection to PostgreSQL.
        """
        self.postgres_loader = PostgresLoadData() #engine is an object created by create_engine() and encapsulates the core functionality needed to connect to the database and manage interactions.

    def normalize_column(self,df,parent_table_id,column_name, additional_fields=None):
        """
        Normalizes a specific column in the DataFrame by extracting data from nested structures and preparing it for relational storage.

        This method iterates over each row in the DataFrame, processes the nested column (like 'actors' or 'awards'), 
        and creates a new DataFrame suitable for insertion into a relational database table.

        Parameters:
        df (DataFrame): The pandas DataFrame containing the data to be normalized.
        parent_table_id (str): The identifier for the parent table (e.g., 'movie_id') used in the normalized data.
        column_name (str): The name of the column containing the nested data to be normalized.
        additional_fields (dict, optional): A dictionary of additional fields to be added to the normalized data. 

        Returns:
        DataFrame: A new DataFrame containing the normalized data with columns for the parent table ID and the normalized items.
        """
        normalized_data = []  # List to store each normalized row
        for index, row in df.iterrows():
                for item in row[column_name]:
                    # Create a base dictionary with 'movie_id' and an ID specific to the item in the column
                    normalized_entry = {
                        parent_table_id: row[parent_table_id],
                        f"{column_name[:-1]}_id": item['id']  # Assumes column_name is plural (e.g., 'actors' -> 'actor_id')
                    }
                    # Apply additional fields if specified
                    if additional_fields:
                        for field_name, field_value in additional_fields.items():
                            # If field_value is a callable (like a function), apply it to the item; otherwise, use the value directly
                            normalized_entry[field_name] = field_value(item) if callable(field_value) else field_value
                    # Append the completed normalized entry to the list
                    normalized_data.append(normalized_entry)
        return pd.DataFrame(normalized_data)
    
    def load_normalized_data(self, df,parent_table_id, column_name, table_name, additional_fields=None):
        """
        Normalizes a specific column in the DataFrame and loads the resulting normalized data into the specified PostgreSQL table.

        This method first normalizes the data from a specified column in the DataFrame, then loads the normalized data 
        into a PostgreSQL table using the PostgresLoader.

        Parameters:
        df (DataFrame): The pandas DataFrame containing the data to be normalized.
        parent_table_id (str): The identifier for the parent table (e.g., 'movie_id') used in the normalized data.
        column_name (str): The name of the column containing the nested data to be normalized.
        table_name (str): The name of the PostgreSQL table where the normalized data will be inserted.
        additional_fields (dict, optional): A dictionary of additional fields to be added to the normalized data. 
                                             Values can be direct values or functions to apply to each item in the column.
        """
        # Normalize the data
        normalized_df = self.normalize_column(df,parent_table_id,column_name, additional_fields)
        # Use the loader to load the normalized data into the specified table
        self.postgres_loader.load_to_postgres(normalized_df, table_name)