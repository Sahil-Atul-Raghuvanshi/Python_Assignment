# importing important packages and classes 
from extract.mongodb_data_extraction import MongoDBDataExtractor
from load.postgres_load_data import PostgresLoadData
from transform.transform import basictransform
import pandas as pd

def main():
    table_name = "directors"

    # Transformation for directors
    def directors_transformation(directors_df):
        """
        Transforms the directors DataFrame by separating the 'name' dictionary into 'first_name' and 'last_name' columns.
        Converts 'birthdate' to date format and drops the original 'name' column.

        Parameters:
        directors_df (DataFrame): A pandas DataFrame containing director information, including a nested 'name' dictionary and 'birthdate'.

        Returns:
        DataFrame: Transformed DataFrame with separate 'first_name', 'last_name' columns, and formatted 'birthdate'.
        """
        # Ensure the DataFrame is not empty before applying transformations
        if not directors_df.empty:
            # Extract 'first_name' and 'last_name' from the nested 'name' dictionary
            directors_df['first_name'] = directors_df['name'].apply(lambda x: x['first_name'])
            directors_df['last_name'] = directors_df['name'].apply(lambda x: x['last_name'])
            
            # Drop the original 'name' column as it’s no longer needed
            directors_df.drop(columns=['name'], inplace=True)
            
            # Convert 'birthdate' to datetime format and extract the date only
            directors_df['birthdate'] = pd.to_datetime(directors_df['birthdate']).dt.date

        return directors_df

    mongo_extractor = MongoDBDataExtractor() #get the database reference object from which we can extract collections
    postgres_loader = PostgresLoadData() #get the postgreSQL database engine
    #engine is an object created by create_engine() and encapsulates the core functionality needed to connect to the database and manage interactions.
    #get the dataframe converted from extracted collection using mongo_extractor object
    df = mongo_extractor.extract_collection(table_name)
    if not df.empty:
        # if transformer is present apply transform method to the dataframe and get the transformed dataframe
        basic_transformed_df = basictransform(df)
        transformed_df = directors_transformation(basic_transformed_df)
        #after transformation load the transformed dataframe to the postgreSQL Database
        postgres_loader.load_to_postgres(transformed_df, table_name)
    else:
        print(f"No Updations in {table_name} collection")