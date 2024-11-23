# importing important packages and classes 
from extract.mongodb_data_extraction import MongoDBDataExtractor
from load.postgres_load_data import PostgresLoadData
from transform.transform import basictransform
import pandas as pd

def main():
    table_name = "actors"

    # Transformation for actors
    def actors_transformation(actors_df):
        """
        Transforms the actors DataFrame by separating the 'name' dictionary into 'first_name' and 'last_name' columns.
        Converts 'birthdate' to date format and drops the original 'name' column.

        Parameters:
        actors_df (DataFrame): A pandas DataFrame containing actor information, including a nested 'name' dictionary and 'birthdate'.

        Returns:
        DataFrame: Transformed DataFrame with separate 'first_name', 'last_name' columns, and formatted 'birthdate'.
        """
        # Ensure the DataFrame is not empty before applying transformations
        if not actors_df.empty:
            # Extract 'first_name' and 'last_name' from the nested 'name' dictionary
            actors_df['first_name'] = actors_df['name'].apply(lambda x: x['first_name'])
            actors_df['last_name'] = actors_df['name'].apply(lambda x: x['last_name'])
            
            # Drop the original 'name' column as it’s no longer needed
            actors_df.drop(columns=['name'], inplace=True)
            
            # Convert 'birthdate' to datetime format and extract the date only
            actors_df['birthdate'] = pd.to_datetime(actors_df['birthdate']).dt.date

        return actors_df
    
    mongo_extractor = MongoDBDataExtractor() #get the database reference object from which we can extract collections
    postgres_loader = PostgresLoadData() #get the postgreSQL database engine
    #engine is an object created by create_engine() and encapsulates the core functionality needed to connect to the database and manage interactions.
    #get the dataframe converted from extracted collection using mongo_extractor object
    df = mongo_extractor.extract_collection(table_name)
    if not df.empty:
        # if transformer is present apply transform method to the dataframe and get the transformed dataframe
        basic_transformed_df = basictransform(df)
        transformed_df = actors_transformation(basic_transformed_df)
        #after transformation load the transformed dataframe to the postgreSQL Database
        postgres_loader.load_to_postgres(transformed_df, table_name)
    else:
        print(f"No Updations in {table_name} collection")
