# importing important packages and classes 
from extract.mongodb_data_extraction import MongoDBDataExtractor
from load.postgres_load_data import PostgresLoadData
from transform.normalizer import Normalizer
from db_config.database_config import DatabaseConfig
from transform.transform import basictransform
import pandas as pd

def main():
    table_name = "movies"
    # Transformation for movies
    def movies_transformation(movies_df):
        """
        Transforms the movies DataFrame by renaming columns, formatting ratings, extracting duration in minutes, 
        and normalizing nested fields into separate join tables. Also, drops unneeded columns after normalizing.

        Parameters:
        movies_df (DataFrame): A pandas DataFrame containing movie details including nested fields like 'director', 'company', and 'awards'.

        Returns:
        DataFrame: Transformed DataFrame with cleaned and formatted data, ready for loading into a database.
        """
        if not movies_df.empty:
            # Rename IMDB_rating column name to rating
            # apply() function is used to apply a function along an axis of the DataFrame (either rows or columns). Default axis is 0 ie. columnwise
            # use rstrip to remove any trailing 0's and 1's
            movies_df['IMDB_rating'] = movies_df['rating'].apply(lambda x: f"{x:.1f}".rstrip('0').rstrip('.') if pd.notnull(x) else None).astype('float')

            # Convert duration to numeric minutes 
            # apply .str.extract() function regular expression (regex) to each value in the duration column and extract the first match
            # \d+ matches one or more digits
            # finally convert it to int using astype function
            movies_df['duration_min'] = movies_df['duration'].str.extract(r'(\d+)').astype(int)

            # Set director_id based on nested fields if it exist
            movies_df['director_id'] = movies_df['director'].apply(lambda x: x['id'] if pd.notnull(x) and 'id' in x else None)
            # Convert director_id to integer, filling any missing values with -1
            movies_df['director_id'] = movies_df['director_id'].fillna(-1).astype(int)

            movies_df['release_year	'] = movies_df['release_year'].astype(int)


            # select only the required columns in the specified order
            movies_df = movies_df[['movie_id', 'title', 'release_year', 'IMDB_rating', 
                                'director_id', 'duration_min', 'companyy','awards','actors','genre','added_date','last_modified_date']]
            #For the remaining columns which contain json string create separate normalized join tables 
            #split the movies table and load the normalized tables to PostgreSQL

            
            normalizer = Normalizer(DatabaseConfig())
            normalizer.load_normalized_data(movies_df,'movie_id','companyy','movie_companies')
            normalizer.load_normalized_data(movies_df,'movie_id','awards','movie_awards',{'won': lambda x: x['won']})
            normalizer.load_normalized_data(movies_df,'movie_id','actors','movie_actors', {'role': lambda x: x['role']})
            normalizer.load_normalized_data(movies_df,'movie_id','genre','movie_genres')

            columns_to_drop = ['genre', 'actors', 'companyy', 'awards']
            movies_df = movies_df.drop(columns=columns_to_drop)
            
        return movies_df


    mongo_extractor = MongoDBDataExtractor() #get the database reference object from which we can extract collections
    postgres_loader = PostgresLoadData() #get the postgreSQL database engine
    #engine is an object created by create_engine() and encapsulates the core functionality needed to connect to the database and manage interactions.
    #get the dataframe converted from extracted collection using mongo_extractor object
    df = mongo_extractor.extract_collection(table_name)
    if not df.empty:
        # if transformer is present apply transform method to the dataframe and get the transformed dataframe
        basic_transformed_df = basictransform(df)
        transformed_df = movies_transformation(basic_transformed_df)
        #after transformation load the transformed dataframe to the postgreSQL Database
        postgres_loader.load_to_postgres(transformed_df, table_name)
    else:
        print(f"No Updations in {table_name} collection")