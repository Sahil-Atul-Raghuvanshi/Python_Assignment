# importing important packages and classes 
from extract.mongodb_data_extraction import MongoDBDataExtractor
from load.postgres_load_data import PostgresLoadData
from transform.transform import basictransform
import pandas as pd

def main():

    table_name = "reviews"
    # Transformation for reviews
    
    def reviews_transformation(reviews_df):
        """
        Transforms the reviews DataFrame by extracting nested fields from the 'review_details' dictionary,
        formatting 'review_date' as a date, and reordering columns.

        Parameters:
        reviews_df (DataFrame): A pandas DataFrame containing review information, including a nested 'review_details' dictionary.

        Returns:
        DataFrame: Transformed DataFrame with extracted review fields and formatted 'review_date'.
        """
        if not reviews_df.empty:
            # Extract columns from the nested 'review_details' dictionary
            # apply() function is used to apply a function along an axis of the DataFrame (either rows or columns). Default axis is 0 ie. columnwise
            reviews_df['reviewer_name'] = reviews_df['review_details'].apply(lambda x: x['reviewer_name'])
            reviews_df['review_date'] = reviews_df['review_details'].apply(lambda x: x['review_date'])
            reviews_df['review_text'] = reviews_df['review_details'].apply(lambda x: x['review_text'])
            reviews_df['rating'] = reviews_df['review_details'].apply(lambda x: x['rating'])
            reviews_df['review_date'] = pd.to_datetime(reviews_df['review_date']).dt.date
            # Drop the 'review_details' column as it's no longer needed
            reviews_df.drop(columns=['review_details'], inplace=True)
            # Reorder columns as specified
            reviews_df = reviews_df[[ 'review_id','movie_id', 'reviewer_name', 'review_date', 'review_text', 'rating','added_date','last_modified_date']]
            
        return reviews_df

    mongo_extractor = MongoDBDataExtractor() #get the database reference object from which we can extract collections
    postgres_loader = PostgresLoadData() #get the postgreSQL database engine
    #engine is an object created by create_engine() and encapsulates the core functionality needed to connect to the database and manage interactions.
    #get the dataframe converted from extracted collection using mongo_extractor object
    df = mongo_extractor.extract_collection(table_name)
    if not df.empty:
        # if transformer is present apply transform method to the dataframe and get the transformed dataframe
        basic_transformed_df = basictransform(df)
        transformed_df = reviews_transformation(basic_transformed_df)
        #after transformation load the transformed dataframe to the postgreSQL Database
        postgres_loader.load_to_postgres(transformed_df, table_name)
    else:
        print(f"No Updations in {table_name} collection")