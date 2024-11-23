# importing important packages and classes 
from extract.mongodb_data_extraction import MongoDBDataExtractor
from load.postgres_load_data import PostgresLoadData
from transform.transform import basictransform

def main():
    table_name = "genres"
    mongo_extractor = MongoDBDataExtractor() #get the database reference object from which we can extract collections
    postgres_loader = PostgresLoadData() #get the postgreSQL database engine
    #engine is an object created by create_engine() and encapsulates the core functionality needed to connect to the database and manage interactions.
    #get the dataframe converted from extracted collection using mongo_extractor object
    df = mongo_extractor.extract_collection(table_name)
    if not df.empty:
        # if transformer is present apply transform method to the dataframe and get the transformed dataframe
        transformed_df = basictransform(df)
        #after transformation load the transformed dataframe to the postgreSQL Database
        postgres_loader.load_to_postgres(transformed_df, table_name)
    else:
        print(f"No Updations in {table_name} collection")