import pandas as pd #used for working with dataframes and series
from db_config.database_config import DatabaseConfig
from utils.timestamp import get_last_load_timestamp

#class to extract data from MongoDB Database
class MongoDBDataExtractor:
    #constructor
    def __init__(self):
        """
        Initializes the MongoDBDataExtractor by establishing connections to MongoDB and PostgreSQL.
        
        This constructor sets up references for both MongoDB and PostgreSQL connections 
        by using the `DatabaseConfig` class.
        """
        #get the reference of database and store it as instance variable db
        self.db = DatabaseConfig().get_mongo_client()
        self.engine= DatabaseConfig().get_postgres_engine()

    def extract_collection(self, collection_name):
        """
        Extracts data from a specified MongoDB collection and loads it into a pandas DataFrame.
        
        This method checks if the collection should be loaded fully (i.e., first load) or incrementally 
        (i.e., based on the last modification timestamp). It fetches the appropriate data accordingly 
        and returns it as a pandas DataFrame.

        Parameters:
        collection_name (str): The name of the MongoDB collection to extract data from.

        Returns:
        DataFrame: A pandas DataFrame containing the extracted data from the MongoDB collection.
        """
        #access the particular collection through db reference
        collection = self.db[collection_name]
        #find method returns cursor which is an iterable object and which can help to iterate over each document
        #convert the cursor to list 
        #find({}) returns all documents
        #store the list in local variable data

        last_load_timestamp=get_last_load_timestamp(collection_name, self.engine)
        # Extract data from MongoDB
        if last_load_timestamp == pd.Timestamp.min:
            print(f"Performing Full Load for {collection_name} table")
            data = list(collection.find({}))
        else: 
            data = list(collection.find({"last_modified_date": {"$gt": last_load_timestamp}}))
            if not pd.DataFrame(data).empty:
                 print(f"Performing Incremental Load for {collection_name} table")

            
        # print("last load timestamp is ",last_load_timestamp)
        #convert the list named "data" to Dataframe and return the dataframe
        return pd.DataFrame(data)
