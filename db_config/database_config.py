# db_config.py
# Import necessary libraries and packages
import os # For accessing environment variables
from pymongo import MongoClient # For connecting to a MongoDB database
from sqlalchemy import create_engine  # For creating a connection to a PostgreSQL database
from dotenv import load_dotenv # For loading environment variables from a .env file

class DatabaseConfig:
    def __init__(self):
        """
        Initializes the DatabaseConfig object by loading environment variables and setting up 
        database connection details for MongoDB and PostgreSQL.
        
        This constructor loads credentials for both PostgreSQL and MongoDB from environment 
        variables and stores them as instance variables.
        """
        load_dotenv()  # Load environment variables

         # Retrieve PostgreSQL credentials and details from environment variables
        self.postgres_user = os.getenv('POSTGRES_USER')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD')
        self.postgres_host = os.getenv('POSTGRES_HOST')
        self.postgres_port = os.getenv('POSTGRES_PORT')
        self.postgres_db = os.getenv('POSTGRES_DB')

        # Retrieve MongoDB credentials and details from environment variables
        self.mongodb_host = os.getenv('MONGODB_HOST')
        self.mongodb_port = os.getenv('MONGODB_PORT')
        self.mongodb_db = os.getenv('MONGODB_DB')

    # Method to get a MongoDB client connection
    def get_mongo_client(self):
        """
        Establishes and returns a connection to the MongoDB client.

        This method creates a MongoDB URI using the stored MongoDB connection details 
        and returns a MongoClient object that connects to the specified MongoDB database.

        Returns:
        MongoClient: A client object connected to the specified MongoDB database.
        """
        # Construct the MongoDB URI using the host and port
        mongo_uri = f"mongodb://{self.mongodb_host}:{self.mongodb_port}"
        # Create and return MongoClient object
        client = MongoClient(mongo_uri)
        # Return a reference to the MongoDB database
        return client[self.mongodb_db]

    # Method to get a PostgreSQL engine connection
    def get_postgres_engine(self):
        """
        Establishes and returns a connection to the PostgreSQL database using SQLAlchemy.

        This method constructs a PostgreSQL connection string using stored credentials and 
        returns an SQLAlchemy engine object that can be used to interact with the PostgreSQL database.

        Returns:
        Engine: An SQLAlchemy engine object connected to the PostgreSQL database.
        """
        # Construct the PostgreSQL connection string using the credentials and details
        return create_engine(f'postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}')
        # Return an SQLAlchemy engine object to connect to the PostgreSQL database
