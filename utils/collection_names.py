from db_config.database_config import DatabaseConfig
def get_collection_names():
    """
    Retrieves a list of collection names from the MongoDB database.

    This function establishes a connection to the MongoDB database using the DatabaseConfig class 
    and retrieves the names of all collections present in the database.

    Returns:
    list: A list of collection names in the MongoDB database.
    """
    db = DatabaseConfig().get_mongo_client()
    collection_names = db.list_collection_names()
    return collection_names