import pandas as pd

def get_last_load_timestamp(collection_name,engine):
    """
    Retrieves the most recent 'last_modified_date' from a specified collection in the database.

    This function queries the database for the maximum value of the 'last_modified_date' column 
    from the given collection. If no data is found or an error occurs, it returns the minimum 
    possible timestamp to indicate no previous load.

    Parameters:
    collection_name (str): The name of the collection (or table) in the database to query.
    engine (SQLAlchemy Engine): The database engine used to establish the connection to the database.

    Returns:
    Timestamp: The most recent 'last_modified_date' from the collection, or the minimum timestamp if no data is found.
    """
    try:
        last_load_timestamp = pd.read_sql(f"SELECT MAX(last_modified_date) FROM {collection_name}", engine)
        last_load_timestamp = last_load_timestamp.iloc[0, 0]  # Extract the timestamp value
        if last_load_timestamp is None:
            # If no data is loaded, start with a default timestamp
            last_load_timestamp = pd.Timestamp.min
    except Exception as e:
        last_load_timestamp = pd.Timestamp.min
    return last_load_timestamp