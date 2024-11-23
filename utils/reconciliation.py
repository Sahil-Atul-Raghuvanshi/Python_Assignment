import pandas as pd
import numpy as np
from db_config.database_config import DatabaseConfig
import pandas as pd
import pickle,os

# Initialize Database Configurations
config = DatabaseConfig()
mongo_client = config.get_mongo_client()
postgres_engine = config.get_postgres_engine()

def get_total_collection_count(mongo_db, collection_name):
    """Get the total document count for a MongoDB collection."""
    return mongo_db[collection_name].count_documents({})

def get_total_table_count(postgres_engine, table_name):
    """Get the total row count for a PostgreSQL table."""
    query = f"SELECT COUNT(*) FROM {table_name}"
    return pd.read_sql(query, postgres_engine).iloc[0, 0]

def get_mongo_incremental_count(mongo_db, collection_name):
    """Get the count of new or modified documents in a MongoDB collection since the last load."""
    last_load_timestamp = pd.Timestamp.min
    if os.path.exists(f"utils/previous_load_timestamps/{collection_name}.pkl"):
        with open(f"utils/previous_load_timestamps/{collection_name}.pkl", "rb") as f:
             last_load_timestamp = pickle.load(f)
    if last_load_timestamp == pd.Timestamp.min:
        return 0
    # Count documents with a timestamp greater than the last load time
    incremental_count = mongo_db[collection_name].count_documents({"last_modified_date": {"$gt": last_load_timestamp}})
    return incremental_count

def get_postgres_incremental_count(postgres_engine, table_name):
    """Get the count of new or modified rows in a PostgreSQL table since the last load."""
    last_load_timestamp = pd.Timestamp.min
    if os.path.exists(f"utils/previous_load_timestamps/{table_name}.pkl"):
        with open(f"utils/previous_load_timestamps/{table_name}.pkl", "rb") as f:
                last_load_timestamp = pickle.load(f)
    if last_load_timestamp == pd.Timestamp.min:
        return 0
    # Query to count rows where the timestamp is greater than the last load time
    query = f"SELECT COUNT(*) FROM {table_name} WHERE last_modified_date > %s"
    incremental_count = pd.read_sql(query, postgres_engine, params=(last_load_timestamp,)).iloc[0, 0]
    return incremental_count

def check_duplicates(postgres_engine, table_name, unique_column):
    """Check if duplicates exist in a specified PostgreSQL table column."""
    query = f"SELECT {unique_column}, COUNT(*) FROM {table_name} GROUP BY {unique_column} HAVING COUNT(*) > 1"
    duplicates = pd.read_sql(query, postgres_engine)
    if duplicates.empty:  # Return No if no duplicates, Yes if duplicates exist
        return "No"
    else:
        return "Yes"
    
def check_normalization(engine, table):
    """Verify if a table is normalized by checking join relationships with associated tables."""
    if table=="movies":
        if verify_join_tables(postgres_engine, 'movies', 'movie_actors', 'movie_id') and verify_join_tables(postgres_engine, 'movies', 'movie_genres', 'movie_id') and  verify_join_tables(postgres_engine, 'movies', 'movie_awards', 'movie_id') and verify_join_tables(postgres_engine, 'movies', 'movie_companies', 'movie_id'):
            return "Yes"
        else:
            return "No"
    else:
        return np.nan

def verify_join_tables(engine, parent_table, child_table, column):
    """Verify if rows in child table link to a valid parent table entry."""
    null_fk_query = f"""
    SELECT {child_table}.{column}
    FROM {child_table}
    WHERE {child_table}.{column} IS NULL
    """
    null_foreign_keys = pd.read_sql(null_fk_query, engine)
    if not child_table == "movie_awards": #a movie can have no awards

        query = f"""
            SELECT {parent_table}.{column} 
            FROM {parent_table}
            LEFT JOIN {child_table} ON {parent_table}.{column} = {child_table}.{column}
            WHERE {child_table}.{column} IS NULL
        """
        missing_links = pd.read_sql(query, engine)
        return missing_links.empty and null_foreign_keys.empty
    else:
        return null_foreign_keys.empty

def check_nested_data_in_tables(postgres_engine,table):
    """Check if specific nested data fields in a table are split into columns."""
    if table=="actors":
        return check_nested_field_split(postgres_engine, table, ['first_name', 'last_name'])
    elif table=="directors":
        return check_nested_field_split(postgres_engine, table, ['first_name', 'last_name'])
    elif table=="reviews":
        return check_nested_field_split(postgres_engine, table, ['reviewer_name', 'review_text', 'review_date', 'rating'])
    else:
        return np.nan

def check_nested_field_split(postgres_engine, table_name, nested_columns):
    """Check if specified columns are present in a PostgreSQL table."""
    query = f"SELECT {', '.join(nested_columns)} FROM {table_name} LIMIT 1"
    result = pd.read_sql(query, postgres_engine)
    if all(col in result.columns for col in nested_columns):
       return "Yes"
    else:
       return "No"

def main():
    results=[]
    # Set database and collection/table names
    db = DatabaseConfig().get_mongo_client()
    collections_tables = {
        'genres': 'genres',
        'actors': 'actors',
        'directors': 'directors',
        'reviews': 'reviews',
        'production_companies': 'production_companies',
        'awards': 'awards',
        'movies': 'movies'
    }
    # Check document-row count match for each collection/table pair
    for collection, table in collections_tables.items():
        mongo_count = get_total_collection_count(db, collection)
        postgres_count = get_total_table_count(postgres_engine, table)
        mongo_incremental_count = get_mongo_incremental_count(db, collection)
        postgres_incremental_count = get_postgres_incremental_count(postgres_engine, table)
        normalization_result = check_normalization(postgres_engine, table)
        split_check_result=check_nested_data_in_tables(postgres_engine, table)
        if collection=="production_companies":
            duplicate_check_result = check_duplicates(postgres_engine, table, "company_id")
        else:
            duplicate_check_result = check_duplicates(postgres_engine, table, f"{collection[:-1]}_id")
        results.append({
            'Collection_Name': collection,
            'MongoDB_Total_Count': mongo_count,
            'PostgreSQL_Total_Count': postgres_count,
            'MongoDB_Incremental_Count': mongo_incremental_count,
            'PostgreSQL_Incremental_Count': postgres_incremental_count,
            'Duplicates?': duplicate_check_result,
            'Normalization_Success?': normalization_result,
            'Split_Columns_Success?': split_check_result
        })
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    print(results_df)
    # Save DataFrame as CSV
    results_df.to_csv('utils/reconciliation_results.csv', index=False)
    print("Results saved to reconciliation_results.csv")
if __name__ == '__main__':
    main()
