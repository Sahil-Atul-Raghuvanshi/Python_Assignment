from utils.collection_names import get_collection_names
import importlib

# Get the list of collection names
collection_names = get_collection_names()
collection_names_to_run=['actors','directors','movies','reviews','genres','awards','production_companies']
# Loop through collection names and dynamically import modules if they exist
for collection_name in collection_names_to_run:
        module_path = f"tables.{collection_name}"
        module = importlib.import_module(module_path)
        
        # Call the main function if it exists
        if hasattr(module, 'main'):
            module.main()
        else:
            print(f"No main function found in {module_path}")
