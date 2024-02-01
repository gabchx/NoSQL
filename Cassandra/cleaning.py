from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('reuters')

# Define the columns for the 'reuters' table
reuters_columns = ['companies', 'date', 'exchanges', 'orgs', 'people', 'places', 'topics']

# Define the columns for the 'text' table
text_columns = ['body', 'dateline', 'title']

# Define the colums for the 'reuters_by_author' table
reuters_by_author_columns = ['author', 'id', 'title', 'companies', 'date', 'exchanges', 'orgs', 'people', 'places', 'text_id', 'topics']


# Function to update rows with empty strings to null
def clean_table(table_name, columns):
    rows = session.execute(f'SELECT id, {", ".join(columns)} FROM {table_name}')
    
    for row in rows:
        updates = []
        for column in columns:
            # Check for empty string
            if getattr(row, column) == "''":
                print(f"Row with id {row.id} has empty string in {column}")
                updates.append(f"{column} = null")
                
        if updates:
            update_query = f"UPDATE {table_name} SET {', '.join(updates)} WHERE id = {row.id}"
            session.execute(update_query)
            print(f"Updated row {row.id}: set empty strings to null")
            
# Function to remove quotes from text attributes
def clean_text_attributes(table_name, columns):
    rows = session.execute(f'SELECT id, {", ".join(columns)} FROM {table_name}')
    
    for row in rows:
        updates = []
        for column in columns:
            text_value = getattr(row, column)
            if text_value and "'" in text_value:  # Check if the text contains quotes
                cleaned_text = text_value.replace("'", "")  # Remove quotes
                updates.append(f"{column} = '{cleaned_text}'")
        
        if updates:
            update_query = f"UPDATE {table_name} SET {', '.join(updates)} WHERE id = {row.id}"
            session.execute(update_query)
            print(f"Update row {row.id}: removed quotes")
            
#clean_text_attributes('reuters', reuters_columns)
#clean_text_attributes('texts', text_columns)
#clean_text_attributes('reuters_by_author', reuters_by_author_columns)
#clean_table('reuters', reuters_columns)
#clean_table('texts', text_columns)
clean_table('reuters_by_author', reuters_by_author_columns)

cluster.shutdown()