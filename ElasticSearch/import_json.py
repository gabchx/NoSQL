import json

# Open the original file for reading
with open('reuters.json', 'r') as original_file:
    data = original_file.readlines()

with open('indexed_reuters.json', 'w') as indexed_file:
    for i, line in enumerate(data):
        obj = json.loads(line)
        # Remove the "_id" field
        obj.pop('_id', None)
        # Create the index line
        index_line = '{"index":{"_index":"articles","_id":' + str(i + 1) + '}}\n'
        indexed_file.write(index_line)
        # Write the JSON object to the indexed file
        json.dump(obj, indexed_file)
        indexed_file.write('\n')