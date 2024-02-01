"""
Reuters' json file import
"""

"""
After downloading the json file from the following link: https://learning.devinci.fr/pluginfile.php/217984/mod_folder/intro/reuters.json.zip
I added brackets at the beginning and end of the file to make it a valid json file.
And replace '}}' by '}},' to make it a valid json array.

Then I created the following keyspace and tables in Cassandra:
cqlsh> create keyspace reuters;
cqlsh> create table reuters (id int primary key, companies text, date text, exchanges text, orgs text, people text, places text, text_id int, topics text);
cqlsh> create table texts (id int primary key, body text, dateline text, title text);
"""

from pprint import pprint
import json
import os
import cassandra #pip install cassandra-driver
from cassandra.cluster import Cluster

# Connection to reuters keyspace
cluster = Cluster()
session = cluster.connect('reuters')

session.execute("TRUNCATE TABLE reuters")
session.execute("TRUNCATE TABLE texts")
json_file = 'Cassandra/reuters.json'

with open(json_file) as data_file:
    data = json.load(data_file)

    for v in data:
        try:
            r_id = int(v['_id'])
            print('Uploading row ' + str(r_id))
            r_companies = "'" + v.get('companies', '') + "'"
            r_date = "'" + v.get('date', '') + "'"
            r_exchanges = "'" + v.get('exchanges', '') + "'"
            r_orgs = "'" + v.get('orgs', '') + "'"
            r_people = "'" + v.get('people', '') + "'"
            r_places = "'" + v.get('places', '') + "'"
            r_text_id = r_id

            t_id = r_id
            t_body = "'" + v['text'].get('body', '') + "'"  # Check if 'body' exists
            t_dateline = "'" + v['text'].get('dateline', '') + "'"
            t_title = "'" + v['text'].get('title', '') + "'"

            r_topics = "'" + v.get('topics', '') + "'"

            # Create and execute prepared statements for insertion into reuters table
            query = "INSERT INTO reuters (id, companies, date, exchanges, orgs, people, places, text_id, topics) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            prepared_stmt = session.prepare(query)
            session.execute(prepared_stmt, [r_id, r_companies, r_date, r_exchanges, r_orgs, r_people, r_places, r_text_id, r_topics])

            # Create and execute prepared statements for insertion into texts table
            query = "INSERT INTO texts (id, body, dateline, title) VALUES (?, ?, ?, ?)"
            prepared_stmt = session.prepare(query)
            session.execute(prepared_stmt, [t_id, t_body, t_dateline, t_title])
        
        except Exception as e:
            print("Une exception s'est produite : ", e)