from cassandra.cluster import Cluster
import json

# Connection to reuters keyspace
cluster = Cluster()
session = cluster.connect('reuters')

session.execute("TRUNCATE TABLE reuters_by_author")

json_file = 'Cassandra/reuters.json'

with open(json_file) as data_file:
    data = json.load(data_file)

    for v in data:
        try:
            r_id = int(v['_id'])
            print('Uploading row ' + str(r_id))
            r_companies = v.get('companies', '')
            r_date = v.get('date', '')
            r_exchanges = v.get('exchanges', '')
            r_orgs = v.get('orgs', '')
            r_people = v.get('people', '')
            r_places = v.get('places', '')
            r_text_id = r_id

            t_id = r_id
            t_body = v['text'].get('body', '')  # Check if 'body' exists
            t_dateline = v['text'].get('dateline', '')
            t_title = v['text'].get('title', '')

            r_topics = v.get('topics', '')

            # Insert into reuters_by_author
            query = "INSERT INTO reuters_by_author (author, id, title, companies, date, exchanges, orgs, people, places, text_id, topics) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            prepared_stmt = session.prepare(query)
            session.execute(prepared_stmt, [r_people, r_id, t_title, r_companies, r_date, r_exchanges, r_orgs, r_people, r_places, r_text_id, r_topics])
        
        except Exception as e:
            print("An exception occurred: ", e)
