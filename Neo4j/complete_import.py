import json
from py2neo import Graph, Node, Relationship

# Initialize the graph connection
graph = Graph("bolt://localhost:7687", auth=('neo4j', 'neo4jneo4j'))

# Load the JSON data from the file
with open('Cassandra/reuters.json') as f:
    data = json.load(f)

# Clear the graph
graph.delete_all()

# Iterate over each item in the loaded JSON data
for idx, doc in enumerate(data):
    _id = doc['_id'] if '_id' in doc else ''
    date = doc['date'] if 'date' in doc else ''
    topics = set(doc['topics'].split()) if 'topics' in doc else set()
    places = set(doc['places'].split()) if 'places' in doc else set()
    people = set(doc['people'].split()) if 'people' in doc else set()
    orgs = set(doc['orgs'].split()) if 'orgs' in doc else set()
    exchanges = set(doc['exchanges'].split()) if 'exchanges' in doc else set()
    companies = set(doc['companies'].split()) if 'companies' in doc else set()

    text = doc['text'] if 'text' in doc else {}
    title = text['title'] if 'title' in text else ''
    dateline = text['dateline'] if 'dateline' in text else ''
    body = text['body'] if 'body' in text else ''

    # Create a new Node representing the document
    doc_node = Node("article", id=_id, date=date, topics=list(topics), places=list(places), people=list(people),
                    orgs=list(orgs), exchanges=list(exchanges), companies=list(companies))

    # Create a new Node representing the text
    text_node = Node("text", title=title, dateline=dateline, body=body)

    # Create a relationship between the document and the text
    relationship = Relationship(doc_node, "has_text", text_node)

    # Save the newly created Node and Relationship
    graph.create(relationship)

    # Save the newly created Node
    graph.create(doc_node)
    print(f"Document {idx} added to the graph")

print("Nodes & relationships added successfully!")