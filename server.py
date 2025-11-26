from basyx.aas.adapter.http import WSGIApp
from basyx.aas.adapter.aasx import DictSupplementaryFileContainer
from aas_mapping.aas_neo4j_adapter.neo_aas_object_store import Neo4jObjectStore
from aas_mapping.aas_neo4j_adapter.aas_neo4j_client import AASNeo4JClient

client = AASNeo4JClient(uri="bolt://localhost:7687", user="neo4j", password="12345678")
store = Neo4jObjectStore(client=client)
application = WSGIApp(object_store=store, file_store=DictSupplementaryFileContainer())

if __name__ == "__main__":
    from werkzeug.serving import run_simple
    run_simple("localhost", 8081, application)

