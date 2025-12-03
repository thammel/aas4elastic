import pathlib

from basyx.aas.adapter.http import WSGIApp
from basyx.aas.adapter.aasx import DictSupplementaryFileContainer
from basyx.aas.adapter.aasx import AASXReader
from aas_mapping.aas_neo4j_adapter.neo_aas_object_store import Neo4jObjectStore
from aas_mapping.aas_neo4j_adapter.aas_neo4j_client import AASNeo4JClient

client = AASNeo4JClient(uri="bolt://localhost:7687", user="neo4j", password="12345678")
obj_store = Neo4jObjectStore(client=client)
file_store = DictSupplementaryFileContainer()

for file in pathlib.Path("/input").glob("*.aasx"):
    with AASXReader(file) as reader:
        reader.read_into(object_store=obj_store, file_store=file_store)
application = WSGIApp(object_store=obj_store, file_store=file_store)

if __name__ == "__main__":
    from werkzeug.serving import run_simple
    run_simple("0.0.0.0", 80, application)

