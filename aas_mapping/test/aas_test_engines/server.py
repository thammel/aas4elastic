import pathlib
import logging

from interfaces.repository import WSGIApp
from basyx.aas.adapter.aasx import DictSupplementaryFileContainer
from basyx.aas.adapter.aasx import AASXReader
from aas_mapping.aas_neo4j_adapter.neo_aas_object_store import Neo4jObjectStore
from aas_mapping.aas_neo4j_adapter.aas_neo4j_client import AASNeo4JClient
from aas_mapping.aas_neo4j_adapter.aas_neo4j_client import AAS_NEO4J_MODEL_CONFIG

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

client = AASNeo4JClient(uri="bolt://neo4j:7687", user="neo4j", password="12345678", model_config=AAS_NEO4J_MODEL_CONFIG)
obj_store = Neo4jObjectStore(client=client)
file_store = DictSupplementaryFileContainer()

if not pathlib.Path("/input").exists():
    print("Input directory '/input' does not exist. No AASX files will be loaded.", flush=True)

for file in pathlib.Path("/input").glob("*.aasx"):
    with AASXReader(file) as reader:
        reader.read_into(object_store=obj_store, file_store=file_store)
        print("Loaded AASX file: %s", file, flush=True)

application = WSGIApp(object_store=obj_store, file_store=file_store)

if __name__ == "__main__":
    from werkzeug.serving import run_simple

    run_simple("0.0.0.0", 80, application)
