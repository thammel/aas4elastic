"""
Neo4j JSON Data Import Script

This script processes JSON files and imports them into a Neo4j graph database,
creating nodes and relationships based on the JSON structure.
"""

from __future__ import annotations

import json
import os
import time
from os.path import isfile, join
from typing import Dict, List, Tuple, Any, Union

from neo4j import GraphDatabase


class Neo4jImporter:
    """Handles importing JSON data into Neo4j graph database."""

    def __init__(self, uri: str = "bolt://localhost:7687", username: str = "neo4j", password: str = "password"):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
        self.uid_counter = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.close()

    def _get_next_uid(self) -> int:
        """Generate unique ID for nodes."""
        self.uid_counter += 1
        return self.uid_counter

    def _process_dict(self, data: Dict[str, Any], label: str) -> Tuple[List[Dict], Dict[str, List]]:
        """Process dictionary data into nodes and relationships."""
        nodes = []
        relationships = {}
        node_properties = {}

        for key, value in data.items():
            if isinstance(value, list):
                child_nodes, child_rels = self._process_list(value, key)
                nodes.extend(child_nodes)
                self._merge_relationships(relationships, child_rels)

                # Create relationship to the last created node
                if child_nodes:
                    self._add_relationship(relationships, key, -1, child_nodes[-1]['uid'], child_nodes[-1]['labels'],
                                           label)

            elif isinstance(value, dict):
                child_nodes, child_rels = self._process_dict(value, key)
                nodes.extend(child_nodes)
                self._merge_relationships(relationships, child_rels)

                # Create relationship to the last created node
                if child_nodes:
                    self._add_relationship(relationships, key, -1, child_nodes[-1]['uid'], child_nodes[-1]['labels'],
                                           label)
            else:
                node_properties[key] = value

        # Create node if it has properties
        if node_properties:
            uid = self._get_next_uid()
            node_properties.update({
                'uid': uid,
                'labels': label
            })

            # Update relationship from_uid placeholders
            self._update_relationship_source(relationships, uid)
            nodes.append(node_properties)

        return nodes, relationships

    def _process_list(self, data: List[Any], label: str) -> Tuple[List[Dict], Dict[str, List]]:
        """Process list data into nodes and relationships."""
        nodes = []
        relationships = {}

        for item in data:
            if isinstance(item, dict):
                child_nodes, child_rels = self._process_dict(item, label)
            elif isinstance(item, list):
                child_nodes, child_rels = self._process_list(item, label)
            else:
                print(f"Warning: Unsupported type in list: {type(item)}")
                continue

            nodes.extend(child_nodes)
            self._merge_relationships(relationships, child_rels)

            # Create relationship to the last created node
            if child_nodes:
                self._add_relationship(relationships, label, -1, child_nodes[-1]['uid'], child_nodes[-1]['labels'],
                                       label)

        return nodes, relationships

    def _add_relationship(self, relationships: Dict[str, List], rel_type: str, from_uid: int,
                          to_uid: int, to_type: str, from_type: str):
        """Add a relationship to the relationships dictionary."""
        if rel_type not in relationships:
            relationships[rel_type] = []

        relationships[rel_type].append({
            'from_uid': from_uid,
            'to_uid': to_uid,
            'to_type': to_type,
            'from_type': from_type
        })

    def _merge_relationships(self, target: Dict[str, List], source: Dict[str, List]):
        """Merge relationships from source into target."""
        for key, value in source.items():
            if key in target:
                target[key].extend(value)
            else:
                target[key] = value.copy()

    def _update_relationship_source(self, relationships: Dict[str, List], uid: int):
        """Update placeholder from_uid values with actual uid."""
        for rel_list in relationships.values():
            for rel in rel_list:
                if rel['from_uid'] == -1:
                    rel['from_uid'] = uid

    def process_json_data(self, data: Dict[str, Any]) -> Tuple[List[Dict], Dict[str, List]]:
        """Process JSON data into nodes and relationships."""
        nodes = []
        relationships = {}

        for key, value in data.items():
            child_nodes, child_rels = self._process_list(value, key)
            nodes.extend(child_nodes)
            self._merge_relationships(relationships, child_rels)

        return nodes, relationships

    def _group_nodes_by_label(self, nodes: List[Dict]) -> Dict[str, List[Dict]]:
        """Group nodes by their labels."""
        grouped = {}
        for node in nodes:
            label = node.pop('labels')
            if label not in grouped:
                grouped[label] = []
            grouped[label].append(node)
        return grouped

    def _filter_valid_relationships(self, relationships: Dict[str, List]) -> Dict[str, List]:
        """Filter out relationships with invalid from_uid."""
        filtered = {}
        for rel_type, rel_list in relationships.items():
            filtered[rel_type] = [rel for rel in rel_list if rel['from_uid'] != -1]
        return filtered

    def _create_nodes(self, grouped_nodes: Dict[str, List[Dict]]) -> Dict[int, int]:
        """Create nodes in Neo4j and return uid to internal_id mapping."""
        create_nodes_query = """
        UNWIND keys($data) AS labelName
        UNWIND $data[labelName] AS properties
        CALL (labelName, properties) {
            CALL apoc.create.node([labelName], properties) YIELD node
            RETURN id(node) AS internal_id, properties.uid AS uid
        }
        RETURN internal_id, uid
        """

        uid_to_internal_id = {}

        with self.driver.session() as session:
            # Create indexes for better performance
            for label in grouped_nodes.keys():
                session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.uid)")

            # Create nodes
            result = session.run(create_nodes_query, data=grouped_nodes)
            for record in result:
                uid_to_internal_id[record['uid']] = record['internal_id']

        return uid_to_internal_id

    def _create_relationships(self, relationships: Dict[str, List], uid_to_internal_id: Dict[int, int]):
        """Create relationships in Neo4j."""
        with self.driver.session() as session:
            total_created = 0

            for rel_type, rel_list in relationships.items():
                # Prepare relationship data
                prepared_rels = []
                for rel in rel_list:
                    if rel['from_uid'] in uid_to_internal_id and rel['to_uid'] in uid_to_internal_id:
                        prepared_rels.append({
                            'from_id': uid_to_internal_id[rel['from_uid']],
                            'to_id': uid_to_internal_id[rel['to_uid']]
                        })

                if prepared_rels:
                    # Create relationships in batch
                    create_rels_query = f"""
                    UNWIND $relationships AS rel
                    MATCH (from_node) WHERE id(from_node) = rel.from_id
                    MATCH (to_node) WHERE id(to_node) = rel.to_id
                    CREATE (from_node)-[:{rel_type}]->(to_node)
                    RETURN count(*) as created
                    """

                    result = session.run(create_rels_query, relationships=prepared_rels)
                    created = result.single()['created']
                    total_created += created
                    print(f"Created {created} relationships of type '{rel_type}'")

            return total_created

    def _process_file_batch(self, directory: str, file_batch: List[str]) -> Tuple[List[Dict], Dict[str, List]]:
        """Process a batch of JSON files and return nodes and relationships."""
        batch_nodes = []
        batch_relationships = {}

        for filename in file_batch:
            try:
                with open(join(directory, filename), 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    nodes, relationships = self.process_json_data(data)
                    batch_nodes.extend(nodes)
                    self._merge_relationships(batch_relationships, relationships)
            except Exception as e:
                print(f"Error processing {filename}: {e}")
                continue

        return batch_nodes, batch_relationships

    def import_json_files(self, directory: str, batch_size: int = 50) -> Dict[str, int]:
        """Import JSON files from directory into Neo4j using batch processing."""
        overall_start_time = time.time()

        # Get all JSON files
        json_files = [f for f in os.listdir(directory)
                      if isfile(join(directory, f)) and f.endswith('.json')]

        total_files = len(json_files)
        total_batches = (total_files + batch_size - 1) // batch_size  # Ceiling division

        print(f"Found {total_files} JSON files")
        print(f"Processing in {total_batches} batches of {batch_size} files each")

        # Initialize counters
        total_nodes_created = 0
        total_relationships_created = 0
        total_processing_time = 0
        total_node_creation_time = 0
        total_relationship_creation_time = 0

        # Process files in batches
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, total_files)
            current_batch = json_files[start_idx:end_idx]

            print(f"\n--- Processing Batch {batch_num + 1}/{total_batches} ---")
            print(f"Files {start_idx + 1}-{end_idx} of {total_files}")

            # Process current batch
            batch_start_time = time.time()
            batch_nodes, batch_relationships = self._process_file_batch(directory, current_batch)

            processing_time = time.time() - batch_start_time
            total_processing_time += processing_time
            print(f"Processed {len(current_batch)} files in {processing_time:.2f} seconds")

            if not batch_nodes:
                print("No nodes to create in this batch, skipping...")
                continue

            # Group nodes and filter relationships for this batch
            grouped_nodes = self._group_nodes_by_label(batch_nodes)
            filtered_relationships = self._filter_valid_relationships(batch_relationships)

            # Create nodes in Neo4j
            node_start_time = time.time()
            uid_to_internal_id = self._create_nodes(grouped_nodes)

            node_count = sum(len(nodes) for nodes in grouped_nodes.values())
            node_creation_time = time.time() - node_start_time
            total_node_creation_time += node_creation_time
            total_nodes_created += node_count
            print(f"Created {node_count} nodes in {node_creation_time:.2f} seconds")

            # Create relationships in Neo4j
            rel_start_time = time.time()
            relationship_count = self._create_relationships(filtered_relationships, uid_to_internal_id)
            relationship_creation_time = time.time() - rel_start_time
            total_relationship_creation_time += relationship_creation_time
            total_relationships_created += relationship_count
            print(f"Created {relationship_count} relationships in {relationship_creation_time:.2f} seconds")

            # Memory cleanup
            del batch_nodes, batch_relationships, grouped_nodes, filtered_relationships, uid_to_internal_id

            batch_total_time = time.time() - batch_start_time
            print(f"Batch {batch_num + 1} completed in {batch_total_time:.2f} seconds")

        total_time = time.time() - overall_start_time

        return {
            'total_files_processed': total_files,
            'batches_processed': total_batches,
            'batch_size': batch_size,
            'nodes_created': total_nodes_created,
            'relationships_created': total_relationships_created,
            'total_time': total_time,
            'processing_time': total_processing_time,
            'node_creation_time': total_node_creation_time,
            'relationship_creation_time': total_relationship_creation_time
        }


def main():
    """Main function to run the import process."""
    directory = "path/to/directory"  # Update this path

    # Configuration
    neo4j_config = {
        'uri': "bolt://localhost:7687",
        'username': "neo4j",
        'password': "password"  # Update with your password
    }

    # Batch configuration
    batch_size = 25  # Process 50 files at a time - adjust based on your memory constraints

    try:
        with Neo4jImporter(**neo4j_config) as importer:
            stats = importer.import_json_files(directory, batch_size=batch_size)

            print("\n" + "=" * 50)
            print("IMPORT SUMMARY")
            print("=" * 50)
            print(f"Total files processed: {stats['total_files_processed']}")
            print(f"Batches processed: {stats['batches_processed']}")
            print(f"Batch size: {stats['batch_size']}")
            print(f"Nodes created: {stats['nodes_created']:,}")
            print(f"Relationships created: {stats['relationships_created']:,}")
            print(f"Total execution time: {stats['total_time']:.2f} seconds")
            print(f"Data processing time: {stats['processing_time']:.2f} seconds")
            print(f"Node creation time: {stats['node_creation_time']:.2f} seconds")
            print(f"Relationship creation time: {stats['relationship_creation_time']:.2f} seconds")

            # Performance metrics
            avg_files_per_second = stats['total_files_processed'] / stats['total_time']
            avg_nodes_per_second = stats['nodes_created'] / stats['total_time']
            print(f"\nPerformance:")
            print(f"Average files per second: {avg_files_per_second:.2f}")
            print(f"Average nodes per second: {avg_nodes_per_second:.2f}")

    except Exception as e:
        print(f"Import failed: {e}")
        raise


if __name__ == "__main__":
    main()
