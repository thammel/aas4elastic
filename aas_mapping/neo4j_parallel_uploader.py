"""
High-Performance Neo4j JSON Data Import Script

This script processes JSON files and imports them into a Neo4j graph database
with optimized performance using parallel processing, connection pooling,
and efficient batch operations.
"""

from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from os.path import isfile, join
from typing import Dict, List, Tuple, Any, Optional, Iterator
from multiprocessing import cpu_count
import threading
from queue import Queue

from neo4j import GraphDatabase


@dataclass
class ProcessingStats:
    """Container for processing statistics."""
    total_files_processed: int = 0
    batches_processed: int = 0
    batch_size: int = 0
    nodes_created: int = 0
    relationships_created: int = 0
    total_time: float = 0.0
    processing_time: float = 0.0
    node_creation_time: float = 0.0
    relationship_creation_time: float = 0.0


class Neo4jParallelImporter:
    """High-performance Neo4j importer with parallel processing capabilities."""

    def __init__(self, uri: str = "bolt://localhost:7687", username: str = "neo4j",
                 password: str = "password", max_connection_pool_size: int = 50,
                 max_workers: int = None):
        self.uri = uri
        self.username = username
        self.password = password
        self.max_workers = max_workers or min(cpu_count(), 8)  # Limit to prevent overwhelming Neo4j
        self.uid_counter = 0
        self.uid_lock = threading.Lock()

        # Create driver with optimized settings
        self.driver = GraphDatabase.driver(
            uri,
            auth=(username, password),
            max_connection_pool_size=max_connection_pool_size,
            connection_acquisition_timeout=30,
            max_transaction_retry_time=15
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.driver.close()

    def _get_next_uid(self) -> int:
        """Thread-safe unique ID generation."""
        with self.uid_lock:
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

                if child_nodes:
                    self._add_relationship(relationships, key, -1, child_nodes[-1]['uid'],
                                           child_nodes[-1]['labels'], label)

            elif isinstance(value, dict):
                child_nodes, child_rels = self._process_dict(value, key)
                nodes.extend(child_nodes)
                self._merge_relationships(relationships, child_rels)

                if child_nodes:
                    self._add_relationship(relationships, key, -1, child_nodes[-1]['uid'],
                                           child_nodes[-1]['labels'], label)
            else:
                node_properties[key] = value

        if node_properties:
            uid = self._get_next_uid()
            node_properties.update({
                'uid': uid,
                'labels': label
            })

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
                continue

            nodes.extend(child_nodes)
            self._merge_relationships(relationships, child_rels)

            if child_nodes:
                self._add_relationship(relationships, label, -1, child_nodes[-1]['uid'],
                                       child_nodes[-1]['labels'], label)

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

    def _process_single_file(self, filepath: str) -> Optional[Tuple[List[Dict], Dict[str, List]]]:
        """Process a single JSON file. Used by parallel processing."""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return self.process_json_data(data)
        except Exception as e:
            print(f"Error processing {filepath}: {e}")
            return None

    def _process_file_batch_parallel(self, directory: str, file_batch: List[str]) -> Tuple[List[Dict], Dict[str, List]]:
        """Process a batch of JSON files in parallel."""
        batch_nodes = []
        batch_relationships = {}

        # Create full file paths
        file_paths = [join(directory, filename) for filename in file_batch]

        # Process files in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all files for processing
            future_to_file = {executor.submit(self._process_single_file, filepath): filepath
                              for filepath in file_paths}

            # Collect results as they complete
            for future in as_completed(future_to_file):
                filepath = future_to_file[future]
                try:
                    result = future.result()
                    if result:
                        nodes, relationships = result
                        batch_nodes.extend(nodes)
                        self._merge_relationships(batch_relationships, relationships)
                except Exception as e:
                    print(f"Error processing {filepath}: {e}")

        return batch_nodes, batch_relationships

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

    def _create_nodes_optimized(self, grouped_nodes: Dict[str, List[Dict]]) -> Dict[int, int]:
        """Create nodes in Neo4j with optimized bulk operations."""
        uid_to_internal_id = {}

        with self.driver.session() as session:
            # Create indexes first for better performance
            for label in grouped_nodes.keys():
                try:
                    session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.uid)")
                except Exception as e:
                    print(f"Warning: Could not create index for {label}: {e}")

            # Process each node type separately for better performance
            for label, nodes in grouped_nodes.items():
                if not nodes:
                    continue

                # Use UNWIND for bulk node creation - much faster than individual creates
                create_query = f"""
                UNWIND $nodes AS nodeData
                CREATE (n:{label})
                SET n = nodeData
                RETURN id(n) AS internal_id, n.uid AS uid
                """

                try:
                    result = session.run(create_query, nodes=nodes)
                    for record in result:
                        uid_to_internal_id[record['uid']] = record['internal_id']
                except Exception as e:
                    print(f"Error creating nodes for label {label}: {e}")
                    # Fallback to individual node creation
                    for node in nodes:
                        try:
                            individual_query = f"CREATE (n:{label}) SET n = $node RETURN id(n) AS internal_id, n.uid AS uid"
                            result = session.run(individual_query, node=node)
                            record = result.single()
                            uid_to_internal_id[record['uid']] = record['internal_id']
                        except Exception as inner_e:
                            print(f"Error creating individual node: {inner_e}")

        return uid_to_internal_id

    def _create_relationships_optimized(self, relationships: Dict[str, List],
                                        uid_to_internal_id: Dict[int, int]) -> int:
        """Create relationships in Neo4j with optimized bulk operations."""
        total_created = 0

        with self.driver.session() as session:
            for rel_type, rel_list in relationships.items():
                if not rel_list:
                    continue

                # Prepare relationship data in bulk
                prepared_rels = []
                for rel in rel_list:
                    if rel['from_uid'] in uid_to_internal_id and rel['to_uid'] in uid_to_internal_id:
                        prepared_rels.append({
                            'from_id': uid_to_internal_id[rel['from_uid']],
                            'to_id': uid_to_internal_id[rel['to_uid']]
                        })

                if not prepared_rels:
                    continue

                # Process relationships in chunks to avoid memory issues
                chunk_size = 1000
                for i in range(0, len(prepared_rels), chunk_size):
                    chunk = prepared_rels[i:i + chunk_size]

                    # Use parameterized query for safety and performance
                    create_rels_query = f"""
                    UNWIND $relationships AS rel
                    MATCH (from_node) WHERE id(from_node) = rel.from_id
                    MATCH (to_node) WHERE id(to_node) = rel.to_id
                    CREATE (from_node)-[:{rel_type}]->(to_node)
                    RETURN count(*) as created
                    """

                    try:
                        result = session.run(create_rels_query, relationships=chunk)
                        created = result.single()['created']
                        total_created += created
                    except Exception as e:
                        print(f"Error creating relationships of type {rel_type}: {e}")

                print(
                    f"Created {sum(1 for rel in rel_list if rel['from_uid'] in uid_to_internal_id and rel['to_uid'] in uid_to_internal_id)} relationships of type '{rel_type}'")

        return total_created

    def import_json_files(self, directory: str, batch_size: int = 50,
                          use_parallel_processing: bool = True) -> ProcessingStats:
        """Import JSON files with optimized performance."""
        overall_start_time = time.time()
        stats = ProcessingStats()

        # Get all JSON files
        json_files = [f for f in os.listdir(directory)
                      if isfile(join(directory, f)) and f.endswith('.json')]

        stats.total_files_processed = len(json_files)
        stats.batch_size = batch_size
        stats.batches_processed = (len(json_files) + batch_size - 1) // batch_size

        print(f"Found {len(json_files)} JSON files")
        print(f"Processing in {stats.batches_processed} batches of {batch_size} files each")
        print(f"Parallel processing: {'Enabled' if use_parallel_processing else 'Disabled'}")
        print(f"Max workers: {self.max_workers}")

        # Process files in batches
        for batch_num in range(stats.batches_processed):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(json_files))
            current_batch = json_files[start_idx:end_idx]

            print(f"\n--- Processing Batch {batch_num + 1}/{stats.batches_processed} ---")
            print(f"Files {start_idx + 1}-{end_idx} of {len(json_files)}")

            # Process current batch
            batch_start_time = time.time()

            if use_parallel_processing:
                batch_nodes, batch_relationships = self._process_file_batch_parallel(directory, current_batch)
            else:
                batch_nodes, batch_relationships = self._process_file_batch_sequential(directory, current_batch)

            processing_time = time.time() - batch_start_time
            stats.processing_time += processing_time
            print(f"Processed {len(current_batch)} files in {processing_time:.2f} seconds")

            if not batch_nodes:
                print("No nodes to create in this batch, skipping...")
                continue

            # Group nodes and filter relationships for this batch
            grouped_nodes = self._group_nodes_by_label(batch_nodes)
            filtered_relationships = self._filter_valid_relationships(batch_relationships)

            # Create nodes in Neo4j with optimized method
            node_start_time = time.time()
            uid_to_internal_id = self._create_nodes_optimized(grouped_nodes)

            node_count = sum(len(nodes) for nodes in grouped_nodes.values())
            node_creation_time = time.time() - node_start_time
            stats.node_creation_time += node_creation_time
            stats.nodes_created += node_count
            print(f"Created {node_count:,} nodes in {node_creation_time:.2f} seconds")

            # Create relationships in Neo4j with optimized method
            rel_start_time = time.time()
            relationship_count = self._create_relationships_optimized(filtered_relationships, uid_to_internal_id)
            relationship_creation_time = time.time() - rel_start_time
            stats.relationship_creation_time += relationship_creation_time
            stats.relationships_created += relationship_count
            print(f"Created {relationship_count:,} relationships in {relationship_creation_time:.2f} seconds")

            # Memory cleanup
            del batch_nodes, batch_relationships, grouped_nodes, filtered_relationships, uid_to_internal_id

            batch_total_time = time.time() - batch_start_time
            print(f"Batch {batch_num + 1} completed in {batch_total_time:.2f} seconds")

        stats.total_time = time.time() - overall_start_time
        return stats

    def _process_file_batch_sequential(self, directory: str, file_batch: List[str]) -> Tuple[
        List[Dict], Dict[str, List]]:
        """Process a batch of JSON files sequentially (fallback method)."""
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


def main():
    """Main function with performance-optimized configuration."""
    directory = "path/to/directory"  # Update this path

    # Performance configuration
    neo4j_config = {
        'uri': "bolt://localhost:7687",
        'username': "neo4j",
        'password': "password",  # Update with your password
        'max_connection_pool_size': 50,  # Increase for better concurrency
        'max_workers': min(cpu_count(), 8)  # Adjust based on your system
    }

    # Processing configuration
    batch_size = 50  # Larger batches for better performance
    use_parallel_processing = True  # Enable parallel file processing

    try:
        with Neo4jParallelImporter(**neo4j_config) as importer:
            print("Starting high-performance import...")
            print(f"System CPU count: {cpu_count()}")
            print(f"Max workers: {neo4j_config['max_workers']}")

            stats = importer.import_json_files(
                directory,
                batch_size=batch_size,
                use_parallel_processing=use_parallel_processing
            )

            # Display comprehensive results
            print("\n" + "=" * 60)
            print("HIGH-PERFORMANCE IMPORT SUMMARY")
            print("=" * 60)
            print(f"Total files processed: {stats.total_files_processed:,}")
            print(f"Batches processed: {stats.batches_processed:,}")
            print(f"Batch size: {stats.batch_size:,}")
            print(f"Nodes created: {stats.nodes_created:,}")
            print(f"Relationships created: {stats.relationships_created:,}")
            print(f"Total execution time: {stats.total_time:.2f} seconds")
            print(f"Data processing time: {stats.processing_time:.2f} seconds")
            print(f"Node creation time: {stats.node_creation_time:.2f} seconds")
            print(f"Relationship creation time: {stats.relationship_creation_time:.2f} seconds")

            # Performance metrics
            if stats.total_time > 0:
                files_per_second = stats.total_files_processed / stats.total_time
                nodes_per_second = stats.nodes_created / stats.total_time
                relationships_per_second = stats.relationships_created / stats.total_time

                print(f"\nPerformance Metrics:")
                print(f"Files per second: {files_per_second:.2f}")
                print(f"Nodes per second: {nodes_per_second:,.0f}")
                print(f"Relationships per second: {relationships_per_second:,.0f}")

                # Efficiency metrics
                processing_efficiency = (stats.processing_time / stats.total_time) * 100
                database_efficiency = ((
                                                   stats.node_creation_time + stats.relationship_creation_time) / stats.total_time) * 100

                print(f"\nEfficiency Breakdown:")
                print(f"File processing: {processing_efficiency:.1f}% of total time")
                print(f"Database operations: {database_efficiency:.1f}% of total time")
                print(f"Overhead: {100 - processing_efficiency - database_efficiency:.1f}% of total time")

    except Exception as e:
        print(f"Import failed: {e}")
        raise


if __name__ == "__main__":
    main()