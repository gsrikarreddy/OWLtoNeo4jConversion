import re
import argparse
from rdflib import Graph, RDFS, URIRef
from collections import defaultdict
from neo4j import GraphDatabase
from tqdm import tqdm
import os
import tempfile


def convert_owl_to_ttl(input_path):
    g = Graph()
    g.parse(input_path, format="xml") 
    temp_ttl = tempfile.NamedTemporaryFile(delete=False, suffix=".ttl")
    g.serialize(destination=temp_ttl.name, format="turtle")
    return temp_ttl.name


def sanitize_label(label):
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '', label.replace(" ", "_").replace("-", "_"))
    if re.match(r'^\d', sanitized):
        sanitized = "L_" + sanitized
    return sanitized or "Unknown"


def extract_fragment(uri):
    if "#" in uri:
        return uri.split("#")[-1]
    return uri.rstrip("/").split("/")[-1]


def parse_ttl_to_json(ttl_path, root_label=None):
    g = Graph()
    g.parse(ttl_path, format="ttl")

    children = defaultdict(list)
    labels = {}
    parents = set()
    all_classes = set()

    for s, p, o in g:
        if p == RDFS.label:
            labels[s] = str(o)
        elif p == RDFS.subClassOf and isinstance(o, URIRef):
            children[o].append(s)
            parents.add(s)
            all_classes.add(s)
            all_classes.add(o)

    for cls in all_classes:
        if cls not in labels:
            labels[cls] = extract_fragment(str(cls))

    if root_label:
        def get_uri_by_label(search_label):
            for uri, label in labels.items():
                if label.lower() == search_label.lower():
                    return uri
            return None

        root_uri = get_uri_by_label(root_label)
        if not root_uri:
            raise ValueError(f"Root label '{root_label}' not found in TTL file.")
        roots = [root_uri]
    else:
        roots = [cls for cls in all_classes if cls not in parents]

    def build_json_tree(node):
        return {
            "id": str(node),
            "label": labels.get(node, extract_fragment(str(node))),
            "children": [build_json_tree(child) for child in children.get(node, [])]
        }

    return [build_json_tree(r) for r in roots]


def count_nodes(node, children_field):
    count = 1
    for child in node.get(children_field, []):
        count += count_nodes(child, children_field)
    return count


def upload_node(tx, node_id, label, assigned_label):
    tx.run(f"""
        MERGE (n:{assigned_label} {{id: $id}})
        SET n.name = $label
    """, id=node_id, label=label)


def create_relationship(tx, parent_id, child_id):
    tx.run("""
        MATCH (p {id: $parent_id})
        MATCH (c {id: $child_id})
        MERGE (c)-[:SUBCLASS_OF]->(p)
    """, parent_id=parent_id, child_id=child_id)


def traverse_and_upload(tx, root_node, id_field, label_field, children_field, progress=None):
    def recurse(node, parent_id=None, parent_label=None, level=0):
        node_id = node.get(id_field)
        raw_label = node.get(label_field, node_id)
        children = node.get(children_field, [])

        current_label = sanitize_label(raw_label)
        assigned_label = current_label if level <= 1 else parent_label

        if node_id:
            upload_node(tx, node_id, raw_label, assigned_label)
            if parent_id:
                create_relationship(tx, parent_id, node_id)
            if progress:
                progress.update(1)

            for child in children:
                recurse(child, parent_id=node_id, parent_label=assigned_label, level=level+1)

    recurse(root_node)


def reset_database(driver):
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")


# ---- Clean run_conversion() for API or CLI use ----
def run_conversion(ttl_path, root_label, neo4j_uri, username, password, preview_only=False):
    if ttl_path.endswith(".owl"):
        ttl_path = convert_owl_to_ttl(ttl_path)

    json_data = parse_ttl_to_json(ttl_path, root_label=root_label)

    if preview_only:
        return {"status": "preview", "json_data": json_data}

    driver = GraphDatabase.driver(neo4j_uri, auth=(username, password))
    reset_database(driver)

    total_nodes = sum(count_nodes(tree, "children") for tree in json_data)

    for tree in json_data:
        with driver.session() as session:
            session.execute_write(lambda tx: traverse_and_upload(tx, tree, "id", "label", "children", progress=None))

    return {
        "status": "success",
        "nodes_uploaded": total_nodes
    }


# ---- CLI entry point ----
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TTL to Neo4j ontology uploader.")
    parser.add_argument("--ttl_file", required=True, help="Path to TTL file")
    parser.add_argument("--root_label", required=False, help="Optional root label to start tree (default: all roots)")
    parser.add_argument("--preview_only", action="store_true", help="Preview the parsed ontology JSON without uploading to Neo4j")
    parser.add_argument("--neo4j_uri", default="bolt://localhost:7687", help="Neo4j URI")
    parser.add_argument("--neo4j_user", default="neo4j", help="Neo4j username")
    parser.add_argument("--neo4j_pass", default="12345678", help="Neo4j password")
    args = parser.parse_args()

    result = run_conversion(
        ttl_path=args.ttl_file,
        root_label=args.root_label,
        neo4j_uri=args.neo4j_uri,
        username=args.neo4j_user,
        password=args.neo4j_pass,
        preview_only=args.preview_only
    )

    print(result)
