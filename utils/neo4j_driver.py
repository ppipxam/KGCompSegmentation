from neo4j import GraphDatabase
import os
from typing import Dict, Any, Hashable


uri = os.environ["NEO4J_URI"]
neo4j_username = os.environ["NEO4J_USERNAME"]
neo4j_pw = os.environ["NEO4J_PASSWORD"]

class Neo4jDriver:

    def get_driver(self, database: str):
        return GraphDatabase.driver(
                uri, 
                auth=(self.username, self.pw), 
                database=database
                )

    def __init__(
            self, 
            default_database: str,
            username=neo4j_username,
            pw=neo4j_pw,
            ):
        self._username = username
        self._pw = pw
        self._driver = self.get_driver(default_database)

    @property
    def driver(self):
        return self._driver

    @property
    def username(self):
        return self._username
    
    @username.setter
    def username(self, new_username):
        self._username = new_username

    @property
    def pw(self):
        return self._pw
    
    @pw.setter
    def pw(self, new_pw):
        self._pw = new_pw

    def run_query(self, query, **kwargs):
        with self.driver as driver:
            result = driver.execute_query(query, **kwargs)
        return result

    @staticmethod
    def create_node_tx(tx, node_type: str, **kwargs):
        properties = ", ".join([f"{k}: ${k}" for k in kwargs.keys()])
        query = (f"CREATE (n:{node_type} {{{properties}}}) "
                "RETURN n.id AS node_id")
        result = tx.run(query, **kwargs)
        record = result.single()
        return record["node_id"]

    @staticmethod
    def create_relationship_tx(
            tx, 
            source_node_type: str,
            source_node_properties: Dict[Hashable, Any],
            target_node_type: str,
            target_node_properties: Dict[Hashable, Any],
            relationship_type: str, 
            **kwargs
            ):
        source_properties = ", ".join([f"{k}: ${k}" for k in source_node_properties.keys()])
        target_properties = ", ".join([f"{k}: ${k}" for k in target_node_properties.keys()])
        relationship_properties = ", ".join([f"{k}: ${k}" for k in kwargs.keys()])
        query = (f"MATCH (s:{source_node_type} {{{source_properties}}}), (t:{target_node_type} {{{target_properties}}}) "
            f"CREATE (s)-[r:{relationship_type} {{{relationship_properties}}}]->(t) "
            )
        print(query)
        query_kwargs = kwargs
        query_kwargs.update(source_node_properties)
        query_kwargs.update(target_node_properties)
        result = tx.run(query, **kwargs)
        return result
    
    @classmethod
    def create_node(
            cls, 
            database: str,
            node_type: str,
            **kwargs
            ):
        cls_obj = cls(database)
        with cls_obj.driver.session() as session:
            session.execute_write(
                cls_obj.create_node_tx,
                node_type=node_type,
                **kwargs
            )

    @classmethod
    def create_relationship(
        cls,
        database: str,
        source_node_type: str,
        source_node_properties: Dict[Hashable, Any],
        target_node_type: str,
        target_node_properties: Dict[Hashable, Any],
        relationship_type: str, 
        **kwargs
    ):
        cls_obj = cls_obj = cls(database)
        with cls_obj.driver.session() as session:
            session.execute_write(
                cls_obj.create_relationship_tx,
                source_node_type=source_node_type,
                source_node_properties=source_node_properties,
                target_node_type=target_node_type,
                target_node_properties=target_node_properties,
                relationship_type=relationship_type,
                **kwargs
            )