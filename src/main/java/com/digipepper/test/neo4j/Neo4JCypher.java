package com.digipepper.test.neo4j;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.tooling.GlobalGraphOperations;

/**
 * @author tf0054
 * @see http://wiki.neo4j.org/content/Getting_Started_With_Java
 *
 */
public class Neo4JCypher {
	public static void main(String args[]) {
		GraphDatabaseService graphDb = new EmbeddedGraphDatabase( "graphdb" );
		//IndexManager index = graphDb.index();
		
		Transaction tx = graphDb.beginTx();
        try {
	        Map<String, Object> props  = new HashMap<String, Object>();
	        props .put( "name", "cypher" );
	        //props .put( "lastname", "Eunis" );
	
	        Map<String, Object> params = new HashMap<String, Object>();
	        params.put( "props", props  );
	
	        ExecutionEngine engine = new ExecutionEngine(graphDb);
	        ExecutionResult result = engine.execute( "create ({props})", params );
	        System.out.println(result);
	        tx.success();
        } finally {
			tx.finish();
			graphDb.shutdown();
        }
	}
}
