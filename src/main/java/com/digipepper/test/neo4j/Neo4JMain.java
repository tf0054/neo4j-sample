package com.digipepper.test.neo4j;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * @author tf0054
 * @see http://wiki.neo4j.org/content/Getting_Started_With_Java
 *
 */
public class Neo4JMain {
	public static void main(String args[]) {
		GraphDatabaseService graphDb = new EmbeddedGraphDatabase( "graphdb" );
		IndexManager index = graphDb.index();
		Transaction tx = graphDb.beginTx();
		try {
			Node firstNode = graphDb.createNode();
			Node secondNode = graphDb.createNode();
			Relationship relationship = firstNode.createRelationshipTo( secondNode, MyRelationshipTypes.KNOWS );
	
			firstNode.setProperty( "name", "Hello" );
			secondNode.setProperty( "name", "world" );
			relationship.setProperty( "message", "brave Neo4j " );
			
			// making index(lucene)
			Index<Node> test01idx = index.forNodes("test01index");
			test01idx.add(firstNode, "name", firstNode.getProperty("name"));
			test01idx.add(secondNode, "name", secondNode.getProperty("name"));

			// searching
			Node objTmp = null;
			for(Relationship rel : firstNode.getRelationships()){
				objTmp = rel.getOtherNode(firstNode);
				System.out.println(objTmp.getProperty("name"));
			}

			// searching with index
			IndexHits<Node> objHits = test01idx.get("name", "Hello");
			objTmp = objHits.next();
			System.out.println(objTmp.getProperty("name"));

			tx.success();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
		finally {
			tx.finish();
		}
	}
}
