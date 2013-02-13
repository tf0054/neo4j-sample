package com.digipepper.test.neo4j;

import java.util.Iterator;

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
public class Neo4JMain {
	public static void main(String args[]) {
		GraphDatabaseService graphDb = new EmbeddedGraphDatabase( "graphdb" );
		IndexManager index = graphDb.index();
		
		Transaction tx = graphDb.beginTx();
		try {
			Node tmpNode = null;
			Node firstNode = null;
			Node secondNode = null;
			Index<Node> test01idx = index.forNodes("test01index");

			if(graphDb.getNodeById(0) == null){
				// making nodes
				firstNode = graphDb.createNode();
				secondNode = graphDb.createNode();
				firstNode.setProperty( "name", "Hello" );
				secondNode.setProperty( "name", "world" );

				// making relation
				Relationship relationship = firstNode.createRelationshipTo( secondNode, MyRelationshipTypes.KNOWS );
				relationship.setProperty( "message", "brave Neo4j " );
				
				// making index(lucene)
				test01idx.add(firstNode, "name", firstNode.getProperty("name"));
				test01idx.add(secondNode, "name", secondNode.getProperty("name"));
												
				System.out.println("id:"+firstNode.getId());
			}else{
				System.out.println("There is db already.");
				
				Iterator<Node> objIte = GlobalGraphOperations.at(graphDb).getAllNodes().iterator();
				while(objIte.hasNext()){
					tmpNode = objIte.next();
					System.out.print("inside: "+tmpNode.getId());
					if(tmpNode.hasProperty("name"))
						System.out.println(","+tmpNode.getProperty("name"));
					else
						System.out.println("");
				}

				GlobalGraphOperations.at(graphDb).getAllNodes();
				
				firstNode = graphDb.getNodeById(1);
				secondNode = graphDb.getNodeById(2);
			}
			
			// searching
			for(Relationship rel : firstNode.getRelationships()){
				tmpNode = rel.getOtherNode(firstNode);
				System.out.println(tmpNode.getProperty("name"));
			}

			// searching with index
			IndexHits<Node> objHits = test01idx.get("name", "Hello");
			tmpNode = objHits.next();
			System.out.println(tmpNode.getProperty("name"));
			objHits.close();

			tx.success();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
		finally {
			tx.finish();
			graphDb.shutdown(); // for safe closing.
		}
	}
}
