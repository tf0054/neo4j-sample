package com.digipepper.test.neo4j;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import net.sf.json.JSON;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.xml.XMLSerializer;

import org.apache.commons.io.IOUtils;
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
 * @see http://stackoverflow.com/questions/14057897/how-to-explore-databases-created-by-an-embedded-neo4j-java-application-and-store
 */
public class Neo4JMainWithXml {
	
	static Long longSecEnd = (long) 0;
	static Long longFrameEnd = (long) 0;
	static Long longSecDelta = (long) 20;
	static Long longSecStartTime = (long) 1246191120;
	
	public static void main(String args[]) {
		GraphDatabaseService graphDb = new EmbeddedGraphDatabase( "graphdb" );
		IndexManager index = graphDb.index();
		
		Transaction tx = graphDb.beginTx();
		try {
			Index<Node> test01idx = index.forNodes("test01index");

//			InputStream is = 
//					Neo4JMainWithXml.class.getResourceAsStream("sample-xml.xml");
//				String xml = IOUtils.toString(is);
			
			String xml = readFileAsString("sample-xml.xml");
			
			XMLSerializer xmlSerializer = new XMLSerializer(); 
			JSON json = xmlSerializer.read( xml );  
						
			HashMap<String, ArrayList<Long[]>> Node_Timeline = new HashMap<String, ArrayList<Long[]>>();
			HashMap<String, ArrayList<Long[]>> Edge_Timeline = new HashMap<String, ArrayList<Long[]>>();

			// *** TODO
//			dumpNodes(graphDb);
//			System.exit(0);
			
			JSONArray objNodes = ( ( ((JSONObject) json)
					.getJSONObject("graph")).getJSONObject("nodes")).getJSONArray("node");
			
			Node_Timeline = mkTimeline(objNodes, "Node");
			
			JSONArray objEdges = ( ( ((JSONObject) json)
					.getJSONObject("graph")).getJSONObject("edges")).getJSONArray("edge");

			Edge_Timeline = mkTimeline(objEdges, "Edge");
						
			Node runNode = graphDb.createNode();
			runNode.setProperty( "name", "RUN" );

			// Frameの作成
			Node objNode1 = null, objNode2 = null;
			IndexHits<Node> objHits = null;
			Relationship relationship = null;

			JSONObject objForNode = null;
			Iterator<JSONObject> objIte = null;

			System.out.println("END:"+longFrameEnd);
			for(int i = 0; i < longFrameEnd; i++){
				objNode1 = graphDb.createNode();
				objNode1.setProperty( "name", "FRAME_" + i );
				objNode1.setProperty( "time", longSecStartTime+i*longSecDelta );
				objNode1.setProperty( "strTime", getDateFromEpoch(longSecStartTime+i*longSecDelta));
				test01idx.add(objNode1, "name", objNode1.getProperty("name"));
				if(i == 0){
					relationship = runNode.createRelationshipTo( objNode1, MyRelationshipTypes.RUN_FRAME_FIRST );
				}else{
					// make relations for frames.
					objHits = test01idx.get("name", "FRAME_"+(i-1));
					objNode2 = objHits.next();
					relationship = objNode2.createRelationshipTo( objNode1, MyRelationshipTypes.FRAME_NEXT );
					relationship = runNode.createRelationshipTo( objNode1, MyRelationshipTypes.RUN_FRAME );
				}
			}
			tx = txCommit(graphDb,tx);
			int i = 0;
			System.out.println("Creating nodes...");				
			objIte = objNodes.iterator();
			while(objIte.hasNext()){
				objForNode = objIte.next();
				objNode1 = graphDb.createNode();
				objNode1.setProperty( "name", "TAG_" + objForNode.getString("@id") );
				relationship = runNode.createRelationshipTo( objNode1, MyRelationshipTypes.RUN_TAG );
				test01idx.add(objNode1, "name", objNode1.getProperty("name"));
				for(Long[] arySpells: Node_Timeline.get(objForNode.getString("@id"))){
					for(Long longSpell: arySpells){
							objHits = test01idx.get("name", "FRAME_"+longSpell);
							objNode2 = objHits.next();
							relationship = objNode1.createRelationshipTo( objNode2, MyRelationshipTypes.FRAME_TAG );
							i++;
					}
				}
			}
			System.out.println("frame rels: "+i);
			tx = txCommit(graphDb,tx);
			
			i = 0;
			System.out.println("Creating edges...");				
			objIte = objEdges.iterator();
			while(objIte.hasNext()){
				// make edges.
				objForNode = objIte.next();
				objNode1 = graphDb.createNode();
				objNode1.setProperty( "name", "EDGE_" + objForNode.getString("@source")+"-"+objForNode.getString("@target"));
				test01idx.add(objNode1, "name", objNode1.getProperty("name"));
				// make relations to nodes.
				relationship = runNode.createRelationshipTo( objNode1, MyRelationshipTypes.RUN_EDGE );
				relationship = mkRelFromName(objNode1, "TAG_"+objForNode.getString("@source"), test01idx);
//				objHits = test01idx.get("name", "TAG_"+objForNode.getString("@source"));
//				if(!objHits.hasNext()){
//					objNode2 = objHits.next();
//					relationship = objNode1.createRelationshipTo( objNode2, MyRelationshipTypes.EDGE_TAG );
//				}
				relationship = mkRelFromName(objNode1, "TAG_"+objForNode.getString("@target"), test01idx);
//				objHits = test01idx.get("name", );
//				if(!objHits.hasNext()){
//					objNode2 = objHits.next();
//					relationship = objNode1.createRelationshipTo( objNode2, MyRelationshipTypes.EDGE_TAG );
//				}
				for(Long[] arySpells: Edge_Timeline.get(objForNode.getString("@source")+"-"+objForNode.getString("@target"))){
					for(Long longSpell: arySpells){
							mkRelFromName(objNode1, "FRAME_"+longSpell, test01idx);
							i++;
					}
				}
			}
			System.out.println("frame rels: "+i);
			tx = txCommit(graphDb,tx);
			
			System.out.println("Done.");				

			tx.success();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} 
		finally {
			tx.finish();
			graphDb.shutdown(); // for safe closing.
		}
	}

	private static Relationship mkRelFromName(Node objNode1, String a, Index<Node> test01idx) {
		Node objNode2;
		IndexHits<Node> objHits;
		//Relationship relationship;
		objHits = test01idx.get("name", a);
		objNode2 = objHits.next();
		return objNode1.createRelationshipTo( objNode2, MyRelationshipTypes.FRAME_EDGE );
	}

	private static HashMap<String, ArrayList<Long[]>> mkTimeline(JSONArray objNodes, String strType) {		
		HashMap<String, ArrayList<Long[]>> Node_Timeline = new HashMap<String, ArrayList<Long[]>>();

		Iterator<JSONObject> objIte;
		objIte = objNodes.iterator();
		while(objIte.hasNext()){
			JSONObject objNode = objIte.next();
			JSONObject objSpells = objNode.getJSONObject("spells");

			ArrayList<Long[]> aryFrameNos = new ArrayList<Long[]>();

			JSONArray aryTmp = null;
			if(objSpells.get("spell") instanceof JSONArray){
				aryTmp = objSpells.getJSONArray("spell");
			}else{
				aryTmp = new JSONArray();
				aryTmp.add(objSpells.getJSONObject("spell"));
			}
			
			//Iterator<JSONObject> objIteIn = null;
			//objIteIn = aryTmp.iterator();
			//while(objIteIn.hasNext()){
			for(Iterator<JSONObject> objIteIn = aryTmp.iterator(); objIteIn.hasNext();){				
				JSONObject a = objIteIn.next();
				aryFrameNos.add(get_intervals(a.getLong("@start"),a.getLong("@end")));
			}
			
			String strTmp = "";
			if(strType.equals("Node"))
				strTmp = objNode.getString("@id");
			else
				strTmp = objNode.getString("@source")+"-"+objNode.getString("@target");
			Node_Timeline.put(strTmp, aryFrameNos);
		}
		
		return Node_Timeline;
	}
	
	private static Long[] get_intervals(Long s, Long e){
		
		// 今のが何枚目からスタートか
		Long longS = (s - longSecStartTime) / longSecDelta;
		Long longE = (e - longSecStartTime) / longSecDelta;
		
		if(e > longSecEnd)
			longSecEnd = e;

		if(longE > longFrameEnd)
			longFrameEnd = longE;
		
		ArrayList<Long> aryTmp = new ArrayList<Long>();
		aryTmp.add((long) 0);
		while(longE > longS){
			aryTmp.add(longS++);	
		}

		// http://www.atmarkit.co.jp/bbs/phpBB/viewtopic.php?topic=16464&forum=12
		int i = 0;
		Long[] dst = new Long[aryTmp.size()];
		Iterator<Long> iter = aryTmp.iterator();
		while(iter.hasNext()){
			dst[i++] = iter.next().longValue();
		} 
		//return (Long[]) aryTmp.toArray(Long);
		return dst;
	}
	
	private static String readFileAsString(String filePath)
			throws java.io.IOException {
		byte[] buffer = new byte[(int) new File(filePath).length()];
		BufferedInputStream f = null;
		try {
			f = new BufferedInputStream(new FileInputStream(filePath));
			f.read(buffer);
		} finally {
			if (f != null)
				try {
					f.close();
				} catch (IOException ignored) {
				}
		}
		return new String(buffer);
	}
	
	private static String getDateFromEpoch(long epochtime){
		SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z", Locale.US);
		return sdf.format(new Date(epochtime*1000));
	}
	
	private static Transaction txCommit(GraphDatabaseService db, Transaction tx){
//        if ( i > 0 && i % 10000 == 0 ) {
            tx.success();
            tx.finish();
            tx = db.beginTx();
            return tx;
//        }
	}

	private static void dumpNodes(GraphDatabaseService graphDb) {
	
			Map<String, Object> props  = new HashMap<String, Object>();
	        props .put( "name", "cypher" );
	
	//        Map<String, Object> params = new HashMap<String, Object>();
	//        params.put( "props", props  );
	
	        ExecutionEngine engine = new ExecutionEngine(graphDb);
	        //ExecutionResult result = engine.execute( "START root=node(1190,1192,1192,1193) RETURN root");
	        //ExecutionResult result = engine.execute( "START root=node(1190,1192,1192,1193) RETURN root");
	        ExecutionResult result = engine.execute( "START root=node(*) MATCH (root)-[r]->()RETURN r,root");
	        System.out.println(result);
	
		}
}
