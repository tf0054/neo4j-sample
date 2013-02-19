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
		IndexManager indexMgr = graphDb.index();
		
		Transaction tx = graphDb.beginTx();
		try {
			Index<Node> tags_HT2009 = indexMgr.forNodes("test01index");

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

			// making nodes
			Node refNode = graphDb.getNodeById(0);
			refNode.setProperty( "name", "REF" );

			Node runNode = graphDb.createNode();
			//runNode.setProperty( "name", "RUN" );
			runNode.setProperty( "name", "HT2009" );
			refNode.createRelationshipTo(runNode, MyRelationshipTypes.HAS_RUN);

			Node timelineNode = graphDb.createNode();
			timelineNode.setProperty( "name", "TIMELINE" );
			runNode.createRelationshipTo(timelineNode, MyRelationshipTypes.HAS_TIMELINE);

			// Frames
			Node nodeTmp = null;
			Relationship relationship = null;

			JSONObject objForNode = null;
			Iterator<JSONObject> objIte = null;
			String[] aryDT = new String[4];
			System.out.println("Total frames:"+longFrameEnd);
			System.out.println("Creating frames...");
			Node nodeYear, nodeMonth, nodeDay, nodeHour;
			for(int i = 0; i <= longFrameEnd; i++){
				nodeTmp = graphDb.createNode();
				nodeTmp.setProperty( "name", "FRAME_" + i );
				nodeTmp.setProperty( "type", "FRAME");
				nodeTmp.setProperty( "timestamp", longSecStartTime+i*longSecDelta );
				nodeTmp.setProperty( "timestamp_end", longSecStartTime+(i+1)*longSecDelta );
				nodeTmp.setProperty( "time", getDateFromEpochStr(longSecStartTime+i*longSecDelta));
				nodeTmp.setProperty( "frame_id", i );
				nodeTmp.setProperty( "length", longSecDelta );
				tags_HT2009.add(nodeTmp, "name", nodeTmp.getProperty("name"));
				if(i == 0){
					relationship = runNode.createRelationshipTo( nodeTmp, MyRelationshipTypes.RUN_FRAME_FIRST );
				}else{
					// make relations for frames.
					relationship = mkRelFromName(nodeTmp, MyRelationshipTypes.FRAME_NEXT, "FRAME_"+(i-1), tags_HT2009, true);
					relationship = runNode.createRelationshipTo( nodeTmp, MyRelationshipTypes.RUN_FRAME );
				}
				//
				aryDT = getDateFromEpochAry(longSecStartTime+i*longSecDelta);
				nodeYear = createOrGetNode(graphDb, tags_HT2009, timelineNode, "year", "",aryDT[0]);
				nodeMonth = createOrGetNode(graphDb, tags_HT2009, nodeYear, "month",aryDT[0],aryDT[1]);
				nodeDay = createOrGetNode(graphDb, tags_HT2009, nodeMonth,"day",aryDT[0]+aryDT[1],aryDT[2]);
				nodeHour = createOrGetNode(graphDb, tags_HT2009, nodeDay,"hour",aryDT[0]+aryDT[1]+aryDT[2],aryDT[3]);
				//
				relationship = nodeHour.createRelationshipTo(nodeTmp, MyRelationshipTypes.TIMELINE_INSTANCE);
				relationship.setProperty("timestamp", nodeTmp.getProperty("timestamp"));
			}
			tx = txCommit(graphDb,tx);
			
			int j = 0;
			System.out.println("Creating nodes...");				
			objIte = objNodes.iterator();
			while(objIte.hasNext()){
				objForNode = objIte.next();
				nodeTmp = graphDb.createNode();
				nodeTmp.setProperty( "name", "TAG_" + objForNode.getString("@id") );
				nodeTmp.setProperty( "label", objForNode.getString("@label") );
				nodeTmp.setProperty( "type", "TAG");
				relationship = runNode.createRelationshipTo( nodeTmp, MyRelationshipTypes.RUN_TAG );
				tags_HT2009.add(nodeTmp, "name", nodeTmp.getProperty("name"));
				for(Long[] arySpells: Node_Timeline.get(objForNode.getString("@id"))){
					for(Long longSpell: arySpells){
						relationship = mkRelFromName(nodeTmp, MyRelationshipTypes.FRAME_TAG, "FRAME_"+longSpell, tags_HT2009, false);
						j++;
					}
				}
			}
			System.out.println("frame rels: "+j);
			tx = txCommit(graphDb,tx);
			
			j = 0;
			System.out.println("Creating edges...");				
			objIte = objEdges.iterator();
			while(objIte.hasNext()){
				// make edges.
				objForNode = objIte.next();
				nodeTmp = graphDb.createNode();
				nodeTmp.setProperty( "name", "EDGE_" + objForNode.getString("@source")+"-"+objForNode.getString("@target"));
				nodeTmp.setProperty( "type", "EDGE");
				nodeTmp.setProperty( "tag1", "TAG_" + objForNode.getString("@source"));
				nodeTmp.setProperty( "tag2", "TAG_" + objForNode.getString("@target"));
				tags_HT2009.add(nodeTmp, "name", nodeTmp.getProperty("name"));
				// make relations to nodes.
				relationship = runNode.createRelationshipTo( nodeTmp, MyRelationshipTypes.RUN_EDGE );
				relationship = mkRelFromName(nodeTmp, MyRelationshipTypes.EDGE_TAG, "TAG_"+objForNode.getString("@source"), tags_HT2009, true);
				relationship = mkRelFromName(nodeTmp, MyRelationshipTypes.EDGE_TAG, "TAG_"+objForNode.getString("@target"), tags_HT2009, true);
				for(Long[] arySpells: Edge_Timeline.get(objForNode.getString("@source")+"-"+objForNode.getString("@target"))){
					for(Long longSpell: arySpells){
						relationship = mkRelFromName(nodeTmp, MyRelationshipTypes.FRAME_EDGE, "FRAME_"+longSpell, tags_HT2009, false);
						relationship.setProperty("weight", Integer.parseInt(objForNode.getString("@weight")));
						j++;
					}
				}
			}
			System.out.println("frame rels: "+j);
			tx = txCommit(graphDb,tx);
			
			System.out.println("Done.");				

			tx.success();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			tx.finish();
			graphDb.shutdown(); // for safe closing.
		}
	}

	private static Node createOrGetNode(GraphDatabaseService graphDb,
			Index<Node> test01idx, Node nodeSource, String strProp, String a, String b) {
		Node nodeTmp = null;
		Relationship rel = null;
		String strName = "TIMELINE_"+strProp.toUpperCase()+"_"+a+b;
		IndexHits<Node> objHits = test01idx.get("name", strName);
		
		if(objHits.hasNext()){
			return objHits.next();
		}else{
			nodeTmp = graphDb.createNode();
			nodeTmp.setProperty( "name", strName);
			test01idx.add(nodeTmp, "name", strName);
			rel = nodeSource.createRelationshipTo(nodeTmp, MyRelationshipTypes.NEXT_LEVEL);
			rel.setProperty(strProp, Integer.parseInt(b));
			return nodeTmp;
		}
	}

	private static Relationship mkRelFromName(Node objNode, MyRelationshipTypes b, String a, Index<Node> test01idx, boolean d) {
		// d is direction (from objNode or to objNode)
		Node nodeTmp = null;
		IndexHits<Node> objHits;
		//Relationship relationship;
		// a = a ^ b
		objHits = test01idx.get("name", a);
		if(objHits.hasNext())
			nodeTmp = objHits.next();
		else
			System.out.println("cannot be found: "+a);
		if(d)
			return objNode.createRelationshipTo( nodeTmp, b);
		else
			return nodeTmp.createRelationshipTo( objNode, b);
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
		//aryTmp.add((long) 0);
		while(longE >= longS){
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
	
	private static Transaction txCommit(GraphDatabaseService db, Transaction tx){
	//        if ( i > 0 && i % 10000 == 0 ) {
	            tx.success();
	            tx.finish();
	            tx = db.beginTx();
	            return tx;
	//        }
		}

	private static String getDateFromEpochStr(long epochtime){
		SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z", Locale.US);
		return sdf.format(new Date(epochtime*1000));
	}

	private static String[] getDateFromEpochAry(long epochtime){
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy,MM,dd,HH", Locale.US);
		return sdf.format(new Date(epochtime*1000)).split(",");
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
}
