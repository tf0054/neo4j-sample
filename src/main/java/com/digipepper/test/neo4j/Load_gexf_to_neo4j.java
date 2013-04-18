package com.digipepper.test.neo4j;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
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

//import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * @author tf0054
 * @see http://wiki.neo4j.org/content/Getting_Started_With_Java
 * @see http://stackoverflow.com/questions/14057897/how-to-explore-databases-created-by-an-embedded-neo4j-java-application-and-store
 */
public class Load_gexf_to_neo4j {
	
	//static Long longSecEnd = (long) 0;
	static Long longFrameEnd = (long) 0; // setted on get_interval()
	static Long longSecDelta = (long) 3600*24;
	static Long longSecStartTime = (long) 1246191120;
	static String format = "";
	
	static SimpleDateFormat sdf = null;
	
	public static void main(String args[]) {
		GraphDatabaseService graphDb = new EmbeddedGraphDatabase( "graphdb" );
		IndexManager indexMgr = graphDb.index();
		
		Transaction tx = graphDb.beginTx();
		try {
			XMLSerializer xmlSerializer = new XMLSerializer(); 
			//Index<Node> objNIndex = indexMgr.forNodes("tags_HT2009");
			Index<Node> objNIndex = indexMgr.forNodes("nodeIndex");
			RelationshipIndex objRIndex = indexMgr.forRelationships( "relationshipIndex" );


			JSON json = xmlSerializer.read( readFileAsString("twitter4_20130212.gexf") );  
			format = ((JSONObject) json).getJSONObject("graph").getString("@timeformat");

			if(((JSONObject) json).getJSONObject("graph").has("@start")){
				System.out.print("Starting time was ajasted. "+longSecStartTime+" -> ");
				if(format.equals("datetime"))
					longSecStartTime = getDateFromW3CStr(((JSONObject) json).getJSONObject("graph").getString("@start"));
				else
					longSecStartTime = ((JSONObject) json).getJSONObject("graph").getLong("@start");
				System.out.println(longSecStartTime);
			}
			
			HashMap<String, ArrayList<long[]>> Node_Timeline = new HashMap<String, ArrayList<long[]>>();
			HashMap<String, ArrayList<long[]>> Edge_Timeline = new HashMap<String, ArrayList<long[]>>();
			
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

			Long a = null;
			JSONObject objForNode = null;
			Iterator<JSONObject> objIte = null;
			String[] aryDT = new String[4];
			System.out.println("Total frames:"+longFrameEnd);
			
			System.out.println("Creating frames...");
			Node nodeYear, nodeMonth, nodeDay, nodeHour;
			for(int i = 0; i <= longFrameEnd; i++){
				a = longSecStartTime+i*longSecDelta;
				nodeTmp = graphDb.createNode();
				nodeTmp.setProperty( "name", "FRAME_" + i );
				nodeTmp.setProperty( "type", "FRAME");
				nodeTmp.setProperty( "timestamp", a);
				nodeTmp.setProperty( "timestamp_end", a+longSecDelta );
				nodeTmp.setProperty( "time", getDateFromEpochStr(a));
				nodeTmp.setProperty( "frame_id", i );
				nodeTmp.setProperty( "length", longSecDelta );
				objNIndex.add(nodeTmp, "name", nodeTmp.getProperty("name")); // add frame nodes to idx
				if(i == 0){
					relationship = runNode.createRelationshipTo( nodeTmp, MyRelationshipTypes.RUN_FRAME_FIRST );
				}else{
					// make relations for frames.
					relationship = mkRelFromName(nodeTmp, MyRelationshipTypes.FRAME_NEXT, "FRAME_"+(i-1), objNIndex, objRIndex, true);
					relationship = runNode.createRelationshipTo( nodeTmp, MyRelationshipTypes.RUN_FRAME );
				}
				// add_to_timeline()
				aryDT = getDateFromEpochAry(a);
				nodeYear = createOrGetNode(graphDb, objNIndex, timelineNode, "year", "", aryDT[0]);
				nodeMonth = createOrGetNode(graphDb, objNIndex, nodeYear, "month", aryDT[0], aryDT[1]);
				nodeDay = createOrGetNode(graphDb, objNIndex, nodeMonth,"day", aryDT[0]+aryDT[1], aryDT[2]);
				nodeHour = createOrGetNode(graphDb, objNIndex, nodeDay,"hour", aryDT[0]+aryDT[1]+aryDT[2], aryDT[3]);
				//
				relationship = nodeHour.createRelationshipTo(nodeTmp, MyRelationshipTypes.TIMELINE_INSTANCE);				relationship.setProperty("timestamp", nodeTmp.getProperty("timestamp"));
				if(i%10000 == 0){
					System.out.print(".");
					tx = txCommit(graphDb,tx);
				}
			}
			tx = txCommit(graphDb,tx);
			
			int j = 0;
			System.out.println("\nCreating nodes...");				
			objIte = objNodes.iterator();
			while(objIte.hasNext()){
				objForNode = objIte.next();
				nodeTmp = graphDb.createNode();
				nodeTmp.setProperty( "name", "TAG_" + objForNode.getString("@id") );
				nodeTmp.setProperty( "id", objForNode.getString("@id") );
				nodeTmp.setProperty( "label", objForNode.getString("@label") );
				nodeTmp.setProperty( "type", "TAG");
				//
				relationship = runNode.createRelationshipTo( nodeTmp, MyRelationshipTypes.RUN_TAG );
				objNIndex.add(nodeTmp, "name", nodeTmp.getProperty("name"));
				objNIndex.add(nodeTmp, "label", nodeTmp.getProperty("label")); // we can use this idx for searching with username.
//				if(nodeTmp.getProperty("label").equals("KFC_223"))
//					System.out.println("found.");
				for(long[] arySpells: Node_Timeline.get(objForNode.getString("@id"))){
					for(long longSpell: arySpells){
						relationship = mkRelFromName(nodeTmp, MyRelationshipTypes.FRAME_TAG, "FRAME_"+longSpell, objNIndex, objRIndex, false);
						if(j%1000 == 0){
							System.out.print(".");System.out.flush();
							tx = txCommit(graphDb,tx);
						}
						j++;
					}
				}
			}
			System.out.println("\nrels: "+j);
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
				objNIndex.add(nodeTmp, "name", nodeTmp.getProperty("name"));
				// make relations to nodes.
				relationship = runNode.createRelationshipTo( nodeTmp, MyRelationshipTypes.RUN_EDGE );
				relationship = mkRelFromName(nodeTmp, MyRelationshipTypes.EDGE_TAG, "TAG_"+objForNode.getString("@source"), objNIndex, objRIndex, true);
				relationship = mkRelFromName(nodeTmp, MyRelationshipTypes.EDGE_TAG, "TAG_"+objForNode.getString("@target"), objNIndex, objRIndex, true);
				for(long[] arySpells: Edge_Timeline.get(objForNode.getString("@source")+"-"+objForNode.getString("@target"))){
					for(long longSpell: arySpells){
						relationship = mkRelFromName(nodeTmp, MyRelationshipTypes.FRAME_EDGE, "FRAME_"+longSpell, objNIndex, objRIndex, false);
						if(objForNode.has("@weight"))
							relationship.setProperty("weight", Integer.parseInt(objForNode.getString("@weight")));
						if(j%1000 == 0){
							System.out.print(".");System.out.flush();
							tx = txCommit(graphDb,tx);
						}
						j++;
					}
				}
			}
			System.out.println("\nrels: "+j);
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

	private static long[] get_intervals(long s, long e){
		
		// Where is my start frame?
		long longS = (s - longSecStartTime) / longSecDelta;
		long longE = (e - longSecStartTime) / longSecDelta;
		
		if(longE > longFrameEnd)
			longFrameEnd = longE;
		
		if(longFrameEnd > 10000)
				System.exit(0);
		
		ArrayList<Long> aryTmp = new ArrayList<Long>();
	
		while(longE >= longS){
			aryTmp.add(longS++);	
		}
	
		return ArrayUtils.toPrimitive(aryTmp.toArray(new Long[0]));
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

	private static Relationship mkRelFromName(Node objNode, MyRelationshipTypes b, String a, Index<Node> test01idx, RelationshipIndex objRIndex, boolean d) {
		// d is direction (from objNode or to objNode)
		int count = 0;
		Node nodeTmp = null;
		String c = b.toString();
		IndexHits<Node> objNHits;
		IndexHits<Relationship> objRHits;
		Relationship relationship;
		// we can use a = a ^ b for changing, but ^ is not implemented on neo4j 
		objNHits = test01idx.get("name", a);
		if(objNHits.hasNext())
			nodeTmp = objNHits.next();
		else
			System.out.println("cannot be found the node: "+a);

		if(d){
			objRHits = objRIndex.query( "type:"+c, objNode, nodeTmp );
			if(objRHits.hasNext()){
				relationship = objRHits.next();
				count = (Integer) relationship.getProperty("count");
				relationship.setProperty("count", count+1);
			}else{
				relationship = objNode.createRelationshipTo( nodeTmp, b);
				relationship.setProperty("count", 1);
				objRIndex.add( relationship, "type", c );
			}
		}else{
			objRHits = objRIndex.query( "type:"+c, nodeTmp, objNode );
			if(objRHits.hasNext()){
				relationship = objRHits.next();
				count = (Integer) relationship.getProperty("count");
				relationship.setProperty("count", count+1);
			}else{
				relationship = nodeTmp.createRelationshipTo( objNode, b);
				relationship.setProperty("count", 1);
				objRIndex.add( relationship, "type", c );
			}
		}
		return relationship;
	}

	private static HashMap<String, ArrayList<long[]>> mkTimeline(JSONArray objNodes, String strType) {		
		HashMap<String, ArrayList<long[]>> Node_Timeline = new HashMap<String, ArrayList<long[]>>();

		//Iterator<JSONObject> objIte;
		//objIte = objNodes.iterator();
		//String strStart, strEnd;
		JSONObject a = null;
		for(Iterator<JSONObject> objIte = objNodes.iterator(); objIte.hasNext();){	
		//while(objIte.hasNext()){
			JSONObject objNode = objIte.next();
			JSONObject objSpells = objNode.getJSONObject("spells");

//			if(objNode.getString("@label").equals("KFC_223"))
//				System.out.println("found.");

			ArrayList<long[]> aryFrameNos = new ArrayList<long[]>();

			JSONArray aryTmp = null;
			if(objSpells.get("spell") instanceof JSONArray){
				aryTmp = objSpells.getJSONArray("spell");
			}else{
				aryTmp = new JSONArray();
				aryTmp.add(objSpells.getJSONObject("spell"));
			}
			for(Iterator<JSONObject> objIteIn = aryTmp.iterator(); objIteIn.hasNext();){				
				a = objIteIn.next();
				if(format.equals("datetime")){
					aryFrameNos.add(get_intervals(
						getDateFromW3CStr(a.getString("@start")),
						getDateFromW3CStr(a.getString("@end"))
					));
				}else
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
	
	private static String getDateFromEpochStr(long epochtime){
		sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z", Locale.US);
		return sdf.format(new Date(epochtime*1000));
	}

	private static long getDateFromW3CStr(String a){
		// W3C-DTF format 2012-10-21T21:49:32
		sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US);
		Date date = null;
		try {
			date = sdf.parse(a);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return date.getTime()/1000;
	}

	private static String[] getDateFromEpochAry(long epochtime){
		sdf = new SimpleDateFormat("yyyy,MM,dd,HH", Locale.US);
		return sdf.format(new Date(epochtime*1000)).split(",");
	}

	private static Transaction txCommit(GraphDatabaseService db, Transaction tx){
	    tx.success();
	    tx.finish();
	    tx = db.beginTx();
	    return tx;
	}

	private static void dumpNodes(GraphDatabaseService graphDb) {		
	        ExecutionEngine engine = new ExecutionEngine(graphDb);
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
