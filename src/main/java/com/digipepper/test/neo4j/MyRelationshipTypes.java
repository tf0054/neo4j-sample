package com.digipepper.test.neo4j;

import org.neo4j.graphdb.RelationshipType;

public enum MyRelationshipTypes implements RelationshipType {
	KNOWS, FRAME_NEXT, EDGE_TAG,
	FRAME_EDGE, FRAME_TAG,
	RUN_FRAME_FIRST, RUN_FRAME, RUN_EDGE, RUN_TAG
}
