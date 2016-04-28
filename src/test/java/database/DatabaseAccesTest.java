package database;

import static org.junit.Assert.*;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;

public class DatabaseAccesTest {

	@Test
	public void testGetGraphDb() {
		GraphDatabaseService graphDb = DatabaseAccess.getGraphDb();

		assertTrue(graphDb.isAvailable(1));
		
		DatabaseAccess.closeGraphDb();
	}

	@Test
	public void testCloseGraphDb() {
		fail("Not yet implemented"); // TODO
	}

	@Test
	public void testDropDatabase() {
		fail("Not yet implemented"); // TODO
	}

}
