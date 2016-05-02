package etl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;

import database.DatabaseAccess;

public class LoadTest {

	@Test
	public void testLoadCityAndSuffix() throws IOException {
		DatabaseAccess.dropDatabase();
		
		GraphDatabaseService graphDb = DatabaseAccess.getGraphDb();
		
		List<Map<String, String>> data = new ArrayList<Map<String, String>>();
		Map<String, String> row = new HashMap<String, String>();
		row.put("city", "leipzig");
		row.put("longitude", "12.3731");
		row.put("latitude", "51.3397");
		data.add(row);
		row = new HashMap<String, String>();
		row.put("city", "pelzig");
		row.put("longitude", "10.1234");
		row.put("latitude", "45.050");
		data.add(row);
		
		Load.loadCityAndSuffix(graphDb, data);
	}

}
