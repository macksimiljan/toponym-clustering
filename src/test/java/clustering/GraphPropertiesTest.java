package clustering;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import database.DatabaseAccess;
import representation.Suffix;

/**
 * Tests for {@link GraphProperties}
 * 
 * @author MM
 *
 */
public class GraphPropertiesTest {

	/** Graph properties object. */
	static GraphProperties prop;
	/** Database. */
	static GraphDatabaseService db;

	/** Initialize graph properties: use the small dataset!. */
	@BeforeClass
	public static void initGraphProp() {
		db = DatabaseAccess.getGraphDb();
		prop = new GraphProperties(db);
	}
	
	/** Close database. */
	@AfterClass
	public static void closeDb() {
		DatabaseAccess.closeGraphDb();
	}

	/** Test method for {@link GraphProperties#getCountNodes()} */
	@Test
	public void testGetCountNodes() {
		long actual = prop.getCountNodes();
		assertEquals(114, actual);
	}

	/** Test method for {@link GraphProperties#getCountCityNodes()} */
	@Test
	public void testGetCountCityNodes() {
		long actual = prop.getCountCityNodes();
		assertEquals(20, actual);
	}

	/** Test method for {@link GraphProperties#getCountSuffixNodes()}*/
	@Test
	public void testGetCountSuffixNodes() {
		long actual = prop.getCountSuffixNodes();
		assertEquals(94, actual);
	}
	
	/** Test method for {@link GraphProperties#getRootNodes()}*/
	@Test
	public void testGetRootNodes() {
		Set<Suffix> roots = prop.getRootNodes();
		Set<String> actual = new HashSet<String>();
		try (Transaction tx = db.beginTx();) {
		for (Suffix root : roots)
			actual.add(root.getStr());
		}
		
		Set<String> expected = new HashSet<String>();
		expected.add("l");
		expected.add("z");
		
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetFrequencyOfSuffixes() {
		int act = prop.getLonelySuffixes().size();
		System.out.println("|lonely|:\t"+act);
		act = prop.getNormalSuffixes().size();
		System.out.println("|normal|:\t"+act);
		act = prop.getFrequentSuffixes().size();
		System.out.println("|frequent|:\t"+act);
		act = prop.getVeryFrequentSuffixes().size();
		System.out.println("|veryFrequ|:\t"+act);
		
		fail("to be implemented");
	}

}
