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

	/** Initialize graph properties: use the small dataset! */
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

	/** Test method for {@link GraphProperties#getCountSuffixNodes()} */
	@Test
	public void testGetCountSuffixNodes() {
		long actual = prop.getCountSuffixNodes();
		assertEquals(94, actual);
	}

	/** Test method for {@link GraphProperties#getRootNodes()} */
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

	/**
	 * Test method for {@link GraphProperties#getLonelySuffixes()} etc.
	 */
	@Test
	public void testGetFrequencyOfSuffixes() {
		int act = prop.getLonelySuffixes().size();
		assertEquals(70, act);
		act = prop.getNormalSuffixes().size();
		assertEquals(10, act);
		act = prop.getFrequentSuffixes().size();
		assertEquals(0, act);
		act = prop.getVeryFrequentSuffixes().size();
		assertEquals(0, act);
	}

	/**
	 * Test method for {@link GraphProperties#addPropertySubsumedCities()}.
	 */
	@Test
	public void testAddPropertySubsumedCities() {
		prop.addPropertySubsumedCities();
		// System.out.println();
		// try (Transaction tx = db.beginTx()) {
		// ResourceIterator<Node> nodes = db.findNodes(Suffix.LABEL);
		// while (nodes.hasNext()) {
		// Node n = nodes.next();
		// System.out.println(n.getId());
		// Map<String, Object> properties = n.getProperties();
		// System.out.println(" |properties|: "+properties.size());
		// for (String key : properties.keySet()) {
		// System.out.println(" "+key + ": " + properties.get(key));
		// }
		// }
		// }
		fail("to be implemented");
	}

	/**
	 * Test method for {@link GraphProperties#getCityNames()}.
	 */
	@Test
	public void testGetCityNames() {
		Set<Suffix> names = prop.getCityNames();
		for (Suffix s : names) {
			System.out.println();
			try (Transaction tx = db.beginTx()) {
				System.out.println(s.getUnderlyingNode().getProperty(Suffix.KEY_STR));
			}
		}
		fail("to be implemented");
	}

}
