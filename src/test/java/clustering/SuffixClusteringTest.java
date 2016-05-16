package clustering;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;

import database.DatabaseAccess;
import representation.Suffix;

/**
 * Tests of {@link SuffixClustering}.
 * 
 * @author MM
 *
 */
public class SuffixClusteringTest {

	/** Graph properties object. */
	static GraphProperties prop;
	/** Database. */
	static GraphDatabaseService db;

	/** Initialize graph properties: use the small dataset! */
	@BeforeClass
	public static void initGraphProp() {
		System.out.println("init database");
		db = DatabaseAccess.getGraphDb();
		System.out.println("init properties");
		prop = new GraphProperties(db);
		System.out.println("add subsumed cities");
		prop.addPropertySubsumedCities();
	}

	/** Close database. */
	@AfterClass
	public static void closeDb() {
		DatabaseAccess.closeGraphDb();
	}

	/**
	 * Test method for
	 * {@link SuffixClustering#determineClusterCandidatesByProportion()}
	 */
	@Test
	public void testDetermineClusterCandidatesByProportion() {
		System.out.println("determine cluster candidates");
		SuffixClustering clustering = new SuffixClustering(db, prop, 0.8f, 0.05f, 0.05f);
		try {
			Set<Suffix> candidates = clustering.determineClusterCandidatesByProportion();
			System.out.println("\n\nCandidates:\n" + candidates);
		} catch (Exception e) {
			e.printStackTrace();
		}

		fail("Not yet implemented.");

	}
	
	/**
	 * Test method for {@link SuffixClustering#calculateMinMax(int)}
	 */
	@Test
	public void testCalculateMinMax() {
		SuffixClustering clustering = new SuffixClustering(db, prop, 0.8f, 0.045f, 0.0324f);
		clustering.calculateMinMax(123);
		int actual = clustering.getMinClusterSize();
		assertEquals(5, actual);
		actual = clustering.getMaxClusterSize();
		assertEquals(119, actual);
	}
}
