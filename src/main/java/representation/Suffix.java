package representation;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import database.DatabaseAccess;

/**
 * Domain entity of a suffix node.
 * 
 * @author MM
 *
 */
public class Suffix {

	/** Label of the domain entity. */
	public static final Label LABEL = DynamicLabel.label("suffix");

	/** Property label 'str'. */
	public static final String KEY_STR = "str";

	/**
	 * Property label 'subsumedCities', i.e. the number of cities which are
	 * associated to this suffix.
	 */
	public static final String KEY_SUBSCITIES = "subsumedCities";
	
	/** Property label for cluster candidates, value in {true, false} or not given. */
	public static final String KEY_CLUSTER = "clusterCandidate";
	
	/** The underlying node of this city. */
	private final Node underlyingNode;

	/**
	 * Creates a new suffix entity.
	 * 
	 * @param underlyingNode
	 *            The underlying node of the new suffix entity.
	 */
	public Suffix(Node underlyingNode) {
		this.underlyingNode = underlyingNode;
	}

	/**
	 * Returns the node representation of this suffix within the database.
	 * 
	 * @return The underlying node.
	 */
	public Node getUnderlyingNode() {
		return underlyingNode;
	}

	/**
	 * Returns the string value of this suffix.
	 * 
	 * @return A string representing the suffix.
	 */
	public String getStr() {
		String s = null;
		try (Transaction tx = DatabaseAccess.getGraphDb().beginTx()) {
			s = (String) this.underlyingNode.getProperty(KEY_STR);
		}
		return s;
	}
	
	/**
	 * Returns the number of subsumed cities.
	 * 
	 * @return Number of subsumed cities.
	 */
	public int getSubsumedCities() {
		int i = -1;
		try (Transaction tx = DatabaseAccess.getGraphDb().beginTx()) {
			i = ((Number) this.underlyingNode.getProperty(KEY_SUBSCITIES)).intValue();
		}
		return i;
	}
	
	/**
	 * Returns the city nodes (= locations) whose name ends with this suffix.
	 * 
	 * @param graphDb
	 * 			A graph database.
	 * @return Set of city nodes.
	 */
	public Set<City> getAssocCityLocations(GraphDatabaseService graphDb) {
		Set<City> cities = new HashSet<City>();
		
		String cyper = "MATCH (n:"+LABEL+")-[*1..]->(c:"+City.LABEL+")"
				+ " WHERE n.str='"+getStr()+"'"
						+ " RETURN c AS city";
		try (Transaction tx = graphDb.beginTx();
				Result rs = graphDb.execute(cyper)) {
			while (rs.hasNext()) {
				Map<String, Object> row = rs.next();
				Node node = (Node) row.get("city");
				City city = new City(node);
				cities.add(city);
			}
		}
		
		return cities;
	}

	@Override
	public int hashCode() {
		return underlyingNode.hashCode();
	}

	@Override
	public boolean equals(Object o) {
		return o instanceof Suffix && underlyingNode.equals(((Suffix) o).getUnderlyingNode());
	}

	public String toString() {
		return "Suffix: " + getStr();
	}

}
