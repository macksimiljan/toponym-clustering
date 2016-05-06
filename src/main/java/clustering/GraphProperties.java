package clustering;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;

import representation.City;
import representation.Suffix;

/**
 * Overview to different descriptive properties of a graph like node counts.
 * 
 * @author MM
 *
 */
public class GraphProperties {

	/** The graph database. */
	private GraphDatabaseService graphDb;
	/** Count of all nodes. */
	private long countNodes;
	/** Count of all city nodes. */
	private long countCityNodes;
	/** Count of all suffix nodes. */
	private long countSuffixNodes;
	/** Set of the root nodes, i.e. final letter. */
	private Set<Suffix> rootNodes;

	/**
	 * Creates manager for graph properties.
	 * 
	 * @param graphDb
	 *            Graph database.
	 */
	public GraphProperties(GraphDatabaseService graphDb) {
		this.graphDb = graphDb;
		this.countNodes = -1;
		this.countCityNodes = -1;
		this.countSuffixNodes = -1;
		this.rootNodes = null;
	}

	/**
	 * Returns the count of all nodes.
	 * 
	 * @return Count of nodes.
	 */
	public long getCountNodes() {
		if (this.countNodes == -1) {
			// cypher query
			final String query = "MATCH (node) RETURN COUNT(node) AS countNodes";
			try (Transaction tx = graphDb.beginTx(); Result rs = graphDb.execute(query)) {
				while (rs.hasNext()) {
					Map<String, Object> row = rs.next();
					this.countNodes = (Long) row.get("countNodes");
				}
			}
		}

		return this.countNodes;
	}

	/**
	 * Returns the count of all city nodes.
	 * 
	 * @return Count of city nodes.
	 */
	public long getCountCityNodes() {
		if (this.countCityNodes == -1) {
			// cypher query
			final String query = "MATCH (node:" + City.LABEL + ") RETURN COUNT(node) AS countCityNodes";
			try (Transaction tx = graphDb.beginTx(); Result rs = graphDb.execute(query)) {
				while (rs.hasNext()) {
					Map<String, Object> row = rs.next();
					this.countCityNodes = (Long) row.get("countCityNodes");
				}
			}
		}
		return this.countCityNodes;
	}

	/**
	 * Returns the count of all suffix nodes.
	 * 
	 * @return Count of suffix nodes.
	 */
	public long getCountSuffixNodes() {
		if (this.countCityNodes == -1) {
			// cypher query
			final String query = "MATCH (node:" + Suffix.LABEL + ") RETURN COUNT(node) AS countSuffixNodes";
			try (Transaction tx = graphDb.beginTx(); Result rs = graphDb.execute(query)) {
				while (rs.hasNext()) {
					Map<String, Object> row = rs.next();
					this.countSuffixNodes = (Long) row.get("countSuffixNodes");
				}
			}
		}
		return this.countSuffixNodes;
	}

	/**
	 * Get the root nodes of the graph, i.e. final letters.
	 * 
	 * @return Root nodes.
	 */
	public Set<Suffix> getRootNodes() {
		if (this.rootNodes == null) {
			this.rootNodes = new HashSet<Suffix>();
			// cypher query
			final String query = "MATCH (root:" + Suffix.LABEL + ") WHERE NOT (:suffix)-->(root) RETURN root";
			try (Transaction tx = graphDb.beginTx(); Result rs = graphDb.execute(query)) {
				Iterator<Node> it = rs.columnAs("root");
				for (Node node : IteratorUtil.asIterable(it)) {
					Suffix suffix = new Suffix(node);
					this.rootNodes.add(suffix);
				}
			}
		}
		return this.rootNodes;
	}
	
	

}