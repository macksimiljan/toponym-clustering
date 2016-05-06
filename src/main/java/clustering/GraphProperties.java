package clustering;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;

import representation.City;
import representation.EdgeTypes;
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
	/** Suffixes which are suffix of exactly one suffix. */
	private Set<Suffix> lonelySuffixes;
	/** Suffixes which are suffix of 2-5 (direct) suffixes. */
	private Set<Suffix> normalSuffixes;
	/** Suffixes which are suffix of 6-15 (direct) suffixes. */
	private Set<Suffix> frequentSuffixes;
	/** Suffixes which are suffix of 16 or more (direct) suffixes. */
	private Set<Suffix> veryFrequentSuffixes;

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
		this.lonelySuffixes = null;
		this.normalSuffixes = null;
		this.frequentSuffixes = null;
		this.veryFrequentSuffixes = null;
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

	public Set<Suffix> getLonelySuffixes() {
		if (this.lonelySuffixes == null)
			determineSuffixFrequency();
		return this.lonelySuffixes;
	}

	public Set<Suffix> getNormalSuffixes() {
		if (this.normalSuffixes == null)
			determineSuffixFrequency();
		return this.normalSuffixes;
	}

	public Set<Suffix> getFrequentSuffixes() {
		if (this.frequentSuffixes == null)
			determineSuffixFrequency();
		return this.frequentSuffixes;
	}

	public Set<Suffix> getVeryFrequentSuffixes() {
		if (this.veryFrequentSuffixes == null)
			determineSuffixFrequency();
		return this.veryFrequentSuffixes;
	}

	private void determineSuffixFrequency() {
		lonelySuffixes = new HashSet<Suffix>();
		normalSuffixes = new HashSet<Suffix>();
		frequentSuffixes = new HashSet<Suffix>();
		veryFrequentSuffixes = new HashSet<Suffix>();

		try (Transaction tx = graphDb.beginTx();) {
			ResourceIterator<Node> iterSuffixes = graphDb.findNodes(Suffix.LABEL);
			while (iterSuffixes.hasNext()) {
				Node n = iterSuffixes.next();
				int degree = n.getDegree(EdgeTypes.IS_SUFFIX_OF, Direction.OUTGOING);
				if (degree == 1)
					lonelySuffixes.add(new Suffix(n));
				else if (degree >= 2 && degree <= 5)
					normalSuffixes.add(new Suffix(n));
				else if (degree >= 6 && degree <= 15)
					frequentSuffixes.add(new Suffix(n));
				else if (degree >= 16)
					veryFrequentSuffixes.add(new Suffix(n));
			}
		}
	}

}