package clustering;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;

import process_control.ClusterProcess;
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
	
	/** Log4j Logger */
	private static Logger log = ClusterProcess.log;

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

	/** for testing ... */
	private long propertyAssignedNodes = 0;

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
		if (this.countSuffixNodes == -1) {
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

	/**
	 * Get suffixes which have exactly one suffix child.
	 * 
	 * @return One-child suffixes.
	 */
	public Set<Suffix> getLonelySuffixes() {
		if (this.lonelySuffixes == null)
			determineSuffixFrequency();
		return this.lonelySuffixes;
	}

	/**
	 * Get suffixes which have 2 to 5 suffix children.
	 * 
	 * @return 2-to-5 suffixes.
	 */
	public Set<Suffix> getNormalSuffixes() {
		if (this.normalSuffixes == null)
			determineSuffixFrequency();
		return this.normalSuffixes;
	}

	/**
	 * Get suffixes which have 6 to 15 suffix children.
	 * 
	 * @return 6-to-15 suffixes.
	 */
	public Set<Suffix> getFrequentSuffixes() {
		if (this.frequentSuffixes == null)
			determineSuffixFrequency();
		return this.frequentSuffixes;
	}

	/**
	 * Get suffixes which have 16 or more suffix children.
	 * 
	 * @return 16+ suffixes.
	 */
	public Set<Suffix> getVeryFrequentSuffixes() {
		if (this.veryFrequentSuffixes == null)
			determineSuffixFrequency();
		return this.veryFrequentSuffixes;
	}

	/**
	 * Categorizes suffix nodes according to their number of children into
	 * lonely (1 child), normal (2-5 children), frequent (6-15 children), and
	 * very frequent (16 and more children).
	 */
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

	/**
	 * Adds the property 'subsumed cities' and its value to the graph. That
	 * property specifies how many cities ends with the current node.
	 */
	public void addPropertySubsumedCities() {
		// check whether property is already present
		boolean isComplete = true;
		for (Suffix root : getRootNodes()) {
			try (Transaction tx = this.graphDb.beginTx()) {
				isComplete = isComplete && root.getUnderlyingNode().hasProperty(Suffix.KEY_SUBSCITIES);
			}
		}
		log.info("'subsumedCities' is property of all suffix nodes:" + isComplete);
		if (isComplete)
			return;

		// add property to each suffix node from bottom to top
		Set<Suffix> bottomLayer = getCityNames();
		while (bottomLayer.size() > 0) {
			log.info("bottomLayer.size(): "+bottomLayer.size());
			bottomLayer = countSubsumedCities(bottomLayer);
		}
	}

	/**
	 * Counts the cities which belongs to each node of the current layer. That
	 * value is computed by summing up the subsumed city count of the child
	 * nodes.
	 * 
	 * @param bottomLayer
	 *            The current layer. For that layer the property will be added.
	 * @return The layer above the current layer, i.e. a set of parent nodes.
	 */
	private Set<Suffix> countSubsumedCities(Set<Suffix> bottomLayer) {
		Set<Suffix> nextLayer = new HashSet<Suffix>();

		// iterate over each suffix node of the current layer
		for (Suffix s : bottomLayer) {
			// 1: get suffix node and outgoing edges
			Node currentSuffix = null;
			Iterator<Relationship> iterator = null;
			int value = 0;
			try (Transaction tx = this.graphDb.beginTx()) {
				currentSuffix = s.getUnderlyingNode();
				
				// suffix already has the property, e.g., because of canceled previous execution				
				if (currentSuffix.hasProperty(Suffix.KEY_SUBSCITIES)) {
					// determine nodes for the next layer
					iterator = currentSuffix.getRelationships(Direction.INCOMING).iterator();
					while (iterator.hasNext()) {
						Relationship edge = iterator.next();
						Suffix node = new Suffix(edge.getStartNode());
						nextLayer.add(node);
					}
					continue; // you are done with this suffix
				} 
				
				// get outgoing edges
				iterator = currentSuffix.getRelationships(Direction.OUTGOING).iterator();				
			}
			
			// 2: iterate over each edge of the current suffix node
			while (iterator.hasNext()) {
				Relationship edge = iterator.next();
				try (Transaction tx = this.graphDb.beginTx()) {
					if (edge.isType(EdgeTypes.IS_NAME_OF)) {
						// basic bottom layer: suffix = city name
						// this is the the case only for the first layer
						value = currentSuffix.getDegree(EdgeTypes.IS_NAME_OF);
						break;
					} else {
						// for all other nodes which are not direct parent of cities
						if (edge.getEndNode().hasProperty(Suffix.KEY_SUBSCITIES)) {
							// sum up all subsumedCities-values of the parents
							value += (Integer) edge.getEndNode().getProperty(Suffix.KEY_SUBSCITIES);
						} else {
							// if at least one parent does not have a proper subsumedCities-value yet
							value = 0;
							break;
						}
					}
				}
			} // end iteration over edges

			// 3: add property to node
			if (value > 0) {				
				propertyAssignedNodes++;
				if (propertyAssignedNodes % 1000 == 0)
					log.info("#nodes with property 'subsumedCities': "+propertyAssignedNodes);
				
				try (Transaction tx = this.graphDb.beginTx()) {
					currentSuffix.setProperty(Suffix.KEY_SUBSCITIES, value);

					// determine nodes for the next layer
					iterator = currentSuffix.getRelationships(Direction.INCOMING).iterator();
					while (iterator.hasNext()) {
						Relationship edge = iterator.next();
						Suffix node = new Suffix(edge.getStartNode());
						nextLayer.add(node);
					}
					// commit changes
					tx.success();
				}
			}
			// System.out.println(" properties: " + currentSuffix.getAllProperties());
		} // end iteration over suffix nodes

		return nextLayer;

	}

	/**
	 * Returns the name of all cities.
	 * 
	 * @return Suffix nodes representing the city names.
	 */
	public Set<Suffix> getCityNames() {
		Set<Suffix> cityNames = new HashSet<Suffix>();

		final String query = "MATCH (n)-[:" + EdgeTypes.IS_NAME_OF + "]->() RETURN n";
		try (Transaction tx = graphDb.beginTx(); Result rs = graphDb.execute(query)) {
			Iterator<Node> it = rs.columnAs("n");
			for (Node node : IteratorUtil.asIterable(it)) {
				Suffix suffix = new Suffix(node);
				cityNames.add(suffix);
			}
		}

		return cityNames;
	}

}